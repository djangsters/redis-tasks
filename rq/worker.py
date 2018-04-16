import logging
import multiprocessing
import os
import signal
import socket
import sys
import threading
import traceback
from contextlib import contextmanager, suppress
from datetime import timedelta

from .connections import get_current_connection
from .defaults import (
    DEAD_WORKER_TTL, WORKER_HEARTBEAT_TIMEOUT, WORKER_HEARTBEAT_FREQ, MAINTENANCE_FREQ)
from .exceptions import DequeueTimeout, ShutdownImminentException, NoSuchWorkerError, JobAborted
from .job import Job
from .queue import Queue
from .registry import expire_registries, worker_registry
from .utils import enum, utcformat, utcnow, utcparse, takes_pipeline, import_attribute
from .version import VERSION

logger = logging.getLogger(__name__)

_local = threading.local()


class ShutdownRequested(BaseException):
    pass


EX_WORKER_SHUTDOWN = 143


def signal_name(signum):
    _signames = dict((getattr(signal, signame), signame)
                     for signame in dir(signal)
                     if signame.startswith('SIG') and '_' not in signame)
    try:
        if sys.version_info[:2] >= (3, 5):
            return signal.Signals(signum).name
        else:
            return _signames[signum]

    except KeyError:
        return 'SIG_UNKNOWN'
    except ValueError:
        return 'SIG_UNKNOWN'


@contextmanager
def critical_section():
    # TODO: _local should not be thread-local
    if not hasattr(_local, 'critical_section'):
        _local.critical_section = 0
    try:
        _local.critical_section += 1
        yield
    finally:
        _local.critical_section -= 1

        if _local.critical_section == 0 and getattr(_local, 'raise_shutdown', False):
            logger.warning('Critical section left, raising ShutdownImminentException')
            raise ShutdownImminentException()


WorkerStatus = enum(
    'WorkerStatus',
    STARTED='started',
    BUSY='busy',
    IDLE='idle'
)


class TaskMiddleware:
    """Base class for a task execution middleware.

    Before task execution, the `start()` methods are called in the order the
    middlewares are listen in TODO[setting]. Any middleware can short-circuit
    this process by returning True or raising an exception in the `start()`
    method. The following `start()` method will then not be called.

    Independently of whether the task ran or a middleware interupted the startup,
    all configured middelwares' `end()` methods are called after execution. If
    the worker running the task did not die and a middleware's `start()` method
    was called for this task, the `end()` method will be called on the same
    instance of the middleware. Otherwise a new instance is created.

    The JobAborted exception has a special meaning in this context. It is raised
    by rq if the job did not fail itself, but was aborted for external reasons.
    It can also be raised by any middleware. If it is passed through or raised
    by the outer-most middleware, reentrant tasks will be reenqueued at the
    front of the queue they came from."""

    def start(self, job):
        """Is called before the job is started.

        Can return True to stop execution and treat the job as succeeded.
        Can raise an exception to stop execution and treat the job as failed."""
        pass

    def end(self, job, exc_type=None, exc_val=None, exc_tb=None):
        """Is called after job exection.

        If the job failed to succeed, the three exc_ parameters will be set.
        This can happen for the following reasons:
        * The job raised an exception
        * The worker shut down during execution
        * The worked died unexpectedly during execution
        * The job reached its timeout
        * Another middleware raised an exception

        Can return True to treat the job as succeeded.
        Returning a non-true value (e.g. None), passes the current state on to
        the next middleware.
        Raising an exception passes the raised exception to the next middleware."""
        pass


class JobOutcome:
    def __init__(self, outcome, *, error_message=None):
        assert outcome in ['success', 'failure', 'aborted']
        self.outcome = outcome
        self.error_message = error_message


class WorkHorse(multiprocessing.Process):
    def run(self, job, worker_connection):
        self.setup_work_horse_signals()

        exc_info = []
        configured_middlewares = []  # TODO: middleware config
        try:
            for middleware in configured_middlewares:
                if middleware.start(job):
                    break
            else:
                job.execute()
        except ShutdownImminentException:
            _, _, exc_tb = sys.exc_info()
            exc_info = JobAborted, JobAborted("Worker shutdown")
        except Exception:
            exc_info = sys.exc_info()

        for middleware in reversed(configured_middlewares):
            try:
                if middleware.end(job, *exc_info):
                    exc_info = []
            except Exception:
                exc_info = sys.exc_info()

        if not exc_info:
            worker_connection.send(JobOutcome('success'))
        elif isinstance(exc_info[1], JobAborted):
            worker_connection.send(JobOutcome('aborted', message=exc_info[1].message))
        else:
            exc_string = ''.join(traceback.format_exception(*exc_info))
            worker_connection.send(JobOutcome('failure', error_message=exc_string))

    def send_signal(self, sig):
        os.kill(self.pid, sig)

    def setup_signals(self):
        """Setup signal handing for the newly spawned work horse."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGUSR1, self.request_stop)

    def request_stop(self, signum, frame):
        if getattr(_local, 'critical_section', 0):
            self.log.warning('Delaying ShutdownImminentException till critical section is finished')
            _local.raise_shutdown = True
        else:
            self.log.warning('Raising ShutdownImminentException to cancel job')
            raise ShutdownImminentException()


class WorkerProcess:
    def __init__(self, queues, burst):
        self.connection = get_current_connection()

        queues = [Queue(name=q) if isinstance(q, str) else q
                  for q in queues]
        # TODO: use heroku infos here
        hostname = socket.gethostname()
        shortname, _, _ = hostname.partition('.')
        description = '{0}.{1}'.format(shortname, self.pid)
        id = generate_worker_id()

        self.maintenance = Maintenance()
        self.worker = Worker(id, description=description, queues=queues)
        self.in_receive_shutdown = 0
        self.shutdown_requested = False
        self.run(burst)

    def run(self, burst=False):
        """Starts the work loop.

        Returns the number of jobs that were processed in burst mode"""
        self.install_signal_handlers()
        self.worker.startup()
        self.log.info("RQ worker {0!r} started, version {1}".format(self.key, VERSION))

        try:
            jobs_processed = 0
            for job in self.job_queue_iter(burst):
                # TODO: The job is in limbo in this moment. If the process
                # crashes here, we lose track of it.
                self.process_job(job)
                jobs_processed += 1

                self.heartbeat()
                self.maintenance.run_if_neccessary()
            else:
                self.log.info("Burst finished, quitting")
                return jobs_processed
        finally:
            self.worker.shutdown()

    def queue_iter(self, burst):
        self.log.debug('Listening on {0}...'.format(
            ', '.join(q.name for q in self.queues)))

        while True:
            self.worker.heartbeat()
            try:
                timeout = None if burst else WORKER_HEARTBEAT_FREQ
                with self.receive_shutdown():
                    job, queue = Queue.dequeue_any(self.queues, timeout)
            except DequeueTimeout:
                continue
            if burst and job is None:
                break
            yield job

    def process_job(self, job):
        with self.connection.pipeline() as pipeline:
            self.worker.start_job(job, pipeline=pipeline)
            job.set_running(self, pipeline=pipeline)

        # TODO: Good Logging
        self.log.info('{0}: {1} ({2})'.format(job.origin, job.description, job.id))
        try:
            outcome = self.execute_job(job)
        except Exception:
            exc_string = ''.join(traceback.format_exception(*sys.exc_info()))
            outcome = JobOutcome('failure', error_message=exc_string)

        with self.connection.pipeline() as pipeline:
            self.worker.end_job(job, pipeline=pipeline)
            if outcome.outcome == 'success':
                job.set_finished(pipeline=pipeline)
            elif outcome.outcome == 'failure':
                job.set_failed(outcome.error_message, pipeline=pipeline)
            elif outcome.outcome == 'aborted':
                job.handle_abort('Worker shut down', pipeline=pipeline)

        # TODO: log outcome
        self.log.info('{0}: {1} ({2})'.format(job.origin, 'Job OK', job.id))
        return True

    def execute_job(self, job):
        work_horse = WorkHorse()
        work_horse.daemon = True
        horse_connection, writer = multiprocessing.Pipe(duplex=False)
        work_horse.start(job, writer)
        outcome = None
        timeout_at = utcnow() + timedelta(seconds=job.timeout)
        try:
            while work_horse.is_alive():
                self.worker.heartbeat()
                if utcnow() > timeout_at:
                    work_horse.send_signal(signal.SIGKILL)
                try:
                    with self.receive_shutdown():
                        work_horse.join(WORKER_HEARTBEAT_FREQ)
                except ShutdownRequested:
                    work_horse.send_signal(signal.SIGUSR1)

            if not horse_connection.poll():
                outcome = JobOutcome('aborted')
            else:
                outcome = horse_connection.recv()
        finally:
            if work_horse.is_alive():
                work_horse.send_signal(signal.SIGKILL)
            if not outcome:
                outcome = JobOutcome('aborted')
        return outcome

    def install_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_stop_signal)
        signal.signal(signal.SIGTERM, self.handle_stop_signal)

    def handle_stop_signal(self, signum, frame):
        self.log.debug('Got signal {0}'.format(signal_name(signum)))
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        with suppress(Exception):
            self.worker.set_shutdown_requested()
        if self.in_receive_shutdown:
            self.log.debug('Initiating shutdown')
            raise ShutdownRequested()

    @contextmanager
    def receive_shutdown(self):
        if self.shutdown_requested:
            self.log.debug('Initiating shutdown')
            raise ShutdownRequested()
        self.in_receive_shutdown += 1
        try:
            # The signal handler might now raise ShutdownRequested
            yield
        finally:
            self.in_receive_shutdown -= 1


class SimpleWorker(WorkerProcess):
    def execute_job(self, *args, **kwargs):
        """Execute job in same thread/process, do not fork()"""
        # TODO: copy some stuff from the workhorse – or maybe initiate an
        # instance and just run in it in ths process?
        return self.perform_job(*args, **kwargs)


class Maintenance:
    def __init__(self):
        self.last_run_at = None
        self.key = 'rq:last_maintenance'

    def run_if_neccessary(self):
        if (not self.last_run_at or
                (utcnow() - self.last_run_at) < timedelta(seconds=MAINTENANCE_FREQ)):
            return
        if self.connection.setnx(self.key, utcnow()):
            self.connection.expire(self.key, MAINTENANCE_FREQ)
            self.run()

    def handle_dead_workers(self):
        dead_worker_ids = worker_registry.get_dead_ids(self)
        for worker_id in dead_worker_ids:
            worker = Worker.fetch(worker_id)
            worker.died()

    def run(self):
        expire_registries()


WorkerState = enum(
    'WorkerState',
    IDLE='idle',
    BUSY='busy',
    DEAD='dead',
)


class Worker:
    redis_worker_namespace_prefix = 'rq:worker:'

    @classmethod
    def all(cls):
        return [cls.fetch(id) for id in worker_registry.get_alive_ids()]

    @classmethod
    def fetch(cls, id):
        return cls(fetch_id=id)

    def __init__(self, id=None, *, description=None, queues=None,
                 fetch_id=None):
        self.connection = get_current_connection()

        if fetch_id:
            self.id = fetch_id
            self.refresh()
            return

        self.id = id
        self.description = description
        self.state = None
        self.queues = queues
        self.current_job_id = None
        self.started_at = None
        self.shutdown_at = None
        self.shutdown_requested_at = None

    def refresh(self):
        obj = {k.decode(): v.decode() for k, v in self.connection.hmgetall(self.key)}
        if not obj:
            raise NoSuchWorkerError(self.id)
        self.state = obj['state']
        self.description = obj['description']
        if obj['queues']:
            self.queues = [Queue(q) for q in obj['queues'].split(',')]
        else:
            self.queues = []
        self.current_job_id = obj['current_job_id']
        for k in ['started_at', 'shutdown_at', 'shutdown_requested_at']:
            setattr(self, k, utcparse(obj[k]) if obj.get(k) else None)

    @takes_pipeline
    def save(self, *, pipeline):
        obj = {
            'description': self.description,
            'state': self.state,
            'queues': ','.join(q.name for q in self.queues),
            'current_job_id': self.current_job_id,
            'started_at': utcformat(self.started_at),
            'shutdown_at': utcformat(self.shutdown_at),
            'shutdown_requested_at': utcformat(self.shutdown_requested_at),
        }
        pipeline.hmset(self.key, obj)

    @property
    def key(self):
        return self.redis_worker_namespace_prefix + self.id

    def heartbeat(self):
        """Send a heartbeat.

        Raises a NoSuchworkerError if the registry considers this worker as dead"""
        worker_registry.heartbeat(self)

    @takes_pipeline
    def startup(self, *, pipeline):
        self.log.debug(f'Registering birth of worker {self.description} ({self.id})')
        if self.connection.exists(self.key):
            # Note that there is a race condition here – this is just a sanity check
            raise Exception('There already was a worker with id {self.id}')
        self.state = WorkerState.IDLE
        self.heartbeat(pipeline=pipeline)
        self.save(pipeline=pipeline)

    @takes_pipeline
    def start_job(self, job, *, pipeline):
        self._set_state(WorkerState.BUSY, pipeline=pipeline)
        self.current_job_id = job.id
        pipeline.hset(self.key, 'current_job_id', job.id)

    @takes_pipeline
    def end_job(self, job, *, pipeline):
        assert self.state == WorkerState.BUSY
        self._set_state(WorkerState.IDLE, pipeline=pipeline)
        self.current_job_id = None
        pipeline.hdel(self.key, 'current_job_id')

    @takes_pipeline
    def shutdown(self, *, pipeline):
        worker_registry.remove(self, pipeline=pipeline)
        self._set_state(WorkerState.DEAD, pipeline=pipeline)
        self.shutdown_at = utcnow()
        pipeline.hset(self.key, 'shutdown_at', utcformat(self.shutdown_at))
        pipeline.expire(self.key, DEAD_WORKER_TTL)

    @takes_pipeline
    def died(self, *, pipeline):
        worker_registry.remove(self, pipeline=pipeline)
        self._set_state(WorkerState.DEAD, pipeline=pipeline)
        self.shutdown_at = utcnow()
        if self.current_job_id:
            Job.fetch(self.current_job_id).handle_abort('Worker died', pipeline=pipeline)
        pipeline.hset(self.key, 'shutdown_at', utcformat(self.shutdown_at))
        pipeline.expire(self.key, DEAD_WORKER_TTL)

    @takes_pipeline
    def _set_state(self, state, *, pipeline=None):
        self.state = state
        pipeline.hset(self.key, 'state', state)

    def set_shutdown_requested(self):
        self.shutdown_requested_at = utcnow()
        self.connection.hset(self.key, 'shutdown_requested_at',
                             utcformat(self.shutdown_requested_at))

    def fetch_current_job(self):
        """Returns the job id of the currently executing job."""
        if self.current_job_id:
            return Job.fetch(self.current_job_id)
