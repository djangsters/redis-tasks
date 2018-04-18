import logging
import multiprocessing
import os
import signal
import socket
import sys
import threading
import traceback
import uuid
from contextlib import contextmanager, suppress, ExitStack
from datetime import timedelta

from .connections import get_current_connection
from .defaults import (DEAD_WORKER_TTL, MAINTENANCE_FREQ,
                       WORKER_HEARTBEAT_FREQ, WORKER_HEARTBEAT_TIMEOUT)
from .exceptions import (DequeueTimeout, TaskAborted, NoSuchWorkerError,
                         ShutdownImminentException, WorkerDied)
from .task import Task, TaskOutcome
from .queue import Queue
from .registry import expire_registries, worker_registry
from .utils import enum, takes_pipeline, utcformat, utcnow, utcparse
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


class CriticalSection:
    def enter(self):
        self.__enter__()

    def exit(self):
        self.__exit__()

    def __enter__(self):
        _local.critical_section += 1

    def __exit__(self, *args):
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


class WorkHorse(multiprocessing.Process):
    def run(self, task, worker_connection):
        cs = CriticalSection()
        cs.enter()
        self.setup_signal_handler()
        worker_connection.send(True)

        outcome = task.execute(pre_run=cs.exit, post_run=self.ignore_shutdown_signal)
        worker_connection.send(outcome)

    def setup_signal_handler(self):
        _local.critical_section = 0
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGUSR1, self.request_stop)

    def ignore_shutdown_signal(self):
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)

    def request_stop(self, signum, frame):
        self.ignore_shutdown_signal()
        if getattr(_local, 'critical_section', 0):
            self.log.warning('Delaying ShutdownImminentException till critical section is finished')
            _local.raise_shutdown = True
        else:
            self.log.warning('Raising ShutdownImminentException to cancel task')
            raise ShutdownImminentException()

    def send_signal(self, sig):
        os.kill(self.pid, sig)


class WorkerProcess:
    def __init__(self, queues, burst):
        self.connection = get_current_connection()

        queues = [Queue(name=q) if isinstance(q, str) else q
                  for q in queues]
        # TODO: use heroku infos here
        hostname = socket.gethostname()
        shortname, _, _ = hostname.partition('.')
        description = '{0}.{1}'.format(shortname, self.pid)

        id = str(uuid.uuid4())

        self.maintenance = Maintenance()
        self.worker = Worker(id, description=description, queues=queues)
        self.in_receive_shutdown = 0
        self.shutdown_requested = False
        self.run(burst)

    def run(self, burst=False):
        """Starts the work loop.

        Returns the number of tasks that were processed in burst mode"""
        self.install_signal_handlers()
        self.worker.startup()
        self.log.info("RQ worker {0!r} started, version {1}".format(self.key, VERSION))

        try:
            tasks_processed = 0
            for task in self.task_queue_iter(burst):
                # TODO: The task is in limbo in this moment. If the process
                # crashes here, we lose track of it.
                self.process_task(task)
                tasks_processed += 1

                self.worker.heartbeat()
                self.maintenance.run_if_neccessary()
            else:
                self.log.info("Burst finished, quitting")
                return tasks_processed
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
                    task, queue = Queue.dequeue_any(self.queues, timeout)
            except DequeueTimeout:
                continue
            if burst and task is None:
                break
            yield task

    def process_task(self, task):
        with self.connection.pipeline() as pipeline:
            self.worker.start_task(task, pipeline=pipeline)
            task.set_running(self, pipeline=pipeline)

        self.log.info('{0}: {1} ({2})'.format(task.origin, task.description, task.id))
        try:
            outcome = self.execute_task(task)
        except Exception:
            exc_string = ''.join(traceback.format_exception(*sys.exc_info()))
            outcome = TaskOutcome('failure', error_message=exc_string)

        with self.connection.pipeline() as pipeline:
            self.worker.end_task(task, pipeline=pipeline)
            task.handle_outcome(outcome, pipeline=pipeline)

        self.log.info('{0}: {1} ({2})'.format(task.origin, 'Task OK', task.id))
        return True

    def execute_task(self, task):
        timeout_at = utcnow() + timedelta(seconds=task.timeout)
        work_horse = WorkHorse()
        work_horse.daemon = True
        horse_connection, writer = multiprocessing.Pipe(duplex=False)
        outcome = None
        try:
            work_horse.start(task, writer)
            # Wait for horse to set up its signal handling
            if horse_connection.poll(5):
                horse_connection.read()
            else:
                return TaskOutcome('aborted', "Workhorse failed to start")
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
                outcome = TaskOutcome('aborted', "Workhorse died")
            else:
                outcome = horse_connection.recv()
        finally:
            if work_horse.is_alive():
                work_horse.send_signal(signal.SIGKILL)
            if not outcome:
                outcome = TaskOutcome('aborted', "Workhorse died")
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


class Maintenance:
    def __init__(self):
        self.last_run_at = None
        self.key = 'rq:last_maintenance'

    def run_if_neccessary(self):
        if (not self.last_run_at or
                (utcnow() - self.last_run_at) < timedelta(seconds=MAINTENANCE_FREQ)):
            return
        # The cleanup tasks are not safe to run in paralell, so use this lock
        # to ensure that only one worker runs them.
        if self.connection.setnx(self.key, utcnow()):
            self.connection.expire(self.key, MAINTENANCE_FREQ)
            self.run()

        redis_value = self.connection.get(self.key)
        # might have expired between a failed SETNX and the GET
        if redis_value:
            self.last_run_at = utcparse(redis_value)

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
        self.current_task_id = None
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
        self.current_task_id = obj['current_task_id']
        for k in ['started_at', 'shutdown_at', 'shutdown_requested_at']:
            setattr(self, k, utcparse(obj[k]) if obj.get(k) else None)

    @takes_pipeline
    def _save(self, fields=None, *, pipeline):
        string_fields = ['description', 'state', 'queues', 'current_task_id']
        date_fields = ['started_at', 'shutdown_at', 'shutdown_requested_at']
        if fields is None:
            fields = string_fields + date_fields

        deletes = []
        store = {}
        for field in fields:
            value = getattr(self, field)
            if field == 'queues':
                store['queues'] = ','.join(q.name for q in self.queues),
            elif value is None:
                deletes.append(field)
            elif field in string_fields:
                store[field] = value
            elif field in date_fields:
                store[field] = utcformat(value)
            else:
                raise AttributeError(f'{field} is not a valid attribute')
        if deletes:
            pipeline.hdel(self.key, *deletes)
        pipeline.hmset(self.key, store)

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
            # This is imperfect due to a race condition, but is only a sanity check
            raise Exception('There already was a worker with id {self.id}')
        self.state = WorkerState.IDLE
        worker_registry.add(self, pipeline=pipeline)
        self._save(pipeline=pipeline)

    @takes_pipeline
    def start_task(self, task, *, pipeline):
        assert self.state == WorkerState.IDLE
        self.state = WorkerState.BUSY
        self.current_task_id = task.id
        self._save(['state', 'current_task_id'], pipeline=pipeline)

    @takes_pipeline
    def end_task(self, task, *, pipeline):
        assert self.state == WorkerState.BUSY
        self.state = WorkerState.IDLE
        self.current_task_id = None
        self._save(['state', 'current_task_id'], pipeline=pipeline)

    @takes_pipeline
    def shutdown(self, *, pipeline):
        assert self.state == WorkerState.IDLE
        worker_registry.remove(self, pipeline=pipeline)
        self.state = WorkerState.DEAD
        self.shutdown_at = utcnow()
        self._save(['state', 'shutdown_at'], pipeline=pipeline)
        pipeline.expire(self.key, DEAD_WORKER_TTL)

    @takes_pipeline
    def died(self, *, pipeline):
        worker_registry.remove(self, pipeline=pipeline)
        self.state = WorkerState.DEAD
        self.shutdown_at = utcnow()
        if self.current_task_id:
            task = Task.fetch(self.current_task_id)
            task.handle_worker_death(pipeline=pipeline)
            self.current_task_id = None
        self._save(['state', 'current_task_id', 'shutdown_at'], pipeline=pipeline)
        pipeline.expire(self.key, DEAD_WORKER_TTL)

    def set_shutdown_requested(self):
        self.shutdown_requested_at = utcnow()
        self.connection.hset(self.key, 'shutdown_requested_at',
                             utcformat(self.shutdown_requested_at))

    def fetch_current_task(self):
        """Returns the task id of the currently executing task."""
        if self.current_task_id:
            return Task.fetch(self.current_task_id)
