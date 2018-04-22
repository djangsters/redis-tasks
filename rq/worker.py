import logging
import time
import multiprocessing
import os
import signal
import socket
import sys
import traceback
import uuid
from contextlib import contextmanager, suppress
from datetime import timedelta

from .conf import connection, settings, RedisKey
from .exceptions import (NoSuchWorkerError, WorkerShutdown)
from .queue import Queue
from .registries import expire_registries, worker_registry
from .task import Task, TaskOutcome, initialize_middlewares
from .utils import atomic_pipeline, enum, utcformat, utcnow, utcparse, import_attribute

logger = logging.getLogger(__name__)


class ShutdownRequested(BaseException):
    pass


class PostponeShutdown:
    _active = {}
    _shutdown_delayed = False

    def activate(self):
        self.__enter__()

    def deactivate(self):
        self.__exit__()

    def __enter__(self):
        if self in self._active:
            raise Exception("PostponeShutdown already active")
        self._active.add(self)

    def __exit__(self, *args):
        self._active.remove(self)

        if not self._active and self._shutdown_delayed:
            logger.warning('Critical section left, raising WorkerShutdown')
            raise WorkerShutdown()

    @classmethod
    def trigger_shutdown(cls):
        if cls._active:
            logger.warning('Delaying WorkerShutdown till critical section is finished')
            cls._shutdown_delayed = True
        else:
            logger.warning('Raising WorkerShutdown to cancel task')
            raise WorkerShutdown()


WorkerStatus = enum(
    'WorkerStatus',
    STARTED='started',
    BUSY='busy',
    IDLE='idle'
)


class WorkHorse(multiprocessing.Process):
    def run(self, task, worker_connection):
        ps = PostponeShutdown()
        ps.activate()
        self.setup_signal_handler()
        worker_connection.send(True)

        outcome = task.execute(pre_run=ps.deactivate, post_run=self.ignore_shutdown_signal)
        worker_connection.send(outcome)

    def setup_signal_handler(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGUSR1, self.request_stop)

    def ignore_shutdown_signal(self):
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)

    def request_stop(self, signum, frame):
        self.ignore_shutdown_signal()
        PostponeShutdown.trigger_shutdown()

    def send_signal(self, sig):
        os.kill(self.pid, sig)


def generate_worker_description(*, pid):
    hostname = socket.gethostname()
    shortname = hostname.split('.', maxsplit=1)[0]
    return '{0}.{1}'.format(shortname, pid)


class WorkerProcess:
    def __init__(self, queues, burst):
        description_generator = import_attribute(settings.WORKER_DESCRIPTION_FUNCTION)
        description = description_generator()
        id = str(uuid.uuid4())
        self.worker = Worker(id, description=description, queues=queues)
        self.maintenance = Maintenance()
        self.in_receive_shutdown = 0
        self.shutdown_requested = False

    def run(self, burst=False):
        """Starts the work loop.

        Returns the number of tasks that were processed in burst mode"""
        self.install_signal_handlers()
        self.worker.startup()
        self.log.info("RQ worker {}({!r}) started".format(
            self.worker.description, self.worker.id))

        if settings.WORKER_PRELOAD_FUNCTION:
            worker_preload = import_attribute(settings.WORKER_PRELOAD_FUNCTION)
            worker_preload(self.worker)
        initialize_middlewares()

        try:
            tasks_processed = 0
            for task in self.task_queue_iter(burst):
                self.process_task(task)
                tasks_processed += 1

                self.maybe_shutdown()
                self.worker.heartbeat()
                self.maintenance.run_if_neccessary()
            else:
                self.log.info("Burst finished, quitting")
                return tasks_processed
        finally:
            self.worker.shutdown()

    def queue_iter(self, burst):
        self.log.debug('Listening on {0}...'.format(
            ', '.join(q.name for q in self.worker.queues)))

        while True:
            self.maybe_shutdown()
            self.worker.heartbeat()
            task = None
            # The queue unblocker might loose entries on worker shutdown, so we
            # regularly try to dequeue unconditionally
            for queue in self.worker.queues:
                self.maybe_shutdown()
                task = queue.dequeue(self.worker)
                if task:
                    break
            if not task and not burst:
                with self.interruptible():
                    queue = Queue.await_multi(self.worker.queues, settings.WORKER_HEARTBEAT_FREQ)
                    if not queue:
                        continue
                # First, attempt to dequeue from the queue whose unblocker we
                # consumed. This makes for more sensible behavior in situations
                # with multiple queues and workers.
                task = queue.dequeue(self.worker)
            if task:
                yield task
            elif burst:
                break

    def process_task(self, task):
        self.worker.start_task(task)

        self.log.info('{0}: {1} ({2})'.format(task.origin, task.description, task.id))
        try:
            outcome = self.execute_task(task)
        except Exception:
            exc_string = ''.join(traceback.format_exception(*sys.exc_info()))
            outcome = TaskOutcome('failure', error_message=exc_string)

        self.worker.end_task(task, outcome)

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
                    with self.interruptible():
                        work_horse.join(settings.WORKER_HEARTBEAT_FREQ)
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
        self.log.debug('Got signal {0}'.format(signum))
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        with suppress(Exception):
            self.worker.set_shutdown_requested()
        if self.in_receive_shutdown:
            self.log.debug('Interrupted, initiating shutdown')
            raise ShutdownRequested()

    @contextmanager
    def interruptible(self):
        self.maybe_shutdown()
        self.in_receive_shutdown += 1
        # The signal handler may now raise ShutdownRequested
        try:
            yield
        finally:
            self.in_receive_shutdown -= 1

    def maybe_shutdown(self):
        if self.shutdown_requested:
            self.log.debug('Initiating shutdown')
            raise ShutdownRequested()


class Maintenance:
    def __init__(self):
        self.last_run_at = None
        self.key = RedisKey('last_maintenance')

    def run_if_neccessary(self):
        if (not self.last_run_at or
                (utcnow() - self.last_run_at) < timedelta(seconds=settings.MAINTENANCE_FREQ)):
            return
        # The cleanup tasks are not safe to run in paralell, so use this lock
        # to ensure that only one worker runs them.
        if connection.setnx(self.key, utcnow()):
            connection.expire(self.key, settings.MAINTENANCE_FREQ)
            self.run()

        redis_value = connection.get(self.key)
        # might have expired between a failed SETNX and the GET
        if redis_value:
            self.last_run_at = utcparse(redis_value)

    def handle_dead_workers(self):
        dead_worker_ids = worker_registry.get_dead_ids(self)
        for worker_id in dead_worker_ids:
            worker = Worker.fetch(worker_id)
            worker.died()

    def run(self):
        self.handle_dead_workers()
        expire_registries()


WorkerState = enum(
    'WorkerState',
    IDLE='idle',
    BUSY='busy',
    DEAD='dead',
)


class Worker:
    @classmethod
    def all(cls):
        return [cls.fetch(id) for id in worker_registry.get_alive_ids()]

    @classmethod
    def fetch(cls, id):
        return cls(fetch_id=id)

    def __init__(self, id=None, *, description=None, queues=None,
                 fetch_id=None):
        self.id = id or fetch_id
        self.key = RedisKey('worker:' + self.id)
        self.task_key = RedisKey('worker_task:' + self.id)

        if fetch_id:
            self.refresh()
            return

        self.description = description
        self.state = None
        self.queues = queues
        self.current_task_id = None
        self.started_at = None
        self.shutdown_at = None
        self.shutdown_requested_at = None

    def refresh(self):
        with connection.pipeline() as pipeline:
            pipeline.hmgetall(self.key)
            pipeline.lrange(self.task_key, 0, -1)
            obj, task_id = pipeline.execute()

        if not obj:
            raise NoSuchWorkerError(self.id)
        assert len(task_id) < 2
        self.current_task_id = task_id.decode() if task_id else None

        obj = {k.decode(): v.decode() for k, v in obj}
        self.state = obj['state']
        self.description = obj['description']
        if obj['queues']:
            self.queues = [Queue(q) for q in obj['queues'].split(',')]
        else:
            self.queues = []
        for k in ['started_at', 'shutdown_at', 'shutdown_requested_at']:
            setattr(self, k, utcparse(obj[k]) if obj.get(k) else None)

    @atomic_pipeline
    def _save(self, fields=None, *, pipeline):
        string_fields = ['description', 'state', 'queues']
        date_fields = ['started_at', 'shutdown_at', 'shutdown_requested_at']
        if fields is None:
            fields = string_fields + date_fields + ['current_task_id']

        deletes = []
        store = {}
        for field in fields:
            value = getattr(self, field)
            if field == 'queues':
                store['queues'] = ','.join(q.name for q in self.queues),
            elif field == 'current_task_id':
                pipeline.delete(self.task_key)
                if value:
                    pipeline.lpush(self.task_key, value)
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

    def heartbeat(self):
        """Send a heartbeat.

        Raises a NoSuchworkerError if the registry considers this worker as dead"""
        worker_registry.heartbeat(self)

    @atomic_pipeline
    def startup(self, *, pipeline):
        logger.debug(f'Registering birth of worker {self.description} ({self.id})')
        self.state = WorkerState.IDLE
        worker_registry.add(self, pipeline=pipeline)
        self._save(pipeline=pipeline)

    @atomic_pipeline
    def start_task(self, task, *, pipeline):
        assert self.state == WorkerState.IDLE
        # The task should be assigned by the queue
        assert self.current_task_id == task.id
        self.state = WorkerState.BUSY
        self._save(['state'], pipeline=pipeline)
        task.set_running(self, pipeline=pipeline)

    @atomic_pipeline
    def end_task(self, task, outcome, *, pipeline):
        assert self.state == WorkerState.BUSY
        self.state = WorkerState.IDLE
        self.current_task_id = None
        self._save(['state', 'current_task_id'], pipeline=pipeline)
        task.handle_outcome(outcome, pipeline=pipeline)

    @atomic_pipeline
    def shutdown(self, *, pipeline):
        assert self.state == WorkerState.IDLE
        worker_registry.remove(self, pipeline=pipeline)
        self.state = WorkerState.DEAD
        self.shutdown_at = utcnow()
        self._save(['state', 'shutdown_at'], pipeline=pipeline)
        pipeline.expire(self.key, settings.DEAD_WORKER_TTL)

    @atomic_pipeline
    def died(self, *, pipeline):
        worker_registry.remove(self, pipeline=pipeline)
        self.shutdown_at = utcnow()
        if self.current_task_id:
            task = Task.fetch(self.current_task_id)
            task.handle_worker_death(pipeline=pipeline)
            self.current_task_id = None
        self.state = WorkerState.DEAD
        self._save(['state', 'current_task_id', 'shutdown_at'], pipeline=pipeline)
        pipeline.expire(self.key, settings.DEAD_WORKER_TTL)

    def set_shutdown_requested(self):
        self.shutdown_requested_at = utcnow()
        connection.hset(self.key, 'shutdown_requested_at',
                        utcformat(self.shutdown_requested_at))

    def fetch_current_task(self):
        """Returns the currently executing task."""
        if self.current_task_id:
            return Task.fetch(self.current_task_id)
