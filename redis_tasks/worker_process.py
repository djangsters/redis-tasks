import logging
import multiprocessing
import os
import signal
import socket
import sys
import threading
import traceback
import uuid
from contextlib import ExitStack as nullcontext
from contextlib import contextmanager
from datetime import timedelta

from .conf import RedisKey, connection, settings, task_middleware
from .exceptions import WorkerShutdown
from .queue import Queue
from .registries import registry_maintenance
from .utils import import_attribute, utcformat, utcnow, utcparse
from .worker import Worker

logger = logging.getLogger('redis_tasks.worker')


class PostponeShutdown:
    _active = set()
    _shutdown_delayed = False

    def activate(self):
        self.__enter__()

    def deactivate(self):
        self.__exit__()

    def __enter__(self):
        self.assert_main_thread()
        self._active.add(self)

    def __exit__(self, *args):
        self.assert_main_thread()
        self._active.remove(self)

        if not self._active and self._shutdown_delayed:
            logger.warning('PostponeShutdown left, raising WorkerShutdown')
            raise WorkerShutdown()

    @staticmethod
    def assert_main_thread():
        if threading.current_thread() != threading.main_thread():
            raise RuntimeError("PostponeShutdown can only be used in main thread")

    @classmethod
    def trigger_shutdown(cls):
        cls.assert_main_thread()
        if cls._active:
            logger.warning('Delaying WorkerShutdown till PostponeShutdown is finished')
            cls._shutdown_delayed = True
        else:
            logger.warning('Raising WorkerShutdown to cancel task')
            raise WorkerShutdown()


class ShutdownRequested(BaseException):
    pass


def generate_worker_description():
    hostname = socket.gethostname()
    shortname = hostname.split('.', maxsplit=1)[0]
    return '{0}.{1}'.format(shortname, os.getpid())


def worker_main(queue_names=["default"], *, burst=False, description=None):
    if isinstance(queue_names, str):
        queue_names = [queue_names]
    process = WorkerProcess([Queue(n) for n in queue_names], description=description)
    try:
        return process.run(burst)
    except ShutdownRequested:
        sys.exit()


class WorkerProcess:
    def __init__(self, queues, *, description=None):
        if not description:
            description_generator = import_attribute(settings.WORKER_DESCRIPTION_FUNCTION)
            description = description_generator()
        id = str(uuid.uuid4())
        self.worker = Worker(id, description=description, queues=queues)
        self.maintenance = Maintenance()
        self.in_interruptible = 0
        self.shutdown_requested = False

    def run(self, burst=False):
        """Starts the work loop.

        Returns the number of tasks that were processed in burst mode"""
        self.install_signal_handlers()
        self.worker.startup()

        if settings.WORKER_PRELOAD_FUNCTION:
            worker_preload = import_attribute(settings.WORKER_PRELOAD_FUNCTION)
            worker_preload(self.worker)

        # Basic test of the middleware setup. If this fails, we would rather
        # have the worker fail to start than to fail every job.
        [x() for x in task_middleware]

        try:
            tasks_processed = 0
            for task in self.queue_iter(burst):
                self.process_task(task)
                tasks_processed += 1
                self.maybe_shutdown()

                self.worker.heartbeat()
                self.maintenance.run_if_neccessary()
                self.maybe_shutdown()
            else:
                logger.info(f"Burst finished after {tasks_processed} tasks, shutting down")
                return tasks_processed
        finally:
            self.worker.shutdown()

    def queue_iter(self, burst):
        logger.debug('Worker listening on {}'.format(
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
        try:
            outcome = self.execute_task(task)
        except Exception:
            # TODO: check whether this is necessary, execute_tasks should not raise
            exc_string = ''.join(traceback.format_exception(*sys.exc_info()))
            outcome = task.get_abort_outcome(exc_string)
        self.worker.end_task(task, outcome)
        return True

    def execute_task(self, task):
        timeout_at = utcnow() + timedelta(seconds=task.timeout)
        horse_connection, writer = multiprocessing.Pipe(duplex=False)
        work_horse = WorkHorse(task, writer)
        work_horse.daemon = True
        try:
            self.worker.heartbeat()
            work_horse.start()
            # Wait for horse to set up its signal handling
            if horse_connection.poll(5):
                assert horse_connection.recv()
            else:
                logger.error('Workhorse failed to start')
                return task.get_abort_outcome('Workhorse failed to start')
            shutdown_requested = False
            while work_horse.is_alive():
                self.worker.heartbeat()
                if utcnow() >= timeout_at:
                    logger.error('Task reached timeout, killing workhorse')
                    work_horse.send_signal(signal.SIGKILL)
                    return task.get_abort_outcome(f'Task timeout ({task.timeout} sec) reached',
                                                  may_requeue=False)
                try:
                    with self.interruptible() if not shutdown_requested else nullcontext():
                        work_horse.join(settings.WORKER_HEARTBEAT_FREQ)
                except ShutdownRequested:
                    logger.debug('ShutdownRequested caught, signaling WorkHorse to shut down')
                    work_horse.send_signal(signal.SIGUSR1)
                    shutdown_requested = True
        finally:
            if work_horse.is_alive():
                work_horse.send_signal(signal.SIGKILL)
                work_horse.join()

        if horse_connection.poll():
            return horse_connection.recv()
        else:
            logger.error('Workhorse died unexpectedly')
            return task.get_abort_outcome('Workhorse died unexpectedly')

    def install_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_stop_signal)
        signal.signal(signal.SIGTERM, self.handle_stop_signal)

    def handle_stop_signal(self, signum, frame):
        logger.debug('Got signal {0}'.format(signum))
        if self.shutdown_requested:
            return
        logger.debug('Shutdown request accepted')
        self.shutdown_requested = True
        if self.in_interruptible:
            logger.debug('Interruptible, raising ShutdownRequested')
            raise ShutdownRequested()

    @contextmanager
    def interruptible(self):
        self.maybe_shutdown()
        self.in_interruptible += 1
        # The signal handler may now raise ShutdownRequested
        try:
            yield
        finally:
            self.in_interruptible -= 1

    def maybe_shutdown(self):
        if self.shutdown_requested:
            logger.debug('Raising ShutdownRequested')
            raise ShutdownRequested()


class Maintenance:
    def __init__(self):
        self.last_run_at = None
        self.key = RedisKey('last_maintenance')

    def run_if_neccessary(self):
        if (self.last_run_at and
                (utcnow() - self.last_run_at) < timedelta(seconds=settings.MAINTENANCE_FREQ)):
            return
        # The cleanup tasks are not safe to run in paralell, so use this lock
        # to ensure that only one worker runs them.
        if connection.setnx(self.key, utcformat(utcnow())):
            connection.expire(self.key, settings.MAINTENANCE_FREQ)
            self.run()

        redis_value = connection.get(self.key)
        # might have expired between a failed SETNX and the GET
        if redis_value:
            # Use min to limit impact of workers with incorrect time
            self.last_run_at = min(utcnow(), utcparse(redis_value.decode()))

    def run(self):
        registry_maintenance()


class WorkHorse(multiprocessing.Process):
    def __init__(self, task, worker_connection):
        super().__init__(daemon=True)
        self.task = task
        self.worker_connection = worker_connection

    def run(self):
        ps = PostponeShutdown()
        ps.activate()
        self.setup_signal_handler()
        logger.debug("WorkHorse started")
        self.worker_connection.send(True)

        @contextmanager
        def shutdown_cm():
            ps.deactivate()
            try:
                yield
            finally:
                self.ignore_shutdown_signal()

        outcome = self.task.execute(shutdown_cm=shutdown_cm())
        self.worker_connection.send(outcome)
        logger.debug("WorkHorse finished")

    def setup_signal_handler(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGUSR1, self.request_stop)

    def ignore_shutdown_signal(self):
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)

    def request_stop(self, signum, frame):
        logger.debug("WorkHorse received shutdown request")
        self.ignore_shutdown_signal()
        PostponeShutdown.trigger_shutdown()

    def send_signal(self, sig):
        os.kill(self.pid, sig)


class TWorker:
    def __init__(self, queues=['default']):
        id = str(uuid.uuid4())
        self.worker = Worker(id,
                             queues=[Queue(n) for n in queues],
                             description=f'TestWorker-{id}')
        self.failed = []
        self.succeeded = []

    def run(self, raise_on_failure=True):
        self.worker.startup()
        i = 0
        while True:
            for queue in self.worker.queues:
                task = queue.dequeue(self.worker)
                if task:
                    break
            else:
                break

            i += 1
            self.worker.start_task(task)
            outcome = task.execute()
            self.worker.end_task(task, outcome)

            if outcome.outcome == 'success':
                self.succeeded.append(task)
            elif outcome.outcome == 'failure':
                self.failed.append(task)
                if raise_on_failure:
                    raise RuntimeError(f"Task {task.description!r} failed\n" + task.error_message)
            else:
                raise RuntimeError("Unexpected task outcome")
        return i
