import logging
import sys
import traceback
import uuid
from contextlib import ExitStack

import redis_tasks

from .conf import RedisKey, connection, settings, task_middlewares
from .exceptions import (
    InvalidOperation, TaskAborted, TaskDoesNotExist, WorkerDied,
    WorkerShutdown)
from .registries import failed_task_registry, finished_task_registry
from .utils import (
    LazyObject, atomic_pipeline, deserialize, enum, generate_callstring,
    import_attribute, serialize, utcformat, utcnow, utcparse)

logger = logging.getLogger(__name__)

TaskStatus = enum(
    'TaskStatus',
    QUEUED='queued',
    FINISHED='finished',
    FAILED='failed',
    CANCELED='canceled',
    RUNNING='running',
)


def redis_task(*args, **kwargs):
    def decorator(f):
        # Constructing TaskProperties instances initializes the settings.
        # We do not want to do this on import of user modules, so be lazy here.
        f._redis_task_properties = LazyObject(lambda: TaskProperties(*args, **kwargs))
        return f

    return decorator


class TaskProperties:
    def __init__(self, reentrant=False, timeout=None):
        self.reentrant = reentrant
        self.timeout = timeout or settings.DEFAULT_TASK_TIMEOUT


class TaskOutcome:
    def __init__(self, outcome, message=None):
        assert outcome in ['success', 'failure', 'aborted']
        self.outcome = outcome
        self.message = message

    def __repr__(self):
        args = [self.outcome]
        if self.message:
            args.append(f'message={self.message!r}')
        return 'TaskOutcome({})'.format(", ".join(args))


class Task:
    def __init__(self, func=None, args=None, kwargs=None, *, fetch_id=None):
        if fetch_id:
            self.id = fetch_id
            self.refresh()
            return

        self.id = str(uuid.uuid4())

        try:
            if isinstance(func, str):
                # TODO: Test
                self.func_name = func
                func = self._get_func()
            else:
                self.func_name = '{0}.{1}'.format(func.__module__, func.__name__)
                assert self._get_func() == func
        except Exception as e:
            raise ValueError('The given task function is not importable') from e
        if not callable(func):
            # TODO: Test
            raise ValueError('The given task function is not callable')

        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        if not isinstance(args, (tuple, list)):
            raise TypeError(f'{args!r} is not a valid args list')
        if not isinstance(kwargs, dict):
            raise TypeError(f'{kwargs!r} is not a valid kwargs dict')
        self.args = args
        self.kwargs = kwargs

        self.error_message = None

        self.description = generate_callstring(self.func_name, self.args, self.kwargs)
        self.status = None
        self.origin = None
        self.meta = {}

        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self.aborted_runs = []

    @classmethod
    def fetch(cls, id):
        return cls(fetch_id=id)

    @property
    def key(self):
        return self.key_for(self.id)

    @classmethod
    def key_for(cls, task_id):
        return RedisKey('task:' + task_id)

    @atomic_pipeline
    def enqueue(self, queue, *, pipeline):
        assert self.status is None
        logger.info(f"Task {self.description} [{self.id}] enqueued")
        self.status = TaskStatus.QUEUED
        self.origin = queue.name
        self.enqueued_at = utcnow()
        self._save(pipeline=pipeline)
        queue.push(self, pipeline=pipeline)

    @atomic_pipeline
    def requeue(self, *, pipeline):
        assert self.status == TaskStatus.RUNNING
        logger.info(f"Task {self.description} [{self.id}] requeued")
        redis_tasks.Queue(self.origin).push(self, at_front=True, pipeline=pipeline)
        self.status = TaskStatus.QUEUED
        self.aborted_runs.append((self.started_at, utcnow()))
        self.started_at = None
        self._save(['status', 'aborted_runs', 'started_at'], pipeline=pipeline)

    @atomic_pipeline
    def set_running(self, worker, *, pipeline):
        assert self.status == TaskStatus.QUEUED
        logger.info(f"Task {self.description} [{self.id}] started")
        self.status = TaskStatus.RUNNING
        self.started_at = utcnow()
        self._save(['status', 'started_at'], pipeline=pipeline)

    @atomic_pipeline
    def set_finished(self, *, pipeline):
        assert self.status == TaskStatus.RUNNING
        logger.info(f"Task {self.description} [{self.id}] finished")
        finished_task_registry.add(self, pipeline=pipeline)
        self.status = TaskStatus.FINISHED
        self.ended_at = utcnow()
        self._save(['status', 'ended_at'], pipeline=pipeline)

    @atomic_pipeline
    def set_failed(self, error_message, *, pipeline):
        assert self.status == TaskStatus.RUNNING
        logger.info(f"Task {self.description} [{self.id}] failed")
        failed_task_registry.add(self, pipeline=pipeline)
        self.status = TaskStatus.FAILED
        self.error_message = error_message
        self.ended_at = utcnow()
        self._save(['status', 'error_message', 'ended_at'], pipeline=pipeline)

    @atomic_pipeline
    def handle_outcome(self, outcome, *, pipeline):
        if outcome.outcome == 'success':
            self.set_finished(pipeline=pipeline)
        elif outcome.outcome == 'failure':
            self.set_failed(outcome.message, pipeline=pipeline)
        elif outcome.outcome == 'aborted':
            if self.is_reentrant:
                self.requeue(pipeline=pipeline)
            else:
                self.set_failed(outcome.message, pipeline=pipeline)

    @atomic_pipeline
    def handle_worker_death(self, *, pipeline):
        if self.status == TaskStatus.QUEUED:
            logger.debug(f"Task {self.description} [{self.id}] had its worker die. Reenqueuing.")
            # The worker died while moving the task
            redis_tasks.Queue(self.origin).push(self, at_front=True, pipeline=pipeline)
        elif self.status == TaskStatus.RUNNING:
            try:
                raise WorkerDied("Worker died")
            except WorkerDied:
                exc_info = sys.exc_info()
            outcome = self._generate_outcome(*exc_info)
            self.handle_outcome(outcome, pipeline=pipeline)
        else:
            raise Exception(f"Unexpected task status: {self.status}")

    def cancel(self):
        queue = redis_tasks.Queue(name=self.origin)
        try:
            queue.remove_and_delete(self)
        except TaskDoesNotExist:
            raise InvalidOperation("Only enqueued jobs can be canceled")
        self.status = TaskStatus.CANCELED

    def _get_func(self):
        return import_attribute(self.func_name)

    def _get_properties(self):
        return getattr(self._get_func(), '_redis_task_properties', TaskProperties())

    @property
    def is_reentrant(self):
        try:
            return self._get_properties().reentrant
        except Exception:
            return TaskProperties().reentrant

    @property
    def timeout(self):
        try:
            return self._get_properties().timeout
        except Exception:
            return TaskProperties().timeout

    @classmethod
    @atomic_pipeline
    def delete_many(cls, task_ids, *, pipeline):
        if task_ids:
            pipeline.delete(*(cls.key_for(task_id) for task_id in task_ids))

    def refresh(self):
        key = self.key
        obj = {k.decode(): v for k, v in connection.hgetall(key).items()}
        if len(obj) == 0:
            raise TaskDoesNotExist('No such task: {0}'.format(key))

        self.func_name = obj['func_name'].decode()
        self.args = deserialize(obj['args'])
        self.kwargs = deserialize(obj['kwargs'])

        for key in ['status', 'origin', 'description', 'error_message']:
            setattr(self, key, obj[key].decode() if key in obj else None)

        for key in ['enqueued_at', 'started_at', 'ended_at']:
            setattr(self, key, utcparse(obj[key].decode()) if key in obj else None)

        self.meta = deserialize(obj['meta']) if obj.get('meta') else {}
        self.aborted_runs = deserialize(obj['aborted_runs']) if obj.get('aborted_runs') else []

    @atomic_pipeline
    def _save(self, fields=None, *, pipeline=None):
        string_fields = ['func_name', 'status', 'description', 'origin', 'error_message']
        date_fields = ['enqueued_at', 'started_at', 'ended_at']
        data_fields = ['args', 'kwargs', 'meta', 'aborted_runs']
        if fields is None:
            fields = string_fields + date_fields + data_fields

        deletes = []
        store = {}
        for field in fields:
            value = getattr(self, field)
            if value is None:
                deletes.append(field)
            elif field in string_fields:
                store[field] = value
            elif field in date_fields:
                store[field] = utcformat(value)
            elif field in data_fields:
                store[field] = serialize(value)
            else:
                raise AttributeError(f'{field} is not a valid attribute')
        if deletes:
            pipeline.hdel(self.key, *deletes)
        if store:
            pipeline.hmset(self.key, store)

    def save_meta(self):
        self._save(['meta'])

    def execute(self, *, shutdown_cm=ExitStack()):
        """Run the task using middleware.

        The `shutdown_cm` parameter is a context manager that will wrap the part
        of the execution in which `WorkerShutdown` is allowed to be raised.

        Returns a TaskOutcome."""
        exc_info = (None, None, None)
        try:
            def run_task(*args, **kwargs):
                with shutdown_cm:
                    try:
                        func = self._get_func()
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to import task function {self.func_name}") from e
                    func(*args, **kwargs)

            def mw_wrapper(mwc, task, run):
                return lambda *args, **kwargs: mwc().run_task(task, run, args, kwargs)

            for middleware_constructor in reversed(task_middlewares):
                run_task = mw_wrapper(middleware_constructor, self, run_task)

            try:
                run_task(*self.args, **self.kwargs)
            except WorkerShutdown as e:
                raise TaskAborted("Worker shutdown") from e
        except Exception:
            exc_info = sys.exc_info()

        return self._generate_outcome(*exc_info)

    def _generate_outcome(self, *exc_info):
        for middleware in reversed(task_middlewares):
            try:
                if middleware().process_outcome(self, *exc_info):
                    exc_info = (None, None, None)
            except Exception:
                exc_info = sys.exc_info()

        if not exc_info[0]:
            return TaskOutcome('success')
        elif isinstance(exc_info[1], TaskAborted):
            return TaskOutcome('aborted', message=exc_info[1].message)
        else:
            exc_string = ''.join(traceback.format_exception(*exc_info))
            return TaskOutcome('failure', message=exc_string)

    def __repr__(self):
        return '<{0} {1}: {2}>'.format(
            self.__class__.__name__, self.id, self.description)
