import logging
import sys
import traceback
import uuid
from contextlib import ExitStack
from functools import partial

import rq
from .conf import RedisKey, connection, settings, task_middlewares
from .exceptions import (InvalidOperation, TaskDoesNotExist, TaskAborted,
                         WorkerDied, WorkerShutdown)
from .registries import failed_task_registry, finished_task_registry
from .utils import (atomic_pipeline, deserialize, enum, generate_callstring,
                    import_attribute, serialize, utcformat, utcnow, utcparse, LazyObject)

logger = logging.getLogger(__name__)

TaskStatus = enum(
    'TaskStatus',
    QUEUED='queued',
    FINISHED='finished',
    FAILED='failed',
    CANCELED='canceled',
    RUNNING='running',
)


def rq_task(*args, **kwargs):
    def decorator(f):
        # Constructing TaskProperties instances initializes the settings.
        # We do not want to do this on import of user modules, so be lazy here.
        f._rq_task_properties = LazyObject(lambda: TaskProperties(*args, **kwargs))
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


class TaskMiddleware:
    def run_task(task, run, args, kwargs):
        return run(*args, **kwargs)

    def process_outcome(task, outcome):
        pass


class TaskMiddlewareOld:
    """Base class for a task execution middleware.

    Before task execution, the `before()` methods are called in the order the
    middlewares are listen in TODO[setting]. Any middleware can short-circuit
    this process by raising an exception in the `before()` method. The following
    `before()` method will then not be called.

    Independently of whether the task ran or a middleware interupted the
    startup, all configured middelwares' `process_outcome()` methods are called
    after execution. If the worker running the task did not die and a
    middleware's `before()` method was called for this task, the
    `process_outcome()` method will be called on the same instance of the
    middleware. Otherwise a new instance is created.

    Finally, if the worker did not die, the `after()` method is called for all
    middlewares that had their `before()` method called. This is guaranteed to
    happen on the same instance. This is the right place to do process-local
    cleanup.

    The TaskAborted exception has a special meaning in this context. It is raised
    by rq if the task did not fail itself, but was aborted for external reasons.
    It can also be raised by any middleware. If it is passed through or raised
    by the outer-most middleware, reentrant tasks will be reenqueued at the
    front of the queue they came from."""

    def before(self, task):
        """Is called before the task is started.

        If this raises an exception, execution is canceled the task is treated as failed."""
        pass

    def process_outcome(self, task, exc_type=None, exc_val=None, exc_tb=None):
        """Process the outcome of the task.

        If the task failed, the three exc_ parameters will be set.
        This can happen for the following reasons:
        * The task raised an exception
        * The worker shut down during execution
        * The worked died unexpectedly during execution
        * The task reached its timeout
        * Another middleware raised an exception

        Can return True to treat the task as succeeded.
        Returning a non-true value (e.g. None), passes the current state on to
        the next middleware.
        Raising an exception passes the raised exception to the next middleware."""
        pass

    def after(self, task):
        """Is called after `process_outcome`.

        This function might not be called if the worker is exiting early"""
        pass


class Task:
    def __init__(self, func=None, args=None, kwargs=None, *, fetch_id=None):
        if fetch_id:
            self.id = fetch_id
            self.refresh()
            return

        self.id = str(uuid.uuid4())

        try:
            self.func_name = '{0}.{1}'.format(func.__module__, func.__name__)
            assert self._get_func() == func
        except Exception as e:
            raise ValueError('The given task function is not importable') from e

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
        self.status = TaskStatus.QUEUED
        self.origin = queue.name
        self.enqueued_at = utcnow()
        self._save(pipeline=pipeline)
        queue.push(self, pipeline=pipeline)

    @atomic_pipeline
    def requeue(self, *, pipeline):
        assert self.status == TaskStatus.RUNNING
        rq.Queue(self.origin).push(self, at_front=True, pipeline=pipeline)
        self.status = TaskStatus.QUEUED
        self.aborted_runs.append((self.started_at, utcnow()))
        self.started_at = None
        self._save(['status', 'aborted_runs', 'started_at'], pipeline=pipeline)

    @atomic_pipeline
    def set_running(self, worker, *, pipeline):
        assert self.status == TaskStatus.QUEUED
        self.status = TaskStatus.RUNNING
        self.started_at = utcnow()
        self._save(['status', 'started_at'], pipeline=pipeline)

    @atomic_pipeline
    def set_finished(self, *, pipeline):
        assert self.status == TaskStatus.RUNNING
        finished_task_registry.add(self, pipeline=pipeline)
        self.status = TaskStatus.FINISHED
        self.ended_at = utcnow()
        self._save(['status', 'ended_at'], pipeline=pipeline)

    @atomic_pipeline
    def set_failed(self, error_message, *, pipeline):
        assert self.status == TaskStatus.RUNNING
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
            # The worker died while moving the task
            rq.Queue(self.origin).push(self, at_front=True, pipeline=pipeline)
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
        queue = rq.Queue(name=self.origin)
        try:
            queue.remove_and_delete(self)
        except TaskDoesNotExist:
            raise InvalidOperation("Only enqueued jobs can be canceled")
        self.status = TaskStatus.CANCELED

    def _get_func(self):
        return import_attribute(self.func_name)

    def _get_properties(self):
        return getattr(self._get_func(), '_rq_task_properties', TaskProperties())

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
                        func = self._get_func()
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
