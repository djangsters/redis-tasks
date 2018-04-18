import inspect
import uuid

from .connections import resolve_connection
from .defaults import JOB_TIMEOUT
from .exceptions import NoSuchJobError
from .queue import Queue
from .registry import (failed_job_registry, finished_job_registry,
                       running_job_registry)
from .serialization import deserialize, serialize
from .utils import (enum, import_attribute, takes_pipeline, utcformat, utcnow,
                    utcparse)

JobStatus = enum(
    'JobStatus',
    QUEUED='queued',
    FINISHED='finished',
    FAILED='failed',
    CANCELED='canceled',
    RUNNING='running',
)


def rq_task(*args, **kwargs):
    return TaskProperties(*args, **kwargs).decorate


class TaskProperties:
    def __init__(self, is_reentrant=False, timeout=None):
        self.is_reentrant = is_reentrant
        self.timeout = timeout

    def decorate(self, f):
        f._rq_job_properties = self
        return f


class Job:
    def __init__(self, func=None, args=None, kwargs=None, *,
                 fetch_id=None, description=None, meta=None):
        self.connection = resolve_connection()

        if fetch_id:
            self.id = fetch_id
            self.refresh()
            return

        self.id = str(uuid.uuid4())

        if inspect.isfunction(func) or inspect.isbuiltin(func):
            self.func_name = '{0}.{1}'.format(func.__module__, func.__name__)
            assert self.func == func
        elif isinstance(func, str):
            self.func_name = func
        else:
            raise TypeError('Expected a function or string, but got: {0}'.format(func))
        if self.func_name.startswith('__main__'):
            raise ValueError("The job's function needs to be importable by the workers")

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

        self.description = description or self.get_call_string()
        self.status = None
        self.origin = None
        self.meta = meta or {}

        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self.aborted_runs = []

    @classmethod
    def fetch(cls, id):
        return cls(fetch_id=id)

    @takes_pipeline
    def enqueue(self, queue, *, pipeline):
        assert self.status is None
        self.status = JobStatus.QUEUED
        self.origin = queue.name
        self.enqueued_at = utcnow()
        self._save(pipeline=pipeline)
        queue.push_job(self, pipeline=pipeline)

    @takes_pipeline
    def requeue(self, *, pipeline):
        assert self.status is JobStatus.RUNNING
        running_job_registry.remove(self, pipeline=pipeline)
        Queue(self.origin).push_job(self, at_front=True, pipeline=pipeline)
        self.status = JobStatus.QUEUED
        self.aborted_runs.append((self.started_at, utcnow()))
        self.started_at = None
        self._save(['status', 'aborted_runs', 'started_at'], pipeline=pipeline)

    @takes_pipeline
    def set_running(self, worker, *, pipeline):
        assert self.status == JobStatus.QUEUED
        running_job_registry.add(self, worker, pipeline=pipeline)
        self.status = JobStatus.RUNNING
        self.started_at = utcnow()
        self._save(['status', 'started_at'], pipeline=pipeline)

    @takes_pipeline
    def set_finished(self, *, pipeline):
        assert self.status == JobStatus.RUNNING
        running_job_registry.remove(self, pipeline=pipeline)
        finished_job_registry.add(self, pipeline=pipeline)
        self.status = JobStatus.FINISHED
        self.ended_at = utcnow()
        self._save(['status', 'ended_at'], pipeline=pipeline)

    @takes_pipeline
    def set_failed(self, error_message, *, pipeline):
        assert self.status == JobStatus.RUNNING
        running_job_registry.remove(self, pipeline=pipeline)
        failed_job_registry.add(self, pipeline=pipeline)
        self.status = JobStatus.FAILED
        self.error_message = error_message
        self.ended_at = utcnow()
        self._save(['status', 'error_message', 'ended_at'], pipeline=pipeline)

    @takes_pipeline
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

    @takes_pipeline
    def cancel(self, *, pipeline):
        # TODO: check which state we are in and react accordingly
        # fail if the job is currently running
        # Need to do this in a transaction
        from .queue import Queue
        if self.origin:
            q = Queue(name=self.origin)
            q.remove(self, pipeline=pipeline)
            self.delete_many([self.id], pipeline=pipeline)

    @takes_pipeline
    def _set_status(self, status, *, pipeline):
        self.status = status
        pipeline.hset(self.key, 'status', self.status)

    @property
    def func(self):
        return import_attribute(self.func_name)

    @property
    def func_properties(self):
        if hasattr(self.func, '_rq_job_properties'):
            return self.func._rq_job_properties
        else:
            return TaskProperties()

    @property
    def is_reentrant(self):
        return self.func_properties.is_reentrant

    @property
    def timeout(self):
        if self.func_properties.timeout is None:
            return JOB_TIMEOUT
        else:
            return self.func_properties.timeout

    @classmethod
    def key_for(cls, job_id):
        return 'rq:job:' + job_id

    @classmethod
    @takes_pipeline
    def delete_many(cls, job_ids, *, pipeline):
        pipeline.delete(*(cls.key_for(job_id) for job_id in job_ids))

    @property
    def key(self):
        return self.key_for(self.id)

    def refresh(self):
        key = self.key
        obj = {k.decode(): v for k, v in self.connection.hgetall(key).items()}
        if len(obj) == 0:
            raise NoSuchJobError('No such job: {0}'.format(key))

        try:
            self.func_name = obj['func_name'].decode()
            self._args = deserialize(obj['args'])
            self._kwargs = deserialize(obj['kwargs'])
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        for key in ['status', 'origin', 'description', 'error_message']:
            setattr(self, key, obj[key].decode() if key in obj else None)

        for key in ['enqueue_at', 'started_at', 'ended_at']:
            setattr(self, key, utcparse(obj[key].decode()) if key in obj else None)

        self.meta = deserialize(obj['meta']) if obj.get('meta') else {}
        self.aborted_runs = deserialize(obj['aborted_runs']) if obj.get('aborted_runs') else []

    @takes_pipeline
    def _save(self, fields=None, *, pipeline=None):
        string_fields = ['func_name', 'status', 'description', 'origin', 'error_message']
        date_fields = ['enqueue_at', 'started_at', 'ended_at']
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
        pipeline.hmset(self.key, store)

    def save_meta(self):
        self._save(['meta'])

    def execute(self):
        return self.func(*self.args, **self.kwargs)

    def get_call_string(self):
        """Returns a string representation of the call, formatted as a regular
        Python function invocation statement."""
        arg_list = [repr(arg) for arg in self.args]
        arg_list += [f'{k}={v!r}' for k, v in self.kwargs.items()]
        args = ', '.join(arg_list)

        return '{0}({1})'.format(self.func_name, args)

    def __repr__(self):
        return '{0}({1!r}, enqueued_at={2!r})'.format(
            self.__class__.__name__, self.id, self.enqueued_at)

    def __str__(self):
        return '<{0} {1}: {2}>'.format(
            self.__class__.__name__, self.id, self.description)
