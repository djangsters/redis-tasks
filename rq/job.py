import inspect
import traceback
from uuid import uuid4

from .connections import resolve_connection
from .defaults import JOB_TIMEOUT
from .exceptions import NoSuchJobError
from .queue import Queue
from .local import LocalStack
from .utils import enum, import_attribute, utcformat, utcnow, utcparse, takes_pipeline
from .registry import running_job_registry, finished_job_registry, failed_job_registry
from .serialization import serialize, deserialize

JobStatus = enum(
    'JobStatus',
    QUEUED='queued',
    FINISHED='finished',
    FAILED='failed',
    CANCELED='canceled',
    RUNNING='running',
)


class Job(object):
    def __init__(self, func=None, args=None, kwargs=None, *, fetch_id=None,
                 description=None,
                 timeout=None, id=None, meta=None,
                 reentrant=None):
        self.connection = resolve_connection()

        if fetch_id:
            self.id = fetch_id
            self.refresh()
            return

        self.id = id or str(uuid4())

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
        self.timeout = timeout or JOB_TIMEOUT
        self.reentrant = reentrant
        self.status = None
        self.origin = None
        self.meta = meta or {}

        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self.unfinished_runs = []

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
        self._set_status(JobStatus.QUEUED, pipeline=pipeline)
        self.unfinshed_runs.append((self.started_at, utcnow()))
        pipeline.hset(self.key, 'unfinished_runs', serialize(self.unfinished_runs))
        self.started_at = None
        pipeline.hdel(self.key, 'started_at')

    @takes_pipeline
    def set_running(self, worker, *, pipeline):
        assert self.status == JobStatus.QUEUED
        running_job_registry.add(self, worker, pipeline=pipeline)
        self._set_status(JobStatus.RUNNING, pipeline=pipeline)
        self.started_at = utcnow()
        pipeline.hset(self.key, 'started_at', utcformat(self.started_at))

    @takes_pipeline
    def set_finished(self, *, pipeline):
        assert self.status == JobStatus.RUNNING
        running_job_registry.remove(self, pipeline=pipeline)
        finished_job_registry.add(self, pipeline=pipeline)
        self._set_status(JobStatus.FINISHED, pipeline=pipeline)
        self.ended_at = utcnow()
        pipeline.hset(self.key, 'ended_at', utcformat(self.ended_at))

    @takes_pipeline
    def set_failed(self, error_message, *, pipeline):
        assert self.status == JobStatus.RUNNING
        running_job_registry.remove(self, pipeline=pipeline)
        failed_job_registry.add(self, pipeline=pipeline)
        self._set_status(JobStatus.FAILED, pipeline=pipeline)
        self.error_message = error_message
        pipeline.hset(self.key, 'error_message', self.error_message)
        self.ended_at = utcnow()
        pipeline.hset(self.key, 'ended_at', utcformat(self.ended_at))

    @takes_pipeline
    def handle_abort(self, abort_reason, *, pipeline):
        if self.reentrant:
            self.requeue(pipeline=pipeline)
        else:
            self.set_failed(abort_reason, pipeline=pipeline)

    @takes_pipeline
    def _set_status(self, status, *, pipeline):
        self.status = status
        pipeline.hset(self.key, 'status', self.status)

    @property
    def func(self):
        return import_attribute(self.func_name)

    @classmethod
    def exists(cls, job_id):
        """Returns whether a job hash exists for the given job ID."""
        conn = resolve_connection()
        return conn.exists(cls.key_for(job_id))

    @classmethod
    def key_for(cls, job_id):
        return 'rq:job:' + job_id

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

        for key in ['timeout']:
            setattr(self, key, int(obj[key]) if key in obj else None)

        self.reentrant = bool(obj.get('reentrant'))
        self.meta = deserialize(obj['meta']) if obj.get('meta') else {}
        self.unfinished_runs = deserialize(obj['unfinished_runs']) if obj.get('unfinished_runs') else []

    @takes_pipeline
    def _save(self, *, pipeline=None):
        """Persists the current job instance to its corresponding Redis key."""
        obj = {}
        obj['func_name'] = self.func_name
        obj['args'] = serialize(self._args)
        obj['kwargs'] = serialize(self._kwargs)

        for key in ['status', 'description', 'origin', 'error_message', 'timeout']:
            if getattr(self, key) is not None:
                obj[key] = getattr(self, key)

        if self.reentrant is not None:
            obj['reentrant'] = '' if self.reentrant else '1'

        for key in ['enqueue_at', 'started_at', 'ended_at']:
            if getattr(self, key) is not None:
                obj[key] = utcformat(getattr(self, key))

        if self.unfinished_runs:
            obj['unfinished_runs'] = serialize(self.unfinished_runs)
        if self.meta:
            obj['meta'] = serialize(self.meta)

        pipeline.hmset(self.key, obj)

    def save_meta(self):
        meta = serialize(self.meta)
        self.connection.hset(self.key, 'meta', meta)

    @takes_pipeline
    def cancel(self, *, pipeline):
        # TODO: check which state we are in and react accordingly
        # fail if the job is currently running
        from .queue import Queue
        if self.origin:
            q = Queue(name=self.origin)
            q.remove(self, pipeline=pipeline)

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
