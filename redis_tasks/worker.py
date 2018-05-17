import logging
from operator import attrgetter

from .conf import RedisKey, connection, settings
from .exceptions import WorkerDoesNotExist
from .queue import Queue
from .registries import worker_registry
from .task import Task
from .utils import atomic_pipeline, enum, utcformat, utcnow, utcparse

logger = logging.getLogger(__name__)

WorkerState = enum(
    'WorkerState',
    IDLE='idle',
    BUSY='busy',
    DEAD='dead',
)


class Worker:
    @classmethod
    def all(cls):
        return list(sorted((cls.fetch(id) for id in worker_registry.get_worker_ids()),
                           key=attrgetter('description')))

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

        self.description = description or f"Worker {id}"
        self.state = None
        self.queues = queues
        if not queues:
            raise ValueError("Worker needs queues")
        self.current_task_id = None
        self.started_at = None
        self.shutdown_at = None

    def refresh(self):
        with connection.pipeline() as pipeline:
            pipeline.hgetall(self.key)
            pipeline.lrange(self.task_key, 0, -1)
            obj, task_id = pipeline.execute()

        if not obj:
            raise WorkerDoesNotExist(self.id)
        assert len(task_id) < 2
        self.current_task_id = task_id[0].decode() if task_id else None

        obj = {k.decode(): v.decode() for k, v in obj.items()}
        self.state = obj['state']
        self.description = obj['description']
        self.queues = [Queue(q) for q in obj['queues'].split(',')]
        for k in ['started_at', 'shutdown_at']:
            setattr(self, k, utcparse(obj[k]) if obj.get(k) else None)

    @atomic_pipeline
    def _save(self, fields=None, *, pipeline):
        string_fields = ['description', 'state']
        date_fields = ['started_at', 'shutdown_at']
        special_fields = ['queues', 'current_task_id']
        if fields is None:
            fields = string_fields + date_fields + special_fields

        deletes = []
        store = {}
        for field in fields:
            value = getattr(self, field)
            if field == 'queues':
                store['queues'] = ','.join(q.name for q in self.queues)
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
        if store:
            pipeline.hmset(self.key, store)

    def heartbeat(self):
        """Send a heartbeat.

        Raises WorkerDoesNotExist if the registry considers this worker as dead"""
        worker_registry.heartbeat(self)

    @atomic_pipeline
    def startup(self, *, pipeline):
        logger.info(f'Worker {self.description} [{self.id}] started')
        self.state = WorkerState.IDLE
        self.started_at = utcnow()
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
        assert self.current_task_id == task.id
        self.state = WorkerState.IDLE
        self.current_task_id = None
        self._save(['state', 'current_task_id'], pipeline=pipeline)
        task.handle_outcome(outcome, pipeline=pipeline)

    @atomic_pipeline
    def shutdown(self, *, pipeline):
        logger.info(f'Worker {self.description} [{self.id}] shut down')
        assert self.state == WorkerState.IDLE
        worker_registry.remove(self, pipeline=pipeline)
        self.state = WorkerState.DEAD
        self.shutdown_at = utcnow()
        self._save(['state', 'shutdown_at'], pipeline=pipeline)
        pipeline.expire(self.key, settings.DEAD_WORKER_TTL)

    @atomic_pipeline
    def died(self, *, pipeline):
        logger.warning(f'Worker {self.description} [{self.id}] died')
        worker_registry.remove(self, pipeline=pipeline)
        self.shutdown_at = utcnow()
        if self.current_task_id:
            task = Task.fetch(self.current_task_id)
            task.handle_worker_death(pipeline=pipeline)
            self.current_task_id = None
        self.state = WorkerState.DEAD
        self._save(['state', 'current_task_id', 'shutdown_at'], pipeline=pipeline)
        pipeline.expire(self.key, settings.DEAD_WORKER_TTL)

    def fetch_current_task(self):
        """Returns the currently executing task."""
        if self.current_task_id:
            return Task.fetch(self.current_task_id)
