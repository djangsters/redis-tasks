from .connections import resolve_connection
from .exceptions import NoSuchWorkerError
from .task import Task
from .utils import current_timestamp, takes_pipeline, decode_list
from .defaults import REGISTRIES_TTL, WORKER_HEARTBEAT_TIMEOUT


class ExpiringRegistry:
    key_template = 'rq:registry:{0}'

    def __init__(self, name='default'):
        self.name = name
        self.key = self.key_template.format(name)
        self.connection = resolve_connection()

    @takes_pipeline
    def add(self, task, *, pipeline):
        pipeline.zadd(self.key, current_timestamp(), task.id)

    def get_task_ids(self):
        return decode_list(self.connection.zrange(self.key, 0, -1))

    @takes_pipeline
    def expire(self, *, pipeline):
        """Remove expired tasks from registry."""
        cutoff_time = current_timestamp() - REGISTRIES_TTL
        expired_task_ids = decode_list(self.connection.zrangebyscore(
            self.zkey, 0, cutoff_time))
        Task.delete_many(expired_task_ids, pipeline=pipeline)
        self.connection.zremrangebyscore(self.zkey, 0, cutoff_time)


finished_task_registry = ExpiringRegistry('finished')
failed_task_registry = ExpiringRegistry('failed')


def expire_registries():
    finished_task_registry.expire()
    failed_task_registry.expire()


class RunningTaskRegistry:
    def __init__(self):
        self.key = self.key_template.format('running')
        self.connection = resolve_connection()

    @takes_pipeline
    def add(self, task, worker, *, pipeline):
        pipeline.hset(self.key, task.id, worker.id)

    @takes_pipeline
    def remove(self, task, *, pipeline):
        pipeline.hdel(self.key, task.id)

    def count(self, task):
        return self.connection.hlen(self.key)

    def __some_getall(self, task):
        return self.connection.hgetall(self.key)


running_task_registry = RunningTaskRegistry()


class WorkerRegistry:
    key = 'rq:workers'

    def __init__(self, name='default'):
        self.connection = resolve_connection()

    @takes_pipeline
    def add(self, worker, *, pipeline):
        pipeline.zadd(self.key, worker.id, current_timestamp())

    def heartbeat(self, worker):
        score = self.connection.zscore(self.key, worker.id)
        if not score or score <= self._get_oldest_valid():
            raise NoSuchWorkerError()
        self.connection.zadd(self.key, worker.id)

    @takes_pipeline
    def remove(self, worker, *, pipeline):
        pipeline.zrem(self.key, worker.id)

    def get_alive_ids(self):
        oldest_valid = self._get_oldest_valid()
        return decode_list(self.connection.zrangebyscore(
            self.key, oldest_valid, '+inf'))

    def get_dead_ids(self):
        oldest_valid = self._get_oldest_valid()
        return decode_list(self.connection.zrangebyscore(
            self.key, '-inf', oldest_valid))

    def _get_oldest_valid_heartbeat(self):
        return current_timestamp() - WORKER_HEARTBEAT_TIMEOUT


worker_registry = WorkerRegistry()
