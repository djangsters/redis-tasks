from .exceptions import NoSuchWorkerError
from .task import Task
from .utils import current_timestamp, atomic_pipeline, decode_list
from .conf import connection, settings, RedisKey


class RunningTaskRegistry:
    def __init__(self):
        self.key = RedisKey('running_tasks')

    @atomic_pipeline
    def add(self, task, worker, *, pipeline):
        pipeline.hset(self.key, task.id, worker.id)

    @atomic_pipeline
    def remove(self, task, *, pipeline):
        pipeline.hdel(self.key, task.id)

    def count(self, task):
        return connection.hlen(self.key)

    def __some_getall(self, task):
        return connection.hgetall(self.key)


running_task_registry = RunningTaskRegistry()


class ExpiringRegistry:
    def __init__(self, name):
        self.key = RedisKey(name + '_tasks')

    @atomic_pipeline
    def add(self, task, *, pipeline):
        pipeline.zadd(self.key, current_timestamp(), task.id)

    def get_task_ids(self):
        return decode_list(connection.zrange(self.key, 0, -1))

    @atomic_pipeline
    def expire(self, *, pipeline):
        """Remove expired tasks from registry."""
        cutoff_time = current_timestamp() - settings.EXPIRING_REGISTRIES_TTL
        expired_task_ids = decode_list(connection.zrangebyscore(
            self.key, 0, cutoff_time))
        Task.delete_many(expired_task_ids, pipeline=pipeline)
        connection.zremrangebyscore(self.key, 0, cutoff_time)


finished_task_registry = ExpiringRegistry('finished')
failed_task_registry = ExpiringRegistry('failed')


def expire_registries():
    finished_task_registry.expire()
    failed_task_registry.expire()


class WorkerRegistry:
    def __init__(self):
        self.key = RedisKey('workers')

    @atomic_pipeline
    def add(self, worker, *, pipeline):
        pipeline.zadd(self.key, worker.id, current_timestamp())

    def heartbeat(self, worker):
        score = connection.zscore(self.key, worker.id)
        if not score or score <= self._get_oldest_valid():
            raise NoSuchWorkerError()
        connection.zadd(self.key, worker.id)

    @atomic_pipeline
    def remove(self, worker, *, pipeline):
        pipeline.zrem(self.key, worker.id)

    def get_alive_ids(self):
        oldest_valid = self._get_oldest_valid()
        return decode_list(connection.zrangebyscore(
            self.key, oldest_valid, '+inf'))

    def get_dead_ids(self):
        oldest_valid = self._get_oldest_valid()
        return decode_list(connection.zrangebyscore(
            self.key, '-inf', oldest_valid))

    def _get_oldest_valid_heartbeat(self):
        return current_timestamp() - settings.WORKER_HEARTBEAT_TIMEOUT


worker_registry = WorkerRegistry()


class QueueRegistry:
    def __init__(self):
        self.key = RedisKey('queues')

    def get_names(self):
        return list(sorted(connection.smembers(self.key)))

    @atomic_pipeline
    def add(self, queue, *, pipeline):
        pipeline.sadd(self.key, queue.name)

    @atomic_pipeline
    def remove(self, queue, *, pipeline):
        pipeline.srem(self.key, queue.name)


queue_registry = QueueRegistry()
