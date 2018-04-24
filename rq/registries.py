import rq
from .exceptions import NoSuchWorkerError
from .utils import current_timestamp, atomic_pipeline, decode_list
from .conf import connection, settings, RedisKey


class ExpiringRegistry:
    def __init__(self, name):
        self.key = RedisKey(name + '_tasks')

    @atomic_pipeline
    def add(self, task, *, pipeline):
        pipeline.zadd(self.key, current_timestamp(), task.id)

    def get_task_ids(self):
        return decode_list(connection.zrange(self.key, 0, -1))

    def expire(self):
        """Remove expired tasks from registry."""
        cutoff_time = current_timestamp() - settings.EXPIRING_REGISTRIES_TTL
        expired_task_ids = decode_list(connection.zrangebyscore(
            self.key, 0, cutoff_time))
        if expired_task_ids:
            connection.zremrangebyscore(self.key, 0, cutoff_time)
            rq.Task.delete_many(expired_task_ids)


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
        pipeline.zadd(self.key, current_timestamp(), worker.id)

    def heartbeat(self, worker):
        score = connection.zscore(self.key, worker.id)
        if not score or score <= self._get_oldest_valid_heartbeat():
            raise NoSuchWorkerError()
        connection.zadd(self.key, current_timestamp(), worker.id)

    @atomic_pipeline
    def remove(self, worker, *, pipeline):
        pipeline.zrem(self.key, worker.id)

    def get_alive_ids(self):
        # TODO: why would we want this? We should probably always return all
        # workers
        oldest_valid = self._get_oldest_valid_heartbeat()
        return decode_list(connection.zrangebyscore(
            self.key, oldest_valid, '+inf'))

    def get_running_task_ids(self):
        task_key_prefix = RedisKey('worker_task:')
        lua = connection.register_script("""
            local workers_key, task_key_prefix = unpack(KEYS)
            local worker_ids = redis.call("ZRANGE", workers_key, 0, -1)
            local task_ids = {}
            for _, worker_id in ipairs(worker_ids) do
                local task_key = task_key_prefix .. worker_id
                local task_id = redis.call("LINDEX", task_key, 0)
                if task_id ~= false then
                    table.insert(task_ids, task_id)
                end
            end
            return task_ids
        """)
        return decode_list(lua(keys=[self.key, task_key_prefix]))

    def get_dead_ids(self):
        oldest_valid = self._get_oldest_valid_heartbeat()
        return decode_list(connection.zrangebyscore(
            self.key, '-inf', oldest_valid))

    def _get_oldest_valid_heartbeat(self):
        return current_timestamp() - settings.WORKER_HEARTBEAT_TIMEOUT


worker_registry = WorkerRegistry()


class QueueRegistry:
    def __init__(self):
        self.key = RedisKey('queues')

    def get_names(self):
        return list(sorted(decode_list(
            connection.smembers(self.key))))

    @atomic_pipeline
    def add(self, queue, *, pipeline):
        pipeline.sadd(self.key, queue.name)

    @atomic_pipeline
    def remove(self, queue, *, pipeline):
        pipeline.srem(self.key, queue.name)


queue_registry = QueueRegistry()
