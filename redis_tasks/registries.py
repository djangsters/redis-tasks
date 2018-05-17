import redis_tasks

from .conf import RedisKey, connection, settings
from .exceptions import WorkerDoesNotExist
from .utils import atomic_pipeline, decode_list


class ExpiringRegistry:
    def __init__(self, name):
        self.key = RedisKey(name + '_tasks')

    @atomic_pipeline
    def add(self, task, *, pipeline):
        timestamp = connection.ftime()
        pipeline.zadd(self.key, {task.id: timestamp})

    def get_task_ids(self, offset=0, length=-1):
        end = offset + length if length >= 0 else length
        return decode_list(connection.zrange(self.key, offset, end))

    def get_tasks(self, offset=0, length=-1):
        return [redis_tasks.task.Task.fetch(x) for x in self.get_task_ids(offset, length)]

    def empty(self):  # TODO: test
        def transaction(pipeline):
            task_ids = decode_list(pipeline.zrange(self.key, 0, -1))
            pipeline.multi()
            pipeline.delete(self.key)
            redis_tasks.task.Task.delete_many(task_ids, pipeline=pipeline)
        connection.transaction(transaction, self.key)

    def count(self):
        return connection.zcard(self.key)

    def expire(self):
        """Remove expired tasks from registry."""
        timestamp = connection.ftime()
        cutoff_time = timestamp - settings.EXPIRING_REGISTRIES_TTL

        def transaction(pipeline):
            expired_task_ids = decode_list(pipeline.zrangebyscore(
                self.key, 0, cutoff_time))
            if expired_task_ids:
                pipeline.multi()
                pipeline.zremrangebyscore(self.key, 0, cutoff_time)
                redis_tasks.task.Task.delete_many(expired_task_ids, pipeline=pipeline)
        connection.transaction(transaction, self.key)


finished_task_registry = ExpiringRegistry('finished')
failed_task_registry = ExpiringRegistry('failed')


def registry_maintenance():
    finished_task_registry.expire()
    failed_task_registry.expire()
    worker_registry.handle_died_workers()


class WorkerRegistry:
    def __init__(self):
        self.key = RedisKey('workers')

    @atomic_pipeline
    def add(self, worker, *, pipeline):
        timestamp = connection.ftime()
        pipeline.zadd(self.key, {worker.id: timestamp})

    def heartbeat(self, worker):
        timestamp = connection.ftime()
        updated = connection.zadd(self.key, {worker.id: timestamp}, xx=True, ch=True)
        if not updated:
            raise WorkerDoesNotExist()

    @atomic_pipeline
    def remove(self, worker, *, pipeline):
        pipeline.zrem(self.key, worker.id)

    def get_worker_ids(self):
        return decode_list(connection.zrangebyscore(
            self.key, '-inf', '+inf'))

    def get_running_tasks(self):
        """Returns a worker_id -> task_id dict"""
        task_key_prefix = RedisKey('worker_task:')
        lua = connection.register_script("""
            local workers_key, task_key_prefix = unpack(KEYS)
            local worker_ids = redis.call("ZRANGE", workers_key, 0, -1)
            local out = {}
            for _, worker_id in ipairs(worker_ids) do
                local task_key = task_key_prefix .. worker_id
                local task_id = redis.call("LINDEX", task_key, 0)
                if task_id ~= false then
                    table.insert(out, worker_id)
                    table.insert(out, task_id)
                end
            end
            return out
        """)
        it = iter(decode_list(lua(keys=[self.key, task_key_prefix])))
        return dict(zip(it, it))

    def handle_died_workers(self):  # TODO: Test
        from redis_tasks.worker import Worker
        died_worker_ids = self.get_dead_ids()
        for worker_id in died_worker_ids:
            worker = Worker.fetch(worker_id)
            worker.died()

    def get_dead_ids(self):
        oldest_valid = connection.ftime() - settings.WORKER_HEARTBEAT_TIMEOUT
        return decode_list(connection.zrangebyscore(
            self.key, '-inf', oldest_valid))


worker_registry = WorkerRegistry()


class QueueRegistry:
    def __init__(self):
        self.key = RedisKey('queues')

    def get_names(self):
        return decode_list(connection.smembers(self.key))

    @atomic_pipeline
    def add(self, queue, *, pipeline):
        pipeline.sadd(self.key, queue.name)

    @atomic_pipeline
    def remove(self, queue, *, pipeline):
        pipeline.srem(self.key, queue.name)


queue_registry = QueueRegistry()
