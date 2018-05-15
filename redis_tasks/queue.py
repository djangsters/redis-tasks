from .conf import RedisKey, connection
from .exceptions import TaskDoesNotExist
from .registries import queue_registry
from .task import Task
from .utils import atomic_pipeline, decode_list


class Queue(object):
    def __init__(self, name='default'):
        self.name = name
        self.key = RedisKey('queue:' + name)
        # We use a separate key for the workers to wait on, as we need to do a
        # multi-key blocking rpop on it, and redis does not have a variant of
        # that operation that is not at risk of losing tasks.
        self.unblock_key = RedisKey('unblock_queue:' + name)

    @classmethod
    def all(cls):
        """Returns an iterable of all Queues."""
        return [cls(name) for name in sorted(queue_registry.get_names())]

    def count(self):
        return connection.llen(self.key)

    def _empty_transaction(self, pipeline):
        task_ids = decode_list(pipeline.lrange(self.key, 0, -1))
        pipeline.multi()
        Task.delete_many(task_ids, pipeline=pipeline)
        pipeline.delete(self.key)
        pipeline.delete(self.unblock_key)

    def empty(self):
        """Removes all messages on the queue."""
        connection.transaction(self._empty_transaction, self.key)

    def delete(self):
        def transaction(pipeline):
            self._empty_transaction(pipeline)
            queue_registry.remove(self, pipeline=pipeline)
        connection.transaction(transaction, self.key)

    def get_task_ids(self, offset=0, length=-1):
        end = -1 - offset
        start = -(length + 1) if length < 0 else end - length
        return [task_id.decode() for task_id in
                reversed(connection.lrange(self.key, start, end))]

    def get_tasks(self, offset=0, length=-1):
        return [Task.fetch(x) for x in self.get_task_ids(offset, length)]

    @atomic_pipeline
    def enqueue_call(self, *args, pipeline, **kwargs):
        """Creates a task to represent the delayed function call and enqueues it."""
        task = Task(*args, **kwargs)
        task.enqueue(self, pipeline=pipeline)
        return task

    @atomic_pipeline
    def push(self, task, *, pipeline, at_front=False):
        """Pushes a task on the queue

        `at_front` inserts the task at the front instead of the back of the queue"""
        queue_registry.add(self, pipeline=pipeline)
        if at_front:
            pipeline.rpush(self.key, task.id)
        else:
            pipeline.lpush(self.key, task.id)
        pipeline.lpush(self.unblock_key, task.id)

    def remove_and_delete(self, task):
        def transaction(pipeline):
            task_ids = decode_list(pipeline.lrange(self.key, 0, -1))
            if task.id not in task_ids:
                raise TaskDoesNotExist()

            pipeline.multi()
            pipeline.lrem(self.key, 0, task.id)
            Task.delete_many([task.id], pipeline=pipeline)

        connection.transaction(transaction, self.key)

    def dequeue(self, worker):
        """Dequeue a task and set it as the current task for `worker`"""
        # Use lua script to atomically clear unblock_key if queue is empty
        lua = connection.register_script("""
            local queue, unblocker, worker_task_list = unpack(KEYS)
            local result = redis.call("RPOPLPUSH", queue, worker_task_list)
            if result == false then
                redis.call("DEL", unblocker)
            end
            return result
        """)
        result = lua(keys=[self.key, self.unblock_key, worker.task_key])
        if result is None:
            return None
        else:
            task_id = result.decode()
            worker.current_task_id = task_id
            return Task.fetch(task_id)

    @classmethod
    def await_multi(cls, queues, timeout):
        """Blocks until one of the passed queues contains a tasks.

        Return the queue that contained a task or None if `timeout` was reached."""
        queue_map = {str(q.unblock_key): q for q in queues}
        result = connection.brpop(queue_map.keys(), timeout)
        if result is None:
            return None
        return queue_map[result[0].decode()]

    def __eq__(self, other):
        if type(other) != type(self):
            return NotImplemented
        else:
            return self.name == other.name

    def __hash__(self):
        return hash((self.__class__.__name__, self.name))

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.name)
