from .conf import RedisKey, connection
from .exceptions import InvalidOperation
from .registries import queue_registry
from .task import Task
from .utils import atomic_pipeline, decode_list


class Queue(object):
    def __init__(self, name='default'):
        self.name = name
        self.key = RedisKey('queue:' + name)
        self.backup_key = RedisKey('queue:' + name + ':backup')

    @classmethod
    def all(cls):
        """Returns an iterable of all Queues."""
        return [cls(name) for name in queue_registry.get_names()]

    def count(self):
        """Returns a count of all messages in the queue."""
        return connection.llen(self.key)

    @atomic_pipeline
    def empty(self, *, force=False, pipeline):
        """Removes all messages on the queue."""
        def transaction(pipeline):
            backup_task_ids = pipeline.smembers(self.backup_key)
            if not force:
                queue_length = pipeline.llen(self.key)
                if len(backup_task_ids) != queue_length:
                    raise InvalidOperation("Queue has tasks in limbo")
            pipeline.multi()
            Task.delete_many(backup_task_ids, pipeline=pipeline)
            pipeline.delete(self.key)
            pipeline.delete(self.backup_key)

        connection.transaction(transaction, self.key, self.backup_key)

    @atomic_pipeline
    def delete(self, *, pipeline):
        self.empty(pipeline=pipeline)
        queue_registry.remove(self, pipeline=pipeline)

    def get_task_ids(self):
        """Return the task IDs in the queue."""
        return [task_id.decode() for task_id in
                connection.lrange(self.key, 0, -1)]

    def get_tasks(self):
        """Returns the tasks in the queue."""
        return [Task.fetch(x) for x in self.get_task_ids()]

    @atomic_pipeline
    def enqueue_call(self, *args, pipeline, **kwargs):
        """Creates a task to represent the delayed function call and enqueues it."""
        task = Task(*args, **kwargs)
        task.enqeue(self, pipeline=pipeline)
        return task

    @atomic_pipeline
    def push_task_id(self, task_id, *, pipeline, at_front=False):
        """Pushes a task on the queue

        `at_front` inserts the task at the front instead of the back of the queue"""
        queue_registry.add(self)
        if at_front:
            pipeline.rpush(self.key, task_id)
        else:
            pipeline.lpush(self.key, task_id)
        pipeline.sadd(self.backup_key, task_id)

    @atomic_pipeline
    def remove(self, task, *, pipeline):
        pipeline.lrem(self.key, 0, task.id)

    @atomic_pipeline
    def remove_backup(self, task, *, pipeline):
        pipeline.srem(self.backup_key, task.id)

    def get_limbo_task_ids(self):
        # sdiffl might be expensive, so check lengths first. We can do this
        # because only the queue key can ever loose tasks.
        with connection.pipeline() as pipeline:
            pipeline.llen(self.key)
            pipeline.scard(self.backup_key)
            queue_length, backup_length = pipeline.execute()
            if queue_length == backup_length:
                return []
        return decode_list(connection.sdiffl(self.backup_key, self.key))

    @classmethod
    def dequeue_mutli(cls, queues, timeout):
        """Returns the task at the front of the given queues.

        If multiple queues hold tasks, the queue appearing earlier in the list
        is used. If `timeout` is None, this function returns immedialtey,
        otherwise it blocks for `timeout` seconds.

        Retuns (queue, task) if a task was dequeued and (None, None) otherwise."""
        queue_map = {q.key: q for q in queues}
        if timeout is None:
            result = connection.rpop_multiple(queue_map.keys())
        else:
            result = connection.brpop(queue_map.keys(), timeout)
        if result is None:
            return None, None
        queue_key, task_id = decode_list(result)
        queue = queue_map[queue_key]
        task = Task.fetch(task_id)
        return task, queue

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.name)
