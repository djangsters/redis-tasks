from .exceptions import (NoSuchTaskError, DeserializationError)
from .task import Task
from .utils import utcnow, atomic_pipeline, decode_list
from .conf import connection, RedisKey
from .registries import queue_registry


class Queue(object):
    def __init__(self, name='default'):
        self.name = name

    @classmethod
    def all(cls):
        """Returns an iterable of all Queues."""
        return [cls(name) for name in queue_registry.get_names()]

    @property
    def key(self):
        return self.key_for(self.name)

    @classmethod
    def key_for(cls, queue_name):
        return RedisKey('queue:' + queue_name)

    def count(self):
        """Returns a count of all messages in the queue."""
        return connection.llen(self.key)

    @atomic_pipeline
    def empty(self, *, pipeline):
        """Removes all messages on the queue."""
        script = b"""
            local prefix = "rq:task:"
            local q = KEYS[1]
            while true do
                local task_id = redis.call("lpop", q)
                if task_id == false then
                    break
                end

                -- Delete the task data
                redis.call("del", prefix..task_id)
            end
        """
        script = connection.register_script(script)
        script(keys=[self.key], client=pipeline)

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
        task_ids = self.get_task_ids()

        def fetch_task(task_id):
            try:
                return Task.fetch(task_id)
            except NoSuchTaskError:
                return None
        return list(filter(None, map(fetch_task, task_ids)))

    @atomic_pipeline
    def push_task(self, task, *, pipeline, at_front=False):
        """Pushes a task on the queue

        `at_front` inserts the task at the front instead of the back of the queue"""
        queue_registry.add(self)
        if at_front:
            pipeline.rpush(self.key, task.id)
        else:
            pipeline.lpush(self.key, task.id)

    @atomic_pipeline
    def enqueue_call(self, *args, pipeline, **kwargs):
        """Creates a task to represent the delayed function call and enqueues it."""
        task = Task(*args, **kwargs)
        task.enqeue(self, pipeline=pipeline)
        return task

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
