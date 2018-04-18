from .exceptions import (DequeueTimeout, NoSuchTaskError, DeserializationError)
from .task import Task
from .utils import utcnow, atomic_pipeline, decode_list
from .conf import connection, RedisKey
from .registries import queue_registry


class Queue(object):
    redis_queues_keys = 'rq:queues'

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
    def remove(self, task_or_id, *, pipeline):
        """Removes Task from queue, accepts either a Task instance or ID."""
        task_id = task_or_id.id if isinstance(task_or_id, Task) else task_or_id
        pipeline.lrem(self.key, 1, task_id)

    @atomic_pipeline
    def push_task(self, task, *, pipeline, at_front=False):
        """Pushes a task id on the queue

        `at_front` inserts the task at the front instead of the back of the queue"""
        queue_registry.add(self)
        if at_front:
            pipeline.lpush(self.key, task.id)
        else:
            pipeline.rpush(self.key, task.id)

    @atomic_pipeline
    def enqueue_call(self, *args, pipeline, **kwargs):
        """Creates a task to represent the delayed function call and enqueues it."""
        task = Task(*args, **kwargs)
        task.enqeue(self, pipeline=pipeline)
        return task

    @classmethod
    def lpop(cls, queue_keys, timeout):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key, whereas BLPOP
        accepts multiple.  So if we want the non-blocking LPOP, we need to
        iterate over all queues, do individual LPOPs, and return the result.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. Please pick a timeout value > 0')
            result = connection.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, task_id = result
            return queue_key, task_id
        else:  # non-blocking variant
            for queue_key in queue_keys:
                blob = connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def dequeue(self, timeout):
        return self.dequeue_any(self, timeout)

    @classmethod
    def dequeue_any(cls, queues, timeout):
        """Class method returning the Task instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        queue_map = {q.key: q for q in queues}
        result = cls.lpop(queue_map.keys(), timeout)
        if result is None:
            return None, None
        queue_key, task_id = decode_list(result)
        queue = queue_map[queue_key]
        try:
            task = Task.fetch(task_id)
        except (NoSuchTaskError, DeserializationError) as e:
            # Attach queue information to the exception for improved error reporting
            e.task_id = task_id
            e.queue = queue
            raise e
        return task, queue

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.name)
