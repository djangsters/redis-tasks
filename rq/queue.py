from .connections import resolve_connection
from .exceptions import (DequeueTimeout, NoSuchJobError, DeserializationError)
from .job import Job, JobStatus
from .utils import utcnow, parse_timeout, takes_pipeline, decode_list


class Queue(object):
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues'

    def __init__(self, name='default'):
        self.connection = resolve_connection()
        self.name = name
        self._key = self.redis_queue_namespace_prefix + name

    @classmethod
    def all(cls):
        """Returns an iterable of all Queues."""
        connection = resolve_connection()

        return [cls.from_queue_key(rq_key.decode())
                for rq_key in connection.smembers(cls.redis_queues_keys)
                if rq_key]

    @classmethod
    def from_queue_key(cls, queue_key):
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: {0}'.format(queue_key))
        name = queue_key[len(prefix):]
        return cls(name)

    def count(self):
        """Returns a count of all messages in the queue."""
        return self.connection.llen(self.key)

    def empty(self):
        """Removes all messages on the queue."""
        script = b"""
            local prefix = "rq:job:"
            local q = KEYS[1]
            local count = 0
            while true do
                local job_id = redis.call("lpop", q)
                if job_id == false then
                    break
                end

                -- Delete the job data
                redis.call("del", prefix..job_id)
                count = count + 1
            end
            return count
        """
        script = self.connection.register_script(script)
        return script(keys=[self.key])

    def get_job_ids(self):
        """Return the job IDs in the queue."""
        return [job_id.decode() for job_id in
                self.connection.lrange(self.key, 0, -1)]

    def get_jobs(self):
        """Returns the jobs in the queue."""
        job_ids = self.get_job_ids()

        def fetch_job(job_id):
            try:
                return Job.fetch(job_id)
            except NoSuchJobError:
                return None
        return list(filter(None, map(fetch_job, job_ids)))

    @takes_pipeline
    def remove(self, job_or_id, *, pipeline):
        """Removes Job from queue, accepts either a Job instance or ID."""
        job_id = job_or_id.id if isinstance(job_or_id, Job) else job_or_id
        pipeline.lrem(self.key, 1, job_id)

    @takes_pipeline
    def push_job(self, job, *, pipeline, at_front=False):
        """Pushes a job id on the queue

        `at_front` inserts the job at the front instead of the back of the queue"""
        # Add Queue key set
        pipeline.sadd(self.redis_queues_keys, self.key)
        if at_front:
            pipeline.lpush(self.key, job.id)
        else:
            pipeline.rpush(self.key, job.id)

    @takes_pipeline
    def enqueue_call(self, *args, pipeline, **kwargs):
        """Creates a job to represent the delayed function call and enqueues it."""
        job = Job(*args, **kwargs)
        job.enqeue(self, pipeline=pipeline)
        return job

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
        connection = resolve_connection()
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. Please pick a timeout value > 0')
            result = connection.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
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
        """Class method returning the Job instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        queue_keys = [q.key for q in queues]
        result = cls.lpop(queue_keys, timeout)
        if result is None:
            return None, None
        queue_key, job_id = decode_list(result)
        queue = cls.from_queue_key(queue_key)
        try:
            job = Job.fetch(job_id)
        except (NoSuchJobError, DeserializationError) as e:
            # Attach queue information to the exception for improved error reporting
            e.job_id = job_id
            e.queue = queue
            raise e
        return job, queue

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.name)
