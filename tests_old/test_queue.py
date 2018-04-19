# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from tests import RQTestCase
from tests.fixtures import (div_by_zero, echo, Number, say_hello,
                            some_calculation)

from rq import get_failed_queue, Queue
from rq.exceptions import InvalidTaskOperationError
from rq.task import Task, TaskStatus
from rq.worker import Worker


class CustomTask(Task):
    pass


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue')
        self.assertEqual(q.name, 'my-queue')
        self.assertEqual(str(q), '<Queue my-queue>')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = Queue()
        self.assertEqual(q.name, 'default')

    def test_equality(self):
        """Mathematical equality of queues."""
        q1 = Queue('foo')
        q2 = Queue('foo')
        q3 = Queue('bar')

        self.assertEqual(q1, q2)
        self.assertEqual(q2, q1)
        self.assertNotEqual(q1, q3)
        self.assertNotEqual(q2, q3)
        self.assertGreater(q1, q3)
        self.assertRaises(TypeError, lambda: q1 == 'some string')
        self.assertRaises(TypeError, lambda: q1 < 'some string')

    def test_empty_queue(self):
        """Emptying queues."""
        q = Queue('example')

        self.testconn.rpush('rq:queue:example', 'foo')
        self.testconn.rpush('rq:queue:example', 'bar')
        self.assertEqual(q.count(), 2)

        q.empty()

        self.assertEqual(q.count(), 0)
        self.assertIsNone(self.testconn.lpop('rq:queue:example'))

    def test_empty_removes_tasks(self):
        """Emptying a queue deletes the associated task objects"""
        q = Queue('example')
        task = q.enqueue(say_hello)
        self.assertTrue(Task.exists(task.id))
        q.empty()
        self.assertFalse(Task.exists(task.id))

    def test_remove(self):
        """Ensure queue.remove properly removes Task from queue."""
        q = Queue('example')
        task = q.enqueue(say_hello)
        self.assertIn(task.id, q.task_ids)
        q.remove(task)
        self.assertNotIn(task.id, q.task_ids)

        task = q.enqueue(say_hello)
        self.assertIn(task.id, q.task_ids)
        q.remove(task.id)
        self.assertNotIn(task.id, q.task_ids)

    def test_tasks(self):
        """Getting tasks out of a queue."""
        q = Queue('example')
        self.assertEqual(q.tasks, [])
        task = q.enqueue(say_hello)
        self.assertEqual(q.tasks, [task])

        # Deleting task removes it from queue
        task.delete()
        self.assertEqual(q.task_ids, [])

    def test_enqueue(self):
        """Enqueueing task onto queues."""
        q = Queue()
        self.assertEqual(q.count(), 0)

        # say_hello spec holds which queue this is sent to
        task = q.enqueue(say_hello, 'Nick', foo='bar')
        task_id = task.id
        self.assertEqual(task.origin, q.name)

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEqual(self.testconn.llen(q_key), 1)
        self.assertEqual(
            self.testconn.lrange(q_key, 0, -1)[0].decode('ascii'),
            task_id)

    def test_enqueue_sets_metadata(self):
        """Enqueueing task onto queues modifies meta data."""
        q = Queue()
        task = Task.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'))

        # Preconditions
        self.assertIsNone(task.enqueued_at)

        # Action
        q.enqueue_task(task)

        # Postconditions
        self.assertIsNotNone(task.enqueued_at)

    def test_dequeue(self):
        """Dequeueing tasks from queues."""
        # Set up
        q = Queue()
        result = q.enqueue(say_hello, 'Rick', foo='bar')

        # Dequeue a task (not a task ID) off the queue
        self.assertEqual(q.count, 1)
        task = q.dequeue()
        self.assertEqual(task.id, result.id)
        self.assertEqual(task.func, say_hello)
        self.assertEqual(task.origin, q.name)
        self.assertEqual(task.args[0], 'Rick')
        self.assertEqual(task.kwargs['foo'], 'bar')

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue_deleted_tasks(self):
        """Dequeueing deleted tasks from queues don't blow the stack."""
        q = Queue()
        for _ in range(1, 1000):
            task = q.enqueue(say_hello)
            task.delete()
        q.dequeue()

    def test_dequeue_instance_method(self):
        """Dequeueing instance method tasks from queues."""
        q = Queue()
        n = Number(2)
        q.enqueue(n.div, 4)

        task = q.dequeue()

        # The instance has been pickled and unpickled, so it is now a separate
        # object. Test for equality using each object's __dict__ instead.
        self.assertEqual(task.instance.__dict__, n.__dict__)
        self.assertEqual(task.func.__name__, 'div')
        self.assertEqual(task.args, (4,))

    def test_dequeue_class_method(self):
        """Dequeueing class method tasks from queues."""
        q = Queue()
        q.enqueue(Number.divide, 3, 4)

        task = q.dequeue()

        self.assertEqual(task.instance.__dict__, Number.__dict__)
        self.assertEqual(task.func.__name__, 'divide')
        self.assertEqual(task.args, (3, 4))

    def test_dequeue_ignores_nonexisting_tasks(self):
        """Dequeuing silently ignores non-existing tasks."""

        q = Queue()
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_task_id(uuid)
        q.push_task_id(uuid)
        result = q.enqueue(say_hello, 'Nick', foo='bar')
        q.push_task_id(uuid)

        # Dequeue simply ignores the missing task and returns None
        self.assertEqual(q.count, 4)
        self.assertEqual(q.dequeue().id, result.id)
        self.assertIsNone(q.dequeue())
        self.assertEqual(q.count, 0)

    def test_dequeue_any(self):
        """Fetching work from any given queue."""
        fooq = Queue('foo')
        barq = Queue('bar')

        self.assertEqual(Queue.dequeue_any([fooq, barq], None), None)

        # Enqueue a single item
        barq.enqueue(say_hello)
        task, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(task.func, say_hello)
        self.assertEqual(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(say_hello, 'for Bar')
        fooq.enqueue(say_hello, 'for Foo')

        task, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(queue, fooq)
        self.assertEqual(task.func, say_hello)
        self.assertEqual(task.origin, fooq.name)
        self.assertEqual(
            task.args[0], 'for Foo',
            'Foo should be dequeued first.'
        )

        task, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(queue, barq)
        self.assertEqual(task.func, say_hello)
        self.assertEqual(task.origin, barq.name)
        self.assertEqual(
            task.args[0], 'for Bar',
            'Bar should be dequeued second.'
        )

    def test_dequeue_any_ignores_nonexisting_tasks(self):
        """Dequeuing (from any queue) silently ignores non-existing tasks."""

        q = Queue('low')
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_task_id(uuid)

        # Dequeue simply ignores the missing task and returns None
        self.assertEqual(q.count, 1)
        self.assertEqual(
            Queue.dequeue_any([Queue(), Queue('low')], None),  # noqa
            None
        )
        self.assertEqual(q.count, 0)

    def test_enqueue_sets_status(self):
        """Enqueueing a task sets its status to "queued"."""
        q = Queue()
        task = q.enqueue(say_hello)
        self.assertEqual(task.get_status(), TaskStatus.QUEUED)

    def test_enqueue_meta_arg(self):
        """enqueue() can set the task.meta contents."""
        q = Queue()
        task = q.enqueue(say_hello, meta={'foo': 'bar', 'baz': 42})
        self.assertEqual(task.meta['foo'], 'bar')
        self.assertEqual(task.meta['baz'], 42)

    def test_enqueue_explicit_args(self):
        """enqueue() works for both implicit/explicit args."""
        q = Queue()

        # Implicit args/kwargs mode
        task = q.enqueue(echo, 1, timeout=1, result_ttl=1, bar='baz')
        self.assertEqual(task.timeout, 1)
        self.assertEqual(task.result_ttl, 1)
        self.assertEqual(
            task.perform(),
            ((1,), {'bar': 'baz'})
        )

        # Explicit kwargs mode
        kwargs = {
            'timeout': 1,
            'result_ttl': 1,
        }
        task = q.enqueue(echo, timeout=2, result_ttl=2, args=[1], kwargs=kwargs)
        self.assertEqual(task.timeout, 2)
        self.assertEqual(task.result_ttl, 2)
        self.assertEqual(
            task.perform(),
            ((1,), {'timeout': 1, 'result_ttl': 1})
        )

    def test_all_queues(self):
        """All queues"""
        q1 = Queue('first-queue')
        q2 = Queue('second-queue')
        q3 = Queue('third-queue')

        # Ensure a queue is added only once a task is enqueued
        self.assertEqual(len(Queue.all()), 0)
        q1.enqueue(say_hello)
        self.assertEqual(len(Queue.all()), 1)

        # Ensure this holds true for multiple queues
        q2.enqueue(say_hello)
        q3.enqueue(say_hello)
        names = [q.name for q in Queue.all()]
        self.assertEqual(len(Queue.all()), 3)

        # Verify names
        self.assertTrue('first-queue' in names)
        self.assertTrue('second-queue' in names)
        self.assertTrue('third-queue' in names)

        # Now empty two queues
        w = Worker([q2, q3])
        w.work(burst=True)

        # Queue.all() should still report the empty queues
        self.assertEqual(len(Queue.all()), 3)

    def test_all_custom_task(self):
        class CustomTask(Task):
            pass

        q = Queue('all-queue')
        q.enqueue(say_hello)
        queues = Queue.all(task_class=CustomTask)
        self.assertEqual(len(queues), 1)
        self.assertIs(queues[0].task_class, CustomTask)

    def test_from_queue_key(self):
        """Ensure being able to get a Queue instance manually from Redis"""
        q = Queue()
        key = Queue.redis_queue_namespace_prefix + 'default'
        reverse_q = Queue.from_queue_key(key)
        self.assertEqual(q, reverse_q)

    def test_from_queue_key_error(self):
        """Ensure that an exception is raised if the queue prefix is wrong"""
        key = 'some:weird:prefix:' + 'default'
        self.assertRaises(ValueError, Queue.from_queue_key, key)

    def test_fetch_task_successful(self):
        """Fetch a task from a queue."""
        q = Queue('example')
        task_orig = q.enqueue(say_hello)
        task_fetch = q.fetch_task(task_orig.id)
        self.assertIsNotNone(task_fetch)
        self.assertEqual(task_orig.id, task_fetch.id)
        self.assertEqual(task_orig.description, task_fetch.description)

    def test_fetch_task_missing(self):
        """Fetch a task from a queue which doesn't exist."""
        q = Queue('example')
        task = q.fetch_task('123')
        self.assertIsNone(task)

    def test_fetch_task_different_queue(self):
        """Fetch a task from a queue which is in a different queue."""
        q1 = Queue('example1')
        q2 = Queue('example2')
        task_orig = q1.enqueue(say_hello)
        task_fetch = q2.fetch_task(task_orig.id)
        self.assertIsNone(task_fetch)

        task_fetch = q1.fetch_task(task_orig.id)
        self.assertIsNotNone(task_fetch)


class TestFailedQueue(RQTestCase):
    def test_get_failed_queue(self):
        """Use custom task class"""
        class CustomTask(Task):
            pass
        failed_queue = get_failed_queue(task_class=CustomTask)
        self.assertIs(failed_queue.task_class, CustomTask)

        failed_queue = get_failed_queue(task_class='rq.task.Task')
        self.assertIsNot(failed_queue.task_class, CustomTask)

    def test_requeue_task(self):
        """Requeueing existing tasks."""
        task = Task.create(func=div_by_zero, args=(1, 2, 3))
        task.origin = 'fake'
        task.save()
        get_failed_queue().quarantine(task, Exception('Some fake error'))  # noqa

        self.assertEqual(Queue.all(), [get_failed_queue()])  # noqa
        self.assertEqual(get_failed_queue().count, 1)

        requeued_task = get_failed_queue().requeue(task.id)

        self.assertEqual(get_failed_queue().count, 0)
        self.assertEqual(Queue('fake').count, 1)
        self.assertEqual(requeued_task.origin, task.origin)

    def test_get_task_on_failed_queue(self):
        default_queue = Queue()
        failed_queue = get_failed_queue()

        task = default_queue.enqueue(div_by_zero, args=(1, 2, 3))

        task_on_default_queue = default_queue.fetch_task(task.id)
        task_on_failed_queue = failed_queue.fetch_task(task.id)

        self.assertIsNotNone(task_on_default_queue)
        self.assertIsNone(task_on_failed_queue)

        task.set_status(TaskStatus.FAILED)

        task_on_default_queue = default_queue.fetch_task(task.id)
        task_on_failed_queue = failed_queue.fetch_task(task.id)

        self.assertIsNotNone(task_on_default_queue)
        self.assertIsNotNone(task_on_failed_queue)
        self.assertTrue(task_on_default_queue.is_failed)

    def test_requeue_nonfailed_task_fails(self):
        """Requeueing non-failed tasks raises error."""
        q = Queue()
        task = q.enqueue(say_hello, 'Nick', foo='bar')

        # Assert that we cannot requeue a task that's not on the failed queue
        with self.assertRaises(InvalidTaskOperationError):
            get_failed_queue().requeue(task.id)

    def test_quarantine_preserves_timeout(self):
        """Quarantine preserves task timeout."""
        task = Task.create(func=div_by_zero, args=(1, 2, 3))
        task.origin = 'fake'
        task.timeout = 200
        task.save()
        get_failed_queue().quarantine(task, Exception('Some fake error'))

        self.assertEqual(task.timeout, 200)

    def test_requeueing_preserves_timeout(self):
        """Requeueing preserves task timeout."""
        task = Task.create(func=div_by_zero, args=(1, 2, 3))
        task.origin = 'fake'
        task.timeout = 200
        task.save()
        get_failed_queue().quarantine(task, Exception('Some fake error'))
        get_failed_queue().requeue(task.id)

        task = Task.fetch(task.id)
        self.assertEqual(task.timeout, 200)

    def test_requeue_sets_status_to_queued(self):
        """Requeueing a task should set its status back to QUEUED."""
        task = Task.create(func=div_by_zero, args=(1, 2, 3))
        task.save()
        get_failed_queue().quarantine(task, Exception('Some fake error'))
        get_failed_queue().requeue(task.id)

        task = Task.fetch(task.id)
        self.assertEqual(task.get_status(), TaskStatus.QUEUED)

    def test_enqueue_preserves_result_ttl(self):
        """Enqueueing persists result_ttl."""
        q = Queue()
        task = q.enqueue(div_by_zero, args=(1, 2, 3), result_ttl=10)
        self.assertEqual(task.result_ttl, 10)
        task_from_queue = Task.fetch(task.id, connection=self.testconn)
        self.assertEqual(int(task_from_queue.result_ttl), 10)

    def test_async_false(self):
        """Task executes and cleaned up immediately if async=False."""
        q = Queue(async=False)
        task = q.enqueue(some_calculation, args=(2, 3))
        self.assertEqual(task.return_value, 6)
        self.assertNotEqual(self.testconn.ttl(task.key), -1)

    def test_custom_task_class(self):
        """Ensure custom task class assignment works as expected."""
        q = Queue(task_class=CustomTask)
        self.assertEqual(q.task_class, CustomTask)

    def test_skip_queue(self):
        """Ensure the skip_queue option functions"""
        q = Queue('foo')
        task1 = q.enqueue(say_hello)
        task2 = q.enqueue(say_hello)
        assert q.dequeue() == task1
        skip_task = q.enqueue(say_hello, at_front=True)
        assert q.dequeue() == skip_task
        assert q.dequeue() == task2

    def test_task_deletion(self):
        """Ensure task.delete() removes itself from FailedQueue."""
        task = Task.create(func=div_by_zero, args=(1, 2, 3))
        task.origin = 'fake'
        task.timeout = 200
        task.save()

        task.set_status(TaskStatus.FAILED)

        failed_queue = get_failed_queue()
        failed_queue.quarantine(task, Exception('Some fake error'))

        self.assertTrue(task.id in failed_queue.get_task_ids())

        task.delete()
        self.assertFalse(task.id in failed_queue.get_task_ids())
