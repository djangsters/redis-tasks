from __future__ import absolute_import

from rq.task import Task, TaskStatus
from rq.queue import FailedQueue, Queue
from rq.utils import current_timestamp
from rq.worker import Worker
from rq.registry import (clean_registries, FinishedTaskRegistry, StartedTaskRegistry)

from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class CustomTask(Task):
    """A custom task class just to test it"""


class TestRegistry(RQTestCase):

    def setUp(self):
        super(TestRegistry, self).setUp()
        self.registry = StartedTaskRegistry(connection=self.testconn)

    def test_cleanup(self):
        """Moving expired tasks to FailedQueue."""
        failed_queue = FailedQueue(connection=self.testconn)
        self.assertEqual(failed_queue.count(), 0)

        queue = Queue(connection=self.testconn)
        task = queue.enqueue(say_hello)

        self.testconn.zadd(self.registry.key, 2, task.id)

        self.registry.cleanup(1)
        self.assertNotIn(task.id, failed_queue.task_ids)
        self.assertEqual(self.testconn.zscore(self.registry.key, task.id), 2)

        self.registry.cleanup()
        self.assertIn(task.id, failed_queue.task_ids)
        self.assertEqual(self.testconn.zscore(self.registry.key, task.id), None)
        task.refresh()
        self.assertEqual(task.get_status(), TaskStatus.FAILED)

    def test_task_execution(self):
        """Task is removed from StartedTaskRegistry after execution."""
        registry = StartedTaskRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        task = queue.enqueue(say_hello)
        self.assertTrue(task.is_queued)

        worker.prepare_task_execution(task)
        self.assertIn(task.id, registry.get_task_ids())
        self.assertTrue(task.is_started)

        worker.perform_task(task, queue)
        self.assertNotIn(task.id, registry.get_task_ids())
        self.assertTrue(task.is_finished)

        # Task that fails
        task = queue.enqueue(div_by_zero)

        worker.prepare_task_execution(task)
        self.assertIn(task.id, registry.get_task_ids())

        worker.perform_task(task, queue)
        self.assertNotIn(task.id, registry.get_task_ids())

    def test_task_deletion(self):
        """Ensure task is removed from StartedTaskRegistry when deleted."""
        registry = StartedTaskRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        task = queue.enqueue(say_hello)
        self.assertTrue(task.is_queued)

        worker.prepare_task_execution(task)
        self.assertIn(task.id, registry.get_task_ids())

        task.delete()
        self.assertNotIn(task.id, registry.get_task_ids())

    def test_get_task_count(self):
        """StartedTaskRegistry returns the right number of task count."""
        timestamp = current_timestamp() + 10
        self.testconn.zadd(self.registry.key, timestamp, 'foo')
        self.testconn.zadd(self.registry.key, timestamp, 'bar')
        self.assertEqual(self.registry.count, 2)
        self.assertEqual(len(self.registry), 2)


class TestFinishedTaskRegistry(RQTestCase):

    def setUp(self):
        super(TestFinishedTaskRegistry, self).setUp()
        self.registry = FinishedTaskRegistry(connection=self.testconn)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:finished:default')

    def test_cleanup(self):
        """Finished task registry removes expired tasks."""
        timestamp = current_timestamp()
        self.testconn.zadd(self.registry.key, 1, 'foo')
        self.testconn.zadd(self.registry.key, timestamp + 10, 'bar')
        self.testconn.zadd(self.registry.key, timestamp + 30, 'baz')

        self.registry.cleanup()
        self.assertEqual(self.registry.get_task_ids(), ['bar', 'baz'])

        self.registry.cleanup(timestamp + 20)
        self.assertEqual(self.registry.get_task_ids(), ['baz'])

    def test_tasks_are_put_in_registry(self):
        """Completed tasks are added to FinishedTaskRegistry."""
        self.assertEqual(self.registry.get_task_ids(), [])
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        # Completed tasks are put in FinishedTaskRegistry
        task = queue.enqueue(say_hello)
        worker.perform_task(task, queue)
        self.assertEqual(self.registry.get_task_ids(), [task.id])

        # When task is deleted, it should be removed from FinishedTaskRegistry
        self.assertEqual(task.get_status(), TaskStatus.FINISHED)
        task.delete()
        self.assertEqual(self.registry.get_task_ids(), [])

        # Failed tasks are not put in FinishedTaskRegistry
        failed_task = queue.enqueue(div_by_zero)
        worker.perform_task(failed_task, queue)
        self.assertEqual(self.registry.get_task_ids(), [])
