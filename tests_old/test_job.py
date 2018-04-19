# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime
import time

from tests import fixtures, RQTestCase
from tests.helpers import strip_microseconds

from rq.exceptions import NoSuchTaskError, DeserializationError
from rq.task import Task, get_current_task, TaskStatus, cancel_task, requeue_task
from rq.queue import Queue, get_failed_queue
from rq.utils import utcformat
from rq.serialization import deserialize, serialize
from rq.worker import Worker


class TestTask(RQTestCase):
    def test_unicode(self):
        """Unicode in task description [issue405]"""
        task = Task.create(
            'myfunc',
            args=[12, "☃"],
            kwargs=dict(snowman="☃", null=None),
        )

        expected_string = "myfunc(12, '☃', null=None, snowman='☃')"

        self.assertEqual(
            task.description,
            expected_string,
        )

    def test_create_empty_task(self):
        """Creation of new empty tasks."""
        task = Task()
        task.description = 'test task'

        # Tasks have a random UUID and a creation date
        self.assertIsNotNone(task.id)
        self.assertIsNotNone(task.created_at)
        self.assertEqual(str(task), "<Task %s: test task>" % task.id)

        # ...and nothing else
        self.assertIsNone(task.origin)
        self.assertIsNone(task.enqueued_at)
        self.assertIsNone(task.started_at)
        self.assertIsNone(task.ended_at)
        self.assertIsNone(task.result)
        self.assertIsNone(task.exc_info)

        with self.assertRaises(ValueError):
            task.func
        with self.assertRaises(ValueError):
            task.instance
        with self.assertRaises(ValueError):
            task.args
        with self.assertRaises(ValueError):
            task.kwargs

    def test_create_param_errors(self):
        """Creation of tasks may result in errors"""
        self.assertRaises(TypeError, Task.create, fixtures.say_hello, args="string")
        self.assertRaises(TypeError, Task.create, fixtures.say_hello, kwargs="string")
        self.assertRaises(TypeError, Task.create, func=42)

    def test_create_typical_task(self):
        """Creation of tasks for function calls."""
        task = Task.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))

        # Tasks have a random UUID
        self.assertIsNotNone(task.id)
        self.assertIsNotNone(task.created_at)
        self.assertIsNotNone(task.description)
        self.assertIsNone(task.instance)

        # Task data is set...
        self.assertEqual(task.func, fixtures.some_calculation)
        self.assertEqual(task.args, (3, 4))
        self.assertEqual(task.kwargs, {'z': 2})

        # ...but metadata is not
        self.assertIsNone(task.origin)
        self.assertIsNone(task.enqueued_at)
        self.assertIsNone(task.result)

    def test_create_instance_method_task(self):
        """Creation of tasks for instance methods."""
        n = fixtures.Number(2)
        task = Task.create(func=n.div, args=(4,))

        # Task data is set
        self.assertEqual(task.func, n.div)
        self.assertEqual(task.instance, n)
        self.assertEqual(task.args, (4,))

    def test_create_task_from_string_function(self):
        """Creation of tasks using string specifier."""
        task = Task.create(func='tests.fixtures.say_hello', args=('World',))

        # Task data is set
        self.assertEqual(task.func, fixtures.say_hello)
        self.assertIsNone(task.instance)
        self.assertEqual(task.args, ('World',))

    def test_create_task_from_callable_class(self):
        """Creation of tasks using a callable class specifier."""
        kallable = fixtures.CallableObject()
        task = Task.create(func=kallable)

        self.assertEqual(task.func, kallable.__call__)
        self.assertEqual(task.instance, kallable)

    def test_task_properties_set_data_property(self):
        """Data property gets derived from the task tuple."""
        task = Task()
        task.func_name = 'foo'
        fname, instance, args, kwargs = deserialize(task.data)

        self.assertEqual(fname, task.func_name)
        self.assertEqual(instance, None)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {})

    def test_data_property_sets_task_properties(self):
        """Task tuple gets derived lazily from data property."""
        task = Task()
        task.data = serialize(('foo', None, (1, 2, 3), {'bar': 'qux'}))

        self.assertEqual(task.func_name, 'foo')
        self.assertEqual(task.instance, None)
        self.assertEqual(task.args, (1, 2, 3))
        self.assertEqual(task.kwargs, {'bar': 'qux'})

    def test_save(self):  # noqa
        """Storing tasks."""
        task = Task.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))

        # Saving creates a Redis hash
        self.assertEqual(self.testconn.exists(task.key), False)
        task.save()
        self.assertEqual(self.testconn.type(task.key), b'hash')

        # Saving writes pickled task data
        unpickled_data = deserialize(self.testconn.hget(task.key, 'data'))
        self.assertEqual(unpickled_data[0], 'tests.fixtures.some_calculation')

    def test_fetch(self):
        """Fetching tasks."""
        # Prepare test
        self.testconn.hset('rq:task:some_id', 'data',
                           "(S'tests.fixtures.some_calculation'\nN(I3\nI4\nt(dp1\nS'z'\nI2\nstp2\n.")
        self.testconn.hset('rq:task:some_id', 'created_at',
                           '2012-02-07T22:13:24Z')

        # Fetch returns a task
        task = Task.fetch('some_id')
        self.assertEqual(task.id, 'some_id')
        self.assertEqual(task.func_name, 'tests.fixtures.some_calculation')
        self.assertIsNone(task.instance)
        self.assertEqual(task.args, (3, 4))
        self.assertEqual(task.kwargs, dict(z=2))
        self.assertEqual(task.created_at, datetime(2012, 2, 7, 22, 13, 24))

    def test_persistence_of_empty_tasks(self):  # noqa
        """Storing empty tasks."""
        task = Task()
        with self.assertRaises(ValueError):
            task.save()

    def test_persistence_of_typical_tasks(self):
        """Storing typical tasks."""
        task = Task.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))
        task.save()

        expected_date = strip_microseconds(task.created_at)
        stored_date = self.testconn.hget(task.key, 'created_at').decode('utf-8')
        self.assertEqual(
            stored_date,
            utcformat(expected_date))

        # ... and no other keys are stored
        self.assertEqual(
            sorted(self.testconn.hkeys(task.key)),
            [b'created_at', b'data', b'description'])

    def test_store_then_fetch(self):
        """Store, then fetch."""
        task = Task.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))
        task.save()

        task2 = Task.fetch(task.id)
        self.assertEqual(task.func, task2.func)
        self.assertEqual(task.args, task2.args)
        self.assertEqual(task.kwargs, task2.kwargs)

        # Mathematical equation
        self.assertEqual(task, task2)

    def test_fetching_can_fail(self):
        """Fetching fails for non-existing tasks."""
        with self.assertRaises(NoSuchTaskError):
            Task.fetch('b4a44d44-da16-4620-90a6-798e8cd72ca0')

    def test_fetching_unreadable_data(self):
        """Fetching succeeds on unreadable data, but lazy props fail."""
        # Set up
        task = Task.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))
        task.save()

        # Just replace the data hkey with some random noise
        self.testconn.hset(task.key, 'data', 'this is no pickle string')
        task.refresh()

        for attr in ('func_name', 'instance', 'args', 'kwargs'):
            with self.assertRaises(DeserializationError):
                getattr(task, attr)

    def test_task_is_unimportable(self):
        """Tasks that cannot be imported throw exception on access."""
        task = Task.create(func=fixtures.say_hello, args=('Lionel',))
        task.save()

        # Now slightly modify the task to make it unimportable (this is
        # equivalent to a worker not having the most up-to-date source code
        # and unable to import the function)
        data = self.testconn.hget(task.key, 'data')
        unimportable_data = data.replace(b'say_hello', b'nay_hello')
        self.testconn.hset(task.key, 'data', unimportable_data)

        task.refresh()
        with self.assertRaises(AttributeError):
            task.func  # accessing the func property should fail

    def test_custom_meta_is_persisted(self):
        """Additional meta data on tasks are stored persisted correctly."""
        task = Task.create(func=fixtures.say_hello, args=('Lionel',))
        task.meta['foo'] = 'bar'
        task.save()

        raw_data = self.testconn.hget(task.key, 'meta')
        self.assertEqual(deserialize(raw_data)['foo'], 'bar')

        task2 = Task.fetch(task.id)
        self.assertEqual(task2.meta['foo'], 'bar')

    def test_custom_meta_is_rewriten_by_save_meta(self):
        """New meta data can be stored by save_meta."""
        task = Task.create(func=fixtures.say_hello, args=('Lionel',))
        task.save()
        serialized = task._to_dict()

        task.meta['foo'] = 'bar'
        task.save_meta()

        raw_meta = self.testconn.hget(task.key, 'meta')
        self.assertEqual(deserialize(raw_meta)['foo'], 'bar')

        task2 = Task.fetch(task.id)
        self.assertEqual(task2.meta['foo'], 'bar')

        # nothing else was changed
        serialized2 = task2._to_dict()
        serialized2.pop('meta')
        self.assertDictEqual(serialized, serialized2)

    def test_result_ttl_is_persisted(self):
        """Ensure that task's result_ttl is set properly"""
        task = Task.create(func=fixtures.say_hello, args=('Lionel',), result_ttl=10)
        task.save()
        Task.fetch(task.id, connection=self.testconn)
        self.assertEqual(task.result_ttl, 10)

        task = Task.create(func=fixtures.say_hello, args=('Lionel',))
        task.save()
        Task.fetch(task.id, connection=self.testconn)
        self.assertEqual(task.result_ttl, None)

    def test_description_is_persisted(self):
        """Ensure that task's custom description is set properly"""
        task = Task.create(func=fixtures.say_hello, args=('Lionel',), description='Say hello!')
        task.save()
        Task.fetch(task.id, connection=self.testconn)
        self.assertEqual(task.description, 'Say hello!')

        # Ensure task description is constructed from function call string
        task = Task.create(func=fixtures.say_hello, args=('Lionel',))
        task.save()
        Task.fetch(task.id, connection=self.testconn)
        self.assertEqual(task.description, "tests.fixtures.say_hello('Lionel')")

    def test_task_access_outside_task_fails(self):
        """The current task is accessible only within a task context."""
        self.assertIsNone(get_current_task())

    def test_task_access_within_task_function(self):
        """The current task is accessible within the task function."""
        q = Queue()
        q.enqueue(fixtures.access_self)  # access_self calls get_current_task() and asserts
        w = Worker([q])
        w.work(burst=True)
        assert get_failed_queue(self.testconn).count == 0

    def test_task_access_within_synchronous_task_function(self):
        queue = Queue(async=False)
        queue.enqueue(fixtures.access_self)

    def test_task_async_status_finished(self):
        queue = Queue(async=False)
        task = queue.enqueue(fixtures.say_hello)
        self.assertEqual(task.result, 'Hi there, Stranger!')
        self.assertEqual(task.get_status(), TaskStatus.FINISHED)

    def test_enqueue_task_async_status_finished(self):
        queue = Queue(async=False)
        task = Task.create(func=fixtures.say_hello)
        task = queue.enqueue_task(task)
        self.assertEqual(task.result, 'Hi there, Stranger!')
        self.assertEqual(task.get_status(), TaskStatus.FINISHED)

    def test_get_result_ttl(self):
        """Getting task result TTL."""
        task_result_ttl = 1
        default_ttl = 2
        task = Task.create(func=fixtures.say_hello, result_ttl=task_result_ttl)
        task.save()
        self.assertEqual(task.get_result_ttl(default_ttl=default_ttl), task_result_ttl)
        self.assertEqual(task.get_result_ttl(), task_result_ttl)
        task = Task.create(func=fixtures.say_hello)
        task.save()
        self.assertEqual(task.get_result_ttl(default_ttl=default_ttl), default_ttl)
        self.assertEqual(task.get_result_ttl(), None)

    def test_get_task_ttl(self):
        """Getting task TTL."""
        ttl = 1
        task = Task.create(func=fixtures.say_hello, ttl=ttl)
        task.save()
        self.assertEqual(task.get_ttl(), ttl)
        task = Task.create(func=fixtures.say_hello)
        task.save()
        self.assertEqual(task.get_ttl(), None)

    def test_ttl_via_enqueue(self):
        ttl = 1
        queue = Queue(connection=self.testconn)
        task = queue.enqueue(fixtures.say_hello, ttl=ttl)
        self.assertEqual(task.get_ttl(), ttl)

    def test_never_expire_during_execution(self):
        """Test what happens when task expires during execution"""
        ttl = 1
        queue = Queue(connection=self.testconn)
        task = queue.enqueue(fixtures.long_running_task, args=(2,), ttl=ttl)
        self.assertEqual(task.get_ttl(), ttl)
        task.save()
        task.perform()
        self.assertEqual(task.get_ttl(), -1)
        self.assertTrue(task.exists(task.id))
        self.assertEqual(task.result, 'Done sleeping...')

    def test_cleanup(self):
        """Test that tasks and results are expired properly."""
        task = Task.create(func=fixtures.say_hello)
        task.save()

        # Tasks with negative TTLs don't expire
        task.cleanup(ttl=-1)
        self.assertEqual(self.testconn.ttl(task.key), -1)

        # Tasks with positive TTLs are eventually deleted
        task.cleanup(ttl=100)
        self.assertEqual(self.testconn.ttl(task.key), 100)

        # Tasks with 0 TTL are immediately deleted
        task.cleanup(ttl=0)
        self.assertRaises(NoSuchTaskError, Task.fetch, task.id, self.testconn)

    def test_delete(self):
        """task.delete() deletes itself from Redis."""
        queue = Queue(connection=self.testconn)
        task = queue.enqueue(fixtures.say_hello)
        task.delete()
        self.assertFalse(self.testconn.exists(task.key))

        self.assertNotIn(task.id, queue.get_task_ids())

    def test_create_task_with_id(self):
        """test creating tasks with a custom ID"""
        queue = Queue(connection=self.testconn)
        task = queue.enqueue(fixtures.say_hello, task_id="1234")
        self.assertEqual(task.id, "1234")
        task.perform()

        self.assertRaises(TypeError, queue.enqueue, fixtures.say_hello, task_id=1234)

    def test_get_call_string_unicode(self):
        """test call string with unicode keyword arguments"""
        queue = Queue(connection=self.testconn)

        task = queue.enqueue(fixtures.echo, arg_with_unicode=fixtures.UnicodeStringObject())
        self.assertIsNotNone(task.get_call_string())
        task.perform()

    def test_create_task_with_ttl_should_have_ttl_after_enqueued(self):
        """test creating tasks with ttl and checks if get_tasks returns it properly [issue502]"""
        queue = Queue(connection=self.testconn)
        queue.enqueue(fixtures.say_hello, task_id="1234", ttl=10)
        task = queue.get_tasks()[0]
        self.assertEqual(task.ttl, 10)

    def test_create_task_with_ttl_should_expire(self):
        """test if a task created with ttl expires [issue502]"""
        queue = Queue(connection=self.testconn)
        queue.enqueue(fixtures.say_hello, task_id="1234", ttl=1)
        time.sleep(1)
        self.assertEqual(0, len(queue.get_tasks()))

    def test_create_and_cancel_task(self):
        """test creating and using cancel_task deletes task properly"""
        queue = Queue(connection=self.testconn)
        task = queue.enqueue(fixtures.say_hello)
        self.assertEqual(1, len(queue.get_tasks()))
        cancel_task(task.id)
        self.assertEqual(0, len(queue.get_tasks()))

    def test_create_failed_and_cancel_task(self):
        """test creating and using cancel_task deletes task properly"""
        failed_queue = get_failed_queue(connection=self.testconn)
        task = failed_queue.enqueue(fixtures.say_hello)
        task.set_status(TaskStatus.FAILED)
        self.assertEqual(1, len(failed_queue.get_tasks()))
        cancel_task(task.id)
        self.assertEqual(0, len(failed_queue.get_tasks()))

    def test_create_and_requeue_task(self):
        """Requeueing existing tasks."""
        task = Task.create(func=fixtures.div_by_zero, args=(1, 2, 3))
        task.origin = 'fake'
        task.save()
        get_failed_queue().quarantine(task, Exception('Some fake error'))  # noqa

        self.assertEqual(Queue.all(), [get_failed_queue()])  # noqa
        self.assertEqual(get_failed_queue().count, 1)

        requeued_task = requeue_task(task.id)

        self.assertEqual(get_failed_queue().count, 0)
        self.assertEqual(Queue('fake').count, 1)
        self.assertEqual(requeued_task.origin, task.origin)
