import time
from concurrent import futures

import pytest

from redis_tasks.exceptions import TaskDoesNotExist
from redis_tasks.queue import Queue
from redis_tasks.registries import queue_registry
from redis_tasks.task import Task
from tests.utils import TaskFactory, WorkerFactory, id_list


def test_queue_basics(assert_atomic):
    worker = WorkerFactory()
    q = Queue()
    assert q.name == 'default'
    assert str(q) == '<Queue default>'
    assert repr(q) == "Queue('default')"

    assert q.count() == 0
    with assert_atomic():
        task = q.enqueue_call()
    q.enqueue_call()
    assert q.count() == 2

    assert q.dequeue(worker).id == task.id
    assert q.count() == 1


def test_remove_and_delete(assert_atomic, connection):
    q = Queue()
    q.enqueue_call()
    task = q.enqueue_call()
    assert q.count() == 2
    assert connection.exists(task.key)
    with assert_atomic():
        q.remove_and_delete(task)
    assert not connection.exists(task.key)
    assert q.count() == 1
    with pytest.raises(TaskDoesNotExist):
        q.remove_and_delete(task)
    assert q.count() == 1


def test_get_tasks():
    q = Queue()
    tasks = [TaskFactory() for i in range(3)]
    for task in tasks:
        task._save()
        q.push(task)
    assert q.get_task_ids() == id_list(tasks)
    assert id_list(q.get_tasks()) == id_list(tasks)
    q.remove_and_delete(tasks[1])
    assert q.get_task_ids() == [tasks[0].id, tasks[2].id]


def test_queue_all():
    q1 = Queue('a')
    q2 = Queue('b')
    q3 = Queue('c')
    q2.enqueue_call()
    q3.enqueue_call()
    assert [x.name for x in Queue.all()] == ['b', 'c']
    q2.delete()
    q3.empty()
    q1.enqueue_call()
    assert [x.name for x in Queue.all()] == ['a', 'c']


def test_unblocking(connection):
    worker = WorkerFactory()
    q = Queue()
    q.enqueue_call()
    assert connection.llen(q.unblock_key)
    assert q.dequeue(worker) is not None
    assert connection.llen(q.unblock_key)
    assert q.dequeue(worker) is None
    assert not connection.llen(q.unblock_key)


def test_push(assert_atomic):
    q = Queue()
    q.enqueue_call()
    q.enqueue_call()
    task = TaskFactory()
    with assert_atomic():
        q.push(task)
    assert q.get_task_ids()[-1] == task.id

    task = TaskFactory()
    q.push(task, at_front=True)
    assert q.get_task_ids()[0] == task.id


class TestEmptyAndDelete:
    def test_empty_empty_queue(self):
        q = Queue()
        q.empty()

    def test_empty(self, assert_atomic, connection, mocker):
        q = Queue()
        t1 = q.enqueue_call()
        t2 = q.enqueue_call()
        assert connection.exists(t1.key)
        with assert_atomic():
            q.empty()
        assert not connection.exists(q.key)
        assert not connection.exists(q.unblock_key)
        assert q.name in queue_registry.get_names()
        assert not connection.exists(t1.key)
        assert not connection.exists(t2.key)

    def test_delete(self, assert_atomic, connection, mocker):
        q = Queue()
        q.enqueue_call()
        q.enqueue_call()
        with assert_atomic():
            q.delete()
        assert not connection.exists(q.key)
        assert not connection.exists(q.unblock_key)
        assert q.name not in queue_registry.get_names()

    def test_delete_transaction(self, assert_atomic, connection, mocker):
        q = Queue()
        task1 = q.enqueue_call()
        task2 = None

        def task_delete(task_ids, **kwargs):
            nonlocal task2
            if not task2:
                # interrupt the transaction with the creation of a new task
                task2 = q.enqueue_call()
            else:
                # assert that the previous transaction attempt was canceled
                assert connection.exists(task1.key)
            orig_delete_many(task_ids, **kwargs)

        orig_delete_many = Task.delete_many
        mocker.patch.object(Task, 'delete_many', new=task_delete)
        assert connection.exists(task1.key)
        q.empty()
        assert not connection.exists(task1.key)
        assert not connection.exists(task2.key)


def test_dequeue(connection):
    worker = WorkerFactory()
    q = Queue()
    assert q.dequeue(worker) is None

    task1 = q.enqueue_call()
    task2 = q.enqueue_call()
    assert connection.llen(q.unblock_key) == 2

    assert q.dequeue(worker).id == task1.id
    assert worker.current_task_id == task1.id
    assert connection.lrange(worker.task_key, 0, -1) == [task1.id.encode()]

    assert q.dequeue(worker).id == task2.id
    assert worker.current_task_id == task2.id
    assert connection.lrange(worker.task_key, 0, -1) == [task2.id.encode(), task1.id.encode()]
    assert connection.llen(q.unblock_key) == 2

    # Clear block_key when queue is empty
    assert q.dequeue(worker) is None
    assert connection.llen(q.unblock_key) == 0


def test_await_multi():
    q1 = Queue('first')
    q2 = Queue('second')

    blocked = False

    def in_thread():
        nonlocal blocked
        blocked = True
        q = Queue.await_multi([q1, q2], 1)
        blocked = False
        return q

    with futures.ThreadPoolExecutor(max_workers=5) as executor:
        future = executor.submit(in_thread)
        time.sleep(0.005)
        assert blocked
        q1.push(TaskFactory())
        time.sleep(0.005)
        assert not blocked
        assert future.result().name == q1.name

        future = executor.submit(in_thread)
        time.sleep(0.005)
        q2.push(TaskFactory())
        q1.push(TaskFactory())
        assert future.result().name == q2.name

    assert Queue.await_multi([q1, q2], 1).name == q1.name


@pytest.mark.slow
def test_queue_await_multi_empty():
    assert Queue.await_multi([Queue()], 1) is None
