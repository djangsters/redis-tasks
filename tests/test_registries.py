import pytest

from rq import registries
from rq.exceptions import NoSuchWorkerError
from rq.conf import RedisKey
from tests.factories import TaskFactory, WorkerFactory, QueueFactory


def test_running_task_registry(assert_atomic, connection):
    registry = registries.running_task_registry
    task = TaskFactory()
    task2 = TaskFactory()
    worker = WorkerFactory()

    assert registry.count() == 0
    with assert_atomic():
        with connection.pipeline() as pipe:
            registry.add(task, worker, pipeline=pipe)
            registry.add(task2, worker, pipeline=pipe)
            pipe.execute()
    assert registry.count() == 2

    registry.remove(task2)
    assert registry.count() == 1
    registry.remove(task2)
    assert registry.count() == 1

    assert registry.count() == 1
    with assert_atomic():
        registry.remove(task)
    assert registry.count() == 0


def test_expiring_registry(connection, settings, mocker, assert_atomic):
    registry = registries.ExpiringRegistry('testexpire')
    task1 = TaskFactory()
    task2 = TaskFactory()
    settings.EXPIRING_REGISTRIES_TTL = 10
    delete_tasks = mocker.patch('rq.task.Task.delete_many')

    assert registry.key == RedisKey('testexpire_tasks')

    timestamp = mocker.patch('rq.registries.current_timestamp')
    timestamp.return_value = 1000
    with assert_atomic():
        registry.add(task1)
    timestamp.return_value = 1004
    registry.add(task2)

    registry.expire()
    assert registry.get_task_ids() == [task1.id, task2.id]

    timestamp.return_value = 1012
    registry.expire()
    assert registry.get_task_ids() == [task2.id]
    delete_tasks.assert_called_once_with([task1.id])


def test_worker_registry(connection, settings, mocker, assert_atomic):
    registry = registries.worker_registry
    worker1 = WorkerFactory()
    worker2 = WorkerFactory()
    settings.WORKER_HEARTBEAT_TIMEOUT = 100

    timestamp = mocker.patch('rq.registries.current_timestamp')
    timestamp.return_value = 1000
    with assert_atomic():
        registry.add(worker1)
    timestamp.return_value = 1050
    registry.add(worker2)
    assert registry.get_alive_ids() == [worker1.id, worker2.id]

    timestamp.return_value = 1120
    assert registry.get_alive_ids() == [worker2.id]
    assert registry.get_dead_ids() == [worker1.id]

    with pytest.raises(NoSuchWorkerError):
        registry.heartbeat(worker1)

    timestamp.return_value = 1140
    registry.heartbeat(worker2)
    timestamp.return_value = 1200
    assert registry.get_alive_ids() == [worker2.id]

    with assert_atomic():
        with connection.pipeline() as pipe:
            registry.remove(worker1, pipeline=pipe)
            registry.remove(worker2, pipeline=pipe)
            pipe.execute()
    assert registry.get_alive_ids() == []
    assert registry.get_dead_ids() == []


def test_queue_registry(assert_atomic):
    registry = registries.queue_registry
    queue1 = QueueFactory()
    queue2 = QueueFactory()

    with assert_atomic():
        registry.add(queue1)
    registry.add(queue2)
    assert registry.get_names() == list(sorted([queue1.name, queue2.name]))

    with assert_atomic():
        registry.remove(queue1)
    assert registry.get_names() == [queue2.name]
    registry.remove(queue2)
    assert registry.get_names() == []
