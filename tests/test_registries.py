import pytest

from redis_tasks import registries
from redis_tasks.conf import RedisKey
from redis_tasks.exceptions import WorkerDoesNotExist
from redis_tasks.task import TaskOutcome
from tests.utils import QueueFactory, Something, TaskFactory, WorkerFactory


def test_expiring_registry(connection, settings, mocker, assert_atomic):
    registry = registries.ExpiringRegistry('testexpire')
    task1 = TaskFactory()
    task2 = TaskFactory()
    settings.EXPIRING_REGISTRIES_TTL = 10
    delete_tasks = mocker.patch('redis_tasks.task.Task.delete_many')

    assert registry.key == RedisKey('testexpire_tasks')

    timestamp = mocker.patch('redis_tasks.conf.RTRedis.time')
    timestamp.return_value = (1000, 0)
    with assert_atomic():
        registry.add(task1)
    timestamp.return_value = (1004, 0)
    registry.add(task2)

    registry.expire()
    assert registry.get_task_ids() == [task1.id, task2.id]

    timestamp.return_value = (1012, 0)
    registry.expire()
    assert registry.get_task_ids() == [task2.id]
    delete_tasks.assert_called_once_with([task1.id], pipeline=Something)


def test_worker_registry(connection, settings, mocker, assert_atomic):
    registry = registries.worker_registry
    worker1 = WorkerFactory()
    worker2 = WorkerFactory()
    settings.WORKER_HEARTBEAT_TIMEOUT = 100

    timestamp = mocker.patch('redis_tasks.conf.RTRedis.time')
    timestamp.return_value = (1000, 0)
    with assert_atomic():
        registry.add(worker1)
    timestamp.return_value = (1050, 0)
    registry.add(worker2)
    assert registry.get_worker_ids() == [worker1.id, worker2.id]

    timestamp.return_value = (1120, 0)
    assert registry.get_dead_ids() == [worker1.id]

    registry.heartbeat(worker1)
    timestamp.return_value = (1140, 0)
    assert registry.get_dead_ids() == []

    timestamp.return_value = (1200, 0)
    assert registry.get_dead_ids() == [worker2.id]

    with assert_atomic():
        registry.remove(worker1)
    # worker is already dead
    with pytest.raises(WorkerDoesNotExist):
        registry.heartbeat(worker1)

    with assert_atomic():
        with connection.pipeline() as pipe:
            registry.remove(worker1, pipeline=pipe)
            registry.remove(worker2, pipeline=pipe)
            pipe.execute()
    assert registry.get_worker_ids() == []
    assert registry.get_dead_ids() == []


def test_worker_reg_running_tasks():
    registry = registries.worker_registry
    queue = QueueFactory()
    t1 = queue.enqueue_call()
    t2 = queue.enqueue_call()
    worker1 = WorkerFactory(queues=[queue])
    worker2 = WorkerFactory(queues=[queue])
    worker1.startup()
    worker2.startup()

    assert registry.get_running_tasks() == dict()
    queue.push(t1)
    queue.dequeue(worker1)
    assert registry.get_running_tasks() == {worker1.id: t1.id}
    worker1.start_task(t1)

    queue.push(t2)
    queue.dequeue(worker2)
    worker2.start_task(t2)
    assert registry.get_running_tasks() == {worker1.id: t1.id, worker2.id: t2.id}
    worker1.end_task(t1, TaskOutcome("success"))
    assert registry.get_running_tasks() == {worker2.id: t2.id}


def test_queue_registry(assert_atomic, connection):
    registry = registries.queue_registry
    queue1 = QueueFactory()
    queue2 = QueueFactory()

    with assert_atomic():
        registry.add(queue1)
    registry.add(queue2)
    assert set(registry.get_names()) == {queue1.name, queue2.name}

    with assert_atomic():
        registry.remove(queue1)
    assert registry.get_names() == [queue2.name]
    registry.remove(queue2)
    assert registry.get_names() == []


def test_maintenance():
    registries.registry_maintenance()
