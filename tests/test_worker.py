import datetime
import random
import uuid

import pytest

from redis_tasks.exceptions import WorkerDoesNotExist
from redis_tasks.registries import failed_task_registry, worker_registry
from redis_tasks.task import TaskOutcome
from redis_tasks.utils import decode_list
from redis_tasks.worker import Worker, WorkerState
from tests.utils import QueueFactory, id_list


def test_state_transitions(time_mocker, connection, assert_atomic):
    worker = Worker('myworker', queues=[QueueFactory(), QueueFactory()])
    time = time_mocker('redis_tasks.worker.utcnow')
    queue = worker.queues[0]

    time.step()
    assert not connection.exists(worker.key, worker.task_key)
    assert worker.id not in worker_registry.get_worker_ids()
    with assert_atomic():
        worker.startup()
    assert worker.id in worker_registry.get_worker_ids()
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.started_at == time.now
        assert w.state == WorkerState.IDLE

    queue.enqueue_call()
    with assert_atomic(exceptions=['hgetall']):
        task = queue.dequeue(worker)
    assert connection.exists(worker.key, worker.task_key) == 2
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.current_task_id == task.id
        assert w.state == WorkerState.IDLE

    with assert_atomic():
        worker.start_task(task)
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.current_task_id == task.id
        assert w.state == WorkerState.BUSY

    with assert_atomic():
        worker.end_task(task, TaskOutcome("success"))
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.current_task_id is None
        assert w.state == WorkerState.IDLE

    time.step()
    assert connection.ttl(worker.key) == -1
    assert worker.id in worker_registry.get_worker_ids()
    with assert_atomic():
        worker.shutdown()
    assert worker.id not in worker_registry.get_worker_ids()
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.state == WorkerState.DEAD
        assert w.shutdown_at == time.now
    assert connection.ttl(worker.key) > 0


def test_died(time_mocker, connection, assert_atomic):
    time = time_mocker('redis_tasks.worker.utcnow')

    # Die while idle
    worker = Worker('idleworker', queues=[QueueFactory()])
    time.step()
    worker.startup()
    time.step()
    assert connection.ttl(worker.key) == -1
    assert worker.id in worker_registry.get_worker_ids()
    with assert_atomic():
        worker.died()
    assert worker.id not in worker_registry.get_worker_ids()
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.state == WorkerState.DEAD
        assert w.shutdown_at == time.now
    assert connection.ttl(worker.key) > 0

    # die whith task in limbo
    worker = Worker('limboworker', queues=[QueueFactory(), QueueFactory()])
    queue = worker.queues[1]
    time.step()
    worker.startup()
    time.step()
    queue.enqueue_call()
    task = queue.dequeue(worker)
    with assert_atomic(exceptions=['hgetall']):
        worker.died()
    assert queue.get_task_ids() == [task.id]
    assert worker.id not in worker_registry.get_worker_ids()
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.state == WorkerState.DEAD
        assert w.current_task_id is None
        assert w.shutdown_at == time.now
    assert connection.ttl(worker.key) > 0

    # die while busy
    worker = Worker('busyworker', queues=[QueueFactory(), QueueFactory()])
    queue = worker.queues[1]
    time.step()
    worker.startup()
    time.step()
    queue.enqueue_call()
    task = queue.dequeue(worker)
    worker.start_task(task)
    with assert_atomic(exceptions=['hgetall']):
        worker.died()
    assert queue.get_task_ids() == []
    assert failed_task_registry.get_task_ids() == [task.id]
    assert worker.id not in worker_registry.get_worker_ids()
    for w in [worker, Worker.fetch(worker.id)]:
        assert w.state == WorkerState.DEAD
        assert w.current_task_id is None
        assert w.shutdown_at == time.now
    assert connection.ttl(worker.key) > 0


def test_fetch_current_task():
    worker = Worker('testworker', queues=[QueueFactory()])
    queue = worker.queues[0]
    queue.enqueue_call()
    assert worker.fetch_current_task() is None
    task = queue.dequeue(worker)
    assert worker.fetch_current_task().id == task.id


def test_all():
    w1 = Worker('w1', queues=[QueueFactory()])
    w2 = Worker('w2', queues=[QueueFactory()])
    assert Worker.all() == []
    w1.startup()
    w2.startup()
    assert id_list(Worker.all()) == [w1.id, w2.id]
    w1.shutdown()
    assert id_list(Worker.all()) == [w2.id]


def test_heartbeat(mocker):
    heartbeat = mocker.patch.object(worker_registry, 'heartbeat')
    worker = Worker('testworker', queues=[QueueFactory()])
    worker.startup()
    worker.heartbeat()
    assert heartbeat.called_once_with(worker)


def test_persistence(assert_atomic, connection, time_mocker):
    time = time_mocker('redis_tasks.worker.utcnow')
    time.step()
    fields = {'description', 'state', 'queues', 'started_at', 'shutdown_at', 'current_task_id'}

    def randomize_data(worker):
        string_fields = ['description', 'state', 'current_task_id']
        date_fields = ['started_at', 'shutdown_at']
        for f in string_fields:
            setattr(worker, f, str(uuid.uuid4()))
        for f in date_fields:
            setattr(worker, f, datetime.datetime(
                random.randint(1000, 9999), 1, 1, tzinfo=datetime.timezone.utc))

        worker.args = tuple(str(uuid.uuid4()) for i in range(4))
        worker.kwargs = {str(uuid.uuid4()): ["d"]}
        worker.meta = {"x": [str(uuid.uuid4())]}
        worker.aborted_runs = ["foo", "bar", str(uuid.uuid4())]

    def as_dict(worker):
        return {f: getattr(worker, f) if f != 'queues' else [q.name for q in worker.queues]
                for f in fields}

    worker = Worker("testworker", queues=[QueueFactory()])
    with assert_atomic():
        worker.startup()
    assert as_dict(worker) == as_dict(Worker.fetch(worker.id))
    worker2 = Worker("worker2", queues=[QueueFactory() for i in range(5)])
    worker2.startup()
    assert as_dict(worker2) == as_dict(Worker.fetch(worker2.id))
    assert as_dict(worker) != as_dict(worker2)

    worker = Worker("testworker", queues=[QueueFactory()])
    worker.startup()
    with assert_atomic():
        worker._save()
    assert set(decode_list(connection.hkeys(worker.key))) <= fields
    assert as_dict(Worker.fetch(worker.id)) == as_dict(worker)

    randomize_data(worker)
    worker._save()
    assert as_dict(Worker.fetch(worker.id)) == as_dict(worker)

    # only deletes
    worker.started_at = None
    worker._save(['started_at'])
    assert as_dict(Worker.fetch(worker.id)) == as_dict(worker)

    for i in range(5):
        store = random.sample(fields, 3)
        copy = Worker.fetch(worker.id)
        randomize_data(copy)
        copy._save(store)
        for f in store:
            setattr(worker, f, getattr(copy, f))
        assert as_dict(Worker.fetch(worker.id)) == as_dict(worker)

    worker = Worker("nonexist", queues=[QueueFactory()])
    with pytest.raises(WorkerDoesNotExist):
        worker.refresh()
    with pytest.raises(WorkerDoesNotExist):
        Worker.fetch("nonexist")
