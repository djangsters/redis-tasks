import datetime
import random
import uuid

import pytest

from redis_tasks.exceptions import InvalidOperation, TaskDoesNotExist
from redis_tasks.registries import (
    failed_task_registry, finished_task_registry, worker_registry)
from redis_tasks.task import Task, TaskOutcome, TaskStatus, redis_task
from redis_tasks.utils import decode_list
from tests.utils import QueueFactory, WorkerFactory


class SomeClass:
    def some_func(self):
        pass

    def misleading_func(self):
        pass


def misleading_func():
    pass


def name_func():
    pass


@redis_task(reentrant=True)
def reentrant_stub():
    pass


def test_init(stub):
    def closure():
        pass

    with pytest.raises(ValueError):
        Task(closure)
    with pytest.raises(ValueError):
        Task(SomeClass.some_func)
    with pytest.raises(ValueError):
        Task(SomeClass().some_func)
    with pytest.raises(ValueError):
        Task(SomeClass().misleading_func)

    with pytest.raises(TypeError):
        Task(stub, args="foo")
    with pytest.raises(TypeError):
        Task(stub, kwargs=["foo"])

    t = Task(name_func, ["foo"], {"bar": "a"})
    assert t.description == "tests.test_task.name_func('foo', bar='a')"


def test_state_transistions(assert_atomic, connection, time_mocker, stub):
    time = time_mocker('redis_tasks.task.utcnow')
    task = Task(reentrant_stub)
    q = QueueFactory()
    w = WorkerFactory()
    w.startup()

    # enqueue
    time.step()
    assert not connection.exists(task.key)
    with assert_atomic():
        task.enqueue(q)
    assert q.get_task_ids() == [task.id]
    assert connection.exists(task.key)
    for t in [task, Task.fetch(task.id)]:
        assert t.enqueued_at == time.now
        assert t.status == TaskStatus.QUEUED
        assert t.origin == q.name

    # dequeue
    task = q.dequeue(w)
    assert q.get_task_ids() == []
    assert worker_registry.get_running_tasks() == {w.id: task.id}

    # set_running
    time.step()
    with assert_atomic():
        w.start_task(task)
    assert worker_registry.get_running_tasks() == {w.id: task.id}
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.RUNNING
        assert t.started_at == time.now

    # requeue
    time.step()
    with assert_atomic():
        w.end_task(task, TaskOutcome("requeue"))
    assert worker_registry.get_running_tasks() == dict()
    assert q.get_task_ids() == [task.id]
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.QUEUED
        assert t.started_at is None

    # set_finished
    task = q.dequeue(w)
    w.start_task(task)
    time.step()
    with assert_atomic():
        w.end_task(task, TaskOutcome("success"))
    assert q.get_task_ids() == []
    assert finished_task_registry.get_task_ids() == [task.id]
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.FINISHED
        assert t.ended_at == time.now

    # set_failed
    task = q.enqueue_call(stub)
    task = q.dequeue(w)
    w.start_task(task)
    assert worker_registry.get_running_tasks() == {w.id: task.id}
    time.step()
    with assert_atomic():
        w.end_task(task, TaskOutcome("failure", message="my error"))
    assert worker_registry.get_running_tasks() == dict()
    assert failed_task_registry.get_task_ids() == [task.id]
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.FAILED
        assert t.error_message == "my error"
        assert t.ended_at == time.now


def test_cancel(assert_atomic, connection):
    q = QueueFactory()
    task = q.enqueue_call()
    with assert_atomic():
        task.cancel()
    assert q.get_task_ids() == []
    assert task.status == TaskStatus.CANCELED
    assert not connection.exists(task.key)

    w = WorkerFactory()
    w.startup()
    task = q.enqueue_call()
    q.dequeue(w)
    with pytest.raises(InvalidOperation):
        with assert_atomic():
            task.cancel()
    assert worker_registry.get_running_tasks() == {w.id: task.id}
    assert connection.exists(task.key)


def test_worker_death(assert_atomic, connection, stub):
    def setup(func):
        w = WorkerFactory()
        w.startup()
        q.enqueue_call(func)
        task = q.dequeue(w)
        return task, w

    q = QueueFactory()

    # Worker died before starting work on the task
    task, w = setup(stub)
    assert worker_registry.get_running_tasks() == {w.id: task.id}
    with assert_atomic(exceptions=['hgetall']):
        w.died()
    assert q.get_task_ids() == [task.id]
    assert worker_registry.get_running_tasks() == dict()
    assert failed_task_registry.get_task_ids() == []

    # Worker died after starting non-reentrant task
    q.empty()
    task, w = setup(stub)
    assert worker_registry.get_running_tasks() == {w.id: task.id}
    w.start_task(task)
    with assert_atomic(exceptions=['hgetall']):
        w.died()
    assert q.get_task_ids() == []
    assert worker_registry.get_running_tasks() == dict()
    assert failed_task_registry.get_task_ids() == [task.id]

    # Worker died after starting reentrant task
    connection.delete(failed_task_registry.key)
    task, w = setup(reentrant_stub)
    assert worker_registry.get_running_tasks() == {w.id: task.id}
    with assert_atomic():
        w.start_task(task)
    with assert_atomic(exceptions=['hgetall']):
        w.died()
    assert q.get_task_ids() == [task.id]
    assert worker_registry.get_running_tasks() == dict()
    assert failed_task_registry.get_task_ids() == []


def test_get_func(stub):
    assert Task(stub)._get_func() == stub


@redis_task(timeout=42)
def my_timeout_func():
    pass


def test_get_properties(settings, stub):
    assert not Task(stub).is_reentrant
    assert Task(reentrant_stub).is_reentrant

    assert Task(stub).timeout == settings.DEFAULT_TASK_TIMEOUT
    assert Task(my_timeout_func).timeout == 42


def test_delete_many(connection, assert_atomic):
    tasks = [Task() for i in range(5)]
    for t in tasks:
        t._save()

    with assert_atomic():
        Task.delete_many([tasks[0].id, tasks[1].id])

    for t in tasks[0:2]:
        assert not connection.exists(t.key)
    for t in tasks[2:5]:
        assert connection.exists(t.key)


def test_persistence(assert_atomic, connection, stub):
    fields = {'func_name', 'args', 'kwargs', 'status', 'origin', 'description',
              'error_message', 'enqueued_at', 'started_at', 'ended_at', 'meta',
              'aborted_runs'}

    def randomize_data(task):
        string_fields = ['func_name', 'status', 'description', 'origin', 'error_message']
        date_fields = ['enqueued_at', 'started_at', 'ended_at']
        for f in string_fields:
            setattr(task, f, str(uuid.uuid4()))
        for f in date_fields:
            setattr(task, f, datetime.datetime(
                random.randint(1000, 9999), 1, 1, tzinfo=datetime.timezone.utc))

        task.args = tuple(str(uuid.uuid4()) for i in range(4))
        task.kwargs = {str(uuid.uuid4()): ["d"]}
        task.meta = {"x": [str(uuid.uuid4())]}
        task.aborted_runs = ["foo", "bar", str(uuid.uuid4())]

    def as_dict(task):
        return {f: getattr(task, f) for f in fields}

    task = Task(stub)
    with assert_atomic():
        task._save()
    assert set(decode_list(connection.hkeys(task.key))) <= fields
    assert as_dict(Task.fetch(task.id)) == as_dict(task)

    randomize_data(task)
    task._save()
    assert as_dict(Task.fetch(task.id)) == as_dict(task)

    # only deletes
    task.enqueued_at = None
    task.error_message = None
    task._save(['enqueued_at', 'error_message'])
    assert task.enqueued_at is None
    assert task.error_message is None

    for i in range(5):
        store = random.sample(fields, 7)
        copy = Task.fetch(task.id)
        randomize_data(copy)
        copy._save(store)
        for f in store:
            setattr(task, f, getattr(copy, f))
        assert as_dict(Task.fetch(task.id)) == as_dict(task)

    copy = Task.fetch(task.id)
    randomize_data(copy)
    copy.meta = task.meta = {"new_meta": "here"}
    copy.save_meta()
    copy.refresh()
    assert as_dict(copy) == as_dict(task)
    assert as_dict(Task.fetch(task.id)) == as_dict(task)

    # save() and save_meta() in same pipeline
    with assert_atomic():
        with connection.pipeline() as pipe:
            task = Task(stub)
            task._save(pipeline=pipe)
            task.meta["a"] = "b"
            task.save_meta(pipeline=pipe)
            pipe.execute()
    assert Task.fetch(task.id).meta == {"a": "b"}

    task = Task(stub)
    with pytest.raises(TaskDoesNotExist):
        task.refresh()
    with pytest.raises(TaskDoesNotExist):
        Task.fetch('nonexist')


def test_init_save_fetch_delete(connection, assert_atomic, stub):
    t = Task(stub, ["foo"])
    assert not connection.exists(t.key)
    with assert_atomic():
        t._save()
    assert connection.exists(t.key)

    fetched = Task.fetch(t.id)
    assert fetched.id == t.id
    assert fetched.args == ["foo"]

    Task.delete_many([t.id])
    assert not connection.exists(t.key)
    with pytest.raises(TaskDoesNotExist):
        Task.fetch(t.id)
