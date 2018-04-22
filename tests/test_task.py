
import pytest
import datetime

from rq import Task, Queue
from rq.task import TaskStatus, TaskOutcome
from rq.exceptions import NoSuchTaskError
from rq.registries import finished_task_registry, failed_task_registry
from rq.utils import utcnow
from tests.utils import TaskFactory, WorkerFactory, QueueFactory, stub, id_list


class SomeClass:
    def some_func(self):
        pass

    def misleading_func(self):
        pass


def misleading_func():
    pass


def test_init():
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

    t = Task(stub, ["foo"], {"bar": "a"})
    assert t.description == "tests.utils.stub('foo', bar='a')"

    t = Task(stub, description="mydesc")
    assert t.description == "mydesc"


class NowMocker:
    def __init__(self, mocker):
        self.seq = 0
        self.mocker = mocker
        self.now = None

    def step(self):
        self.seq += 1
        self.now = utcnow().replace(second=self.seq, microsecond=0)
        self.mocker.patch('rq.task.utcnow', return_value=self.now)
        return self.now


def test_state_transistions(assert_atomic, connection, mocker):
    time = NowMocker(mocker)
    task = Task(stub)
    q = Queue('foo')
    w = WorkerFactory()

    # enqueue
    time.step()
    assert not connection.exists(task.key)
    with assert_atomic():
        task.enqueue(q)
    assert q.get_task_ids() == [task.id]
    assert connection.exists(task.key)
    for t in [task, Task.fetch(task.id)]:
        assert task.enqueued_at == time.now
        assert t.status == TaskStatus.QUEUED
        assert t.origin == q.name

    # set_running
    task = q.dequeue(w)
    assert q.get_task_ids() == []
    time.step()
    with assert_atomic():
        task.set_running(WorkerFactory())
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.RUNNING
        assert t.started_at == time.now

    # requeue
    time.step()
    with assert_atomic():
        task.requeue()
    assert q.get_task_ids() == [task.id]
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.QUEUED
        assert t.started_at is None

    # set_finished
    task.set_running(WorkerFactory())
    time.step()
    with assert_atomic():
        task.set_finished()
    assert finished_task_registry.get_task_ids() == [task.id]
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.FINISHED
        assert t.ended_at == time.now

    # set_failed
    task = Task(stub)
    with assert_atomic():
        with connection.pipeline() as pipe:
            task.enqueue(q, pipeline=pipe)
            task.set_running(w, pipeline=pipe)
            pipe.execute()
    time.step()
    with assert_atomic():
        task.set_failed("my error")
    assert failed_task_registry.get_task_ids() == [task.id]
    for t in [task, Task.fetch(task.id)]:
        assert t.status == TaskStatus.FAILED
        assert t.error_message == "my error"
        assert t.ended_at == time.now


def test_init_save_fetch_delete(connection, assert_atomic):
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
    with pytest.raises(NoSuchTaskError):
        Task.fetch(t.id)


def test_lifetimes():
    pass  # TODO
