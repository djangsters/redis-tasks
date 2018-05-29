import sys
from contextlib import contextmanager

import pytest

from redis_tasks.exceptions import TaskAborted, WorkerShutdown
from redis_tasks.task import Task
from tests.utils import Something


def test_successful_execute(mocker, stub):
    task = Task(stub, ["foo"], {"foo": "bar"})
    outcome = task.execute()
    assert stub.mock.called_once_with("foo", foo="bar")
    assert outcome.outcome == 'success'

    task = Task(stub)
    stub.mock.reset_mock()
    outcome = task.execute()
    assert stub.mock.called_once_with("foo", foo="bar")
    assert outcome.outcome == 'success'


def test_failed_execute(mocker, stub):
    stub.mock.side_effect = ValueError("TestException")
    task = Task(stub)
    outcome = task.execute()
    assert stub.mock.called_once_with()
    assert outcome.outcome == 'failure'
    assert outcome.message.splitlines()[-1] == 'ValueError: TestException'


def test_aborted_execute(mocker, stub):
    stub.mock.side_effect = WorkerShutdown()
    task = Task(stub)
    outcome = task.execute()
    assert stub.mock.called_once_with()
    assert outcome.outcome == 'failure'
    assert outcome.message.splitlines()[-1] == 'redis_tasks.exceptions.TaskAborted: Worker shutdown'


def test_broken_task(stub):
    task = Task(stub)
    task.func_name = "nonimportable.function"
    outcome = task.execute()
    assert outcome.outcome == 'failure'
    assert outcome.message.splitlines()[-1].startswith(
        'RuntimeError: Failed to import task function')


def test_shutdown_cm(mocker, stub):
    @contextmanager
    def entry_shutdown_cm():
        raise WorkerShutdown()
        yield

    task = Task(stub)
    outcome = task.execute(shutdown_cm=entry_shutdown_cm())
    assert not stub.mock.called
    assert outcome.outcome == 'failure'
    assert 'Worker shutdown' in outcome.message.splitlines()[-1]

    @contextmanager
    def exit_shutdown_cm():
        yield
        raise WorkerShutdown()

    stub.mock.reset_mock()
    outcome = task.execute(shutdown_cm=exit_shutdown_cm())
    assert stub.mock.called_once_with()
    assert outcome.outcome == 'failure'
    assert 'Worker shutdown' in outcome.message.splitlines()[-1]

    in_cm = False

    @contextmanager
    def reporting_cm():
        nonlocal in_cm
        in_cm = True
        yield
        in_cm = False

    def checking_func():
        assert in_cm

    stub.mock.side_effect = checking_func
    outcome = task.execute(shutdown_cm=reporting_cm())
    assert outcome.outcome == 'success'


def test_generate_outcome(stub):
    task = Task(stub)
    assert task._generate_outcome(None, None, None).outcome == 'success'

    try:
        raise TypeError('mytest')
    except TypeError as e:
        exc_info = sys.exc_info()
    outcome = task._generate_outcome(*exc_info)
    assert outcome.outcome == 'failure'
    assert 'mytest' in outcome.message

    outcome = task._generate_outcome(TaskAborted, TaskAborted("a message"), None)
    assert outcome.outcome == 'failure'
    assert outcome.message == 'redis_tasks.exceptions.TaskAborted: a message\n'


class CMCheckMiddleware:
    """Check that no middleware functions are run inside the shutdown_cm"""
    def __init__(self):
        self.in_cm = False
        self.failed = False

    def __enter__(self):
        self.in_cm = True

    def __exit__(self, *args):
        self.in_cm = False

    def run_task(self, task, run, args, kwargs):
        self.failed |= self.in_cm
        try:
            run(*args, **kwargs)
        finally:
            self.failed |= self.in_cm

    def process_outcome(self, task, *exc_info):
        self.failed |= self.in_cm

    def __call__(self):
        return self


class _SpyMiddleware:
    history = []

    @classmethod
    def reset(cls):
        cls.history = []

    def run_task(self, task, run, args, kwargs):
        self.history.append((self, 'before', (task, *args)))
        if getattr(self, 'raise_before', None):
            raise self.raise_before
        try:
            run(*args)
        finally:
            self.history.append((self, 'after', (task, *args)))
            if getattr(self, 'raise_after', None):
                raise self.raise_after

    def process_outcome(self, *args):
        self.history.append((self, 'process_outcome', args))
        if getattr(self, 'outcome', None):
            if isinstance(self.outcome, Exception):
                raise self.outcome
            else:
                return self.outcome

    def __call__(self):
        return self


@pytest.fixture()
def SpyMiddleware():
    yield _SpyMiddleware
    _SpyMiddleware.reset()


def test_middleware_order(mocker, SpyMiddleware, stub):
    task = Task(stub)
    spies = [SpyMiddleware(), SpyMiddleware()]
    cmcheck = CMCheckMiddleware()
    mocker.patch('redis_tasks.task.task_middleware', new=[cmcheck, *spies])
    outcome = task.execute(shutdown_cm=cmcheck)
    assert outcome.outcome == "success"
    assert SpyMiddleware.history == [
        (spies[0], 'before', (task, )),
        (spies[1], 'before', (task, )),
        (spies[1], 'after', (task, )),
        (spies[0], 'after', (task, )),
        (spies[1], 'process_outcome', (task, None, None, None)),
        (spies[0], 'process_outcome', (task, None, None, None))]
    assert not cmcheck.failed


def test_middleware_raise_before(mocker, SpyMiddleware, stub):
    task = Task(stub)
    spies = [SpyMiddleware(), SpyMiddleware(), SpyMiddleware()]
    mocker.patch('redis_tasks.task.task_middleware', new=spies)
    spies[1].raise_before = ArithmeticError()
    outcome = task.execute()
    assert outcome.outcome == "failure"
    assert 'ArithmeticError' in outcome.message
    assert SpyMiddleware.history == [
        (spies[0], 'before', (task, )),
        (spies[1], 'before', (task, )),
        (spies[0], 'after', (task, )),
        (spies[2], 'process_outcome', (task, ArithmeticError, Something, Something)),
        (spies[1], 'process_outcome', (task, ArithmeticError, Something, Something)),
        (spies[0], 'process_outcome', (task, ArithmeticError, Something, Something))]


def test_middleware_raise_after(mocker, SpyMiddleware, stub):
    task = Task(stub)
    spies = [SpyMiddleware(), SpyMiddleware()]
    mocker.patch('redis_tasks.task.task_middleware', new=spies)
    spies[1].raise_after = ArithmeticError()
    outcome = task.execute()
    assert outcome.outcome == "failure"
    assert 'ArithmeticError' in outcome.message
    assert SpyMiddleware.history == [
        (spies[0], 'before', (task, )),
        (spies[1], 'before', (task, )),
        (spies[1], 'after', (task, )),
        (spies[0], 'after', (task, )),
        (spies[1], 'process_outcome', (task, ArithmeticError, Something, Something)),
        (spies[0], 'process_outcome', (task, ArithmeticError, Something, Something))]


def test_outcome_middlewares(mocker, SpyMiddleware, stub):
    task = Task(stub)
    spies = [SpyMiddleware() for i in range(2)]
    mocker.patch('redis_tasks.task.task_middleware', new=spies)
    assert task._generate_outcome(None, None, None).outcome == 'success'
    assert SpyMiddleware.history == [
        (spies[1], 'process_outcome', (task, None, None, None)),
        (spies[0], 'process_outcome', (task, None, None, None))]

    SpyMiddleware.reset()
    spies = [SpyMiddleware() for i in range(3)]
    mocker.patch('redis_tasks.task.task_middleware', new=spies)
    spies[2].outcome = True
    spies[1].outcome = ArithmeticError()
    spies[0].outcome = None
    sentinel = mocker.sentinel.error
    outcome = task._generate_outcome(None, sentinel, None)
    assert SpyMiddleware.history == [
        (spies[2], 'process_outcome', (task, None, sentinel, None)),
        (spies[1], 'process_outcome', (task, None, None, None)),
        (spies[0], 'process_outcome', (task, ArithmeticError, spies[1].outcome, Something))]
    assert outcome.outcome == 'failure'
    assert outcome.message.splitlines()[-1] == 'ArithmeticError'


def test_middleware_constructor_exception(SpyMiddleware, mocker, stub):
    task = Task(stub)
    spies = [SpyMiddleware() for i in range(2)]
    mws = [spies[0], "nope", spies[1]]
    mocker.patch('redis_tasks.task.task_middleware', new=mws)
    assert task.execute().outcome == 'failure'
    assert SpyMiddleware.history == [
        (spies[0], 'before', (task, )),
        (spies[0], 'after', (task, )),
        (spies[1], 'process_outcome', (task, TypeError, Something, Something)),
        (spies[0], 'process_outcome', (task, TypeError, Something, Something))]
    SpyMiddleware.reset()
