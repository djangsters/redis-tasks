import os
import copy
import socket
import signal
import datetime
from contextlib import contextmanager
from unittest import mock
import multiprocessing

import pytest
import redis

from rq import conf

os.environ[conf.ENVIRONMENT_VARIABLE] = 'tests.app.settings'


def pytest_unconfigure(config):
    do_clear_redis()


class ModifiableSettings:
    pass


@pytest.fixture()
def settings():
    from rq import conf
    # ensure the settings are initialized
    conf.settings.DEFAULT_TASK_TIMEOUT
    original_dict = conf.settings.__dict__
    new_dict = copy.deepcopy(original_dict)
    conf.settings.__dict__ = new_dict
    mod_settings = ModifiableSettings()
    mod_settings.__dict__ = new_dict
    yield mod_settings
    conf.settings.__dict__ = original_dict


@pytest.fixture(autouse=True, scope="function")
def mock_signal(mocker):
    """Prevent tests from messing with the signal handling of the pytest process"""
    main_pid = os.getpid()
    orig_signal = signal.signal

    def wrapped_signal(*args, **kwargs):
        if os.getpid() == main_pid:
            return
        else:
            return orig_signal(*args, **kwargs)

    mocker.patch('signal.signal', new=wrapped_signal)
    yield


@pytest.fixture(autouse=True, scope="function")
def clear_redis():
    do_clear_redis()
    yield


def do_clear_redis():
    with conf.connection.pipeline() as pipeline:
        for key in conf.connection.scan_iter(conf.RedisKey('*')):
            pipeline.delete(key)
        pipeline.execute()


@pytest.fixture(scope="function")
def connection():
    yield conf.connection


class AtomicRedis:
    def __init__(self, wrap, exceptions):
        self.wrapped = wrap
        self.exceptions = exceptions + ['ftime']
        self._atomic_counter = 0

    def __getattr__(self, name):
        __tracebackhide__ = True
        if name in self.exceptions:
            return getattr(self.wrapped, name)
        if name not in ["pipeline", "transaction", "register_script"]:
            raise Exception(f"Attempted call to connection.{name} in assert_atomic")
        if self._atomic_counter > 0:
            raise Exception(f"Second call to connection function in assert_atomic")
        self._atomic_counter += 1
        return getattr(self.wrapped, name)


@pytest.fixture(scope="function")
def assert_atomic(mocker):
    @contextmanager
    def cm(*, exceptions=[]):
        real_connection = conf.connection._wrapped
        with mock.patch.dict(conf.connection.__dict__,
                             _wrapped=AtomicRedis(real_connection, exceptions)):
            yield
    yield cm


class TimeMocker:
    def __init__(self, mocker, target):
        from rq.utils import utcnow
        self.seq = 0
        self.mocker = mocker
        self.target = target
        self.now = utcnow().replace(microsecond=0)

    def step(self, seconds=1):
        self.seq += 1
        self.now += datetime.timedelta(seconds=seconds)
        self.mocker.patch(self.target, return_value=self.now)
        return self.now


@pytest.fixture()
def time_mocker(mocker):
    yield lambda target: TimeMocker(mocker, target)


@pytest.fixture(autouse=True)
def kill_child_processes():
    yield
    for child in multiprocessing.active_children():
        print(f"Killing left over process {child.name}")
        os.kill(child.pid, signal.SIGKILL)
