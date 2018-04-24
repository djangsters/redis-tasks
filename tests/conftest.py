import os
import copy
from contextlib import contextmanager
from unittest import mock

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
        self.exceptions = exceptions
        self._atomic_counter = 0

    def __getattr__(self, name):
        __tracebackhide__ = True
        if name in self.exceptions:
            return getattr(self.wrapped, name)
        if name not in ["pipeline", "transaction"]:
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
