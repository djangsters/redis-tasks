from rq import conf
from rq import defaults
import os
from types import SimpleNamespace


def test_settings(mocker):
    mocker.patch.dict(os.environ, {conf.ENVIRONMENT_VARIABLE: 'tests.app.settings_a'})
    s = conf.Settings()
    assert s.REDIS_PREFIX == "test_foo"
    assert s.EXTRA_OPTION == "bar"
    assert s.WORKER_HEARTBEAT_FREQ == defaults.WORKER_HEARTBEAT_FREQ

    s = conf.Settings()
    s.configure(SimpleNamespace(REDIS_PREFIX="barbar"))
    assert s.REDIS_PREFIX == "barbar"
    assert s.WORKER_HEARTBEAT_FREQ == defaults.WORKER_HEARTBEAT_FREQ


def test_mock_settings(settings):
    assert settings.DEFAULT_JOB_TIMEOUT == defaults.DEFAULT_JOB_TIMEOUT
    settings.DEFAULT_JOB_TIMEOUT = "foo"
    assert conf.settings.DEFAULT_JOB_TIMEOUT == "foo"


def test_mock_settings_after(settings):
    assert settings.DEFAULT_JOB_TIMEOUT != "foo"
    assert conf.settings.DEFAULT_JOB_TIMEOUT != "foo"


def test_RedisKey(settings):
    rk = conf.RedisKey("foo")
    settings.REDIS_PREFIX = "bar"
    assert str(rk) == "bar:foo"
    settings.REDIS_PREFIX = "zoo"
    assert str(rk) == "zoo:foo"
