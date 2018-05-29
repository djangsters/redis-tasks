import importlib
import os
from types import SimpleNamespace

import redis

from redis_tasks import defaults

from .utils import LazyObject, import_attribute

ENVIRONMENT_VARIABLE = "RT_SETTINGS_MODULE"


class Settings:
    def __init__(self):
        self._initialized = False

    def _configure_from_env(self, setting_name=None):
        settings_module = os.environ.get(ENVIRONMENT_VARIABLE)
        if not settings_module:
            raise Exception(
                f"redis_tasks settings are not configured. "
                "You must either define the environment variable "
                f"{ENVIRONMENT_VARIABLE} or call settings.configure() before "
                "accessing redis_tasks.")

        mod = importlib.import_module(settings_module)
        self._setup(mod)

    def _setup(self, settings_module):
        for setting in dir(defaults):
            if setting.isupper():
                setattr(self, setting, getattr(defaults, setting))

        for setting in dir(settings_module):
            if setting.isupper():
                setattr(self, setting, getattr(settings_module, setting))

        self._initialized = True

    def __getattr__(self, name):
        if not self._initialized:
            self._configure_from_env(setting_name=name)
        return self.__dict__[name]

    def configure(self, settings):
        if self._initialized:
            raise RuntimeError('Settings already configured.')
        self._setup(settings)

    def configure_from_dict(self, dct):
        self.configure(SimpleNamespace(**dct))


settings = Settings()


class RTRedis(redis.StrictRedis):
    RESPONSE_CALLBACKS = redis.StrictRedis.RESPONSE_CALLBACKS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_response_callback('EXISTS', int)

    def pipeline(self, transaction=True, shard_hint=None):
        return RTPipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)

    def exists(self, *keys):
        return self.execute_command('EXISTS', *keys)

    def ftime(self):
        seconds, microseconds = self.time()
        return seconds + microseconds * 10**-6

    def zadd(self, name, items, nx=False, xx=False, ch=False, incr=False):
        if nx and xx:
            raise redis.RedisError("ZADD can't use both NX and XX modes")
        pieces = []
        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        if ch:
            pieces.append('CH')
        if incr:
            pieces.append('INCR')
        for k, v in items.items():
            pieces.extend([v, k])
        return self.execute_command('ZADD', name, *pieces)


class RTPipeline(redis.client.BasePipeline, RTRedis):
    pass


@LazyObject
def connection():
    return RTRedis.from_url(settings.REDIS_URL)


@LazyObject
def task_middleware():  # TODO: test
    def middleware_constructor(class_path):
        return import_attribute(class_path)

    return [middleware_constructor(x) for x in settings.MIDDLEWARE]


class RedisKey:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return settings.REDIS_PREFIX + ':' + self.name

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.name == other.name
