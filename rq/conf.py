import importlib
import os
import uuid
import redis

from rq import defaults
from .utils import LazyObject

ENVIRONMENT_VARIABLE = "RQ_SETTINGS_MODULE"


class Settings:
    def __init__(self):
        self._initialized = False

    def _configure_from_env(self, setting_name=None):
        settings_module = os.environ.get(ENVIRONMENT_VARIABLE)
        if not settings_module:
            desc = f"setting {setting_name}" if setting_name else "settings"
            raise Exception(
                f"Requested {desc}, but settings are not configured. "
                "You must either define the environment variable "
                f"{ENVIRONMENT_VARIABLE} or call settings.configure() before "
                "accessing settings.")

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


settings = Settings()


@LazyObject
def connection():
    return redis.StrictRedis.from_url(settings.REDIS_URL)


class RedisKey:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return settings.REDIS_PREFIX + ':' + self.name

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.name == other.name
