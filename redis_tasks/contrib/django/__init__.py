from itertools import chain

from django.apps import AppConfig
from django.conf import settings as django_settings

from ...conf import settings

SETTINGS_PREFIX = 'RT_'


class RTDjango(AppConfig):
    name = 'redis_tasks.contrib.django'
    label = 'redis_tasks'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        settings.configure(DjangoSettingsProxy(dict(
            SENTRY_INSTANCE="raven.contrib.django.models.client",
            SCHEDULER_TIMEZONE=django_settings.TIME_ZONE,
        )))


class DjangoSettingsProxy:
    def __init__(self, fallbacks):
        self.fallbacks = fallbacks

    def __getattr__(self, name):
        if hasattr(django_settings, SETTINGS_PREFIX + name):
            return getattr(django_settings, SETTINGS_PREFIX + name)
        elif name in self.fallbacks:
            return self.fallbacks[name]
        else:
            raise AttributeError(name)

    def __dir__(self):
        return set(chain((x[len(SETTINGS_PREFIX):]
                          for x in dir(django_settings)
                          if x.startswith(SETTINGS_PREFIX)),
                         self.fallbacks.keys()))
