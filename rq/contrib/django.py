from django.apps import AppConfig
from django.conf import settings as django_settings
from .conf import settings

SETTINGS_PREFIX = 'RQ_'


class RQDjango(AppConfig):
    def __init__(self):
        settings.configure(DjangoSettingsProxy())


class DjangoSettingsProxy:
    def __getattr__(self, name):
        return getattr(django_settings, SETTINGS_PREFIX + name)

    def __dir__(self):
        return (x[len(SETTINGS_PREFIX):]
                for x in django_settings
                if x.startswith(SETTINGS_PREFIX))
