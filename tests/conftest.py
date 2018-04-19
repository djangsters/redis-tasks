import os
import copy

import pytest


class ModifiableSettings:
    pass


@pytest.fixture()
def settings():
    from rq import conf
    os.environ[conf.ENVIRONMENT_VARIABLE] = 'tests.app.empty_settings'
    # ensure the settings are initialized
    conf.settings.DEFAULT_JOB_TIMEOUT
    original_dict = conf.settings.__dict__
    new_dict = copy.deepcopy(original_dict)
    conf.settings.__dict__ = new_dict
    mod_settings = ModifiableSettings()
    mod_settings.__dict__ = new_dict
    yield mod_settings
    settings.__dict__ = original_dict
