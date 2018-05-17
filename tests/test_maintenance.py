import time

import pytest

from redis_tasks.worker_process import Maintenance


@pytest.mark.slow
def test_run_if_necessary(mocker, settings):
    settings.MAINTENANCE_FREQ = 2
    maintenance = Maintenance()
    run = mocker.patch.object(Maintenance, 'run')
    maintenance.run_if_neccessary()
    assert run.call_count == 1

    maintenance.run_if_neccessary()
    assert run.call_count == 1

    time.sleep(2)
    maintenance.run_if_neccessary()
    assert run.call_count == 2
