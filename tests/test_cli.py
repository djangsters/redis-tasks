import click
import pytest

from redis_tasks.queue import Queue
from tests.utils import QueueFactory, WorkerFactory


@pytest.fixture
def cli_run():
    from redis_tasks.cli import main as cli_main
    from click.testing import CliRunner
    runner = CliRunner()

    def run(*args):
        return runner.invoke(cli_main, args,
                             catch_exceptions=False, standalone_mode=False)

    return run


def test_empty(cli_run, stub):
    queues = [QueueFactory() for i in range(5)]
    for q in queues:
        q.enqueue_call(stub)

    with pytest.raises(click.UsageError):
        cli_run('empty')

    assert all(q.count() == 1 for q in queues)

    cli_run('empty', queues[0].name, queues[1].name)
    assert queues[0].count() == 0
    assert queues[1].count() == 0
    assert queues[2].count() == 1

    assert set(queues) == set(Queue.all())
    cli_run('empty', '--delete', queues[1].name, queues[2].name)
    assert set(queues) - {queues[1], queues[2]} == set(Queue.all())

    cli_run('empty', '--all')
    assert all(q.count() == 0 for q in queues)


def test_worker(cli_run, mocker):
    worker_main = mocker.patch('redis_tasks.cli.worker_main')
    log_config = mocker.patch('logging.basicConfig')
    cli_run('worker')
    assert worker_main.called_once_with(['default'])
    assert log_config.called_once()
    assert log_config.call_args[1]["level"] == "INFO"

    cli_run('worker', '--quiet')
    assert log_config.call_args[1]["level"] == "WARNING"

    cli_run('worker', '--verbose')
    assert log_config.call_args[1]["level"] == "DEBUG"

    cli_run('worker', '-d', 'foo')
    assert worker_main.call_args[1]["description"] == 'foo'


def test_info(cli_run, stub):
    cli_run('info')

    for i in range(2):
        QueueFactory().enqueue_call(stub)

    for i in range(3):
        WorkerFactory().startup()

    result = cli_run('info')
    assert "2 queue(s), 2 task(s) total" in result.output
    assert "3 worker(s)" in result.output
