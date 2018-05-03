# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from click.testing import CliRunner
from redis_tasks import get_failed_queue, Queue
from redis_tasks.compat import is_python_version
from redis_tasks.task import Task
from redis_tasks.cli import main
from redis_tasks.cli.helpers import read_config_file, CliConfig
import pytest

from tests import RQTestCase
from tests.fixtures import div_by_zero

if is_python_version((2, 7), (3, 2)):
    from unittest import TestCase
else:
    from unittest2 import TestCase  # noqa


class TestRQCli(RQTestCase):

    @pytest.fixture(autouse=True)
    def set_tmpdir(self, tmpdir):
        self.tmpdir = tmpdir

    def assert_normal_execution(self, result):
        if result.exit_code == 0:
            return True
        else:
            print("Non normal execution")
            print("Exit Code: {}".format(result.exit_code))
            print("Output: {}".format(result.output))
            print("Exception: {}".format(result.exception))
            self.assertEqual(result.exit_code, 0)

    """Test redis_tasks_cli script"""
    def setUp(self):
        super(TestRQCli, self).setUp()
        db_num = self.testconn.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num

        task = Task.create(func=div_by_zero, args=(1, 2, 3))
        task.origin = 'fake'
        task.save()
        get_failed_queue().quarantine(task, Exception('Some fake error'))  # noqa

    def test_config_file(self):
        settings = read_config_file('tests.dummy_settings')
        self.assertIn('REDIS_HOST', settings)
        self.assertEqual(settings['REDIS_HOST'], 'testhost.example.com')

    def test_config_file_option(self):
        """"""
        cli_config = CliConfig(config='tests.dummy_settings')
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['host'],
            'testhost.example.com',
        )
        runner = CliRunner()
        result = runner.invoke(main, ['info', '--config', cli_config.config])
        self.assertEqual(result.exit_code, 1)

    def test_empty_nothing(self):
        """redis_tasks empty -u <url>"""
        runner = CliRunner()
        result = runner.invoke(main, ['empty', '-u', self.redis_url])
        self.assert_normal_execution(result)
        self.assertEqual(result.output.strip(), 'Nothing to do')

    def test_empty_failed(self):
        """redis_tasks empty -u <url> failed"""
        runner = CliRunner()
        result = runner.invoke(main, ['empty', '-u', self.redis_url, 'failed'])
        self.assert_normal_execution(result)
        self.assertEqual(result.output.strip(), '1 tasks removed from failed queue')

    def test_empty_all(self):
        """redis_tasks empty -u <url> failed --all"""
        runner = CliRunner()
        result = runner.invoke(main, ['empty', '-u', self.redis_url, '--all'])
        self.assert_normal_execution(result)
        self.assertEqual(result.output.strip(), '1 tasks removed from failed queue')

    def test_requeue(self):
        """redis_tasks requeue -u <url> --all"""
        runner = CliRunner()
        result = runner.invoke(main, ['requeue', '-u', self.redis_url, '--all'])
        self.assert_normal_execution(result)
        self.assertEqual(result.output.strip(), 'Requeueing 1 tasks from failed queue')

        result = runner.invoke(main, ['requeue', '-u', self.redis_url, '--all'])
        self.assert_normal_execution(result)
        self.assertEqual(result.output.strip(), 'Nothing to do')

    def test_info(self):
        """redis_tasks info -u <url>"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url])
        self.assert_normal_execution(result)
        self.assertIn('1 queues, 1 tasks total', result.output)

    def test_info_only_queues(self):
        """redis_tasks info -u <url> --only-queues (-Q)"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url, '--only-queues'])
        self.assert_normal_execution(result)
        self.assertIn('1 queues, 1 tasks total', result.output)

    def test_info_only_workers(self):
        """redis_tasks info -u <url> --only-workers (-W)"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url, '--only-workers'])
        self.assert_normal_execution(result)
        self.assertIn('0 workers, 1 queues', result.output)

    def test_worker(self):
        """redis_tasks worker -u <url> -b"""
        runner = CliRunner()
        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b'])
        self.assert_normal_execution(result)

    def test_worker_pid(self):
        """redis_tasks worker -u <url> /tmp/.."""
        pid = self.tmpdir.join('redis_tasks.pid')
        runner = CliRunner()
        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '--pid', str(pid)])
        self.assertTrue(len(pid.read()) > 0)
        self.assert_normal_execution(result)

    def test_exception_handlers(self):
        """redis_tasks worker -u <url> -b --exception-handler <handler>"""
        q = Queue()
        failed_q = get_failed_queue()
        failed_q.empty()

        runner = CliRunner()

        # If exception handler is not given, failed task goes to FailedQueue
        q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b'])
        self.assertEqual(failed_q.count, 1)

        # Black hole exception handler doesn't add failed tasks to FailedQueue
        q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b',
                             '--exception-handler', 'tests.fixtures.black_hole'])
        self.assertEqual(failed_q.count, 1)
