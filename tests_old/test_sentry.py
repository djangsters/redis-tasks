# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from redis_tasks import get_failed_queue, Queue, Worker
from redis_tasks.contrib.sentry import register_sentry

from tests import RQTestCase


class FakeSentry(object):
    servers = []

    def captureException(self, *args, **kwds):
        pass  # we cannot check this, because worker forks


class TestSentry(RQTestCase):

    def test_work_fails(self):
        """Non importable tasks should be put on the failed queue event with sentry"""
        q = Queue()
        failed_q = get_failed_queue()

        # Action
        q.enqueue('_non.importable.task')
        self.assertEqual(q.count, 1)

        w = Worker([q])
        register_sentry(FakeSentry(), w)

        w.work(burst=True)

        # Postconditions
        self.assertEqual(failed_q.count, 1)
        self.assertEqual(q.count, 0)
