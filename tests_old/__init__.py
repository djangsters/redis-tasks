# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

from redis import StrictRedis
from rq import pop_connection, push_connection
from rq.compat import is_python_version

if is_python_version((2, 7), (3, 2)):
    import unittest
else:
    import unittest2 as unittest  # noqa


def find_empty_redis_database():
    """Tries to connect to a random Redis database (starting from 4), and
    will use/connect it when no keys are in there.
    """
    for dbnum in range(4, 17):
        testconn = StrictRedis(db=dbnum)
        empty = len(testconn.keys('*')) == 0
        if empty:
            return testconn
    assert False, 'No empty Redis database found to run tests in.'


def slow(f):
    import os
    from functools import wraps

    @wraps(f)
    def _inner(*args, **kwargs):
        if os.environ.get('RUN_SLOW_TESTS_TOO'):
            f(*args, **kwargs)

    return _inner


class RQTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up connection to Redis
        testconn = find_empty_redis_database()
        push_connection(testconn)

        # Store the connection (for sanity checking)
        cls.testconn = testconn

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.testconn.flushdb()

    def tearDown(self):
        # Flush afterwards
        self.testconn.flushdb()
