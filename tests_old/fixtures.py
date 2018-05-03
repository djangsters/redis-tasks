# -*- coding: utf-8 -*-
"""
This file contains all tasks that are used in tests.  Each of these test
fixtures has a slighty different characteristics.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time
import sys

from redis_tasks import Connection, get_current_task, get_current_connection, Queue
from redis_tasks.decorators import task
from redis_tasks.compat import PY2
from redis_tasks.worker import HerokuWorker


def say_pid():
    return os.getpid()


def say_hello(name=None):
    """A task with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def say_hello_unicode(name=None):
    """A task with a single argument and a return value."""
    return unicode(say_hello(name))


def do_nothing():
    """The best task in the world."""
    pass


def div_by_zero(x):
    """Prepare for a division-by-zero exception."""
    return x / 0


def some_calculation(x, y, z=1):
    """Some arbitrary calculation with three numbers.  Choose z smartly if you
    want a division by zero exception.
    """
    return x * y / z


def create_file(path):
    """Creates a file at the given path.  Actually, leaves evidence that the
    task ran."""
    with open(path, 'w') as f:
        f.write('Just a sentinel.')


def create_file_after_timeout(path, timeout):
    time.sleep(timeout)
    create_file(path)


def access_self():
    assert get_current_connection() is not None
    assert get_current_task() is not None


def modify_self(meta):
    j = get_current_task()
    j.meta.update(meta)
    j.save()


def modify_self_and_error(meta):
    j = get_current_task()
    j.meta.update(meta)
    j.save()
    return 1 / 0


def echo(*args, **kwargs):
    return (args, kwargs)


class Number(object):
    def __init__(self, value):
        self.value = value

    @classmethod
    def divide(cls, x, y):
        return x * y

    def div(self, y):
        return self.value / y


class CallableObject(object):
    def __call__(self):
        return u"I'm callable"


class UnicodeStringObject(object):
    def __repr__(self):
        if PY2:
            return u'é'.encode('utf-8')
        else:
            return u'é'


with Connection():
    @task(queue='default')
    def decorated_task(x, y):
        return x + y


def black_hole(task, *exc_info):
    # Don't fall through to default behaviour (moving to failed queue)
    return False


def long_running_task(timeout=10):
    time.sleep(timeout)
    return 'Done sleeping...'


def run_dummy_heroku_worker(sandbox, _imminent_shutdown_delay):
    """
    Run the work horse for a simplified heroku worker where perform_task just
    creates two sentinel files 2 seconds apart.
    :param sandbox: directory to create files in
    :param _imminent_shutdown_delay: delay to use for HerokuWorker
    """
    sys.stderr = open(os.path.join(sandbox, 'stderr.log'), 'w')

    class TestHerokuWorker(HerokuWorker):
        imminent_shutdown_delay = _imminent_shutdown_delay

        def perform_task(self, task, queue):
            create_file(os.path.join(sandbox, 'started'))
            # have to loop here rather than one sleep to avoid holding the GIL
            # and preventing signals being received
            for i in range(20):
                time.sleep(0.1)
            create_file(os.path.join(sandbox, 'finished'))

    w = TestHerokuWorker(Queue('dummy'))
    w.main_work_horse(None, None)
