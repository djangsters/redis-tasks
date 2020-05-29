.. redis-tasks documentation master file, created by
   sphinx-quickstart on Mon May 18 11:09:59 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to redis-tasks' documentation!
=======================================

Redis tasks is a framework for background tasks execution.
It uses redis for its shared resources like queues.
It's able to provide reliable tasks execution for long
running tasks even in environments, where it's expected
that workers can be restarted at any time, like Heroku.

Disclaimer
----------
``redis-tasks`` does not store any kind of task results.
It's your tasks' code responsibility to store task results
using a persistence layer of your choice if needed.

Installation
============

Your can install `redis-tasks` using pip like this::

    $ pip install redis-tasks

You have to either set the :envvar:`RT_SETTINGS_MODULE` or manually
call `settings.configure()` passing in a python settings module
before you can use redis-tasks in your code.


Quickstart Guide
================

Prerequisite: You have successfully installed redis-tasks and have a redis insteance ready.
In this Quickstart Guide we'll use "redis://localhost:6379" as our REDIS_URL

Very basic example
------------------
1. Create a tasks module `tasks.py` and write a function that you want to execute in a worker using redis-tasks

.. code:: python

    import urllib.request
    from redis_tasks import redis_task

    @redis_task()
    def print_len_of_url_content(url):
        with urllib.request.urlopen(url) as f:
            print(len(f.read()))

2. Write an rt_settings module to supply your custom config to redis-tasks
   In this example we just pass the REDIS_URL from the enviroment

.. code:: python

    import os
    REDIS_URL = os.environ['REDIS_URL']

3. In a separate terminal/shell start a worker::

   $ export REDIS_URL='redis://127.0.0.1:6379'
   $ export RT_SETTINGS_MODULE=rt_settings 
   $ python -m redis_tasks.cli worker

You should see a message that the worker started successfully::

    16:15:39 INFO    redis_tasks.worker: Worker iw-T460p.14152 [0e015d7a-79c7-42be-8b02-add7161c5951] started

4. Using an ipython shell or whereever needed in your code, you can
   now enqueue this tasks to be executed by the worker at any time like this:

.. note:: Make sure that your the python process is also started with the same ENV VARS as the worker process, see export calls in step 3.

.. code:: python

    from redis_tasks import Queue
    default_queue = Queue()
    from tasks import print_len_of_url_content
    default_queue.enqueue_call(print_len_of_url_content,
                               kwargs={'url': 'https://www.readthedocs.org/'})

In the worker logs you should see the task being started and finished::

    16:15:53 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://www.google.com/') [095c3eb9-7073-4ad6-a7cc-16885ee962a4] started
    12862
    16:15:53 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://www.google.com/') [095c3eb9-7073-4ad6-a7cc-16885ee962a4] finished

Basic example (including scheduler)
-----------------------------------

5. To add regularly executed tasks we extend the rt_settings module to this:

.. code:: python

    import os
    from redis_tasks import crontab, once_per_day, run_every

    REDIS_URL = os.environ['REDIS_URL']

    TIMEZONE = "UTC"  # timezone to be used by scheduler
    SCHEDULE = {
        'print_len_of_readthedocs_org': {  # the task entry identifier
            'task': 'tasks.print_len_of_url_content',  # dotted module path to function to be executed
            'schedule': run_every(minutes=20),
            #'schedule': once_per_day('06:00'),  # Alternative 1
            #"schedule": crontab("0 0 * * 1")},  # Alternative 2
            'args': [],
            'kwargs': {'url': 'https://readthedocs.org/'},
        },
    }


6. In a separate shell/terminal Start a scheduler process::

   $ export REDIS_URL='redis://127.0.0.1:6379'
   $ export RT_SETTINGS_MODULE=rt_settings
   $ python -m redis_tasks.cli scheduler

Verify that you see a successful startup message like::

   16:50:33 INFO  redis_tasks.scheduler: redis_tasks scheduler started

Every time the scheduler enqueues something, you should see a message like this in it's logs::

   16:53:00 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://readthedocs.org/') [f7d4f223-929c-4eb6-b3a7-66f61dd027b7] enqueued

And the worker will log the following messages when it starts/finishes the task processing::

   16:55:53 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://readthedocs.org/') [095c3eb9-7073-4ad6-a7cc-16885ee962a4] started
   12862
   16:55:53 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://readthedocs.org/') [095c3eb9-7073-4ad6-a7cc-16885ee962a4] finished


.. toctree::
   :maxdepth: 2

   settings


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
