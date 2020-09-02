Welcome to redis-tasks' documentation!
=======================================

``redis tasks`` is a framework for reliable background tasks execution,
specifically for environments where workers are expected to restart frequently,
like Heroku. The task queueing is handled by ``redis``.

Disclaimer
----------
``redis-tasks`` does not store any kind of task results.
If you need to store task results, you have to use
a persistence layer of your choice in your task code.

Installation
============

Your can install ``redis-tasks`` using pip like this::

    $ pip install redis-tasks

.. note:: ``redis-tasks`` only runs on python versions 3.6 and above.

| You have to set the :envvar:`RT_SETTINGS_MODULE` environent variable 
  before you can use redis-tasks in your code.
|
| Alternativaly you can call :py:meth:`redis_tasks.conf.settings.configure`
  passing in a python settings module or call
  :py:meth:`redis_tasks.conf.settings.configure_from_dict` passing in a dict
  of settings.
| See :py:class:`redis_tasks.conf.Settings` for more details.

Quickstart Guide
================

Prerequisite: You have successfully installed ``redis-tasks`` and have a ``redis`` insteance ready.
Prerequisite: You have successfully installed ``redis-tasks`` and have a running ``redis`` instance.
In this Quickstart Guide we'll use ``"redis://localhost:6379"`` as our :any:`REDIS_URL`

1. Create a `tasks.py` module and write a function that you want to execute in a worker using ``redis-tasks``

.. code:: python

    import urllib.request
    from redis_tasks import redis_task

    @redis_task()
    def print_len_of_url_content(url):
        with urllib.request.urlopen(url) as f:
            print(len(f.read()))

2. Create a `settings.py` module to supply your custom config to ``redis-tasks``
   In this example we just pass the :any:`REDIS_URL` from the enviroment

.. code:: python

    import os
    REDIS_URL = os.environ['REDIS_URL']

3. In a separate terminal/shell start a worker::

   $ export REDIS_URL='redis://127.0.0.1:6379'
   $ export RT_SETTINGS_MODULE=settings 
   $ redis_tasks worker

You should see a message that the worker started successfully::

    16:15:39 INFO    redis_tasks.worker: Worker iw-T460p.14152 [0e015d7a-79c7-42be-8b02-add7161c5951] started

4. In a separate terminal/shell you can
   now enqueue this tasks to be executed by the worker at any time like this:

.. note:: Make sure that the python process is also started with the same ENV VARS as the worker process, see export calls in step 3.

.. code:: python

    from redis_tasks import Queue
    from tasks import print_len_of_url_content
    Queue().enqueue_call(
        print_len_of_url_content,
        kwargs={'url': 'https://www.readthedocs.org/'})

In the worker logs you should see the task being started and finished::

    16:15:53 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://www.readthedocs.org/') [095c3eb9-7073-4ad6-a7cc-16885ee962a4] started
    12862
    16:15:53 INFO      redis_tasks.task: Task tasks.print_len_of_url_content(url='https://www.readthedocs.org/') [095c3eb9-7073-4ad6-a7cc-16885ee962a4] finished

Adding Scheduled Tasks
----------------------
So far the configuration allowed you to run the tasks almost immediately. What if you want to specify the exact time?

5. To add regularly executed tasks we extend the `settings.py` module to this:

.. code:: python

    import os
    from redis_tasks import run_every

    REDIS_URL = os.environ['REDIS_URL']

    TIMEZONE = "UTC"  # timezone to be used by scheduler
    SCHEDULE = {
        'print_len_of_readthedocs_org': {
            'task': 'tasks.print_len_of_url_content',
            'schedule': run_every(minutes=20),
            'args': [],
            'kwargs': {'url': 'https://readthedocs.org/'},
        },
    }

For more details on SCHEDULE configuration see :doc:`scheduler`


6. In a separate shell/terminal start a scheduler process::

   $ export REDIS_URL='redis://127.0.0.1:6379'
   $ export RT_SETTINGS_MODULE=rt_settings
   $ redis_tasks scheduler

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
   :caption: Usage:

   Command Line Interface <cli>
   settings
   scheduler
   middleware
   taskgraph


.. toctree::
   :maxdepth: 2
   :caption: API Docs:

   redis_tasks

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


How it works
==================

Signal handling
---------------

In order to assure reliable task processing, ``redis-tasks`` ships with the
``SIGTERM`` and ``SIGINT`` signals handling, making it compatible with
platforms like Heroku. When one of the signals is sent, the ``WorkerProcess``
requests shutdown from the ``WorkHorse`` (a process taking care of a task). The
``WorkHorse`` ensures that the task was handled properly and interrupts the task
processing only during the actual task run. The outcome of the task execution is
determined based on whether the task is reentrant or not. Reentrant tasks are
re-enqueued to be the first to run after the worker restart, while not reentrant
tasks are considered failed. Only after that the ``WorkerProcess`` is shut down.
