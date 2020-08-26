Scheduler
=========

``redis-tasks`` provides developers with a built-in task scheduler.
A schedule is defined as a setting ``RT_SCHEDULE``, which is a dict of the
structure ``id`` -> ``entry``. An ``entry`` itself is a dict with the
following keys:

- ``task``: Import path of the function to be run, e.g. ``mymodule.myfunc``
- ``schedule``: Schedule for this task (see below)
- ``args``: args for the task function (optional)
- ``kwargs``: kwargs for the task function (optional)
- ``singleton``: Boolean to specify whether this task should not be on the queue
    multiple times (optional, defaults to ``True``)
- ``queue``: Queue to put this task on (optional, defaults to
    ``RT_SCHEDULER_QUEUE``)

``schedule`` for a task can be represented in multiple ways:

* as a crontab, for example to run a task every Monday you would set::

    "schedule": redis_tasks.crontab("0 0 * * 1")

* as a periodic schedule defined with ``redis_tasks.run_every()``, which
    accepts the following keyword-only arguments:

    - ``hours``
    - ``minutes``
    - ``seconds``
    - ``start_at``: a string in the format <hours>:<minutes>

    For example if you want your task run every 6 hours starting at 5:00 every
    day, you would set::

        "schedule": redis_tasks.run_every(hours=6, start_at="05:00")

    This would run a task at 5:00, 11:00 , 17:00, 23:00.
* as a daily task defined with ``redis_tasks.once_per_day()``, which accepts a
    single string argument in the format <hours>:<minutes>.
    For example to run a task every day at 12:00, you would set::

        "schedule": redis_tasks.once_per_day("12:00")

Here is an example from step 5. of our Quickstart Guide

.. code:: python

    SCHEDULE = {
        'print_len_of_readthedocs_org': {  # the task entry identifier
            'task': 'tasks.print_len_of_url_content',
            'schedule': run_every(minutes=20),
            # 'schedule': once_per_day('06:00'),
            # 'schedule': crontab('0 0 * * 1'),
            'args': [],
            'kwargs': {'url': 'https://readthedocs.org/'},
        }
    }
