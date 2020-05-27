Scheduler
=========

``redis-tasks`` provides developers with an built-in task scheduler.
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

1. as a crontab, for example to run a task every Monday you would set::

    "schedule": redis_tasks.crontab("0 0 * * 1")

2. as a periodic schedule defined with ``redis_tasks.run_every()``, which
    accepts the following keyword-only arguments:

    - ``hours``
    - ``minutes``
    - ``seconds``
    - ``start_at``: a string in the format <hours>:<minutes>

    For example if you want your task run every 6 hours starting at 5:00 every
    day, you would set::

        "schedule": redis_tasks.run_every(hours=6, start_at="05:00")

    This would run a task at 5:00, 11:00 , 17:00, 23:00.
3. as a daily task defined with ``redis_tasks.once_per_day()``, which accepts a
    single string argument in the format <hours>:<minutes>.
    For example to run a task every day at 12:00, you would set::

        "schedule": redis_tasks.once_per_day("12:00")
