Installation
============

Your can install redis tasks using pip like this::

    $ pip install redis-tasks

Or add `redis-tasks` to your requirements.in/.txt

You have to set the :envvar:`RT_SETTINGS_MODULE` or manually
call `settings.configure()` passing in a settings module
before you can use redis-tasks in your code.

Settings
========

Defaults
--------

.. code-block:: python

    DEFAULT_TASK_TIMEOUT = 60 * 60 * 24  # 1 day
    EXPIRING_REGISTRIES_TTL = 60 * 60 * 24 * 7  # 7 days
    DEAD_WORKER_TTL = 60 * 60  # 1 hour
    WORKER_HEARTBEAT_FREQ = 10  # 10 seconds
    WORKER_HEARTBEAT_TIMEOUT = 60  # 1 minute
    MAINTENANCE_FREQ = 60 * 1  # 1 minute

    REDIS_URL = "redis://127.0.0.1:6379"
    REDIS_PREFIX = "redis_tasks"
    MIDDLEWARE = []
    WORKER_PRELOAD_FUNCTION = None
    WORKER_DESCRIPTION_FUNCTION = "redis_tasks.worker_process.generate_worker_description"
    TIMEZONE = "UTC"

    # SCHEDULE is an id -> entry dict. The entries are dicts with the following keys:
    #   task: Import path of the function to be run, e.g. "mymodule.myfunc"
    #   schedule: Schedule for this task, e.g. crontab("2 4 * * mon,fri")
    #   args, kwargs: args and kwargs for the tak function (optional)
    #   singleton: Boolean to specify whether this task should not be on the queue
    #              multiple times (optional, defaults to True)
    #   queue: Queue to put this task on (optional)
    SCHEDULE = {}
    SCHEDULER_QUEUE = 'default'
    SCHEDULER_MAX_CATCHUP = 60 * 60 * 1  # 1 hour

.. envvar:: RT_SETTINGS_MODULE

    The :envvar:`RT_SETTINGS_MODULE` environment variable can be used to override the 
    default settings using a standard python module.
    It is processed as a standard python dotted module path.

