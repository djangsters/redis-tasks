Available Settings
==================

.. envvar:: RT_SETTINGS_MODULE

    The :envvar:`RT_SETTINGS_MODULE` environment variable can be used to override the 
    default settings using a standard python module.
    It is processed as a standard python dotted module path.

.. attribute:: REDIS_URL

    :default: "redis://127.0.0.1:6379"

    A URL of the redis instance to be used by ``redis-tasks``.

.. attribute:: REDIS_PREFIX

    :default: "redis_tasks"

    A redis key prefix, that will be used as a namespace for all redis keys used by ``redis-tasks``.

.. attribute:: TIMEZONE

    :default: "UTC"

    Timezone to be used by the scheduler.
    The timezone string must be understood by ``pytz``

.. attribute:: DEFAULT_TASK_TIMEOUT

    :default: 86400  # one day

    Default for maximum number of seconds a task is allowed to be executed.
    Can be overridden on a per task level using timeout argument of 
    :py:func:`redis_tasks.task.redis_task` decorator
    Workhorse processes executing a task for longer than this amount of seconds
    will be killed using ``9``/``SIGKILL`` signal.

.. attribute:: EXPIRING_REGISTRIES_TTL

    :default: 604800  # 7 days

    Registries are used for tracking successfully ``[finished]`` or unsuccessfully ``[failed]``
    processed tasks and allow to inspect the history.
    After the amount of seconds defined in this setting, the entries will expire.
    Thus this is the maximum time length for which task processing history is available.

.. attribute:: DEAD_WORKER_TTL

    :default: 3600  # 1 hour

.. attribute:: WORKER_HEARTBEAT_FREQ

   :default: 10  # 10 seconds

.. attribute:: WORKER_HEARTBEAT_TIMEOUT

   :default: 60  # 1 minute

.. attribute:: WORKER_PRELOAD_FUNCTION

   :default: None

.. attribute:: WORKER_DESCRIPTION_FUNCTION

   :default: "redis_tasks.worker_process.generate_worker_description"

.. attribute:: MIDDLEWARE

    :default: []  # empty list

    A list of task middlewares, supplied as importable python dotted path strings

    For more details on ``MIDDLEWARE`` configuration see :doc:`middleware`

.. attribute:: SCHEDULE

    :default: {}  # empty dict

    ``SCHEDULE`` is the main configuration of tasks that should be run by the
    the scheduler regularly.

    It is an ``id`` -> ``entry`` dict.
    The entries are dicts with the following keys:

    ``task``: Import path of the function to be run, e.g. "mymodule.myfunc"

    ``schedule``: Schedule for this task, e.g. crontab("2 4 * * mon,fri")

    ``args``, ``kwargs``: args and kwargs for the task function (optional)

    ``singleton``: Boolean to specify whether this task should not be on the queue multiple times (optional, defaults to True)

    ``queue``: Queue to put this task on (optional)

    For more details on ``SCHEDULE`` configuration see :doc:`scheduler`

.. attribute:: SCHEDULER_QUEUE

    :default: "default"

    Name of the queue to be used by the scheduler when enqueuing scheduled tasks.
    It will be used when no `queue` key is provided on a SCHEDULE` entry.

.. attribute:: SCHEDULER_MAX_CATCHUP

    :default: 3600  # 1 hour
