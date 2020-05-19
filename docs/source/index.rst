Welcome to redis-tasks's documentation!
=======================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:



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
