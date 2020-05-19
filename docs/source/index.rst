.. redis-tasks documentation master file, created by
   sphinx-quickstart on Mon May 18 11:09:59 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to redis-tasks' documentation!
=======================================

Redis tasks is a framework for background tasks execution.
It uses redis for it's shared resources like queues.
It's able to provide reliable tasks execution for long
running tasks even in environments, where it's expected
that workers can be restarted at any time, like heroku.

Disclaimer
----------
`redis-tasks` does not store any kind of task results.
It's your tasks' code responsibility to store task results
using a persistence layer of your choice if needed.

Installation
============

Your can install `redis-tasks` using pip like this::

    $ pip install redis-tasks

Or add `redis-tasks` to your requirements.in/.txt

You'll have to set the :envvar:`RT_SETTINGS_MODULE` or manually
call `settings.configure()` passing in a python settings module
before you can use redis-tasks in your code.


Quickstart Guide
================

TBD

.. toctree::
   :maxdepth: 2

   settings


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
