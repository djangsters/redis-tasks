.. redis-tasks documentation master file, created by
   sphinx-quickstart on Mon May 18 11:09:59 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

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

The Middleware
---------------

As you know a middleware is a piece of code which is sitting between some kind of 
request and the actual code that is going to be executed and ``redis-tasks`` is no 
exception. Just before the actual running, the framework passes the task as first
class function to the middleware. This means that the worker had processed the
information from the ``Queue``, gotten the task and its arguments and created a 
middleware instance. And then you are responsible for manually calling your
task. 


In the following example we've created a middleware for delaying the task 
execution:

.. code:: python

   import time

   class SleepMiddleware:
      def run_task(self, task, run, args, kwargs):
         time.sleep(1)
         run(*args, *kwargs)

You need to declare it as a plain python ``class`` and it must have method called 
``run_task``. The most important parameter here is ``run``. As we mentioned above the 
framework leaves the calling in your hands. You don't need to return anything since
the returned result of the ``run_task`` is completely ignored. 

If you want to use your custom middleware you need to include it into your 
``rt_settings``. Let's assume that your code lies in a file named `middleware.py`
in the root directory of your project. You have to declare it like this:

.. code:: python

   MIDDLEWARE = [
      "middleware.SleepMiddleware",
   ]

But wait, what if you want to do stuff right after the task is done? You use the
same approach as above but instead of declaring ``run_task`` method you name it 
``process_outcome``. In the example below we are sending a report via an email:

.. code:: python

   import smtplib
   from email.message import EmailMessage

   class ReportMiddleware:
      def process_outcome(self, task, *exc_info):
         msg = EmailMessage()
         msg['Subject'] = 'Daily Report'
         msg['From'] = 'development@test.com'
         msg['To'] = 'client@test.com'

         s = smtplib.SMTP('localhost')
         s.send_message(msg)
         s.quit()

And again you don't have the result of the task here. If you want to 
activate this middleware you need to declare it in `rt_settings` the 
same way as the previous one.