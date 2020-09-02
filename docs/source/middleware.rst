The Middleware
==============

A middleware is a piece of code which is sitting between some kind of request 
and the actual code that is going to be executed and ``redis-tasks`` is no 
exception. Just before the actual running, the framework passes the task as first
class function to the middleware. That means the worker had processed the
information from the ``Queue`` already, gotten the task and its arguments and created a 
middleware instance. Then it calls a method of this instance with appropriate 
information. You are free to decide if, when and how to run the actual task.

In the following example we've created a middleware for delaying the task 
execution:

.. code:: python

   import time

   class SleepMiddleware:
      def run_task(self, task, run, args, kwargs):
         time.sleep(1)
         run(*args, *kwargs)

The middleware definition is a simple Python class, which must have a method called run_task. The function takes two obligatory parameters - task and run which define respectively the name of the task and the reference to the function which should be executed. As can be seen, the code before the execution of the run call is what is of our interest.

In order to use the previously defined Middleware additional entries to the `settings.py` file are necessary. It is a good practice to put the middleware definitions in a separate directory, e.g. middleware, Then the definition would be:

.. code:: python

   MIDDLEWARE = [
      "middleware.SleepMiddleware",
   ]

But wait, what if you want to do stuff right after the task is done? You use the
same approach as above but instead of declaring ``run_task`` method you name it 
``process_outcome``. In the example below we are sending a report via an email:
``process_outcome`` is always called, regardless of the outcome (also if some task raised exceptions for example)
The ``task`` that produced the outcome is passed in as the first argument to the method.

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

And again you don't have the result of the task here. You should either use
database, filesystem or even remote resource. By adding it to
``rt_settings`` you are making it active, just like the first example.
