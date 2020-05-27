Ordering the tasks
==================

Occasionally you will find yourself in a situation where you want a particular
task to get started when other finish. For instance, you have serious data processing
and you want to send a report once all is finished. It is highly unlikely your
code to be run with only with one worker and one queue. And you want to guarantee
strictly defined sequence.

For such purpose ``redis-tasks`` offers a ``TaskGraph`` class  which allows you to
specify the exact order:

.. code:: python

    import tasks
    from redis_tasks.contrib.graph import TaskGraph


    graph = TaskGraph()  # Initialize the graph

    after_node = graph.add_task(
        dict(func=tasks.send_report))
    before_node = graph.add_task(
        dict(func=tasks.data_processing,
             args=['http://www.python.org']))

    graph.add_dependency(before_node, after_node)
    graph.enqueue()  # Push them into the queue

First you need to add the tasks as nodes and second to specify the
dependency. In case you have more tasks to handle in specific manner you can use
the  ``chain`` helper:

.. code:: python

    import tasks
    from redis_tasks.contrib.graph import chain


    nodes = [
        dict(func=tasks.data_processing,
             args=['http://www.python.org']),
        dict(func=tasks.data_processing,
             args=['https://www.djangoproject.com']),
        dict(func=tasks.send_report),
    ]

    graph = chain(nodes)
    graph.enqueue()

In this case the last task on the list will be the last one to be executed.
