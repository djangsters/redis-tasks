from redis_tasks.queue import Queue
from redis_tasks.task import Task
from redis_tasks.worker import Worker

worker_sequence = 0
queue_sequence = 0
task_sequence = 0


def _stub():
    pass


def TaskFactory():
    global task_sequence
    task_sequence += 1
    task = Task(func=_stub)
    task.id = f'task_{task_sequence}'
    return task


def WorkerFactory(*, queues=None):
    global worker_sequence
    worker_sequence += 1
    id = f'worker_{worker_sequence}'
    description = f"Worker {worker_sequence}"
    if queues is None:
        queues = [QueueFactory()]
    return Worker(id, description=description, queues=queues)


def QueueFactory():
    global queue_sequence
    queue_sequence += 1
    name = f"queue_{queue_sequence}"
    return Queue(name)


def id_list(lst):
    return [x.id for x in lst]


class AnythingType(object):
    def __eq__(self, other):
        return True

    def __ne__(self, other):
        return False

    def __repr__(self):
        return 'Anything'


Anything = AnythingType()


class SomethingType(object):
    def __eq__(self, other):
        return other is not None

    def __ne__(self, other):
        return other is None

    def __repr__(self):
        return 'Something'


Something = SomethingType()
