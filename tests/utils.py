from rq import Task, Queue, Worker


worker_sequence = 0
queue_sequence = 0
task_sequence = 0


def stub():
    pass


def TaskFactory():
    global task_sequence
    task_sequence += 1
    task = Task(func=stub)
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
