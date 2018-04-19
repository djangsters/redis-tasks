from rq import Task, Queue, Worker


worker_sequence = 0
queue_sequence = 0
task_sequence = 0


def dummy_task_target():
    pass


def TaskFactory():
    global task_sequence
    task_sequence += 1
    task = Task(func=dummy_task_target)
    task.id = f'task_{task_sequence}'
    return task


def WorkerFactory():
    global worker_sequence
    worker_sequence += 1
    id = f'worker_{worker_sequence}'
    description = f"Worker {worker_sequence}"
    return Worker(id, description=description)


def QueueFactory():
    global queue_sequence
    queue_sequence += 1
    name = f"queue_{queue_sequence}"
    return Queue(name)
