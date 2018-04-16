from ..worker import TaskMiddleware


class SentryMiddleware(TaskMiddleware):
    def __init__(self, client):
        self.client = client

    def start(self, task):
        self.client.context.activate()
        self.client.transaction.push(task.func_name)

    def end(self, task, *exc_info):
        if exc_info and exc_info[0]:
            self.client.captureException(
                exc_info=exc_info,
                extra={
                    'task_id': task.id,
                    'func': task.func_name,
                    'args': task.args,
                    'kwargs': task.kwargs,
                    'description': task.description,
                })
        self.client.transaction.pop(task.func_name)
        self.client.context.clear()
