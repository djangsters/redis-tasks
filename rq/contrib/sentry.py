from ..worker import TaskMiddleware
from ..conf import settings
from ..utils import import_attribute


class SentryMiddleware(TaskMiddleware):
    def __init__(self):
        self.client = import_attribute(settings.SENTRY_INSTANCE)
        self.in_context = False

    def before(self, task):
        self.client.context.activate()
        self.client.context.merge({'extra': {
            'task_id': task.id,
            'task_func': task.func_name,
            'task_args': task.args,
            'task_kwargs': task.kwargs,
            'task_description': task.description,
        }})
        self.client.transaction.push(task.func_name)
        self.in_context = True

    def process_outcome(self, task, *exc_info):
        if not exc_info[0]:
            return
        worker_died = not self.in_context
        if worker_died:
            self.before(task)
        self.client.captureException(exc_info=exc_info)
        if worker_died:
            self.after(task)

    def after(self, task):
        self.in_context = False
        self.client.transaction.pop(task.func_name)
        self.client.context.clear()
