from ..worker import TaskMiddleware
from ..exceptions import TaskAborted
from ..conf import settings
from ..utils import import_attribute
from contextlib import contextmanager


class SentryMiddleware(TaskMiddleware):
    def __init__(self):
        self.client = import_attribute(settings.SENTRY_INSTANCE)

    @contextmanager
    def context(self, task):
        self.client.context.activate()
        self.client.context.merge({'extra': {
            'task_id': task.id,
            'task_func': task.func_name,
            'task_args': task.args,
            'task_kwargs': task.kwargs,
            'task_description': task.description,
        }})
        self.client.transaction.push(task.func_name)
        try:
            yield
        finally:
            self.client.transaction.pop(task.func_name)
            self.client.context.clear()

    def run_task(self, task, run, args, kwargs):
        with self.context(task):
            run(*args, **kwargs)

    def process_outcome(self, task, *exc_info):
        if not exc_info[0]:
            return
        if isinstance(exc_info[1], TaskAborted) and task.is_reentrant:
            return
        with self.context(task):
            self.client.captureException(exc_info=exc_info)

# TODO: wrap worker?
