from contextlib import contextmanager

from raven.transport.threaded import ThreadedHTTPTransport

from ..conf import settings
from ..utils import LazyObject, import_attribute

sentry = LazyObject(lambda: import_attribute(settings.SENTRY_INSTANCE))


def set_client(client):
    global sentry
    sentry = client


class SentryMiddleware:
    @contextmanager
    def context(self, task):
        sentry.context.activate()
        sentry.context.merge({'extra': {
            'task_id': task.id,
            'task_func': task.func_name,
            'task_args': task.args,
            'task_kwargs': task.kwargs,
            'task_description': task.description,
        }})
        sentry.transaction.push(task.func_name)
        try:
            yield
        finally:
            sentry.transaction.pop(task.func_name)
            sentry.context.clear()

    def run_task(self, task, run, args, kwargs):
        with self.context(task):
            run(*args, **kwargs)

    def process_outcome(self, task, *exc_info):
        try:
            if not exc_info[0]:
                return
            with self.context(task):
                sentry.captureException(exc_info=exc_info)
        finally:
            self.wait_for_messages()

    def wait_for_messages(self):
        transport = sentry.remote.get_transport()
        if isinstance(transport, ThreadedHTTPTransport):
            worker = transport.get_worker()
            worker._timed_queue_join(10)
