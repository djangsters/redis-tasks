import os

from ..worker import generate_worker_description as default_description


def generate_worker_description(*args, **kwargs):
    if 'DYNO' in os.environ:
        return os.environ['DYNO']
    else:
        return default_description(*args, **kwargs)
