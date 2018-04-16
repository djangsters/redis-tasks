import calendar
import datetime
import importlib
from functools import wraps

from .compat import as_text
from .connections import resolve_connection
from .exceptions import TimeoutFormatError


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


def utcnow():
    return datetime.datetime.utcnow()


def utcformat(dt):
    return dt.strftime(as_text('%Y-%m-%dT%H:%M:%SZ'))


def utcparse(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%SZ')


def current_timestamp():
    """Returns current UTC timestamp"""
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)
    return type(str(name), (), values)


def decode_list(lst):
    return [x.decode() for x in lst]


def takes_pipeline(f):
    @wraps(f)
    def wrapper(*args, pipeline=None, **kwargs):
        pipe = pipeline
        if not pipeline:
            connection = resolve_connection()
            pipe = connection._pipeline()
        try:
            ret = f(*args, pipeline=pipe, **kwargs)
            if not pipeline:
                pipe.execute()
            return ret
        finally:
            if not pipeline:
                pipe.reset()
