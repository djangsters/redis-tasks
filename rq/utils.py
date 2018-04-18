import calendar
import datetime
import importlib
import operator
import pickle
from functools import wraps

from .conf import connection
from .exceptions import DeserializationError


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


def utcnow():
    return datetime.datetime.utcnow()


def utcformat(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


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


def serialize(obj):
    pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(bytes_obj):
    """Unpickles a string, but raises a unified DeserializationError in case anything fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = pickle.loads(bytes_obj)
    except Exception as e:
        raise DeserializationError('Could not unpickle', bytes_obj) from e
    return obj


def atomic_pipeline(f):
    @wraps(f)
    def wrapper(*args, pipeline=None, **kwargs):
        pipe = pipeline
        if not pipeline:
            pipe = connection.pipeline()
        try:
            ret = f(*args, pipeline=pipe, **kwargs)
            if not pipeline:
                pipe.execute()
            return ret
        finally:
            if not pipeline:
                pipe.reset()


empty = object()
osetattr = object.__setattr__
ogetattr = object.__getattr__


def new_method_proxy(func):
    def inner(self, *args, **kwargs):
        wrapped = ogetattr(self, '_wrapped')
        if wrapped is empty:
            ogetattr(self, '_setup')()
            wrapped = ogetattr(self, '_wrapped')
        return func(ogetattr(self, '_wrapped'), *args, **kwargs)
    return inner


class LazyObject:
    def __init__(self, func):
        osetattr(self, '_wrapped', empty)
        osetattr(self, '_setupfunc', func)

    def _setup(self):
        value = ogetattr(self, '_setupfunc')()
        osetattr(self, '_wrapped', value)

    __getattr__ = new_method_proxy(getattr)
    __setattr__ = new_method_proxy(setattr)
    __delattr__ = new_method_proxy(delattr)

    __bytes__ = new_method_proxy(bytes)
    __str__ = new_method_proxy(str)
    __bool__ = new_method_proxy(bool)

    __dir__ = new_method_proxy(dir)
    __getitem__ = new_method_proxy(operator.getitem)
    __setitem__ = new_method_proxy(operator.setitem)
    __delitem__ = new_method_proxy(operator.delitem)
    __iter__ = new_method_proxy(iter)
    __reversed__ = new_method_proxy(reversed)
    __len__ = new_method_proxy(len)
    __contains__ = new_method_proxy(operator.contains)

    def __repr__(self):
        if ogetattr(self, '_wrapped') is empty:
            repr_attr = ogetattr(self, '_setupfunc')
        else:
            repr_attr = ogetattr(self, '_wrapped')
        return f'<{type(self).__name__}: {repr_attr!r}>'
