import datetime
import importlib
import operator
import pickle
from functools import wraps

from .exceptions import DeserializationError


def import_attribute(name):
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


def generate_callstring(func_name, args, kwargs):  # TODO: test
    arg_list = [repr(arg) for arg in args]
    arg_list += [f'{k}={v!r}' for k, v in kwargs.items()]
    args = ', '.join(arg_list)
    return f'{func_name}({args})'


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def utcformat(dt):
    assert dt.tzinfo and dt.tzinfo.utcoffset(dt).total_seconds() == 0
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def utcparse(string):
    parsed = datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%SZ')
    return parsed.replace(tzinfo=datetime.timezone.utc)


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)
    return type(str(name), (), values)


def one(iterable):
    it = iter(iterable)
    try:
        value = next(it)
    except StopIteration:
        raise ValueError('too few items in iterable (expected 1)')
    try:
        next(it)
    except StopIteration:
        return value
    else:
        raise ValueError('too many items in iterable (expected 1)')


def decode_list(lst):
    return [x.decode() for x in lst]


def decode_dict(dct):
    return {k.decode(): v.decode() for k, v in dct.items()}


def is_serializable(obj):
    if obj is None:
        return True
    if isinstance(obj, (int, str, float, bool, datetime.datetime)):
        return True
    if isinstance(obj, (tuple, list, set)):
        return all(is_serializable(x) for x in obj)
    if isinstance(obj, dict):
        return all(is_serializable(k) and is_serializable(v) for k, v in obj.items())
    return False


def serialize(obj):
    if not is_serializable(obj):
        raise ValueError("Passed object contains non-serializable values")
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(bytes_obj):
    try:
        obj = pickle.loads(bytes_obj)
    except Exception as e:
        raise DeserializationError('Could not unpickle', bytes_obj) from e
    return obj


def atomic_pipeline(f):
    from .conf import connection

    @wraps(f)
    def pipeline_wrapper(*args, pipeline=None, **kwargs):
        if pipeline is None:
            with connection.pipeline() as pipe:
                ret = f(*args, pipeline=pipe, **kwargs)
                pipe.execute()
                return ret
        else:
            return f(*args, pipeline=pipeline, **kwargs)

    return pipeline_wrapper


empty = object()


def new_method_proxy(func):
    def inner(self, *args, **kwargs):
        __tracebackhide__ = True
        if self._wrapped is empty:
            self._setup()
        return func(self._wrapped, *args, **kwargs)
    return inner


class LazyObject:
    def __init__(self, func):
        self.__dict__['_wrapped'] = empty
        self.__dict__['_setupfunc'] = func

    def _setup(self):
        self.__dict__['_wrapped'] = self._setupfunc()
        del self.__dict__['_setupfunc']

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
        if self._wrapped is empty:
            repr_attr = self._setupfunc
        else:
            repr_attr = self._wrapped
        return f'<{type(self).__name__}: {repr_attr!r}>'
