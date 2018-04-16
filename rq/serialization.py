import datetime
import pickle

from .execeptions import DeserializationError


def serialize(obj):
    pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(bytes_obj):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = pickle.loads(bytes_obj)
    except Exception as e:
        raise DeserializationError('Could not unpickle', bytes_obj) from e
    return obj
