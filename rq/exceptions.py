# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


class NoSuchJobError(Exception):
    pass


class NoSuchWorkerError(Exception):
    pass


class InvalidJobOperationError(Exception):
    pass


class DeserializationError(Exception):
    def __init__(self, message, raw_data):
        super().__init__(message)
        self.raw_data = raw_data


class DequeueTimeout(Exception):
    pass


class JobTimeoutException(Exception):
    """Raised when a job takes longer to complete than the allowed maximum timeout value."""
    pass


class ShutDownImminentException(BaseException):
    pass


class TimeoutFormatError(Exception):
    pass
