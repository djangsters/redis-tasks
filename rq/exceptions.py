

class NoSuchJobError(Exception):
    pass


class NoSuchWorkerError(Exception):
    pass


class DeserializationError(Exception):
    def __init__(self, message, raw_data):
        super().__init__(message)
        self.raw_data = raw_data


class DequeueTimeout(Exception):
    pass


class ShutdownImminentException(BaseException):
    pass
