class TaskAborted(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class TaskDoesNotExist(Exception):
    pass


class InvalidOperation(Exception):
    pass


class WorkerDoesNotExist(Exception):
    pass


class DeserializationError(Exception):
    def __init__(self, message, raw_data):
        super().__init__(message)
        self.raw_data = raw_data


class WorkerShutdown(BaseException):
    pass
