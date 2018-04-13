import signal


class JobTimeoutException(Exception):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """
    pass


class UnixSignalDeathPenalty:
    def __init__(self, timeout):
        self._timeout = timeout

    def __enter__(self):
        self.setup_death_penalty()

    def __exit__(self, type, value, traceback):
        try:
            self.cancel_death_penalty()
        except JobTimeoutException:
            # Weird case: we're done with the with body, but now the alarm is
            # fired.  We may safely ignore this situation and consider the
            # body done.
            pass

        return False

    def handle_death_penalty(self, signum, frame):
        raise JobTimeoutException('Job exceeded maximum timeout '
                                  'value ({0} seconds)'.format(self._timeout))

    def setup_death_penalty(self):
        signal.signal(signal.SIGALRM, self.handle_death_penalty)
        signal.alarm(self._timeout)

    def cancel_death_penalty(self):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
