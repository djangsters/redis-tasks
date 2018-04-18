# -*- coding: utf-8 -*-
# flake8: noqa
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from .connections import (Connection, get_current_connection, pop_connection,
                          push_connection, use_connection)
from .task import cancel_task, get_current_task
from .queue import get_failed_queue, Queue
from .version import VERSION
from .worker import SimpleWorker, Worker, critical_section

__version__ = VERSION
