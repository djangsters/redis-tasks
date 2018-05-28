import datetime
import logging
import random
import signal
import threading
import time
import uuid
from contextlib import suppress

import croniter

from .conf import RedisKey, connection, settings
from .exceptions import TaskDoesNotExist
from .queue import Queue
from .smear_dst import DstSmearingTz
from .task import Task, TaskStatus
from .utils import (
    LazyObject, atomic_pipeline, decode_dict, utcformat, utcnow, utcparse)

logger = logging.getLogger(__name__)

local_tz = LazyObject(lambda: DstSmearingTz(settings.SCHEDULER_TIMEZONE))


class CrontabSchedule:
    def __init__(self, crontab):
        self.crontab = crontab

    def get_next(self, after):
        after = local_tz.from_utc(after)
        iter = croniter.croniter(self.crontab, after, ret_type=datetime.datetime)
        return local_tz.to_utc(iter.get_next())


crontab = CrontabSchedule


def once_per_day(time_str):
    hour, minute = time_str.split(':')
    return CrontabSchedule(f'{minute} {hour} * * *')


class PeriodicSchedule:
    def __init__(self, *, hours=0, minutes=0, seconds=0, start_at=None):
        self.interval = hours * 60 * 60 + minutes * 60 + seconds
        if start_at is None:
            start_at = random.uniform(0, self.interval)
        elif isinstance(start_at, str):
            hours, minutes = start_at.split(':')
            start_at = int(hours) * 60 * 60 + int(minutes) * 60
        self.start_at = start_at
        assert self.interval < 12 * 60 * 60
        assert self.start_at < 24 * 60 * 60

    def get_next(self, after):
        after = after.astimezone(local_tz.tz)
        midnight = local_tz.tz.localize(
            after.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None),
            is_dst=True)
        after_time = (after - midnight).total_seconds()
        if self.start_at > after_time:
            next_time = self.start_at
        else:
            since_start = after_time - self.start_at
            remaining = self.interval - since_start % self.interval
            next_time = after_time + remaining

        next = local_tz.tz.normalize(midnight + datetime.timedelta(seconds=next_time))
        if next.day != after.day:
            midnight = local_tz.tz.localize(
                after.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=None),
                is_dst=False) + datetime.timedelta(seconds=1)
            next = midnight + datetime.timedelta(seconds=self.start_at)
        return next.astimezone(datetime.timezone.utc)


run_every = PeriodicSchedule


class SchedulerEntry:
    def __init__(self, id, config):
        self.id = id
        self.key = RedisKey(f"schedule_entry:{self.id}")
        self.singleton = config.get('singleton', True)
        self.task_template = [config['task'],
                              config.get('args', ()), config.get('kwargs', {})]
        self.queue = Queue(config.get('queue', settings.SCHEDULER_QUEUE))
        self.schedule = config['schedule']

        self.last_save = None
        stored = decode_dict(connection.hgetall(self.key))
        prev_run = stored.get("prev_run")
        self.prev_run = utcparse(prev_run) if prev_run else utcnow()
        self.prev_task_id = stored.get("prev_task_id")

        self.next_run = self.schedule.get_next(self.prev_run)

        # Make sure task config is valid
        Task(*self.task_template)

    @atomic_pipeline
    def save(self, *, pipeline):
        pipeline.hset(self.key, "prev_run", utcformat(self.prev_run))
        if self.prev_task_id:
            pipeline.hset(self.key, "prev_task_id", self.prev_task_id)
        else:
            pipeline.hdel(self.key, "prev_task_id")
        ttl = max(24 * 60 * 60, settings.SCHEDULER_MAX_CATCHUP * 5)
        pipeline.expire(self.key, ttl)
        self.last_save = utcnow()

    @atomic_pipeline
    def process(self, now, *, pipeline):
        max_catchup = now - datetime.timedelta(seconds=settings.SCHEDULER_MAX_CATCHUP)
        self.next_run = self.schedule.get_next(max(max_catchup, self.prev_run))
        if self.next_run > now:
            if (not self.last_save or
                    (now - self.last_save).total_seconds() >= settings.SCHEDULER_MAX_CATCHUP):
                self.save(pipeline=pipeline)
            return

        self.prev_run = now

        if self.singleton:
            self.next_run = self.schedule.get_next(now)
            if self.is_enqueued():
                logger.info(f'Schedule entry "{self.id}" already enqueued or running, skipping')
            else:
                self.enqueue(pipeline=pipeline)
        else:
            while self.next_run <= now:
                self.enqueue(pipeline=pipeline)
                self.next_run = self.schedule.get_next(self.next_run)
        self.save(pipeline=pipeline)

    def is_enqueued(self):
        if self.prev_task_id:
            with suppress(TaskDoesNotExist):
                prev_task = Task.fetch(self.prev_task_id)
                if prev_task.status not in [TaskStatus.FINISHED, TaskStatus.FAILED]:
                    return True
        return False

    @atomic_pipeline
    def enqueue(self, *, pipeline):
        task = self.queue.enqueue_call(*self.task_template, pipeline=pipeline)
        self.prev_task_id = task.id
        return task


class Scheduler:
    def __init__(self):
        self.schedule = [SchedulerEntry(k, v) for k, v in settings.SCHEDULE.items()]
        self.shutdown_requested = threading.Event()

    def setup_signal_handler(self):
        def stop(signum, frame):
            logger.info('Initiating redis_tasks scheduler shutdown')
            self.shutdown_requested.set()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)

    def run(self):
        if not self.schedule:
            logger.error("No schedule configured, nothing to do")
            return

        self.setup_signal_handler()

        HEARTBEAT_FREQ = 10
        with Mutex(timeout=HEARTBEAT_FREQ + 2) as mutex:
            logger.info('redis_tasks scheduler started')
            while not self.shutdown_requested.is_set():
                mutex.extend()
                now = utcnow()
                for entry in self.schedule:
                    entry.process(now)

                next_run = min(x.next_run for x in self.schedule)
                next_heartbeat = now + datetime.timedelta(seconds=HEARTBEAT_FREQ)
                wait_for = (utcnow() - min(next_run, next_heartbeat)).total_seconds()
                self.shutdown_requested.wait(wait_for)
        logger.info('redis_tasks scheduler shut down')


def scheduler_main():
    Scheduler().run()


class Mutex(object):
    expire_script = None

    def __init__(self, *, timeout):
        self.key = RedisKey('scheduler')
        self.timeout = timeout
        self.token = None

        # KEYS[1]: lock key, ARGS[1]: token, ARGS[2]: milliseconds
        # return 1 if the lock was held and the expire executed, otherwise 0
        self.expire_script = connection.register_script("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('pexpire', KEYS[1], ARGV[2])
            else
                return 0
            end""")

    def __enter__(self):
        if not self.acquire(wait=False):
            wait_for = self.timeout + 1
            logger.warning("Found signs of an already running scheduler instance, "
                           f"waiting {wait_for} seconds for it to disappear")
            try:
                self.acquire(wait=wait_for)
            except TimeoutError:
                raise RuntimeError("redis_tasks scheduler already running")
        return self

    def __exit__(self, *exc):
        if self.token:
            self.expire_script(keys=[self.key], args=[self.token, 0])
            self.token = None

    def acquire(self, wait=None):
        token = str(uuid.uuid1()).encode()
        stop_trying_at = time.time() + wait
        while True:
            acquired = connection.set(self.key, token, nx=True, px=int(self.timeout * 1000))
            if acquired:
                self.token = token
                return True
            elif not wait:
                return False
            elif time.time() > stop_trying_at:
                raise TimeoutError
            time.sleep(0.1)

    def extend(self):
        if not self.expire_script(keys=[self.key], args=[self.token, int(self.timeout * 1000)]):
            raise RuntimeError("Cannot refresh a lock that's no longer owned")
