import datetime
import time
from concurrent import futures

import pytest
import pytz

from redis_tasks import Queue
from redis_tasks.scheduler import (
    CrontabSchedule, Mutex, PeriodicSchedule, Scheduler, SchedulerEntry)
from redis_tasks.task import TaskOutcome
from redis_tasks.utils import one, utcnow
from tests.utils import WorkerFactory


def iter_schedule(schedule, start, count):
    for i in range(count):
        start = schedule.get_next(start)
        yield start


def test_crontab_schedule(settings):
    settings.TIMEZONE = "Europe/Berlin"
    tz = pytz.timezone('Europe/Berlin')
    schedule = CrontabSchedule('30 2 * * *')
    start = datetime.datetime(2017, 3, 24, tzinfo=pytz.utc)
    assert list(dt.astimezone(tz) for dt in iter_schedule(schedule, start, 5)) == [
        tz.localize(dt, is_dst=None) for dt in [
            datetime.datetime(2017, 3, 24, 2, 30),
            datetime.datetime(2017, 3, 25, 2, 30),
            # DST Transition
            datetime.datetime(2017, 3, 26, 3, 0),
            datetime.datetime(2017, 3, 27, 2, 30),
            datetime.datetime(2017, 3, 28, 2, 30),
        ]]

    start = datetime.datetime(2017, 10, 27, tzinfo=pytz.utc)
    assert list(dt.astimezone(tz) for dt in iter_schedule(schedule, start, 5)) == [
        tz.localize(datetime.datetime(2017, 10, 27, 2, 30)),
        tz.localize(datetime.datetime(2017, 10, 28, 2, 30), is_dst=True),
        tz.localize(datetime.datetime(2017, 10, 29, 2, 0), is_dst=False),
        tz.localize(datetime.datetime(2017, 10, 30, 2, 30), is_dst=False),
        tz.localize(datetime.datetime(2017, 10, 31, 2, 30)),
    ]


def test_periodic_schedule(settings):
    settings.TIMEZONE = "Europe/Berlin"
    tz = pytz.timezone('Europe/Berlin')

    # Test transition to DST
    schedule = PeriodicSchedule(minutes=90, start_at="01:00")
    start = tz.localize(datetime.datetime(2017, 3, 26))
    assert list(dt.astimezone(tz) for dt in iter_schedule(schedule, start, 6)) == [
        tz.localize(dt, is_dst=None) for dt in [
            datetime.datetime(2017, 3, 26, 1, 0),
            datetime.datetime(2017, 3, 26, 3, 30),
            datetime.datetime(2017, 3, 26, 5, 0),
            datetime.datetime(2017, 3, 26, 6, 30),
            datetime.datetime(2017, 3, 26, 8, 0),
            datetime.datetime(2017, 3, 26, 9, 30),
        ]]

    # Test night-wrap
    schedule = PeriodicSchedule(minutes=17, start_at="23:00")
    start = tz.localize(datetime.datetime(2017, 4, 1, 22))
    assert list(dt.astimezone(tz) for dt in iter_schedule(schedule, start, 6)) == [
        tz.localize(dt, is_dst=None) for dt in [
            datetime.datetime(2017, 4, 1, 23, 00),
            datetime.datetime(2017, 4, 1, 23, 17),
            datetime.datetime(2017, 4, 1, 23, 34),
            datetime.datetime(2017, 4, 1, 23, 51),
            datetime.datetime(2017, 4, 2, 23, 00),
            datetime.datetime(2017, 4, 2, 23, 17),
        ]]

    assert (schedule.get_next(tz.localize(datetime.datetime(2017, 4, 1, 23, 10))) ==
            tz.localize(datetime.datetime(2017, 4, 1, 23, 17)))

    # Test transition from DST
    schedule = PeriodicSchedule(minutes=40, start_at="01:00")
    start = tz.localize(datetime.datetime(2017, 10, 29, 0))
    assert list(dt.astimezone(tz) for dt in iter_schedule(schedule, start, 6)) == [
        tz.localize(datetime.datetime(2017, 10, 29, 1, 00)),
        tz.localize(datetime.datetime(2017, 10, 29, 1, 40), is_dst=True),
        tz.localize(datetime.datetime(2017, 10, 29, 2, 20), is_dst=True),
        tz.localize(datetime.datetime(2017, 10, 29, 2, 00), is_dst=False),
        tz.localize(datetime.datetime(2017, 10, 29, 2, 40), is_dst=False),
        tz.localize(datetime.datetime(2017, 10, 29, 3, 20)),
    ]


class TestSchedulerEntry:
    def time(self, *args):
        return datetime.datetime(2000, 1, 1, *args, tzinfo=pytz.utc)

    def test_sanity_check(self):
        schedule = CrontabSchedule('* * * * *')
        with pytest.raises(ValueError):
            SchedulerEntry("a", {"task": "broken", "schedule": schedule})

    def test_init_and_save(self, time_mocker, connection, settings,
                           assert_atomic, mocker, stub):
        time = time_mocker('redis_tasks.scheduler.utcnow')
        schedule = CrontabSchedule('* * * * *')
        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule})
        assert se.id == "a"
        assert se.prev_run == time.now
        assert se.next_run and se.next_run != se.prev_run
        assert se.prev_task_id is None
        time.step()
        with assert_atomic():
            se.save()
        assert se.last_save == time.now

        time.step()
        loaded = SchedulerEntry("a", {"task": stub.path, "schedule": schedule})
        assert loaded.prev_run == se.prev_run
        assert loaded.next_run == se.next_run
        assert loaded.prev_task_id is None

        se.prev_task_id = "b"
        se.save()
        loaded = SchedulerEntry("a", {"task": stub.path, "schedule": schedule})
        assert loaded.prev_task_id == "b"

        settings.SCHEDULER_MAX_CATCHUP = 60
        se.save()
        assert connection.ttl(se.key) == 24 * 60 * 60
        settings.SCHEDULER_MAX_CATCHUP = 24 * 60 * 60
        se.save()
        assert connection.ttl(se.key) == 24 * 60 * 60 * 5

        assert se.singleton is True
        assert se.queue.name == "default"

        se = SchedulerEntry("a", {
            "task": stub.path,
            "schedule": schedule,
            "singleton": False,
            "queue": "myq",
        })
        assert se.singleton is False
        assert se.queue.name == "myq"

    def test_enqueue(self, assert_atomic, stub):
        schedule = CrontabSchedule('* * * * *')
        queue = Queue()
        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule})
        with assert_atomic():
            task = se.enqueue()
        assert task.origin == "default"
        assert task._get_func() == stub

        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule,
                                  "args": ["a"], "kwargs": {"c": "d"}})

        task = se.enqueue()
        assert task.args == ["a"]
        assert task.kwargs == {"c": "d"}
        assert queue.count() == 2

    def test_is_enqueued(self, stub):
        schedule = CrontabSchedule('* * * * *')
        queue = Queue()
        worker = WorkerFactory()
        worker.startup()
        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule})
        assert se.is_enqueued() is False

        task = se.enqueue()
        assert se.is_enqueued() is True
        task.cancel()
        assert se.is_enqueued() is False

        se.enqueue()
        assert se.is_enqueued() is True
        task = queue.dequeue(worker)
        assert se.is_enqueued() is True
        worker.start_task(task)
        assert se.is_enqueued() is True
        worker.end_task(task, TaskOutcome("success"))
        assert se.is_enqueued() is False

        se.enqueue()
        task = queue.dequeue(worker)
        worker.start_task(task)
        assert se.is_enqueued() is True
        worker.end_task(task, TaskOutcome("failure"))
        assert se.is_enqueued() is False

    def test_process_singleton(self, mocker, settings, stub):
        queue = Queue()
        settings.SCHEDULER_MAX_CATCHUP = 60 * 60 * 24
        schedule = mocker.Mock()
        get_next = schedule.get_next
        get_next.return_value = self.time(1)
        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule,
                                  "singleton": True})
        se.prev_run = self.time(0)

        get_next.reset_mock()
        se.process(self.time(0))
        assert se.next_run == self.time(1)
        get_next.assert_called_once_with(self.time(0))
        assert se.is_enqueued() is False

        se.process(self.time(1))
        assert se.is_enqueued() is True
        se.process(self.time(2))
        assert queue.count() == 1
        queue.empty()

        assert se.is_enqueued() is False
        se.process(self.time(2))
        assert se.is_enqueued() is True

        queue.empty()
        get_next.reset_mock()
        get_next.side_effect = [self.time(1), self.time(3)]
        se.prev_run = self.time(0)
        se.process(self.time(2))
        assert queue.count() == 1
        assert get_next.call_count == 2
        get_next.assert_any_call(self.time(0))
        get_next.assert_called_with(self.time(2))
        assert se.next_run == self.time(3)

    def test_process_max_catchup(self, mocker, settings, stub):
        settings.SCHEDULER_MAX_CATCHUP = 60 * 60 * 1
        schedule = mocker.Mock()
        get_next = schedule.get_next
        get_next.return_value = self.time(4)
        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule,
                                  "singleton": True})
        get_next.reset_mock()
        se.prev_run = self.time(0)
        se.process(self.time(2))
        get_next.assert_called_once_with(self.time(1))
        assert se.is_enqueued() is False

    def test_process_non_singleton(self, mocker, settings, stub):
        queue = Queue()
        settings.SCHEDULER_MAX_CATCHUP = 60 * 60 * 24
        schedule = mocker.Mock()
        get_next = schedule.get_next
        get_next.return_value = self.time(2)
        se = SchedulerEntry("a", {"task": stub.path, "schedule": schedule,
                                  "singleton": False})

        get_next.reset_mock()
        se.prev_run = self.time(0)
        se.process(self.time(1))
        get_next.assert_called_once_with(self.time(0))
        assert se.next_run == self.time(2)
        assert se.is_enqueued() is False
        assert se.prev_run == self.time(0)

        get_next.reset_mock()
        get_next.side_effect = [self.time(2), self.time(3)]
        se.process(self.time(2))
        assert se.is_enqueued() is True
        assert get_next.call_count == 2
        get_next.assert_any_call(self.time(0))
        get_next.assert_any_call(self.time(2))
        assert se.prev_run == self.time(2)

        queue.empty()
        get_next.reset_mock()
        get_next.side_effect = [self.time(3), self.time(4), self.time(5)]
        se.process(self.time(4))
        assert se.next_run == self.time(5)
        assert queue.count() == 2
        assert get_next.call_count == 3


class TestMutex:
    def test_basic(self, caplog):
        with Mutex(timeout=1) as m:
            m.extend()
        assert not caplog.records

    @pytest.mark.slow
    def test_blocked(self, caplog):
        with Mutex(timeout=3):
            with pytest.raises(RuntimeError):
                with Mutex(timeout=0.1):
                    pass
        assert 'already running' in one(caplog.records).message

    @pytest.mark.slow
    def test_unblocked(self, caplog):
        with Mutex(timeout=0.01):
            with Mutex(timeout=0.01):
                assert 'already running' in one(caplog.records).message

    @pytest.mark.slow
    def test_extend(self):
        with Mutex(timeout=0.02) as m:
            time.sleep(0.01)
            m.extend()
            time.sleep(0.01)
            m.extend()
            time.sleep(0.01)
            m.extend()

    def test_missed_extend(self, caplog):
        with Mutex(timeout=0.001) as m:
            time.sleep(0.002)
            with pytest.raises(RuntimeError):
                m.extend()


class FixedSchedule:
    def __init__(self, times):
        self.times = times

    def get_next(self, after):
        return next(t for t in self.times if t > after)


class TestScheduler:
    def test_basic(self, settings, mocker, time_mocker, stub):
        mocktime = time_mocker('redis_tasks.scheduler.utcnow')
        schedule = mocker.Mock()
        schedule.get_next.return_value = mocktime.now
        settings.SCHEDULE = {'mytask': {
            'task': stub.path,
            'schedule': schedule,
        }}
        queue = Queue()

        def stop_scheduler():
            time.sleep(0.02)
            scheduler.shutdown_requested.set()

        scheduler = Scheduler()
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(stop_scheduler)
            scheduler.run()
        assert queue.count() == 1

    def test_scheduling(self, settings, mocker, stub):
        in_ms = lambda ms: utcnow() + datetime.timedelta(milliseconds=ms)  # noqa
        settings.SCHEDULE = {
            'a': {'task': stub.path,
                  'args': ["a"],
                  'singleton': False,
                  'schedule': FixedSchedule([in_ms(10), in_ms(30), in_ms(1000)])},
            'b': {'task': stub.path,
                  'args': ["b"],
                  'singleton': False,
                  'schedule': FixedSchedule([in_ms(20), in_ms(1000)])},
        }
        queue = Queue()

        def stop_scheduler():
            time.sleep(0.04)
            scheduler.shutdown_requested.set()

        scheduler = Scheduler()
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(stop_scheduler)
            scheduler.run()
        assert [t.args for t in queue.get_tasks()] == [["a"], ["b"], ["a"]]
