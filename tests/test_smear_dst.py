import datetime

import pytz

from redis_tasks.smear_dst import DstSmearingTz


def test_utc():
    tz = DstSmearingTz('UTC')
    dt = datetime.datetime(2000, 1, 1, 12, 1, 5)
    assert tz.to_utc(dt) == dt.replace(tzinfo=pytz.UTC)
    assert tz.from_utc(dt.replace(tzinfo=pytz.UTC)) == dt


def test_japan():
    tz = DstSmearingTz('Asia/Tokyo')
    dt = datetime.datetime(2000, 1, 1, 12, 1, 5)
    assert tz.from_utc(tz.to_utc(dt)) == dt


def test_germany():
    tz = DstSmearingTz('Europe/Berlin')

    dt = datetime.datetime(2017, 1, 1, 18, 30, 23)
    assert tz.to_utc(dt) == datetime.datetime(2017, 1, 1, 17, 30, 23, tzinfo=pytz.UTC)
    assert tz.from_utc(tz.to_utc(dt)) == dt

    dt = datetime.datetime(2017, 3, 26, 2, 17, 30)
    assert tz.to_utc(dt) == datetime.datetime(2017, 3, 26, 0, 51, 40, tzinfo=pytz.UTC)
    assert tz.from_utc(tz.to_utc(dt)) == dt

    dt = datetime.datetime(2017, 10, 29, 2, 30, 0)
    assert tz.to_utc(dt) == datetime.datetime(2017, 10, 29, 1, 0, 0, tzinfo=pytz.UTC)
    assert tz.from_utc(tz.to_utc(dt)) == dt
