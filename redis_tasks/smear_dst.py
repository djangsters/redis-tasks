import bisect
import datetime
from collections import namedtuple

import pytz
from pytz.tzinfo import DstTzInfo

Transition = namedtuple('Transition', [
    'start', 'end',
    'utc_start', 'utc_end',
    'old_utcoffset', 'new_utcoffset'])


class DstSmearingTz:
    def __init__(self, name):
        self._transition_times = []
        self._transition_times_utc = []
        self._transition_infos = []

        self.tz = pytz.timezone(name)
        if isinstance(self.tz, DstTzInfo):
            self._load_transitions()

    def _load_transitions(self):
        prev_utcoffset = None
        HOUR = datetime.timedelta(hours=1)
        for transition_time, (utcoffset, dstoffset, tzname) in \
                zip(self.tz._utc_transition_times, self.tz._transition_info):
            if prev_utcoffset is not None:
                if abs((prev_utcoffset - utcoffset).total_seconds()) == 3600:
                    if prev_utcoffset < utcoffset:
                        start = transition_time + prev_utcoffset - HOUR
                        end = transition_time + utcoffset + HOUR
                    else:
                        start = transition_time + utcoffset
                        end = transition_time + prev_utcoffset
                    transition = Transition(
                        start=start, end=end,
                        utc_start=transition_time - HOUR, utc_end=transition_time + HOUR,
                        old_utcoffset=prev_utcoffset, new_utcoffset=utcoffset,
                    )
                    self._transition_times.append(start)
                    self._transition_times.append(end)
                    self._transition_times_utc.append(transition.utc_start)
                    self._transition_times_utc.append(transition.utc_end)
                    self._transition_infos.append((True, transition))
                    self._transition_infos.append((False, transition))
            prev_utcoffset = utcoffset

    def from_utc(self, utc):
        if utc.utcoffset() is None or utc.utcoffset().total_seconds() != 0:
            raise ValueError('Datetime is not in utc')
        if not self._transition_infos:
            return utc.astimezone(self.tz).replace(tzinfo=None)
        utc = utc.replace(tzinfo=None)
        idx = bisect.bisect_right(self._transition_times_utc, utc) - 1
        in_trans, transition = self._transition_infos[idx]
        if not in_trans:
            return utc + transition.new_utcoffset
        else:
            percentage = (utc - transition.utc_start) / (transition.utc_end - transition.utc_start)
            utcoffset = (transition.old_utcoffset * (1 - percentage) +
                         transition.new_utcoffset * percentage)
            return utc + utcoffset

    def to_utc(self, dt):
        if dt.tzinfo is not None:
            raise ValueError('Not naive datetime (tzinfo is already set)')
        if not self._transition_infos:
            return self.tz.localize(dt).astimezone(pytz.UTC)
        idx = bisect.bisect_right(self._transition_times, dt) - 1
        in_trans, transition = self._transition_infos[idx]
        if not in_trans:
            utc = dt - transition.new_utcoffset
        else:
            percentage = (dt - transition.start) / (transition.end - transition.start)
            utcoffset = (transition.old_utcoffset * (1 - percentage) +
                         transition.new_utcoffset * percentage)
            utc = dt - utcoffset
        return utc.replace(tzinfo=pytz.UTC)
