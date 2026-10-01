"""Working days in Spain: weekends, national holidays, an optional region and extra dates.

Administrative deadlines (Ley 39/2015, art. 30) count only working days: Saturdays, Sundays and holidays are left out.
``Calendar`` answers "is this a working day", moves a date to the next working day and adds working days.
"""

from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache
from typing import Iterable

REGIONS = ("", "ES-MD", "ES-CT", "ES-AN", "ES-VC", "ES-GA", "ES-PV")


def easter(year: int) -> date:
    """Gregorian Easter Sunday (anonymous algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


@lru_cache(maxsize=64)
def holidays(year: int, region: str = "") -> frozenset[date]:
    """National holidays in Spain plus the most stable regional ones (moved Sunday holidays are not modelled)."""
    e = easter(year)
    days = {date(year, 1, 1), date(year, 1, 6), e - timedelta(days=2), date(year, 5, 1), date(year, 8, 15), date(year, 10, 12),
            date(year, 11, 1), date(year, 12, 6), date(year, 12, 8), date(year, 12, 25)}
    region = (region or "").upper()
    if region in ("ES-MD", "ES-AN", "ES-GA", "ES-PV", "ES-VC", "ES-CT"):
        if region != "ES-CT":
            days.add(e - timedelta(days=3))          # Holy Thursday
        else:
            days.add(e + timedelta(days=1))          # Easter Monday
    extra = {"ES-MD": [(5, 2)], "ES-CT": [(6, 24), (9, 11), (12, 26)], "ES-AN": [(2, 28)], "ES-VC": [(3, 19), (10, 9)],
             "ES-GA": [(5, 17), (7, 25)], "ES-PV": [(7, 25)]}
    for month, day in extra.get(region, []):
        days.add(date(year, month, day))
    return frozenset(days)


class Calendar:
    def __init__(self, region: str = "", extra: Iterable[date] = ()):
        self.region = region if region in REGIONS else ""
        self.extra = frozenset(extra)

    def is_holiday(self, day: date) -> bool:
        return day in holidays(day.year, self.region) or day in self.extra

    def is_working_day(self, day: date) -> bool:
        return day.weekday() < 5 and not self.is_holiday(day)

    def next_working_day(self, day: date) -> date:
        """``day`` itself when it is a working day, else the first working day after it."""
        for _ in range(60):
            if self.is_working_day(day):
                return day
            day += timedelta(days=1)
        return day

    def add_working_days(self, start: date, days: int) -> date:
        """The ``days``-th working day after ``start`` (counting starts the day after ``start``)."""
        current, remaining = start, max(0, int(days))
        while remaining > 0:
            current += timedelta(days=1)
            if self.is_working_day(current):
                remaining -= 1
        return current

    def count_working_days(self, start: date, end: date) -> int:
        """Working days after ``start`` up to and including ``end`` (0 when end <= start)."""
        n, current = 0, start
        while current < end:
            current += timedelta(days=1)
            if self.is_working_day(current):
                n += 1
        return n
