"""Working days in Spain for deadlines: weekends, national and regional holidays, extra dates.

Administrative deadlines (Ley 39/2015, art. 30) count only working days: Saturdays, Sundays and holidays are left out.
The tables and the arithmetic are the family's (``hoard_link.bizdays``); this module keeps the names Kafka's rules use
(``is_working_day``, ``next_working_day``, ``add_working_days``, ``count_working_days``) and the empty region for «national only».
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from .hoard_link import bizdays as shared

REGIONS = ("", "ES-MD", "ES-CT", "ES-AN", "ES-VC", "ES-GA", "ES-PV")

easter = shared.easter


def holidays(year: int, region: str = "") -> frozenset[date]:
    """National holidays in Spain plus those of the region (a fixed holiday that falls on a Sunday moves to Monday where the community does so)."""
    region = (region or "").upper()
    return frozenset(shared.holidays(year, region if region in REGIONS and region else "ES"))


class Calendar(shared.Calendar):
    def __init__(self, region: str = "", extra: Iterable[date] = ()):
        region = region if region in REGIONS else ""
        super().__init__(region or "ES", extra)
        self.region = region

    def is_holiday(self, day: date) -> bool:
        return super().is_holiday(day)

    def is_working_day(self, day: date) -> bool:
        return self.is_business_day(day)

    def next_working_day(self, day: date) -> date:
        """``day`` itself when it is a working day, else the first working day after it."""
        return self.next_business_day(day, include_self=True)

    def add_working_days(self, start: date, days: int) -> date:
        """The ``days``-th working day after ``start`` (counting starts the day after ``start``)."""
        return self.add_business_days(start, max(0, int(days)))

    def count_working_days(self, start: date, end: date) -> int:
        """Working days after ``start`` up to and including ``end`` (0 when end <= start)."""
        if end <= start:
            return 0
        return self.business_days_between(start + timedelta(days=1), end + timedelta(days=1))
