"""The Spanish rules Kafka applies when it turns dates into deadlines. Pure functions; every rule is documented in docs/RULES.md.

* Ley 39/2015, art. 30: administrative days are working days unless the text says «naturales»; Saturdays, Sundays and holidays
  do not count; counting starts the day after notification; months and years run «de fecha a fecha»; a last day that is not a
  working day moves to the next working day.
* Ley 50/1980, art. 22 (as amended by Ley 20/2015): the policyholder opposes a renewal with at least one month's written notice;
  the insurer, with two.
* Real Decreto Legislativo 7/2021 (consumer law): legal guarantee of 3 years for new goods, from delivery.
* dgt.es: 20 natural days from notification to pay a fine with the 50 % reduction or to make allegations.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from ..bizdays import Calendar
from ..util import add_months

UNITS = ("day", "month", "year")
FINE_DISCOUNT_DAYS = 20
INSURANCE_NOTICE_MONTHS = 1
DEFAULT_WARRANTY_YEARS = 3


def admin_deadline(base: date, n: int, unit: str, working: bool, cal: Calendar) -> date:
    """Last day of a period of ``n`` ``unit``s that starts the day after ``base`` (the notification).

    * ``day`` + ``working``: the n-th working day after ``base`` (weekends and holidays skipped).
    * ``day`` + not working: ``base`` + n natural days.
    * ``month`` / ``year``: the same day number ``n`` months (years) later, or the last day of the month when it has no such day.
    In every case a last day that is not a working day moves to the next working day.
    """
    if unit not in UNITS:
        raise ValueError(f"unit must be one of {UNITS}")
    if n < 0:
        raise ValueError("n must not be negative")
    if unit == "day":
        end = cal.add_working_days(base, n) if working else base + timedelta(days=n)
    elif unit == "month":
        end = add_months(base, n)
    else:
        end = add_months(base, 12 * n)
    return cal.next_working_day(end)


def fine_discount_deadline(notified: date, cal: Calendar) -> date:
    """DGT fines: 20 natural days from the notification to pay with the 50 % reduction or to make allegations."""
    return admin_deadline(notified, FINE_DISCOUNT_DAYS, "day", False, cal)


def insurance_cancel_by(renewal: date, months: int = INSURANCE_NOTICE_MONTHS) -> date:
    """Ley 50/1980 art. 22: the policyholder must give written notice at least one month before the period ends."""
    return add_months(renewal, -months)


def warranty_end(base: date, years: float = DEFAULT_WARRANTY_YEARS, months: Optional[int] = None) -> date:
    """Legal guarantee: ``years`` (default 3) from delivery, or an explicit number of months."""
    total = months if months is not None else int(round(years * 12))
    return add_months(base, total)


def period_end(start: date, n: int, unit: str) -> date:
    """End of a contractual period (permanence, commitment): calendar arithmetic, no working-day shifting."""
    if unit == "day":
        return start + timedelta(days=n)
    return add_months(start, n if unit == "month" else 12 * n)


def moved(original: date, final: date) -> bool:
    return final != original
