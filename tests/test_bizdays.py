"""Spanish working days: weekends, national and regional holidays, extra dates."""

from datetime import date

from kafka_hoard.bizdays import Calendar, easter, holidays


def test_easter_dates():
    assert easter(2026) == date(2026, 4, 5) and easter(2027) == date(2027, 3, 28) and easter(2025) == date(2025, 4, 20)


def test_national_holidays_2026():
    h = holidays(2026, "")
    for d in (date(2026, 1, 1), date(2026, 1, 6), date(2026, 4, 3), date(2026, 5, 1), date(2026, 8, 15), date(2026, 10, 12),
              date(2026, 11, 1), date(2026, 12, 6), date(2026, 12, 8), date(2026, 12, 25)):
        assert d in h, d
    assert date(2026, 4, 2) not in h


def test_regional_holidays():
    assert date(2026, 5, 2) in holidays(2026, "ES-MD") and date(2026, 5, 2) not in holidays(2026, "")
    assert date(2026, 4, 6) in holidays(2026, "ES-CT") and date(2026, 9, 11) in holidays(2026, "ES-CT")
    assert date(2026, 4, 2) in holidays(2026, "ES-MD") and date(2026, 4, 2) not in holidays(2026, "ES-CT")


def test_working_days_and_next():
    cal = Calendar("ES-MD")
    assert cal.is_working_day(date(2026, 10, 1)) and not cal.is_working_day(date(2026, 10, 3)) and not cal.is_working_day(date(2026, 10, 12))
    assert cal.next_working_day(date(2026, 10, 3)) == date(2026, 10, 5)
    assert cal.next_working_day(date(2026, 10, 12)) == date(2026, 10, 13)
    assert cal.next_working_day(date(2026, 10, 1)) == date(2026, 10, 1)


def test_add_working_days_counts_from_the_next_day():
    cal = Calendar("ES-MD")
    assert cal.add_working_days(date(2026, 9, 28), 10) == date(2026, 10, 13)       # 12 Oct is a holiday
    assert cal.add_working_days(date(2026, 10, 1), 0) == date(2026, 10, 1)
    assert cal.add_working_days(date(2026, 10, 2), 1) == date(2026, 10, 5)


def test_count_working_days():
    cal = Calendar("")
    assert cal.count_working_days(date(2026, 10, 1), date(2026, 10, 9)) == 6
    assert cal.count_working_days(date(2026, 10, 9), date(2026, 10, 1)) == 0


def test_extra_holidays_and_unknown_region():
    cal = Calendar("ES-MD", [date(2026, 10, 2)])
    assert not cal.is_working_day(date(2026, 10, 2)) and cal.next_working_day(date(2026, 10, 2)) == date(2026, 10, 5)
    assert Calendar("ZZ-99").region == ""
