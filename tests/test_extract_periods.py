"""Relative periods in the text and the Spanish counting rules."""

from datetime import date

import pytest

from kafka_hoard.bizdays import Calendar, easter, holidays
from kafka_hoard.extract import periods, rules
from kafka_hoard.util import add_months, fold


def one(text):
    found = periods.find_periods(fold(text))
    assert found, text
    return found[0]


@pytest.mark.parametrize("text,n,unit,working", [
    ("en el plazo de diez (10) días hábiles", 10, "day", True),
    ("dispone de quince días", 15, "day", None),
    ("20 días naturales desde la notificación", 20, "day", False),
    ("en un mes", 1, "month", None),
    ("plazo de dos meses", 2, "month", None),
    ("permanencia de 12 meses", 12, "month", None),
    ("compromiso de 24 meses", 24, "month", None),
    ("garantía de 3 años", 3, "year", None),
    ("treinta días hábiles", 30, "day", True),
    ("veintiún días naturales", 21, "day", False),
    ("two weeks", 14, "day", None),
    ("30 working days", 30, "day", True),
    ("una semana", 7, "day", None),
])
def test_relative_periods(text, n, unit, working):
    p = one(text)
    assert (p.n, p.unit, p.working) == (n, unit, working)


def test_purpose_and_base_are_guessed_from_the_words_around():
    assert one("20 días naturales para el pronto pago con reducción del 50 %").purpose == "fine_pay"
    assert one("plazo de diez días hábiles para presentar alegaciones desde el día siguiente a la notificación").purpose == "appeal"
    assert one("compromiso de permanencia de 12 meses desde la fecha de efecto").purpose == "permanence"
    assert one("compromiso de permanencia de 12 meses desde la fecha de efecto").base == "effect"
    assert one("garantía de 3 años desde la entrega").purpose == "warranty"


def test_phone_numbers_and_page_counts_are_not_periods():
    assert periods.find_periods(fold("llame al 91 123 45 67 o consulte la página 3 de 10")) == []


# ---------------------------------------------------------------- holidays and working days
def test_easter_dates():
    assert easter(2026) == date(2026, 4, 5)
    assert easter(2027) == date(2027, 3, 28)


def test_national_and_madrid_holidays():
    h = holidays(2026, "ES-MD")
    for d in (date(2026, 1, 1), date(2026, 1, 6), date(2026, 4, 3), date(2026, 5, 1), date(2026, 8, 15), date(2026, 10, 12), date(2026, 12, 8), date(2026, 12, 25)):
        assert d in h, d
    assert date(2026, 5, 2) in h                                        # Comunidad de Madrid
    assert date(2026, 3, 19) not in h                                   # San José is not a Madrid holiday


def test_calendar_working_days():
    cal = Calendar("ES-MD")
    assert not cal.is_working_day(date(2026, 10, 10))        # Saturday
    assert not cal.is_working_day(date(2026, 10, 11))        # Sunday
    assert not cal.is_working_day(date(2026, 10, 12))        # Monday, Fiesta Nacional
    assert cal.is_working_day(date(2026, 10, 13))
    assert cal.next_working_day(date(2026, 10, 10)) == date(2026, 10, 13)
    assert cal.next_working_day(date(2026, 10, 13)) == date(2026, 10, 13)


def test_calendar_extra_holidays():
    cal = Calendar("ES-MD", [date(2026, 10, 13)])
    assert not cal.is_working_day(date(2026, 10, 13))
    assert cal.next_working_day(date(2026, 10, 10)) == date(2026, 10, 14)


# ---------------------------------------------------------------- Ley 39/2015, art. 30
CAL = Calendar("ES-MD")


def test_working_days_start_the_day_after_and_skip_weekends():
    # notified on Wednesday 7 Oct 2026: 3 working days = Thu 8, Fri 9, (Mon 12 is a holiday) Tue 13
    assert rules.admin_deadline(date(2026, 10, 7), 3, "day", True, CAL) == date(2026, 10, 13)


def test_friday_base_counts_from_monday():
    # Friday 2 Oct 2026 + 5 working days = Mon 5, Tue 6, Wed 7, Thu 8, Fri 9
    assert rules.admin_deadline(date(2026, 10, 2), 5, "day", True, CAL) == date(2026, 10, 9)


def test_ten_working_days_over_a_holiday():
    # notified Mon 28 Sep 2026: 29, 30, 1, 2, 5, 6, 7, 8, 9, (12 holiday) 13
    assert rules.admin_deadline(date(2026, 9, 28), 10, "day", True, CAL) == date(2026, 10, 13)


def test_natural_days_and_a_last_day_on_the_weekend_move_to_monday():
    # 13 Sep 2026 + 20 natural days = Sat 3 Oct -> Monday 5 Oct
    assert rules.admin_deadline(date(2026, 9, 13), 20, "day", False, CAL) == date(2026, 10, 5)
    # 12 Sep + 20 = Fri 2 Oct: no move
    assert rules.admin_deadline(date(2026, 9, 12), 20, "day", False, CAL) == date(2026, 10, 2)


def test_natural_days_ending_on_a_holiday_move_on():
    # 22 Sep 2026 + 20 = Sun 12 Oct... Monday 12 Oct is a holiday -> Tuesday 13 Oct
    assert rules.admin_deadline(date(2026, 9, 22), 20, "day", False, CAL) == date(2026, 10, 13)


def test_months_run_from_date_to_date():
    assert rules.admin_deadline(date(2026, 3, 11), 2, "month", False, CAL) == date(2026, 5, 11)


def test_31_january_plus_one_month_is_the_last_day_of_february():
    # 28 Feb 2026 is a Saturday: it moves to Monday 2 Mar
    assert add_months(date(2026, 1, 31), 1) == date(2026, 2, 28)
    assert rules.admin_deadline(date(2026, 1, 31), 1, "month", False, CAL) == date(2026, 3, 2)
    # 29 Feb 2028 exists
    assert add_months(date(2028, 1, 31), 1) == date(2028, 2, 29)


def test_years():
    assert rules.admin_deadline(date(2026, 10, 1), 1, "year", False, CAL) == date(2027, 10, 1)


def test_zero_days_and_invalid_input():
    assert rules.admin_deadline(date(2026, 10, 7), 0, "day", True, CAL) == date(2026, 10, 7)
    with pytest.raises(ValueError):
        rules.admin_deadline(date(2026, 10, 7), 3, "fortnight", True, CAL)
    with pytest.raises(ValueError):
        rules.admin_deadline(date(2026, 10, 7), -1, "day", True, CAL)


def test_dgt_twenty_natural_days():
    # 14 Sep + 20 = Sun 4 Oct -> Mon 5 Oct
    assert rules.fine_discount_deadline(date(2026, 9, 14), CAL) == date(2026, 10, 5)
    # 10 Sep + 20 = Wed 30 Sep: no move
    assert rules.fine_discount_deadline(date(2026, 9, 10), CAL) == date(2026, 9, 30)


def test_insurance_cancel_by_is_one_month_before():
    assert rules.insurance_cancel_by(date(2026, 11, 30)) == date(2026, 10, 30)
    assert rules.insurance_cancel_by(date(2026, 3, 31)) == date(2026, 2, 28)


def test_warranty_end_three_years_and_override():
    assert rules.warranty_end(date(2026, 8, 15)) == date(2029, 8, 15)
    assert rules.warranty_end(date(2026, 8, 15), years=2) == date(2028, 8, 15)
    assert rules.warranty_end(date(2026, 8, 15), months=30) == date(2029, 2, 15)
    assert rules.warranty_end(date(2024, 2, 29), years=3) == date(2027, 2, 28)


def test_period_end_has_no_working_day_shift():
    assert rules.period_end(date(2026, 9, 1), 12, "month") == date(2027, 9, 1)
    assert rules.period_end(date(2026, 9, 1), 30, "day") == date(2026, 10, 1)
