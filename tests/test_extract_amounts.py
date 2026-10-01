"""Amount formats and the choice of the labelled total."""

import pytest

from kafka_hoard.extract import amounts
from kafka_hoard.util import fold


@pytest.mark.parametrize("token,value", [
    ("1.234,56", 1234.56), ("1,234.56", 1234.56), ("1234,56", 1234.56), ("1234.56", 1234.56), ("12,5", 12.5), ("0,99", 0.99),
    ("1.234", 1234.0), ("12.345.678", 12345678.0), ("1,234,567", 1234567.0), ("48,40", 48.4), ("320", 320.0),
])
def test_parse_number(token, value):
    assert amounts.parse_number(token) == value


@pytest.mark.parametrize("token", ["", "abc", "1.2.3a", "..", "€"])
def test_parse_number_rejects_non_numbers(token):
    assert amounts.parse_number(token) is None


def pick(text):
    best, reduced = amounts.choose_amount(fold(text), text)
    return (best.value if best else None), (reduced.value if reduced else None)


@pytest.mark.parametrize("text,value", [
    ("Total a pagar: 1.234,56 €", 1234.56),
    ("Importe total: €1,234.56", 1234.56),
    ("Total factura: 1234,56 EUR", 1234.56),
    ("Prima total: 320,00 €", 320.0),
    ("Total (IVA incluido): 249,90 €", 249.9),
    ("Importe del recibo: 45,00 €", 45.0),
    ("Importe de la sanción: 200,00 €", 200.0),
    ("Amount due: $1,500.00", 1500.0),
    ("Total: 99,99 €", 99.99),
])
def test_labelled_totals(text, value):
    assert pick(text)[0] == value


def test_ignores_base_iva_and_subtotal_when_a_total_exists():
    text = "Subtotal: 40,00 €\nBase imponible: 40,00 €\nIVA 21 %: 8,40 €\nTotal a pagar: 48,40 €"
    assert pick(text)[0] == 48.4


def test_total_a_pagar_beats_a_larger_unlabelled_number():
    assert pick("Saldo anterior 9.999,00 €\nTotal a pagar: 48,40 €")[0] == 48.4


def test_fine_keeps_full_and_reduced_amount():
    best, reduced = pick("Importe de la sanción: 200,00 €\nImporte con reducción del 50 % por pronto pago: 100,00 €")
    assert best == 200.0 and reduced == 100.0


def test_no_amount():
    assert pick("Sin importes aquí, solo texto 12/10/2026 a las 10:30") == (None, None)


def test_dates_and_times_are_not_amounts():
    assert pick("Total: 15/10/2026 10:30")[0] is None
