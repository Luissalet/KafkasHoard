"""Regressions found while trying realistic documents."""

from kafka_hoard.extract import amounts
from kafka_hoard.privacy import mask_text as mask
from kafka_hoard.util import fold


def test_reduction_label_belongs_to_the_next_amount_only():
    text = "Importe de la multa: 100,00 €. Importe con reducción del 50%: 50,00 €\nFecha"
    best, reduced = amounts.choose_amount(fold(text), text)
    assert best.value == 100.0
    assert reduced is not None and reduced.value == 50.0


def test_a_reference_ending_in_a_letter_is_not_a_card():
    assert mask("Referencia: 202620000123456X") == "Referencia: 202620000123456X"
