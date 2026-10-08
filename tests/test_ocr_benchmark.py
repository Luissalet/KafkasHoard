"""The benchmark must fail on missing, duplicated or reordered page output."""
from scripts.eval_document_ocr import score


def test_missing_page_is_an_error_even_if_other_pages_are_perfect():
    expected = [{"number": 1, "text": "Avena -12,50 EUR"}, {"number": 2, "text": "Leche 4,75 EUR"}]
    result = score(expected, expected[:1])
    assert result["empty_pages"] == 1 and result["cer"] > 0 and not result["page_order_complete"]


def test_duplicate_and_reordered_pages_are_not_a_complete_document():
    expected = [{"number": 1, "text": "Avena"}, {"number": 2, "text": "Leche"}]
    assert not score(expected, expected[::-1])["page_order_complete"]
    assert not score(expected, [*expected, expected[0]])["page_order_complete"]


def test_scoring_keeps_signs_and_amounts_while_tolerating_layout_spaces():
    expected = [{"number": 1, "text": "Avena  -12,50 EUR"}]
    assert score(expected, [{"number": 1, "text": "avena\n-12,50 EUR"}])["cer"] == 0
    assert score(expected, [{"number": 1, "text": "Avena 12,50 EUR"}])["cer"] > 0
    assert score(expected, [{"number": 1, "text": "Avena 12,50 EUR"}])["numeric_mismatch_pages"] == 1
    assert score(expected, [{"number": 1, "text": "Avena − 12.50EUR"}])["numeric_mismatch_pages"] == 0
