"""Masking of personal identifiers in what the assistant sees."""

from kafka_hoard.privacy import mask_obj, mask_text


def test_dni_and_nie_are_masked():
    assert mask_text("DNI 12345678Z y NIE X1234567L") == "DNI [oculto: DNI] y NIE [oculto: NIE]"


def test_a_valid_iban_is_masked_but_a_random_code_is_not():
    assert "[oculto: IBAN]" in mask_text("Cuenta ES91 2100 0418 4502 0005 1332 de la titular")
    assert "[oculto: IBAN]" in mask_text("IBAN ES9121000418450200051332")
    assert mask_text("Referencia ES12 3456 7890 1234 5678") == "Referencia ES12 3456 7890 1234 5678"


def test_a_card_number_needs_to_pass_luhn():
    assert mask_text("Tarjeta 4111 1111 1111 1111") == "Tarjeta [oculto: tarjeta]"
    assert mask_text("Pedido 1234 5678 9012 3456") == "Pedido 1234 5678 9012 3456"


def test_phone_numbers_are_masked_when_labelled_or_prefixed():
    assert "[oculto: teléfono]" in mask_text("Teléfono: 612 345 678")
    assert "[oculto: teléfono]" in mask_text("llama al +34 612345678 mañana")
    assert mask_text("Importe 612345678 céntimos") == "Importe 612345678 céntimos"


def test_masking_is_idempotent_and_leaves_normal_text_alone():
    text = "Prima total: 320,00 € · póliza 5550012345 · vence el 30/11/2026"
    assert mask_text(text) == text
    once = mask_text("DNI 12345678Z")
    assert mask_text(once) == once and mask_text("") == ""


def test_mask_obj_walks_lists_and_dicts_but_keeps_keys_and_numbers():
    data = {"DNI 12345678Z": ["DNI 12345678Z", {"x": "ok", "n": 3}], "t": ("12345678Z",), "f": 1.5, "none": None}
    out = mask_obj(data)
    assert "DNI 12345678Z" in out and out["DNI 12345678Z"][0] == "DNI [oculto: DNI]"
    assert out["DNI 12345678Z"][1] == {"x": "ok", "n": 3} and out["t"] == ("[oculto: DNI]",) and out["f"] == 1.5 and out["none"] is None
