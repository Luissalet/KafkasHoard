"""Masking of personal identifiers in what the assistant sees (MCP and the agent route). The local UI shows everything.

DNI / NIE / IBAN / card numbers / phone numbers become «[oculto: DNI]» and so on, unless the user explicitly asks for that exact
number (``reveal=true`` on the tools that return text).
"""

from __future__ import annotations

import re
from typing import Any

DNI = re.compile(r"\b\d{8}[A-HJ-NP-TV-Z]\b")
NIE = re.compile(r"\b[XYZ]\d{7}[A-Z]\b")
IBAN_SPACED = re.compile(r"\b[A-Z]{2}\d{2}(?:[ \-]?[A-Z0-9]{4}){3,7}(?:[ \-]?[A-Z0-9]{1,4})?\b")
CARD = re.compile(r"(?<![\w])(?:\d[ \-]?){12,18}\d(?![\w])")  # a reference such as 2026…456X is not a card
PHONE_LABELLED = re.compile(r"(?i)(tel(?:[eé]fono)?\.?|tfno\.?|m[oó]vil|phone|mobile|contacto|whatsapp)(\s*[:.]?\s*)((?:\+|00)?[\d][\d .\-]{7,16}\d)")
PHONE_PREFIXED = re.compile(r"(?<![\w+])(?:\+34|0034)[ .\-]?[6-9]\d{2}[ .\-]?\d{3}[ .\-]?\d{3}\b")


def _luhn(digits: str) -> bool:
    total, flip = 0, False
    for ch in reversed(digits):
        d = int(ch)
        if flip:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        flip = not flip
    return total % 10 == 0


def _iban_ok(raw: str) -> bool:
    iban = re.sub(r"[ \-]", "", raw).upper()
    if not 15 <= len(iban) <= 34:
        return False
    moved = iban[4:] + iban[:4]
    try:
        number = int("".join(str(int(c, 36)) for c in moved))
    except ValueError:
        return False
    return number % 97 == 1


def mask_text(text: str) -> str:
    """Replace identifiers in free text. Pure and idempotent."""
    if not text:
        return text

    def iban(m: re.Match) -> str:
        return "[oculto: IBAN]" if _iban_ok(m.group(0)) else m.group(0)

    def card(m: re.Match) -> str:
        digits = re.sub(r"\D", "", m.group(0))
        return "[oculto: tarjeta]" if 13 <= len(digits) <= 19 and _luhn(digits) else m.group(0)

    text = IBAN_SPACED.sub(iban, text)
    text = DNI.sub("[oculto: DNI]", text)
    text = NIE.sub("[oculto: NIE]", text)
    text = CARD.sub(card, text)
    text = PHONE_LABELLED.sub(lambda m: f"{m.group(1)}{m.group(2)}[oculto: teléfono]", text)
    text = PHONE_PREFIXED.sub("[oculto: teléfono]", text)
    return text


def mask_obj(value: Any) -> Any:
    """Mask every string inside lists and dicts (keys stay)."""
    if isinstance(value, str):
        return mask_text(value)
    if isinstance(value, list):
        return [mask_obj(v) for v in value]
    if isinstance(value, tuple):
        return tuple(mask_obj(v) for v in value)
    if isinstance(value, dict):
        return {k: mask_obj(v) for k, v in value.items()}
    return value
