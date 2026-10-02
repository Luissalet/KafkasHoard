"""Masking of personal identifiers in what the assistant sees (MCP and the agent route). The local UI shows everything.

DNI / NIE / IBAN / card numbers / phone numbers become «[oculto: DNI]» and so on, unless the user explicitly asks for that exact
number (``reveal=true`` on the tools that return text). Detection is the family's (``hoard_link.idcheck``): only values whose
check letter, mod-97 or Luhn digit is right count, so a reference that merely looks like an ID is left alone.
"""

from __future__ import annotations

from typing import Any

from .hoard_link import idcheck

_TOKENS = {"DNI": "[oculto: DNI]", "NIE": "[oculto: NIE]", "IBAN": "[oculto: IBAN]", "CARD": "[oculto: tarjeta]", "PHONE_ES": "[oculto: teléfono]"}


def mask_text(text: str) -> str:
    """Replace identifiers in free text. Pure and idempotent."""
    if not text:
        return text
    out = text
    for hit in reversed(idcheck.scan_pii(text, kinds=_TOKENS)):
        out = out[:hit.start] + _TOKENS[hit.kind] + out[hit.end:]
    return out


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
