"""Small pure helpers: identifiers, issuer keys and day labels."""

from __future__ import annotations

import re
import secrets
import time
from datetime import date, datetime
from typing import Optional

from .hoard_link.text import fold

_ID_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz"


def new_id(prefix: str, now: Optional[float] = None) -> str:
    """``<prefix>_<time><random>``: sortable by creation time, unique enough for a local database."""
    millis = int((now if now is not None else time.time()) * 1000)
    stamp = ""
    for _ in range(7):
        millis, rest = divmod(millis, 32)
        stamp = _ID_ALPHABET[rest] + stamp
    tail = "".join(secrets.choice(_ID_ALPHABET) for _ in range(5))
    return f"{prefix}_{stamp}{tail}"


LEGAL_SUFFIX = re.compile(r"\b(s\s*a\s*u|s\s*l\s*u|s\s*a|s\s*l|s\s*c|s\s*coop|sau|slu|sa|sl|ltd|llc|gmbh|inc|plc|ag|bv|nv)\b$")


def issuer_key(name: str) -> str:
    """Normalised lowercase ASCII key of an issuer: no accents, punctuation or legal suffix («Mutua Madrileña S.A.» -> «mutua madrilena»)."""
    text = fold(name or "")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    for _ in range(2):
        text = LEGAL_SUFFIX.sub("", text).strip()
    return text


def ts_date(ts: Optional[float]) -> Optional[date]:
    return datetime.fromtimestamp(float(ts)).date() if ts else None


def human_day(day: date, es: bool = True) -> str:
    months_es = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")
    months_en = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    if es:
        return f"{day.day} {months_es[day.month - 1]} {day.year}"
    return f"{day.day} {months_en[day.month - 1]} {day.year}"
