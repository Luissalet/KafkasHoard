"""Small pure helpers: identifiers, text folding, month arithmetic and ISO dates."""

from __future__ import annotations

import calendar
import re
import secrets
import time
import unicodedata
from datetime import date, datetime, timedelta
from typing import Optional

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


def fold_char(ch: str) -> str:
    base = unicodedata.normalize("NFD", ch)[:1] or ch
    low = base.lower()
    return low if len(low) == 1 else base


def fold(text: str) -> str:
    """Lowercase and strip accents without changing the length, so offsets in the folded text point into the original."""
    return "".join(fold_char(c) for c in text)


LEGAL_SUFFIX = re.compile(r"\b(s\s*a\s*u|s\s*l\s*u|s\s*a|s\s*l|s\s*c|s\s*coop|sau|slu|sa|sl|ltd|llc|gmbh|inc|plc|ag|bv|nv)\b$")


def issuer_key(name: str) -> str:
    """Normalised lowercase ASCII key of an issuer: no accents, punctuation or legal suffix («Mutua Madrileña S.A.» -> «mutua madrilena»)."""
    text = fold(name or "")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    for _ in range(2):
        text = LEGAL_SUFFIX.sub("", text).strip()
    return text


def add_months(day: date, months: int) -> date:
    """The same day number ``months`` later; when that month has no such day, its last day (31 Jan + 1 month = 28 or 29 Feb)."""
    index = day.year * 12 + (day.month - 1) + months
    year, month = divmod(index, 12)
    month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def iso(day: Optional[date]) -> str:
    return day.isoformat() if day else ""


def parse_iso(text: Optional[str]) -> Optional[date]:
    try:
        return date.fromisoformat((text or "").strip()[:10])
    except ValueError:
        return None


def ts_date(ts: Optional[float]) -> Optional[date]:
    return datetime.fromtimestamp(float(ts)).date() if ts else None


def human_day(day: date, es: bool = True) -> str:
    months_es = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")
    months_en = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    if es:
        return f"{day.day} {months_es[day.month - 1]} {day.year}"
    return f"{day.day} {months_en[day.month - 1]} {day.year}"


def money_text(amount: Optional[float], currency: str = "EUR", es: bool = True) -> str:
    if amount is None:
        return ""
    text = f"{amount:,.2f}"
    if es:
        text = text.replace(",", "X").replace(".", ",").replace("X", ".")
    symbol = "€" if (currency or "EUR").upper() == "EUR" else currency
    return f"{text} {symbol}" if es else (f"{symbol}{text}" if symbol == "€" else f"{text} {symbol}")


def days_between(later: date, earlier: date) -> int:
    return (later - earlier).days


def clamp_text(text: str, limit: int) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def safe_add_days(day: date, days: int) -> date:
    try:
        return day + timedelta(days=days)
    except OverflowError:
        return day
