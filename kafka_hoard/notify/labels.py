"""Notification type -> human label (ES / EN) and the text of a notification."""

from __future__ import annotations

from typing import Any

from ..model import N_DOC, N_OVERDUE, N_PRICE, N_SOON, NOTIFY_TYPES

LABELS: dict[str, dict[str, str]] = {
    "es": {N_SOON: "Plazo próximo", N_OVERDUE: "Plazo vencido", N_PRICE: "Cambio de precio", N_DOC: "Documento nuevo", "test": "Prueba"},
    "en": {N_SOON: "Upcoming deadline", N_OVERDUE: "Overdue deadline", N_PRICE: "Price change", N_DOC: "New document", "test": "Test"},
}
assert all(t in LABELS["es"] and t in LABELS["en"] for t in NOTIFY_TYPES)

WORDS = {"es": {"open": "Ver documento", "test_title": "Prueba de notificación", "test_body": "Si lo lees, este canal funciona."},
         "en": {"open": "Open document", "test_title": "Notification test", "test_body": "If you can read this, the channel works."}}

TYPE_TAGS = {N_SOON: "alarm_clock", N_OVERDUE: "rotating_light", N_PRICE: "chart_with_upwards_trend", N_DOC: "page_facing_up", "test": "bell"}


def label(kind: str, lang: str = "es") -> str:
    return LABELS.get(lang, LABELS["es"]).get(kind, kind.replace("_", " ").capitalize() if kind else "")


def format_price(price: Any, currency: Any, lang: str = "es") -> str:
    try:
        value = float(price)
    except (TypeError, ValueError):
        return ""
    text = f"{value:,.2f}"
    if lang == "es":
        text = text.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{text} {'€' if str(currency or 'EUR').upper() == 'EUR' else currency}"


def compose(event: dict[str, Any], lang: str = "es") -> tuple[str, str]:
    """``(title, body)``: the engine already writes both in the user's language; this only guards empty values."""
    title = str(event.get("title") or "").strip() or label(str(event.get("type") or ""), lang) or "Kafka's Hoard"
    return title, str(event.get("summary") or "").strip()
