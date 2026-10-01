"""Shared test helpers for the extraction tests."""

from __future__ import annotations

from datetime import date
from typing import Optional

from docs import TODAY
from kafka_hoard.bizdays import Calendar
from kafka_hoard.extract.pipeline import extract
from kafka_hoard.extract.types import Ctx, Meta


def run(text: str, *, today: date = TODAY, received: Optional[date] = None, subject: str = "", from_address: str = "", from_name: str = "",
        lang: str = "es", **ctx_kw):
    ctx = Ctx(today=today, cal=Calendar("ES-MD"), lang=lang, **ctx_kw)
    meta = Meta(from_name=from_name, from_address=from_address, subject=subject, received=received)
    return extract([text], meta, ctx)


def by_key(ex) -> dict:
    return {d.key: d for d in ex.deadlines}
