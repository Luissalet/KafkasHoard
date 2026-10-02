"""The family agenda: what Kafka knows that has a date, for the hub's Today view and the family calendar.

``GET /api/family/agenda`` (installed in ``main.py`` with ``hoard_link.fam_agenda``) answers with the open deadlines of the
window. Deadlines of other apps keep the kind of thing they are (maintenance, follow-up, exam); the priority comes from how
close or overdue the date is and how serious the kind is (a missed appeal or fine discount costs money, a warranty end does not).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from . import model as M
from .util import clamp_text, parse_iso

KIND_OF = {
    M.RENEWAL: "renewal", M.CANCEL_BY: "renewal", M.PERMANENCE_END: "renewal", M.EXPIRY: "renewal",
    M.ITV: "maintenance", M.WARRANTY_END: "deadline", M.PAYMENT: "deadline", M.APPEAL: "deadline",
    M.FINE_DISCOUNT: "deadline", M.TAX_DUE: "deadline", M.CUSTOM: "deadline",
}
# a deadline another app keeps here is the kind of thing that app deals with
SOURCE_KIND = {"homehoard": "maintenance", "people": "followup", "funes": "followup", "hypatia": "exam"}
SERIOUS = (M.APPEAL, M.FINE_DISCOUNT, M.CANCEL_BY, M.TAX_DUE, M.PAYMENT)


def agenda_kind(t: dict[str, Any]) -> str:
    source = str(t.get("source") or "")
    if source in SOURCE_KIND and t.get("kind") == M.CUSTOM:
        return SOURCE_KIND[source]
    return KIND_OF.get(t.get("kind") or M.CUSTOM, "deadline")


def agenda_priority(deadline_kind: str, days_left: int) -> str:
    """Overdue and last-day deadlines are urgent when missing them costs money (appeal, fine discount, cancellation window, tax,
    payment), high otherwise; the nearer, the higher."""
    serious = deadline_kind in SERIOUS
    if days_left <= 0:
        return "urgent" if serious else "high"
    if days_left <= 3:
        return "high" if serious else "normal"
    if days_left <= 14:
        return "normal" if serious else "low"
    return "low"


def items(svc: Any, date_from: date, date_to: date, sphere: str = "") -> list[dict[str, Any]]:
    """Open deadlines dated inside the window, as agenda items."""
    today = svc.engine.today()
    base = svc.engine.base_url()
    rows = svc.store.deadlines(states=[M.OPEN], date_from=date_from.isoformat(), date_to=date_to.isoformat(), limit=1000)
    docs: dict[str, Optional[dict[str, Any]]] = {}
    out = []
    for t in rows:
        day = parse_iso(t["date"])
        if day is None:
            continue
        doc = None
        if t.get("doc_id"):
            if t["doc_id"] not in docs:
                docs[t["doc_id"]] = svc.store.find_document(t["doc_id"])
            doc = docs[t["doc_id"]]
        kind = agenda_kind(t)
        days_left = (day - today).days
        detail = " · ".join(x for x in (clamp_text(t.get("basis") or "", 160), f"«{doc['title']}»" if doc else "") if x)
        if t.get("url"):
            url = t["url"]
        elif doc and base:
            url = f"{base}/#/documentos/{doc['id']}"
        else:
            url = f"{base}/#/plazos" if base else ""
        out.append({"id": f"kafka:deadline:{t['id']}", "title": t["title"], "start": t["date"], "all_day": True, "kind": kind,
                    "priority": agenda_priority(t["kind"], days_left), "url": url, "detail": detail})
    return out

