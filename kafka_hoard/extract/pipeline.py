"""The extraction: pages of text (+ what is known about the document) -> kind, issuer, reference, amount, dated facts and deadlines.

Rules only, deterministic and offline. The optional model pass (``extract/llm.py``) hands in validated ``Hints`` that add to
what the rules found. Text is untrusted: nothing in it is ever executed or followed as an instruction.
"""

from __future__ import annotations

import re
from bisect import bisect_right
from datetime import date
from typing import Any, Optional

from .. import model as M
from ..util import fold, iso
from . import amounts, dates, issuers, kinds, periods, refs
from .deadlines import build_deadlines
from .types import Ctx, Extraction, Hints, Meta

REVIEW_BELOW = 60
EXPECT_AMOUNT = (M.INVOICE, M.RECEIPT, M.BILL, M.INSURANCE, M.SUBSCRIPTION, M.FINE, M.TAX, M.BANK, M.PAYSLIP)
DEADLINE_KINDS_NEEDING_ONE = (M.FINE, M.OFFICIAL)
ITEM_LABEL = re.compile(r"(?:^|\n|[.;]\s)\s*(?:producto|articulo|modelo|item|descripcion|concepto|product)\s*[:\-]\s*([^\n.;]{3,80})", re.I)
PLATE = re.compile(r"(?i:matr[ií]cula|plate)\s*[:\-]?\s*(\d{4}\s?-?\s?[BCDFGHJKLMNPRSTVWXYZ]{3}|[A-Z]{1,2}\s?-?\s?\d{4}\s?-?\s?[A-Z]{1,2})\b")
MONTHLY = re.compile(r"\bmensual(?:es)?\b|al\s+mes\b|/\s*mes\b|por\s+mes\b|per\s+month|monthly|cada\s+mes|\bmes\b\s*$")
YEARLY = re.compile(r"\banual(?:es)?\b|al\s+ano\b|/\s*ano\b|por\s+ano\b|per\s+year|yearly|annual(?:ly)?|cada\s+ano|12\s+meses\s+de\s+suscripcion")


class _Joined:
    """The pages joined into one text, with the page of every offset."""

    def __init__(self, pages: list[str]):
        self.parts: list[int] = []
        chunks: list[str] = []
        offset = 0
        for text in pages:
            self.parts.append(offset)
            chunks.append(text)
            offset += len(text) + 2
        self.text = "\n\n".join(chunks)

    def page_of(self, offset: int) -> int:
        return max(1, bisect_right(self.parts, offset))


def _line_around(text: str, start: int, end: int, limit: int = 170) -> str:
    ls = text.rfind("\n", 0, start) + 1
    le = text.find("\n", end)
    line = text[ls: len(text) if le == -1 else le].strip()
    return (line[:limit] + "…") if len(line) > limit else line


def _fact(field: str, value: Any, evidence: str = "", page: Optional[int] = None, confidence: int = 70) -> dict[str, Any]:
    return {"field": field, "value": value, "evidence": evidence, "page": page, "confidence": confidence}


def _apply_hints(joined: _Joined, folded: str, hints: Hints, hits: list[dates.DateHit], rel: list[periods.RelPeriod]) -> None:
    """Add model-found dates and periods, only when their evidence is literally in the text."""
    text = joined.text
    for item in hints.dates:
        try:
            day = date.fromisoformat(str(item.get("date")))
        except ValueError:
            continue
        evidence = str(item.get("evidence") or "").strip()
        role = str(item.get("role") or "")
        if not evidence or role not in {r for r, _ in dates.ROLE_PATTERNS} | {"issue"}:
            continue
        pos = text.find(evidence)
        if pos < 0:
            pos = folded.find(fold(evidence))
        if pos < 0:
            continue
        # the date itself must be written in the evidence
        if not any(h.date == day for h in dates.find_dates(fold(evidence))):
            continue
        if any(h.date == day and h.role == role for h in hits):
            continue
        hit = dates.DateHit(day, pos, pos + len(evidence), evidence, role=role, label="model", certain=True)
        hit.evidence, hit.page = _line_around(text, pos, pos + len(evidence)), joined.page_of(pos)
        hits.append(hit)
    for item in hints.relative:
        try:
            n = int(item.get("n"))
        except (TypeError, ValueError):
            continue
        unit = str(item.get("unit") or "")
        evidence = str(item.get("evidence") or "").strip()
        if unit not in ("day", "month", "year") or not 0 < n <= 120 or not evidence or evidence not in text:
            continue
        working = item.get("working")
        rel.append(periods.RelPeriod(n, unit, working if isinstance(working, bool) else None, str(item.get("purpose") or "generic"),
                                     str(item.get("base") or ""), 0, 0, evidence))


def extract(pages: list[str], meta: Meta, ctx: Ctx, hints: Optional[Hints] = None) -> Extraction:
    joined = _Joined([p or "" for p in pages])
    text = joined.text
    folded = fold(text)
    ex = Extraction(text_length=len(text.strip()))
    if not text.strip():
        ex.notes.append("no_text")
        ex.state = M.REVIEW
        ex.title = meta.subject or meta.file_name
        return ex

    # ---- issuer and kind
    issuer = issuers.detect_issuer(text, folded, meta.from_name, meta.from_address)
    context = fold(meta.subject + "\n") + folded if meta.subject else folded
    result = kinds.classify(context, issuer.name, issuer.category)
    if hints and hints.kind in M.KINDS and (result.kind == M.OTHER or result.score < 8):
        result.kind, result.score = hints.kind, max(result.score, 8)
        result.reasons = [*result.reasons, "model"]
    ov = ctx.overrides or {}
    if ov.get("kind") in M.KINDS:
        result.kind, result.score, result.reasons = ov["kind"], max(result.score, 12), ["user"]
    if hints and hints.issuer and not issuer.name:
        issuer = issuers.Issuer(hints.issuer, issuers.issuer_key(hints.issuer), issuers.category_of_name(hints.issuer), "model", 40)
    if ov.get("issuer"):
        issuer = issuers.Issuer(str(ov["issuer"]), issuers.issuer_key(str(ov["issuer"])), issuers.category_of_name(str(ov["issuer"])), "user", 100)
    ex.issuer = issuer
    ex.kind, ex.kind_score, ex.kind_reasons = result.kind, result.score, result.reasons
    ex.facts.append(_fact("kind", ex.kind, ", ".join(result.reasons[:4]), None, min(95, 30 + result.score * 4)))
    if issuer.name:
        ex.facts.append(_fact("issuer", issuer.name, issuer.source, None, issuer.confidence))

    # ---- dates, roles and relative periods
    hits = dates.find_dates(folded)
    dates.assign_roles(folded, hits)
    dates.attach_evidence(text, folded, hits, joined.page_of)
    rel = periods.find_periods(folded)
    if hints:
        _apply_hints(joined, folded, hints, hits, rel)
    if ov.get("issue_date"):
        try:
            forced = date.fromisoformat(str(ov["issue_date"]))
        except ValueError:
            forced = None
        if forced:
            hits[:] = [h for h in hits if h.role != "issue"]
            hits.append(dates.DateHit(forced, 0, 0, "", role="issue", label="user", certain=True, evidence="(fecha corregida a mano)", page=1))
    hits.sort(key=lambda h: h.start)
    # a page number like «Página 1/2» never is a date; roles that make no sense are dropped by the builder
    ex.dated = dates.Dated(hits)
    ex.periods = rel

    # ---- kind-specific mapping of generic roles
    if ex.kind == M.VEHICLE:
        for h in hits:
            if h.role == "expiry" and not any(x.role == "itv_next" for x in hits):
                h.role = "itv_next"
    for h in hits:
        ex.facts.append(_fact(f"date:{h.role or 'unlabelled'}", h.date.isoformat(), h.evidence, h.page, 85 if h.certain else 45))

    # ---- reference, amount, item
    ref_hits = refs.find_refs(text, folded)
    main, stable = refs.choose_ref(ref_hits, ex.kind)
    if main:
        ex.ref, ex.ref_what = main.value, main.what
        ex.facts.append(_fact("ref", main.value, _line_around(text, main.start, main.end), joined.page_of(main.start), 80))
    elif hints and hints.ref:
        ex.ref = hints.ref[:60]
    if ov.get("ref"):
        ex.ref = str(ov["ref"])[:60]
        ex.series_ref = ex.ref
    if stable and not ov.get("ref"):
        ex.series_ref = stable.value
    best, reduced = amounts.choose_amount(folded, text)
    if best is not None:
        ex.amount, ex.currency = best.value, best.currency
        ex.facts.append(_fact("amount", best.value, _line_around(text, best.start, best.end), joined.page_of(best.start), 85 if best.labelled else 45))
    elif hints and hints.amount is not None and hints.amount >= 0:
        ex.amount, ex.currency = hints.amount, "EUR"
    if reduced is not None:
        ex.amount_reduced = reduced.value
        ex.facts.append(_fact("amount_reduced", reduced.value, _line_around(text, reduced.start, reduced.end), joined.page_of(reduced.start), 75))
    if ov.get("amount") is not None:
        ex.amount, ex.currency = float(ov["amount"]), ex.currency or "EUR"
    im = ITEM_LABEL.search(text)
    if ov.get("item"):
        ex.item = str(ov["item"])[:120]
    elif im:
        ex.item = re.sub(r"\s+", " ", im.group(1)).strip(" .:-")[:120]
        ex.facts.append(_fact("item", ex.item, _line_around(text, im.start(1), im.end(1)), joined.page_of(im.start(1)), 60))

    if not ex.item and ex.kind in (M.VEHICLE, M.FINE, M.INSURANCE):
        pm = PLATE.search(text)
        if pm and ex.kind == M.VEHICLE:
            ex.item = re.sub(r"\s+", " ", pm.group(1)).strip()
            ex.facts.append(_fact("item", ex.item, _line_around(text, pm.start(1), pm.end(1)), joined.page_of(pm.start(1)), 70))

    # ---- document-level dates
    issue = ex.dated.first("issue", "purchase", "delivery")
    if issue is None and ex.kind in (M.INVOICE, M.RECEIPT, M.BILL, M.BANK, M.PAYSLIP, M.OTHER, M.WARRANTY):
        issue = next((h for h in hits if not h.role), None)
    ex.issue_date = iso(issue.date) if issue else ""
    pf, pt = ex.dated.first("period_from", "effect_from"), ex.dated.first("period_to", "effect_to")
    ex.period_from, ex.period_to = iso(pf.date) if pf else "", iso(pt.date) if pt else ""

    # subscription rhythm
    if ex.kind == M.SUBSCRIPTION or ex.kind == M.CONTRACT:
        tail = folded
        ex.period_hint = "monthly" if MONTHLY.search(tail) and not YEARLY.search(tail) else "yearly" if YEARLY.search(tail) else ""

    # ---- title
    who = issuer.name
    label = M.kind_label(ex.kind, ctx.lang)
    suffix = ex.ref if ex.ref and ex.kind in (M.INVOICE, M.INSURANCE, M.CONTRACT, M.FINE, M.OFFICIAL, M.BILL) else (ex.issue_date[:7] if ex.issue_date else "")
    ex.title = " · ".join(x for x in (who or (meta.subject or meta.file_name or ""), label if ex.kind != M.OTHER else "", suffix) if x)[:160]

    # ---- deadlines
    ex.deadlines = build_deadlines(ex, ctx, meta)

    # ---- confidence and state
    conf = 0
    if ex.kind != M.OTHER:
        conf += min(60, 20 + ex.kind_score * 4)
    conf += 15 if issuer.confidence >= 65 else 10 if issuer.confidence >= 40 else 5 if issuer.name else 0
    certain_dates = any(h.certain for h in hits)
    conf += 15 if certain_dates else 8 if hits else 0
    if ex.kind in EXPECT_AMOUNT:
        conf += 10 if (best is not None and best.labelled) else 5 if best is not None else 0
    else:
        conf += 8
    conf += 5 if ex.deadlines else 0
    ex.confidence = max(0, min(100, conf))
    needs_one = ex.kind in DEADLINE_KINDS_NEEDING_ONE and not ex.deadlines
    if needs_one:
        ex.notes.append("no_deadline_found")
    if ex.kind == M.OTHER:
        ex.notes.append("kind_unknown")
    if not hits and ex.kind != M.MANUAL:
        ex.notes.append("no_dates")
    dates_expected = ex.kind != M.MANUAL     # a manual is complete without any date
    ex.state = M.REVIEW if (ex.kind == M.OTHER or ex.confidence < REVIEW_BELOW or (dates_expected and not hits) or needs_one) else M.OK
    return ex
