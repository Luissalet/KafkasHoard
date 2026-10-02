"""From what was extracted (kind, issuer, dated roles, relative periods) to deadline drafts, each with its basis and evidence."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

from .. import model as M
from ..util import human_day
from . import rules
from .dates import DateHit
from .periods import RelPeriod
from .texts import tr, years_text
from .types import Ctx, DeadlineDraft, Extraction, Meta

ADMIN_KINDS = (M.FINE, M.OFFICIAL, M.TAX)
NO_WARRANTY_CATEGORIES = ("utility", "telco", "insurer", "admin", "bank")
ADMIN_STALE_DAYS = 60   # an administrative deadline that ended longer ago than this is history, not news


def _lead(ctx: Ctx, kind: str) -> list[int]:
    return list(ctx.leads.get(kind) or M.DEFAULT_LEADS.get(kind, [7, 1]))


def _who(ex: Extraction, ctx: Ctx) -> str:
    return ex.issuer.name or ex.title or M.kind_label(ex.kind, ctx.lang)


def _evidence(hit: Optional[DateHit]) -> tuple[str, Optional[int]]:
    return (hit.evidence, hit.page) if hit else ("", None)


def _fmt(day: date, ctx: Ctx) -> str:
    return human_day(day, ctx.lang != "en")


def _future(day: date, ctx: Ctx) -> bool:
    return day >= ctx.today


def _is_admin_doc(ex: Extraction) -> bool:
    return ex.kind in ADMIN_KINDS or ex.issuer.category == "admin"


def _unit_word(lang: str, unit: str) -> str:
    return tr(lang, f"unit_{unit}")


def _base_for_period(p: RelPeriod, ex: Extraction, ctx: Ctx, meta: Meta) -> tuple[Optional[date], str, str, Optional[DateHit]]:
    """(base date, basis key, certainty, hit) for a relative period: the notification, else the mail's date, else the document's."""
    notified = ex.dated.first("notified")
    effect = ex.dated.first("effect_from")
    issue = ex.dated.first("issue", "purchase")
    delivery = ex.dated.first("delivery")
    if p.base == "effect" and effect:
        return effect.date, "basis_effect", "explicit", effect
    if p.base == "delivery" and delivery:
        return delivery.date, "basis_delivery", "explicit", delivery
    if notified:
        return notified.date, "basis_notified", "explicit", notified
    if meta.received and (p.base in ("notified", "") or not issue):
        return meta.received, "basis_received", "mail", None
    if issue:
        return issue.date, "basis_issue", "document", issue
    if meta.received:
        return meta.received, "basis_received", "mail", None
    return None, "", "", None


def _admin_period_draft(p: RelPeriod, ex: Extraction, ctx: Ctx, meta: Meta, kind: str, key: str, title: str) -> Optional[DeadlineDraft]:
    base, base_key, how, hit = _base_for_period(p, ex, ctx, meta)
    if base is None:
        ex.notes.append("period_without_base")
        return None
    working = p.working if p.working is not None else (True if p.unit == "day" else False)
    end = rules.admin_deadline(base, p.n, p.unit, working, ctx.cal)
    raw_end = base + timedelta(days=p.n) if (p.unit == "day" and not working) else None
    unit = p.unit
    unit_text = _unit_word(ctx.lang, unit)
    kind_text = tr(ctx.lang, "kind_working" if working else "kind_natural") if p.unit == "day" else ""
    kind_en = tr("en", "kind_working" if working else "kind_natural") if p.unit == "day" else ""
    basis = tr(ctx.lang, "period_basis", n=p.n, unit=unit_text, kind_es=kind_text, kind_en=kind_text or kind_en, base_es=tr("es", base_key),
               base_en=tr("en", base_key), date=_fmt(base, ctx)).replace("  ", " ")
    if working:
        basis += " " + tr(ctx.lang, "period_basis_working").capitalize() + "."
    elif p.unit != "day":
        basis += " " + tr(ctx.lang, "period_basis_months").capitalize() + "."
    if raw_end is not None and raw_end != end:
        basis += " " + tr(ctx.lang, "moved_basis", original=_fmt(raw_end, ctx))
    elif p.unit != "day":
        raw = rules.period_end(base, p.n, p.unit)
        if raw != end:
            basis += " " + tr(ctx.lang, "moved_basis", original=_fmt(raw, ctx))
    confidence = {"explicit": 85, "document": 60, "mail": 65}.get(how, 60)
    if (ex.kind in ADMIN_KINDS and end < ctx.today - timedelta(days=ADMIN_STALE_DAYS)):
        return None
    ev, page = _evidence(hit)
    return DeadlineDraft(key, kind, title, end, basis, ev or p.raw, page, confidence, _lead(ctx, kind), ex.amount, "none")


def build_deadlines(ex: Extraction, ctx: Ctx, meta: Meta) -> list[DeadlineDraft]:
    out: list[DeadlineDraft] = []
    if ex.kind == M.MANUAL:      # an instruction manual is kept for searching, it never sets a date
        return out
    who = _who(ex, ctx)
    lang = ctx.lang
    dated = ex.dated
    kind = ex.kind

    def add(d: Optional[DeadlineDraft]) -> None:
        if d is None:
            return
        if any(o.key == d.key and o.date == d.date for o in out):
            return
        out.append(d)

    # ---- payments (invoice, bill, subscription, insurance receipt)
    due = dated.first("due")
    if due and kind in (M.INVOICE, M.BILL, M.SUBSCRIPTION, M.INSURANCE, M.RECEIPT, M.CONTRACT) and _future(due.date, ctx):
        recurring = "monthly" if kind == M.SUBSCRIPTION and ex.period_hint == "monthly" else "none"
        add(DeadlineDraft("payment", M.PAYMENT, tr(lang, "payment_title", who=who), due.date,
                          tr(lang, "payment_basis", label=due.label or "vencimiento", date=_fmt(due.date, ctx)), due.evidence, due.page,
                          85 if due.certain else 60, _lead(ctx, M.PAYMENT), ex.amount, recurring))

    # ---- renewals
    if kind == M.INSURANCE:
        hit = dated.first("renewal") or dated.first("effect_to")
        end_date, basis_key, certain, ev = None, "", True, ("", None)
        if hit:
            end_date, ev = hit.date, _evidence(hit)
            basis_key, certain = "renewal_basis_insurance", hit.certain
        else:
            start = dated.first("effect_from")
            if start and re.search(r"\banual\b|un\s+ano|12\s+meses|\bannual\b|\b1\s+ano", ex.period_hint or "annual"):
                end_date, ev, basis_key, certain = _plus_year(start.date), _evidence(start), "renewal_basis_year", False
        if end_date and _future(end_date, ctx):
            add(DeadlineDraft("renewal", M.RENEWAL, tr(lang, "renewal_title", who=who), end_date, tr(lang, basis_key, date=_fmt(end_date, ctx)),
                              ev[0], ev[1], 85 if certain else 55, _lead(ctx, M.RENEWAL), ex.amount, "yearly"))
            cancel = rules.insurance_cancel_by(end_date)
            if _future(cancel, ctx):
                add(DeadlineDraft("cancel_by", M.CANCEL_BY, tr(lang, "cancel_title", who=who), cancel,
                                  tr(lang, "cancel_basis", end=_fmt(end_date, ctx)), ev[0], ev[1], 85 if certain else 55,
                                  _lead(ctx, M.CANCEL_BY), None, "yearly"))
    elif kind in (M.SUBSCRIPTION, M.CONTRACT):
        hit = dated.first("renewal") or (dated.first("effect_to") if kind == M.CONTRACT else None)
        if hit is None and kind == M.SUBSCRIPTION:
            hit = dated.first("due")
        if hit and _future(hit.date, ctx):
            monthly = kind == M.SUBSCRIPTION and ex.period_hint == "monthly"
            if monthly:
                add(DeadlineDraft("payment", M.PAYMENT, tr(lang, "payment_title", who=who), hit.date,
                                  tr(lang, "payment_basis", label=hit.label or "renovación", date=_fmt(hit.date, ctx)), hit.evidence, hit.page,
                                  80 if hit.certain else 55, _lead(ctx, M.PAYMENT), ex.amount, "monthly"))
            else:
                add(DeadlineDraft("renewal", M.RENEWAL, tr(lang, "renewal_title", who=who), hit.date,
                                  tr(lang, "renewal_basis", date=_fmt(hit.date, ctx)), hit.evidence, hit.page,
                                  80 if hit.certain else 55, _lead(ctx, M.RENEWAL), ex.amount, "yearly" if ex.period_hint == "yearly" else "none"))

    # ---- permanence (telcos and contracts), explicit or by months from the effect date
    perm = dated.first("permanence_end")
    if perm and _future(perm.date, ctx):
        add(DeadlineDraft("permanence_end", M.PERMANENCE_END, tr(lang, "permanence_title", who=who), perm.date,
                          tr(lang, "permanence_basis_text", date=_fmt(perm.date, ctx)), perm.evidence, perm.page, 85, _lead(ctx, M.PERMANENCE_END), None, "none"))
    else:
        for p in ex.periods:
            if p.purpose != "permanence" or p.unit == "day" and p.n < 30:
                continue
            start_hit = dated.first("effect_from") or dated.first("issue", "purchase")
            start, label = (start_hit.date, _hit_label(start_hit, lang)) if start_hit else (
                (meta.received, tr(lang, "basis_received")) if meta.received else (None, ""))
            if start is None:
                continue
            end = rules.period_end(start, p.n, p.unit)
            if _future(end, ctx):
                add(DeadlineDraft("permanence_end", M.PERMANENCE_END, tr(lang, "permanence_title", who=who), end,
                                  tr(lang, "permanence_basis_months", n=p.n, unit=_unit_word(lang, p.unit), start_label=label,
                                     start=_fmt(start, ctx)), start_hit.evidence if start_hit else p.raw, start_hit.page if start_hit else None,
                                  75 if start_hit and start_hit.certain else 55, _lead(ctx, M.PERMANENCE_END), None, "none"))
            break

    # ---- legal guarantee
    if _warranty_applies(ex):
        base_hit = dated.first("delivery") or dated.first("purchase") or dated.first("issue")
        base_date, base_name = None, ""
        if base_hit:
            base_date = base_hit.date
            base_name = {"delivery": "delivery", "purchase": "purchase"}.get(base_hit.role, "issue")
        elif meta.received:
            base_date, base_name = meta.received, "received"
        explicit = next((p for p in ex.periods if p.purpose == "warranty" and p.unit in ("month", "year")), None)
        months = ctx.warranty_months
        from_text = False
        if months is None and explicit is not None:
            months, from_text = explicit.n * (12 if explicit.unit == "year" else 1), True
        if months is None:
            months = ctx.warranty_years * 12
        if base_date and months > 0:
            end = rules.warranty_end(base_date, months=months)
            if _future(end, ctx):
                what = ex.item or who
                basis = tr(lang, "warranty_basis_text" if from_text else "warranty_basis", years=years_text(lang, months),
                           base_es=tr("es", f"base_{base_name}"), base_en=tr("en", f"base_{base_name}"), date=_fmt(base_date, ctx))
                add(DeadlineDraft("warranty", M.WARRANTY_END, tr(lang, "warranty_title", what=what), end, basis,
                                  base_hit.evidence if base_hit else "", base_hit.page if base_hit else None,
                                  80 if base_hit and base_hit.role in ("delivery", "purchase") else 60, _lead(ctx, M.WARRANTY_END), ex.amount, "none"))

    # ---- fines: 20 natural days from the notification (or the period the text states)
    if kind == M.FINE:
        _fine_deadlines(ex, ctx, meta, add)
    if kind in (M.OFFICIAL, M.TAX):
        _admin_periods(ex, ctx, meta, add, skip_fine=False)

    # ---- vehicle and identity documents
    if kind == M.VEHICLE:
        hit = dated.first("itv_next") or dated.first("expiry")
        if hit and _future(hit.date, ctx):
            add(DeadlineDraft("itv", M.ITV, tr(lang, "itv_title", what=ex.item or tr(lang, "vehicle_generic")), hit.date,
                              tr(lang, "itv_basis", date=_fmt(hit.date, ctx)), hit.evidence, hit.page, 85 if hit.certain else 60,
                              _lead(ctx, M.ITV), None, "none"))
    elif kind == M.IDENTITY:
        hit = dated.first("expiry") or dated.first("effect_to")
        if hit and _future(hit.date, ctx):
            add(DeadlineDraft("expiry", M.EXPIRY, tr(lang, "expiry_title", what=_identity_name(ex, lang)), hit.date,
                              tr(lang, "expiry_basis", date=_fmt(hit.date, ctx)), hit.evidence, hit.page, 85 if hit.certain else 60,
                              _lead(ctx, M.EXPIRY), None, "none"))
    else:
        hit = dated.first("expiry")
        if hit and hit.certain and _future(hit.date, ctx) and kind not in (M.PAYSLIP, M.BANK):
            add(DeadlineDraft("expiry", M.EXPIRY, tr(lang, "expiry_title", what=who), hit.date, tr(lang, "expiry_basis", date=_fmt(hit.date, ctx)),
                              hit.evidence, hit.page, 70, _lead(ctx, M.EXPIRY), None, "none"))

    out.sort(key=lambda d: d.date)
    return out


def _hit_label(hit: DateHit, lang: str) -> str:
    """«la fecha de alta», «la fecha de efecto» … the words next to the date, as a noun phrase."""
    label = (hit.label or "").strip()
    if lang != "en" and label.startswith("fecha"):
        return "la " + label
    return tr(lang, "basis_effect" if hit.role == "effect_from" else "basis_issue")


def _plus_year(day: date) -> date:
    return rules.period_end(day, 1, "year")


def _warranty_applies(ex: Extraction) -> bool:
    if ex.issuer.category in NO_WARRANTY_CATEGORIES:
        return False
    if ex.kind in (M.RECEIPT, M.WARRANTY):
        return True
    if ex.kind == M.INVOICE and ex.issuer.category == "shop":
        return True
    return bool(ex.item) and ex.kind in (M.INVOICE, M.RECEIPT, M.WARRANTY, M.OTHER)


def _identity_name(ex: Extraction, lang: str) -> str:
    text = " ".join(f.get("evidence", "") for f in ex.facts[:40]).lower() + " " + (ex.title or "").lower()
    for needle, key in (("pasaporte", "doc_passport"), ("passport", "doc_passport"), ("conducir", "doc_driving"), ("driving", "doc_driving"),
                        ("sanitaria", "doc_health"), ("residencia", "doc_residence"), ("nie", "doc_nie"), ("dni", "doc_dni"), ("identidad", "doc_dni")):
        if needle in text:
            return tr(lang, key)
    return tr(lang, "doc_generic")


def _fine_deadlines(ex: Extraction, ctx: Ctx, meta: Meta, add) -> None:
    lang = ctx.lang
    who = _who(ex, ctx)
    explicit = next((p for p in ex.periods if p.purpose == "fine_pay"), None)
    notified = ex.dated.first("notified")
    base, how, hit = None, "", None
    if notified:
        base, how, hit = notified.date, "explicit", notified
    elif meta.received:
        base, how = meta.received, "mail"
    else:
        issue = ex.dated.first("issue")
        if issue:
            base, how, hit = issue.date, "document", issue
    if base is None:
        ex.notes.append("no_notification_date")
    else:
        if explicit is not None and (explicit.n, explicit.unit) != (rules.FINE_DISCOUNT_DAYS, "day"):
            n, unit, working = explicit.n, explicit.unit, bool(explicit.working)
        else:
            n, unit, working = rules.FINE_DISCOUNT_DAYS, "day", False
        end = rules.admin_deadline(base, n, unit, working, ctx.cal)
        raw_end = base + timedelta(days=n) if (unit == "day" and not working) else None
        base_name = {"explicit": tr(lang, "basis_notified"), "mail": tr(lang, "basis_received"), "document": tr(lang, "basis_issue")}[how]
        if n == rules.FINE_DISCOUNT_DAYS and unit == "day" and not working:
            basis = tr(lang, "fine_basis", n=n, date=_fmt(base, ctx))
        else:
            basis = tr(lang, "fine_basis_period", n=n, unit=_unit_word(lang, unit), kind_es=tr("es", "kind_working" if working else "kind_natural"),
                       kind_en=tr("en", "kind_working" if working else "kind_natural"), date=_fmt(base, ctx))
        if how != "explicit":
            basis += " Base: " + base_name + "."
        if raw_end is not None and raw_end != end:
            basis += " " + tr(lang, "moved_basis", original=_fmt(raw_end, ctx))
        confidence = {"explicit": 90, "mail": 65, "document": 50}[how]
        if end >= ctx.today - timedelta(days=ADMIN_STALE_DAYS):
            ev, page = _evidence(hit)
            add(DeadlineDraft("fine_discount", M.FINE_DISCOUNT, tr(lang, "fine_title", who=who), end, basis, ev, page, confidence,
                              _lead(ctx, M.FINE_DISCOUNT), ex.amount_reduced or ex.amount, "none"))
    # periods the text states for allegations or appeals
    _admin_periods(ex, ctx, meta, add, skip_fine=True)
    due = ex.dated.first("due")
    if due and due.date >= ctx.today:
        add(DeadlineDraft("payment", M.PAYMENT, tr(lang, "pay_title", who=who), due.date,
                          tr(lang, "payment_basis", label=due.label or "plazo de pago", date=_fmt(due.date, ctx)), due.evidence, due.page,
                          80 if due.certain else 55, _lead(ctx, M.PAYMENT), ex.amount, "none"))


def _admin_periods(ex: Extraction, ctx: Ctx, meta: Meta, add, skip_fine: bool) -> None:
    lang = ctx.lang
    who = _who(ex, ctx)
    seen: set[tuple[str, int, str]] = set()
    for p in ex.periods:
        if p.purpose not in ("appeal", "pay", "fine_pay"):
            continue
        if skip_fine and p.purpose == "fine_pay":
            continue
        if ex.kind == M.FINE and p.purpose == "pay":
            continue
        ident = (p.purpose, p.n, p.unit)
        if ident in seen:
            continue
        seen.add(ident)
        if p.purpose == "appeal":
            kind, title, key = M.APPEAL, tr(lang, "appeal_title", who=who), f"appeal:{p.n}{p.unit[0]}"
        elif ex.kind == M.TAX:
            kind, title, key = M.TAX_DUE, tr(lang, "tax_title", who=who), f"tax:{p.n}{p.unit[0]}"
        else:
            kind, title, key = M.PAYMENT, tr(lang, "pay_title", who=who), f"pay:{p.n}{p.unit[0]}"
        add(_admin_period_draft(p, ex, ctx, meta, kind, key, title))
    # a tax document with a plain «hasta el …» / «plazo de ingreso» date
    if ex.kind == M.TAX:
        due = ex.dated.first("due")
        if due and due.date >= ctx.today:
            add(DeadlineDraft("tax", M.TAX_DUE, tr(lang, "tax_title", who=who), due.date, tr(lang, "tax_basis", date=_fmt(due.date, ctx)),
                              due.evidence, due.page, 85 if due.certain else 60, _lead(ctx, M.TAX_DUE), ex.amount, "none"))
