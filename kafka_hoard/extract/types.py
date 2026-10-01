"""Value objects of the extraction."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

from ..bizdays import Calendar
from .dates import Dated
from .issuers import Issuer
from .periods import RelPeriod


@dataclass
class Meta:
    """What is known about a document besides its text (a mail, a file name)."""
    from_name: str = ""
    from_address: str = ""
    subject: str = ""
    received: Optional[date] = None
    source: str = "upload"
    file_name: str = ""


@dataclass
class Ctx:
    today: date
    cal: Calendar = field(default_factory=Calendar)
    lang: str = "es"
    warranty_years: int = 3
    warranty_months: Optional[int] = None        # per-document override (months)
    leads: dict[str, list[int]] = field(default_factory=dict)
    skip_kinds: tuple[str, ...] = ()
    overrides: dict[str, Any] = field(default_factory=dict)     # what the user fixed by hand: kind, issuer, ref, amount, issue_date, item


@dataclass
class Hints:
    """Validated facts from the optional model pass: they add to (never replace) what the rules found."""
    kind: str = ""
    issuer: str = ""
    ref: str = ""
    amount: Optional[float] = None
    dates: list[dict[str, Any]] = field(default_factory=list)       # {role, date (ISO), evidence}
    relative: list[dict[str, Any]] = field(default_factory=list)    # {n, unit, working, purpose, evidence}


@dataclass
class DeadlineDraft:
    key: str
    kind: str
    title: str
    date: date
    basis: str
    evidence: str = ""
    page: Optional[int] = None
    confidence: int = 70
    remind: list[int] = field(default_factory=list)
    amount: Optional[float] = None
    recurring: str = "none"

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "kind": self.kind, "title": self.title, "date": self.date.isoformat(), "basis": self.basis,
                "evidence": self.evidence, "page": self.page, "confidence": self.confidence, "remind": list(self.remind),
                "amount": self.amount, "recurring": self.recurring}


@dataclass
class Extraction:
    kind: str = "other"
    kind_score: int = 0
    kind_reasons: list[str] = field(default_factory=list)
    issuer: Issuer = field(default_factory=lambda: Issuer("", "", "other", "none", 0))
    ref: str = ""
    ref_what: str = ""
    series_ref: str = ""
    amount: Optional[float] = None
    amount_reduced: Optional[float] = None
    currency: str = ""
    issue_date: str = ""
    period_from: str = ""
    period_to: str = ""
    item: str = ""
    title: str = ""
    dated: Dated = field(default_factory=Dated)
    periods: list[RelPeriod] = field(default_factory=list)
    deadlines: list[DeadlineDraft] = field(default_factory=list)
    facts: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    confidence: int = 0
    state: str = "review"
    period_hint: str = ""        # monthly | yearly | "" (subscriptions)
    text_length: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind, "kind_score": self.kind_score, "kind_reasons": self.kind_reasons, "issuer": self.issuer.to_dict(),
            "ref": self.ref, "series_ref": self.series_ref, "amount": self.amount, "amount_reduced": self.amount_reduced,
            "currency": self.currency, "issue_date": self.issue_date, "period_from": self.period_from, "period_to": self.period_to,
            "item": self.item, "title": self.title, "dates": [h.to_dict() for h in self.dated.hits],
            "relative": [{"n": p.n, "unit": p.unit, "working": p.working, "purpose": p.purpose, "base": p.base, "raw": p.raw} for p in self.periods],
            "deadlines": [d.to_dict() for d in self.deadlines], "notes": self.notes, "confidence": self.confidence, "state": self.state,
        }
