"""Reference numbers: policy, contract, invoice, expedient, fine bulletin."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .. import model as M

NO = r"(?:n\.?\s*[ºo°]\.?|num\.?|numero|nro\.?|no\.?|number|#)"
TOKEN = r"(?P<tok>[A-Za-z0-9][A-Za-z0-9\-/._]{3,34})"
SEP = r"\s*[:.\-–]?\s*"

# (label, kind of reference, stable across periods?)
LABELS: list[tuple[str, str, bool]] = [
    (rf"{NO}\s+de\s+poliza{SEP}", "policy", True), (rf"poliza\s+{NO}{SEP}", "policy", True), (rf"poliza{SEP}(?=[A-Za-z0-9]*\d)", "policy", True),
    (rf"policy\s+{NO}{SEP}", "policy", True), (rf"policy{SEP}(?=[A-Za-z0-9\-]*\d)", "policy", True),
    (rf"{NO}\s+de\s+contrato{SEP}", "contract", True), (rf"contrato\s+{NO}{SEP}", "contract", True), (rf"referencia\s+del\s+contrato{SEP}", "contract", True),
    (rf"contract\s+{NO}{SEP}", "contract", True), (rf"{NO}\s+de\s+abonado{SEP}", "contract", True), (rf"{NO}\s+de\s+cliente{SEP}", "customer", True),
    (rf"codigo\s+de\s+cliente{SEP}", "customer", True), (r"\bcups\b" + SEP, "supply", True),
    (rf"{NO}\s+de\s+factura{SEP}", "invoice", False), (rf"factura(?:\s+de\s+[a-z]+)?\s+{NO}{SEP}", "invoice", False), (rf"{NO}\s+factura{SEP}", "invoice", False),
    (rf"numero\s+de\s+factura{SEP}", "invoice", False), (rf"invoice\s+(?:{NO}|number){SEP}", "invoice", False), (rf"factura{SEP}(?=[A-Za-z0-9\-/]*\d)", "invoice", False),
    (rf"{NO}\s+de\s+boletin{SEP}", "bulletin", False), (rf"boletin(?:\s+de\s+denuncia)?{SEP}(?:{NO}{SEP})?(?=[A-Za-z0-9\-/]*\d)", "bulletin", False),
    (rf"{NO}\s+de\s+expediente{SEP}", "expedient", False), (rf"expediente{SEP}(?:{NO}{SEP})?(?=[A-Za-z0-9\-/]*\d)", "expedient", False),
    (rf"referencia{SEP}(?=[A-Za-z0-9\-/]*\d)", "reference", False), (rf"{NO}\s+de\s+recibo{SEP}", "invoice", False),
    (rf"{NO}\s+de\s+pedido{SEP}", "order", False), (rf"order\s+{NO}{SEP}", "order", False), (rf"pedido{SEP}(?=[A-Za-z0-9\-/]*\d)", "order", False),
]
COMPILED = [(re.compile(rf"(?<![a-z0-9])(?:{label}){TOKEN}"), what, stable) for label, what, stable in LABELS]

# which reference is THE reference of a document, by kind
PREFERENCE = {
    M.INSURANCE: ["policy", "contract", "reference"], M.CONTRACT: ["contract", "policy", "customer", "reference"],
    M.INVOICE: ["invoice", "order", "reference"], M.BILL: ["invoice", "contract", "supply", "customer"], M.RECEIPT: ["order", "invoice", "reference"],
    M.FINE: ["bulletin", "expedient", "reference"], M.OFFICIAL: ["expedient", "reference", "bulletin"], M.TAX: ["reference", "expedient", "invoice"],
    M.SUBSCRIPTION: ["contract", "customer", "invoice", "order"], M.WARRANTY: ["order", "invoice", "reference"], M.VEHICLE: ["reference"],
}


@dataclass
class RefHit:
    value: str
    what: str
    stable: bool
    start: int
    end: int


def find_refs(original: str, folded: str) -> list[RefHit]:
    out: list[RefHit] = []
    seen: set[int] = set()
    for pattern, what, stable in COMPILED:
        for m in pattern.finditer(folded):
            start, end = m.start("tok"), m.end("tok")
            if start in seen:
                continue
            value = original[start:end].rstrip(".-_/")
            if not re.search(r"\d", value) or len(value) < 4:
                continue
            seen.add(start)
            out.append(RefHit(value, what, stable, start, end))
    out.sort(key=lambda r: r.start)
    return out


def choose_ref(hits: list[RefHit], kind: str) -> tuple[Optional[RefHit], Optional[RefHit]]:
    """(the document's reference, the stable reference that identifies a series across periods, if any)."""
    if not hits:
        return None, None
    order = PREFERENCE.get(kind, ["policy", "contract", "invoice", "expedient", "bulletin", "reference", "customer", "order"])
    main = None
    for what in order:
        main = next((h for h in hits if h.what == what), None)
        if main:
            break
    main = main or hits[0]
    stable = main if main.stable else next((h for h in hits if h.stable and h.what in ("policy", "contract", "supply")), None)
    return main, stable


AMAZON_ORDER = re.compile(r"(?<!\d)(\d{3}-\d{7}-\d{7})(?!\d)")


def find_order_ref(original: str, folded: str) -> str:
    """The order number of a purchase document, or ''. A labelled order reference («nº de pedido», «order number») wins;
    an unlabelled 3-7-7 digit number (the format large marketplaces use) is accepted on its own."""
    for hit in find_refs(original, folded):
        if hit.what == "order":
            return hit.value
    found = AMAZON_ORDER.search(original)
    return found.group(1) if found else ""
