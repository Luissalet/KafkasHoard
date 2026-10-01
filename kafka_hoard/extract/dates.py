"""Dates in Spanish and English text and the role each one plays (issue, due, renewal, expiry, notified…).

Everything works on the *folded* text (lowercase, no accents, same length as the original) so offsets can be used on both.
Text is untrusted data: every function is pure and bounded.
"""

from __future__ import annotations

import re
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

MONTHS = {
    "enero": 1, "ene": 1, "febrero": 2, "feb": 2, "marzo": 3, "mar": 3, "abril": 4, "abr": 4, "mayo": 5, "may": 5, "junio": 6, "jun": 6,
    "julio": 7, "jul": 7, "agosto": 8, "ago": 8, "septiembre": 9, "setiembre": 9, "sept": 9, "sep": 9, "set": 9, "octubre": 10, "oct": 10,
    "noviembre": 11, "nov": 11, "diciembre": 12, "dic": 12,
    "january": 1, "jan": 1, "february": 2, "march": 3, "april": 4, "apr": 4, "june": 6, "july": 7, "august": 8, "aug": 8,
    "september": 9, "october": 10, "november": 11, "december": 12, "dec": 12,
}
_MONTH_ALT = "|".join(sorted(MONTHS, key=len, reverse=True))

MIN_YEAR, MAX_YEAR = 1990, 2100

_ISO = re.compile(r"(?<![\d/.\-])(\d{4})-(\d{2})-(\d{2})(?![\d/\-])")
_NUMERIC = re.compile(r"(?<![\d/.\-])(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4}|\d{2})(?![\d/\-]|\.\d)")
_DMY_TEXT = re.compile(
    rf"(?<![\w/])(\d{{1,2}})\s*(?:st|nd|rd|th|o|º|°)?\s*(?:de\s+)?({_MONTH_ALT})\b\.?,?\s*(?:de\s+|del\s+|of\s+)?(\d{{4}})(?!\d)")
_MDY_TEXT = re.compile(rf"(?<![\w])({_MONTH_ALT})\b\.?\s+(\d{{1,2}})\s*(?:st|nd|rd|th)?,?\s+(\d{{4}})(?!\d)")


@dataclass
class DateHit:
    date: date
    start: int
    end: int
    raw: str
    role: str = ""
    label: str = ""          # the words that gave the role
    certain: bool = False    # the role came from a label next to the date (not from a fallback)
    page: int = 1
    evidence: str = ""

    def to_dict(self) -> dict:
        return {"date": self.date.isoformat(), "role": self.role, "raw": self.raw, "page": self.page, "evidence": self.evidence,
                "label": self.label}


def _valid(year: int, month: int, day: int) -> Optional[date]:
    if not (MIN_YEAR <= year <= MAX_YEAR):
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def find_dates(folded: str) -> list[DateHit]:
    """Every date in the text, in order of appearance (overlaps resolved in favour of the earlier, longer match)."""
    hits: list[DateHit] = []
    for m in _ISO.finditer(folded):
        d = _valid(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d:
            hits.append(DateHit(d, m.start(), m.end(), m.group(0)))
    for m in _NUMERIC.finditer(folded):
        day, month, year = int(m.group(1)), int(m.group(2)), m.group(3)
        y = int(year) + 2000 if len(year) == 2 else int(year)
        d = _valid(y, month, day)
        if d:
            hits.append(DateHit(d, m.start(), m.end(), m.group(0)))
    for m in _DMY_TEXT.finditer(folded):
        d = _valid(int(m.group(3)), MONTHS[m.group(2)], int(m.group(1)))
        if d:
            hits.append(DateHit(d, m.start(), m.end(), m.group(0)))
    for m in _MDY_TEXT.finditer(folded):
        d = _valid(int(m.group(3)), MONTHS[m.group(1)], int(m.group(2)))
        if d:
            hits.append(DateHit(d, m.start(), m.end(), m.group(0)))
    hits.sort(key=lambda h: (h.start, -(h.end - h.start)))
    out: list[DateHit] = []
    for h in hits:
        if out and h.start < out[-1].end:
            continue
        out.append(h)
    return out


# ---------------------------------------------------------------------------------------------------------- roles
def _split_top(pattern: str) -> list[str]:
    """Split a regex on its top-level ``|`` so that each alternative is searched on its own (finditer would let the first
    alternative that matches hide a longer one that starts later)."""
    parts, depth, current, i = [], 0, [], 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":
            current.append(pattern[i:i + 2])
            i += 2
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "|" and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    parts.append("".join(current))
    return parts


class _Alts:
    def __init__(self, pattern: str):
        self.alts = [re.compile(part) for part in _split_top(pattern)]

    def finditer(self, text: str):
        for alt in self.alts:
            yield from alt.finditer(text)


def _p(pattern: str) -> _Alts:
    return _Alts(pattern)


ROLE_PATTERNS: list[tuple[str, _Alts]] = [
    ("issue", _p(r"fecha\s+(?:de\s+)?(?:la\s+)?(?:factura|emision|expedicion|edicion|documento|ticket|recibo\s+de\s+compra)|fecha\s+factura|"
                 r"invoice\s+date|date\s+of\s+issue|issue\s+date|issued\s+on|emitid[oa]\s+el|expedid[oa]\s+el|\bfecha\b|\bdate\b")),
    ("due", _p(r"fecha\s+(?:de\s+)?vencimiento|vencimiento|fecha\s+de\s+cargo|se\s+cargara(?:\s+en\s+su\s+cuenta)?(?:\s+el)?|cargo\s+en\s+(?:su\s+)?cuenta(?:\s+el)?|"
               r"fecha\s+limite\s+de\s+pago|fecha\s+limite|pagar\s+antes\s+del|pago\s+antes\s+del|antes\s+del|due\s+date|payment\s+due|"
               r"pay\s+by|fecha\s+de\s+adeudo|adeudo|fecha\s+de\s+pago|se\s+pasara\s+al\s+cobro(?:\s+el)?|fecha\s+de\s+cobro|"
               r"plazo\s+de\s+pago|plazo\s+de\s+ingreso|(?:pago|pagar|abonar|ingreso|ingresar|abono)\s+(?:\w+\s+){0,3}hasta(?:\s+el)?|"
               r"se\s+cobrara(?:\s+el)?|cobro\s+el|fecha\s+de\s+domiciliacion|se\s+le\s+cargara(?:\s+el)?|"
               r"proximo\s+(?:cobro|cargo|pago|recibo)|next\s+(?:billing(?:\s+date)?|payment|charge)|billing\s+date")),
    ("effect_from", _p(r"fecha\s+de\s+efecto|fecha\s+efecto|efecto\s+desde|efecto\s*:|vigencia\s+desde|inicio\s+de\s+(?:la\s+)?vigencia|"
                       r"inicio\s+del\s+seguro|inicio\s+de\s+cobertura|desde\s+las\s+\d{1,2}[:.]\d{2}\s*(?:horas?)?\s*(?:del|de\s+el)?|"
                       r"desde\s+el|\bdesde\b|effective\s+date|start\s+date|valid\s+from|fecha\s+de\s+inicio|fecha\s+de\s+alta|inicio\s*:")),
    ("effect_to", _p(r"fecha\s+de\s+fin|fin\s+de\s+(?:la\s+)?vigencia|vigencia\s+hasta|hasta\s+las\s+\d{1,2}[:.]\d{2}\s*(?:horas?)?\s*(?:del|de\s+el)?|"
                     r"hasta\s+el|\bhasta\b|fecha\s+de\s+finalizacion|fin\s+del\s+(?:seguro|contrato|periodo)|end\s+date|valid\s+to|"
                     r"fecha\s+de\s+vencimiento\s+del\s+seguro")),
    ("period_from", _p(r"periodo\s+de\s+facturacion|periodo\s+facturado|periodo\s+de\s+consumo|periodo\s+de\s+liquidacion|periodo|billing\s+period")),
    ("renewal", _p(r"proxima\s+renovacion|fecha\s+de\s+renovacion|renovacion|se\s+renovara(?:\s+(?:automaticamente\s+)?el)?|"
                   r"renovar(?:se)?\s+(?:automaticamente\s+)?el|renews?\s+(?:automatically\s+)?(?:on)?|renewal\s+date|next\s+renewal|"
                   r"(?:fecha\s+de\s+)?vencimiento\s+(?:de\s+la\s+|del\s+)(?:poliza|seguro|contrato|periodo\s+de\s+seguro)|"
                   r"vencimiento\s+de\s+la\s+(?:poliza|cobertura)|prorroga\s+el|proxima\s+prorroga")),
    ("purchase", _p(r"fecha\s+de\s+(?:la\s+)?(?:compra|venta|pedido|operacion)|fecha\s+compra|order\s+date|date\s+of\s+purchase|purchase\s+date|"
                    r"comprad[oa]\s+el|pedido\s+realizado\s+el|fecha\s+del\s+pedido")),
    ("delivery", _p(r"fecha\s+de\s+entrega|entregad[oa]\s+el|delivered\s+on|delivery\s+date")),
    ("expiry", _p(r"valid[oa]\s+hasta|vigente\s+hasta|fecha\s+de\s+caducidad|fecha\s+caducidad|caducidad|caduca(?:\s+el)?|validez|valid\s+until|"
                  r"expires?(?:\s+on)?|expiry(?:\s+date)?|fecha\s+de\s+expiracion|fecha\s+de\s+validez|expiration(?:\s+date)?")),
    ("notified", _p(r"fecha\s+de\s+(?:la\s+)?notificacion|puesta\s+a\s+disposicion|notificad[oa]\s+(?:a\s+usted\s+)?el|fecha\s+de\s+recepcion|"
                    r"recibid[oa]\s+el|notification\s+date|date\s+of\s+notification|fecha\s+de\s+comparecencia|notificacion\s+(?:practicada\s+)?el|"
                    r"acceso\s+a\s+la\s+notificacion|fecha\s+de\s+envio\s+de\s+la\s+notificacion")),
    ("itv_next", _p(r"proxima\s+(?:inspeccion|itv|revision)(?:\s+(?:periodica|antes\s+del|el))?|fecha\s+de\s+la\s+proxima\s+inspeccion|next\s+inspection")),
    ("inspection", _p(r"fecha\s+de\s+(?:la\s+)?inspeccion|inspeccionado\s+el|fecha\s+de\s+la\s+itv")),
    ("permanence_end", _p(r"permanencia(?:\s+\w+){0,4}\s+hasta(?:\s+el)?|fin\s+de\s+(?:la\s+)?permanencia|fin\s+(?:del\s+)?compromiso|"
                          r"compromiso\s+de\s+permanencia\s+hasta|periodo\s+de\s+permanencia\s+hasta|permanencia\s+minima\s+hasta")),
    ("offence", _p(r"fecha\s+de\s+(?:la\s+)?(?:infraccion|denuncia|los\s+hechos|comision)|hecho\s+denunciado|fecha\s+y\s+hora\s+de\s+la\s+infraccion")),
    ("birth", _p(r"fecha\s+de\s+nacimiento|nacimiento|date\s+of\s+birth|\bborn\b|f\.\s*nac")),
]

_CONNECTOR = re.compile(r"^\s*(?:al|a|hasta(?:\s+el)?|hasta\s+las\s+\d{1,2}[:.]\d{2}(?:\s*horas?)?(?:\s+del|\s+de\s+el)?|-|–|—|to|until|through|y\s+hasta(?:\s+el)?)\s*$")
_FROM_TO = {"effect_from": "effect_to", "period_from": "period_to"}
_PERIOD_WORDS = re.compile(r"periodo|facturacion|consumo|liquidacion|billing")


def _line_bounds(text: str) -> list[int]:
    starts = [0]
    for m in re.finditer(r"\n", text):
        starts.append(m.end())
    return starts


def _best_label(context: str) -> tuple[str, str, int]:
    """(role, label text, end offset in context) of the label closest to the end of ``context``; longest wins a tie."""
    best: tuple[int, int, str, str] = (-1, 0, "", "")
    for role, pattern in ROLE_PATTERNS:
        for m in pattern.finditer(context):
            key = (m.end(), m.end() - m.start())
            if key > (best[0], best[1]):
                best = (m.end(), m.end() - m.start(), role, m.group(0))
    return best[2], best[3], best[0]


def _labels_in(context: str) -> list[tuple[int, str, str]]:
    """Non-overlapping labels of ``context`` left to right: [(start, role, text)]. Longest match first at each place."""
    found: list[tuple[int, int, str, str]] = []
    for role, pattern in ROLE_PATTERNS:
        for m in pattern.finditer(context):
            found.append((m.start(), m.end(), role, m.group(0)))
    found.sort(key=lambda f: (f[0], -(f[1] - f[0])))
    out: list[tuple[int, str, str]] = []
    last_end = -1
    for start, end, role, text in found:
        if start < last_end:
            continue
        out.append((start, role, text))
        last_end = end
    return out


def assign_roles(folded: str, hits: list[DateHit], window: int = 90) -> list[DateHit]:
    """Give each date the role of the label nearest before it on its line (or alone on the previous line), then pair
    «from … to» ranges and map table headers («Fecha factura   Fecha vencimiento») onto the dates under them."""
    starts = _line_bounds(folded)
    line_no = lambda off: bisect_right(starts, off) - 1  # noqa: E731

    def line_text(n: int) -> str:
        if n < 0 or n >= len(starts):
            return ""
        end = starts[n + 1] - 1 if n + 1 < len(starts) else len(folded)
        return folded[starts[n]:end]

    by_line: dict[int, list[DateHit]] = {}
    for h in hits:
        by_line.setdefault(line_no(h.start), []).append(h)

    for n, row in by_line.items():
        # table header on the previous line: as many labels as dates, in the same order
        prev = n - 1
        while prev >= 0 and not line_text(prev).strip():
            prev -= 1
        header = _labels_in(line_text(prev)) if prev >= 0 else []
        own_prefix = folded[starts[n]:row[0].start]
        if len(row) >= 2 and len(header) == len(row) and not _best_label(own_prefix[-window:])[0]:
            for h, (_, role, text) in zip(row, header):
                h.role, h.label, h.certain = role, text, True
            continue
        for index, h in enumerate(row):
            seg_start = max(starts[n], h.start - window)
            if index > 0:
                seg_start = max(seg_start, row[index - 1].end)
            context = folded[seg_start:h.start]
            role, label, end = _best_label(context)
            if role and len(context) - end <= 40:
                h.role, h.label, h.certain = role, label.strip(), True
                continue
            if not context.strip(" \t:-–—.,;()[]") or not role:
                prev = n - 1
                while prev >= 0 and not line_text(prev).strip():
                    prev -= 1
                if prev >= 0 and index == 0:
                    ptext = line_text(prev).strip()
                    if len(ptext) <= 70:
                        prole, plabel, pend = _best_label(ptext)
                        if prole and len(ptext) - pend <= 12:
                            h.role, h.label, h.certain = prole, plabel.strip(), True

    # «from … to» ranges: the second date takes the matching "to" role
    for a, b in zip(hits, hits[1:]):
        if not _CONNECTOR.match(folded[a.end:b.start]):
            continue
        base = a.role
        if base in _FROM_TO:
            b.role, b.certain, b.label = _FROM_TO[base], True, a.label
        elif not base or base in ("issue",):
            ctx = folded[max(0, a.start - window):a.start]
            kind = "period" if _PERIOD_WORDS.search(ctx) else "effect"
            a.role, b.role = f"{kind}_from", f"{kind}_to"
            a.certain = b.certain = True
    return hits


def attach_evidence(text: str, folded: str, hits: list[DateHit], page_of) -> None:
    """Fill ``page`` and ``evidence`` (the line around the date, trimmed) of each hit."""
    starts = _line_bounds(text)
    for h in hits:
        n = bisect_right(starts, h.start) - 1
        end = starts[n + 1] - 1 if n + 1 < len(starts) else len(text)
        line = text[starts[n]:end].strip()
        h.evidence = (line[:170] + "…") if len(line) > 170 else line
        h.page = page_of(h.start)


@dataclass
class Dated:
    """Dates of a document grouped by role (first occurrence wins; the nearest to the top of the document)."""
    hits: list[DateHit] = field(default_factory=list)

    def first(self, *roles: str) -> Optional[DateHit]:
        for role in roles:
            for h in self.hits:
                if h.role == role:
                    return h
        return None

    def all(self, role: str) -> list[DateHit]:
        return [h for h in self.hits if h.role == role]
