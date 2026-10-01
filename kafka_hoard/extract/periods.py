"""Relative periods in the text («en el plazo de diez (10) días hábiles», «permanencia de 12 meses», «garantía de 3 años»)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

NUMBER_WORDS = {
    "un": 1, "uno": 1, "una": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5, "seis": 6, "siete": 7, "ocho": 8, "nueve": 9, "diez": 10,
    "once": 11, "doce": 12, "trece": 13, "catorce": 14, "quince": 15, "dieciseis": 16, "diecisiete": 17, "dieciocho": 18,
    "diecinueve": 19, "veinte": 20, "veintiun": 21, "veintiuno": 21, "veintiuna": 21, "veintidos": 22, "veintitres": 23,
    "veinticuatro": 24, "veinticinco": 25, "veintiseis": 26, "veintisiete": 27, "veintiocho": 28, "veintinueve": 29, "treinta": 30,
    "cuarenta": 40, "cincuenta": 50, "sesenta": 60, "noventa": 90,
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10, "twelve": 12,
    "fourteen": 14, "fifteen": 15, "twenty": 20, "thirty": 30, "sixty": 60, "ninety": 90,
}
_WORD_ALT = "|".join(sorted(NUMBER_WORDS, key=len, reverse=True))
_NUM = rf"(?P<num>\d{{1,3}}|(?:{_WORD_ALT})(?:\s+y\s+(?:{_WORD_ALT}))?)"
PERIOD = re.compile(
    rf"(?<![\w/.,-]){_NUM}(?:\s*\(\s*\d{{1,3}}\s*\))?\s*(?:(?P<pre>working|business|calendar|natural)\s+)?(?P<unit>dias?|meses|mes|anos?|years?|months?|days?|semanas?|weeks?)\b"
    rf"(?:\s*(?P<mod>habiles|naturales|laborables|calendario|working|business|calendar))?")

PURPOSES: list[tuple[str, re.Pattern]] = [
    ("fine_pay", re.compile(r"pronto\s+pago|reduccion\s+del\s+50|pago\s+voluntario|periodo\s+voluntario|con\s+la\s+reduccion")),
    ("appeal", re.compile(r"alegacion|recurso|reposicion|alzada|recurrir|impugn|reclamacion|audiencia|contestar|responder|subsan|aportar|"
                          r"requerimiento|presentar|comparecer|allegations|appeal|respond")),
    ("pay", re.compile(r"\bpagar|\bpago|ingreso|ingresar|abonar|abono|liquidar|payment|pay\b")),
    ("permanence", re.compile(r"permanencia|compromiso")),
    ("warranty", re.compile(r"garantia|warranty")),
    ("cancel", re.compile(r"\bbaja\b|cancelar|cancelacion|preaviso|desistir|desistimiento|rescindir|notice\s+period")),
]
BASE_HINTS: list[tuple[str, re.Pattern]] = [
    ("notified", re.compile(r"notificacion|notificado|recepcion|recibo\s+de\s+la\s+presente|puesta\s+a\s+disposicion|dia\s+siguiente\s+al\s+de\s+la\s+notif")),
    ("effect", re.compile(r"fecha\s+de\s+efecto|entrada\s+en\s+vigor|efecto\s+del\s+contrato|alta\s+del\s+servicio|activacion")),
    ("issue", re.compile(r"fecha\s+de\s+(?:firma|contrato|emision|compra)|firma\s+del\s+contrato|desde\s+la\s+firma")),
    ("delivery", re.compile(r"entrega|delivery")),
]
WORKING = {"habiles", "laborables", "working", "business"}
NATURAL = {"naturales", "calendario", "calendar", "natural"}


@dataclass
class RelPeriod:
    n: int
    unit: str                       # day | month | year
    working: Optional[bool]         # True / False when the text says so; None when it does not
    purpose: str                    # fine_pay | appeal | pay | permanence | warranty | cancel | generic
    base: str                       # notified | effect | issue | delivery | "" (not stated)
    start: int
    end: int
    raw: str


def _num(token: str) -> Optional[int]:
    token = token.strip()
    if token.isdigit():
        return int(token)
    parts = [p for p in re.split(r"\s+y\s+", token) if p]
    try:
        return sum(NUMBER_WORDS[p] for p in parts)
    except KeyError:
        return None


def find_periods(folded: str) -> list[RelPeriod]:
    out: list[RelPeriod] = []
    for m in PERIOD.finditer(folded):
        n = _num(m.group("num"))
        if n is None or not 0 < n <= 120:
            continue
        unit_raw = m.group("unit")
        unit = "day" if unit_raw.startswith(("dia", "day")) else "month" if unit_raw.startswith(("mes", "month")) else \
            "year" if unit_raw.startswith(("ano", "year")) else "week"
        if unit == "week":
            n, unit = n * 7, "day"
        mod = m.group("mod") or m.group("pre") or ""
        working = True if mod in WORKING else False if mod in NATURAL else None
        # a «hábiles» / «naturales» just before the unit is not part of the pattern; look at the words right after the match
        after = folded[m.end():m.end() + 14]
        am = re.match(r"\s*(habiles|naturales|laborables)", after)
        if working is None and am:
            working = am.group(1) in WORKING
        ctx_start, ctx_end = max(0, m.start() - 160), min(len(folded), m.end() + 200)
        # stay within the paragraph
        para_start = folded.rfind("\n\n", 0, m.start())
        para_end = folded.find("\n\n", m.end())
        if para_start != -1:
            ctx_start = max(ctx_start, para_start)
        if para_end != -1:
            ctx_end = min(ctx_end, para_end)
        ctx = folded[ctx_start:ctx_end]
        purpose = "generic"
        best: Optional[float] = None
        bonus = {"fine_pay": 40, "permanence": 25, "warranty": 25, "cancel": 10}
        for name, pattern in PURPOSES:
            for pm in pattern.finditer(ctx):
                dist = abs((ctx_start + pm.start()) - m.start()) - bonus.get(name, 0)
                if best is None or dist < best:
                    best, purpose = dist, name
        base = ""
        for name, pattern in BASE_HINTS:
            if pattern.search(ctx):
                base = name
                break
        out.append(RelPeriod(n, unit, working, purpose, base, m.start(), m.end(), m.group(0)))
    return out
