"""Amounts in Spanish («1.234,56 €») and English («€1,234.56») formats, preferring labelled totals."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_CUR = r"(?:€|eur(?:os?)?|usd|\$|£|gbp)"
_NUM = r"\d{1,3}(?:[.,\s]\d{3})+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?"
MONEY = re.compile(rf"(?:(?P<pre>{_CUR})\s*)?(?P<num>{_NUM})(?!\d)(?:\s*(?P<post>{_CUR}\b|€))?")

CURRENCIES = {"€": "EUR", "eur": "EUR", "euro": "EUR", "euros": "EUR", "usd": "USD", "$": "USD", "£": "GBP", "gbp": "GBP"}


def parse_number(token: str, prefix_currency: bool = False) -> Optional[float]:
    """Parse «1.234,56», «1,234.56», «1234,56», «12,5», «1.234» (thousands) into a float; None when it is not a number."""
    tok = token.strip().replace(" ", " ")
    if not tok or not re.fullmatch(r"[\d.,\s]+", tok):
        return None
    tok = tok.replace(" ", "")
    if not tok or not re.search(r"\d", tok):
        return None
    has_dot, has_comma = "." in tok, "," in tok
    if has_dot and has_comma:
        decimal = "," if tok.rfind(",") > tok.rfind(".") else "."
        thousands = "." if decimal == "," else ","
        tok = tok.replace(thousands, "").replace(decimal, ".")
    elif has_comma:
        parts = tok.split(",")
        if len(parts) > 2 or (len(parts[-1]) == 3 and prefix_currency and len(parts[0]) <= 3):
            tok = tok.replace(",", "")                       # 1,234,567 or €1,234
        else:
            tok = tok.replace(",", ".")                      # 12,5 / 1234,56
    elif has_dot:
        parts = tok.split(".")
        if len(parts) > 2 or (len(parts[-1]) == 3 and 1 <= len(parts[0]) <= 3 and not prefix_currency):
            tok = tok.replace(".", "")                       # 1.234 / 12.345.678 (Spanish thousands)
    try:
        value = float(tok)
    except ValueError:
        return None
    return value if 0 <= value < 1e9 else None


@dataclass
class AmountHit:
    value: float
    currency: str
    start: int
    end: int
    raw: str
    label: str = ""
    priority: int = 0
    labelled: bool = False
    page: int = 1
    evidence: str = ""


def money_hits(folded: str, original: str) -> list[AmountHit]:
    """Every number written with a currency marker (before or after)."""
    out: list[AmountHit] = []
    for m in MONEY.finditer(folded):
        pre, post = m.group("pre"), m.group("post")
        marker = (pre or post or "").lower()
        if not marker:
            continue
        value = parse_number(m.group("num"), prefix_currency=bool(pre) and not post)
        if value is None:
            continue
        out.append(AmountHit(value, CURRENCIES.get(marker, "EUR"), m.start(), m.end(), original[m.start():m.end()]))
    return out


# label -> priority (higher = more likely to be THE amount to pay)
TOTAL_LABELS: list[tuple[int, re.Pattern]] = [
    (10, re.compile(r"total\s+a\s+pagar|importe\s+total\s+a\s+pagar|total\s+a\s+ingresar|importe\s+a\s+pagar|importe\s+a\s+cargar|"
                    r"amount\s+due|total\s+due|importe\s+de\s+la\s+sancion|importe\s+de\s+la\s+multa|importe\s+de\s+la\s+deuda|"
                    r"importe\s+a\s+ingresar|total\s+deuda|importe\s+de\s+la\s+liquidacion")),
    (9, re.compile(r"importe\s+total|total\s+factura|total\s+de\s+la\s+factura|total\s+recibo|prima\s+total|total\s+prima|"
                   r"total\s*\(\s*iva\s+incluido\s*\)|total\s+iva\s+incluido|importe\s+del\s+recibo|total\s+amount|total\s+invoice|"
                   r"total\s+importe|total\s+pedido|total\s+de\s+la\s+compra|total\s+compra")),
    (7, re.compile(r"importe\s+de\s+la\s+prima|cuota\s+total|prima\s+anual|prima\s+del\s+seguro|total\s+cuota|importe\s+anual|"
                   r"cuota\s+mensual|cuota\s+anual|precio\s+total|precio\s+final|amount\s+paid|importe\s+pagado|total\s+pagado")),
    (5, re.compile(r"(?<![\w])total(?![\w])|importe(?![\w])|amount(?![\w])|prima(?![\w])")),
]
IGNORE_BEFORE = re.compile(r"base\s+imponible|subtotal|sub-total|cuota\s+iva|\biva\b|\bvat\b|\btax\b|impuesto|descuento|recargo|"
                           r"bonificacion|gastos\s+de\s+envio|envio|propina|cambio|entregado|devuelto|consumo|exento|retencion|irpf|"
                           r"base\s+i\.?\s*v\.?\s*a|neto|prima\s+neta|cuota\s+neta")
REDUCED_LABELS = re.compile(r"(?:con\s+)?(?:la\s+)?reduccion\s+(?:del\s+)?50\s*%?|importe\s+reducido|pronto\s+pago|50\s*%\s+de\s+reduccion|"
                            r"reducida|importe\s+con\s+descuento\s+del\s+50")


def _line_span(text: str, pos: int) -> tuple[int, int]:
    start = text.rfind("\n", 0, pos) + 1
    end = text.find("\n", pos)
    return start, len(text) if end == -1 else end


def _first_money(region: str):
    """First number of ``region`` that is not part of a date, a time or a reference («12/10/2026», «10:30»)."""
    for m in MONEY.finditer(region):
        before = region[m.start() - 1:m.start()] if m.start() else ""
        after = region[m.end():m.end() + 1]
        if before in ("/", "-", ":") or after in ("/", ":", "%") or region[m.end():m.end() + 2].strip().startswith("%"):
            continue
        if before.isalpha() and before not in ("€",):
            continue
        return m
    return None


def labelled_amounts(folded: str, original: str) -> list[AmountHit]:
    """Amounts that follow a total-like label on the same line (or alone on the next line)."""
    out: list[AmountHit] = []
    seen: set[int] = set()
    for priority, pattern in TOTAL_LABELS:
        for m in pattern.finditer(folded):
            ls, le = _line_span(folded, m.end())
            prefix = folded[ls:m.start()]
            if IGNORE_BEFORE.search(prefix[-30:]) and priority <= 5:
                continue
            # the label must not be a prefix of an ignored phrase («total descuento», «importe iva»)
            tail = folded[m.end():m.end() + 14]
            if re.match(r"\s*(?:de\s+)?(?:descuento|iva|impuesto|base|bonificacion|recargo|envio)", tail):
                continue
            region_end = le
            region = folded[m.end():region_end]
            nm = _first_money(region)
            if nm is None or not re.search(r"\d", region[:40]):
                # amount alone on the next line
                nxt_end = folded.find("\n", le + 1)
                nxt_end = len(folded) if nxt_end == -1 else nxt_end
                nxt = folded[le + 1:nxt_end]
                if len(nxt.strip()) <= 24:
                    nm2 = _first_money(nxt)
                    if nm2 and nm2.start() <= 3:
                        region, nm, base = nxt, nm2, le + 1
                    else:
                        continue
                else:
                    continue
            else:
                base = m.end()
            if nm.start() > 60:
                continue
            pre, post = nm.group("pre"), nm.group("post")
            value = parse_number(nm.group("num"), prefix_currency=bool(pre) and not post)
            if value is None:
                continue
            pos = base + nm.start()
            if pos in seen:
                continue
            seen.add(pos)
            marker = (pre or post or "").lower()
            out.append(AmountHit(value, CURRENCIES.get(marker, "EUR"), pos, base + nm.end(), original[pos:base + nm.end()],
                                 label=original[m.start():m.end()].strip(), priority=priority, labelled=True))
    return out


def choose_amount(folded: str, original: str) -> tuple[Optional[AmountHit], Optional[AmountHit]]:
    """(amount, reduced amount). Labelled totals win; the reduced (50 %) fine amount is the one next to a reduction label."""
    labelled = labelled_amounts(folded, original)
    reduced: Optional[AmountHit] = None
    plain = money_hits(folded, original)
    starts = sorted({x.start for x in plain})
    ends = sorted({x.end for x in plain})
    for h in plain + labelled:
        ls, le = _line_span(folded, h.start)
        # The label of an amount sits between it and its neighbours: «Importe: 100,00 €. Importe con reducción: 50,00 €»
        # must not lend the reduction label to the 100.
        prev_end = max([e for e in ends if e <= h.start] or [ls])
        next_start = min([s for s in starts if s >= h.end] or [le])
        before = folded[max(ls, prev_end, h.start - 80):h.start]
        after = folded[h.end:min(le, next_start, h.end + 60)]
        after = re.split(r"[.;|]\s|\s{2,}", after, maxsplit=1)[0]  # the next sentence labels the next amount
        context = before + " " + after
        if REDUCED_LABELS.search(context):
            reduced = reduced or h
    best: Optional[AmountHit] = None
    pool = [h for h in labelled if h is not reduced and not (reduced and h.start == reduced.start)]
    if pool:
        best = max(pool, key=lambda h: (h.priority, -h.start))
    else:
        candidates = [h for h in plain if not (reduced and h.start == reduced.start)]
        # unlabelled: skip amounts on ignored lines (tax, base) and take the largest
        keep = []
        for h in candidates:
            ls, le = _line_span(folded, h.start)
            if IGNORE_BEFORE.search(folded[ls:h.start]):
                continue
            keep.append(h)
        if keep:
            best = max(keep, key=lambda h: h.value)
    if reduced is not None and best is not None and reduced.value >= best.value:
        reduced = None
    return best, reduced
