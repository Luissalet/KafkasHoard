"""Document dossier for a question, and the clause-level requirements diff.

Pure and deterministic: archived page text in, structured data out. No database, clock, network or model. Every claim comes from
a date, amount, quantity or obligation that the rules in this file can read from the text or from a table cell, and carries the
citation of the line it was read from. Nothing here is written by a language model (``derived_by`` is always ``rules``); if a
model is ever added, its text must be marked ``derived_by: model``.

A dossier has sections, each one depending on source versions:

* ``source:<doc_id>``  passages and claims of one source; depends on that source's text hash.
* ``synthesis``        claims of all sources grouped by subject, with the contradictions side by side; depends on every source.
* ``conclusion``       what all sources agree on, what conflicts, open questions; depends on the synthesis.

A disagreement is never resolved here: both (or all) positions are listed with their sources and ``resolved`` stays false.
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import date
from difflib import SequenceMatcher
from typing import Any, Optional

from .extract import amounts as amounts_mod
from .extract import dates as dates_mod
from .hoard_link.text import fold

MAX_CLAIMS_PER_SOURCE = 300
MAX_PASSAGES = 12
MAX_LINES = 6000
MAX_SOURCES = 12
MAX_CLAUSES = 1500

STOP = set("""
a al algo and are as at be by con cual cuales cuando cuanto cuantos cuanta de del donde el ella ellas ellos en entre era es esta estan
este esto estos fue for from ha han has have hay how in is it la las le les lo los me mi mis muy no nos o of on or para pero por
que quien se ser si sin sobre son su sus the their then there this to un una uno unos unas what when where which who with ya y
""".split())


# ------------------------------------------------------------------------------------------------------------ hashes and lines
def text_sha(pages: list[dict[str, Any]]) -> str:
    h = hashlib.sha256()
    for p in sorted(pages, key=lambda x: int(x["page"])):
        h.update(f"{int(p['page'])}\x1f{p.get('text') or ''}\x1e".encode("utf-8"))
    return h.hexdigest()


def source_lines(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in sorted(pages, key=lambda x: int(x["page"])):
        for n, raw in enumerate(str(p.get("text") or "").splitlines(), start=1):
            text = raw.strip()
            if text:
                out.append({"page": int(p["page"]), "line": n, "text": text})
    if len(out) > MAX_LINES:
        out = out[:MAX_LINES]
    return out


def cite(doc_id: str, page: int, line: Optional[int] = None) -> str:
    return f"[{doc_id} · p. {page} · l. {line}]" if line else f"[{doc_id} · p. {page}]"


# ------------------------------------------------------------------------------------------------------------ vocabulary
# Subject concepts, from most specific to most generic: the first one that appears near a value names its subject.
CONCEPTS: list[tuple[str, re.Pattern]] = [(name, re.compile(rx)) for name, rx in [
    ("warranty", r"garantia|warranty|guarantee"),
    ("penalty", r"penaliz|recargo|penalty|sancion|multa|late fee|interes(?:es)? de demora|liquidated damages"),
    ("deposit", r"fianza|deposito|deposit|senal|arras"),
    ("notice", r"preaviso|aviso previo|notice period|notice of"),
    ("cancellation", r"cancelacion|cancel|desistimiento|rescision|rescind|resolucion del contrato|termination"),
    ("renewal", r"renov"),
    ("discount", r"descuento|discount|bonificacion|rebate"),
    ("tax", r"\biva\b|\bvat\b|impuesto|\btax\b"),
    ("delivery", r"entrega|entregar|deliver|envio|shipment|shipping"),
    ("signature", r"firma|firmar|signature|sign\b|signing"),
    ("start", r"inicio|comienzo|entrada en vigor|efectos? desde|start|effective|commence"),
    ("end", r"finalizacion|terminacion|vigencia|caducidad|expir|end date|fin del|fecha de fin"),
    ("payment", r"\bpago\b|abono|payment|cobro|vencimiento de (?:la )?factura|\bpay\b"),
    ("price", r"importe|precio|total|tarifa|cuota|coste|costo|cuesta|price|amount|\bfee\b|\bcost\b|prima|honorarios"),
    ("deadline", r"plazo|fecha limite|vencimiento|vence\b|deadline|due date|\bdue\b|antes del|no later than|a mas tardar|hasta el|\bby\b"),
]]
CONCEPT_ORDER = [n for n, _ in CONCEPTS]

_MODAL_POS = re.compile(r"\bdebera(?:n)?\b|\bdebe(?:n)?\b|obligatori|se requiere|es necesari|sera necesari|imprescindible|\bexigible|"
                        r"\bmust\b|\bshall\b|\brequired\b|\bmandatory\b|\bneed(?:s)? to\b|\bhas to\b|\bhave to\b|se exige|sera preciso|es preciso")
_MODAL_NEG = re.compile(r"\bno (?:se )?(?:requiere|exige|sera|es|debera|debe|podra|puede|se permite|estara|esta)\b(?: necesari| obligatori| preciso| exigible| permitid|)|"
                        r"prohibid|\bmust not\b|\bshall not\b|\bnot required\b|\bno longer required\b|\bis not required\b|\bno es preciso\b|"
                        r"\bsin necesidad de\b|\bno (?:necesit|requier|exig)|\bmay not\b|\bcannot\b|\bmust never\b")

_QTY = re.compile(r"(?<![\d.,/-])(\d+(?:[.,]\d+)?)\s*(dias habiles|dias laborables|dias naturales|dias|dia|semanas|semana|meses|mes|anos|ano|horas|hora|"
                  r"working days|business days|calendar days|days|day|weeks|week|months|month|years|year|hours|hour|%|por ciento|percent)(?![a-z])")
_UNIT = {"dias habiles": "working_days", "dias laborables": "working_days", "working days": "working_days", "business days": "working_days",
         "dias naturales": "days", "calendar days": "days", "dias": "days", "dia": "days", "days": "days", "day": "days",
         "semanas": "weeks", "semana": "weeks", "weeks": "weeks", "week": "weeks", "meses": "months", "mes": "months", "months": "months",
         "month": "months", "anos": "years", "ano": "years", "years": "years", "year": "years", "horas": "hours", "hora": "hours",
         "hours": "hours", "hour": "hours", "%": "percent", "por ciento": "percent", "percent": "percent"}
_AMOUNT_HEAD = re.compile(r"importe|precio|total|coste|costo|cuota|tarifa|amount|price|cost|fee|prima|€|\beur|\busd|\$|honorarios|base imponible")
_NUMBER_ONLY = re.compile(r"^[-+]?\s*(?:\d{1,3}(?:[.,\s]\d{3})+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)\s*(?:€|eur|euros|usd|\$)?$")


def parse_qty(raw: str) -> Optional[float]:
    token = raw.replace(" ", "")
    if "," in token and "." in token:
        token = token.replace(".", "").replace(",", ".") if token.rfind(",") > token.rfind(".") else token.replace(",", "")
    elif "," in token:
        token = token.replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def concept_in(folded: str, kind: str) -> str:
    """The most specific subject concept named in a piece of text; ``deadline`` is the generic one for dates, ``price`` for money."""
    for name, rx in CONCEPTS:
        if rx.search(folded):
            if kind == "amount" and name == "deadline":
                continue
            if kind == "date" and name == "price":
                continue
            return name
    return ""


def stems(text: str) -> list[str]:
    out = []
    for w in re.findall(r"[a-z]{3,}", fold(text)):
        if w in STOP or _MODAL_POS.fullmatch(w):
            continue
        out.append(w[:5])
    return out


# ------------------------------------------------------------------------------------------------------------ claim extraction
def _clean_label(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip(" \t:;,.-–—()[]|\"'")
    return text[-140:] if len(text) > 140 else text


def _contexts(folded: str, start: int, end: int) -> tuple[str, str]:
    """The words just before and after a value, up to the previous/next sentence break."""
    left = folded[max(0, start - 90):start]
    cut = max(left.rfind(". "), left.rfind(";"), left.rfind(" y el "), left.rfind(" and "))
    if cut >= 0:
        left = left[cut + 1:]
    right = folded[end:end + 45]
    cut = min([i for i in (right.find(". "), right.find(";")) if i >= 0] or [len(right)])
    return left, right[:cut]


def _mk(kind: str, folded: str, original: str, start: int, end: int, value: Any, unit: str = "", label_hint: str = "",
        raw: str = "", line_date: str = "") -> dict[str, Any]:
    left, right = _contexts(folded, start, end)
    concept = concept_in(fold(label_hint), kind) if label_hint else ""
    near = left
    cut = max(near.rfind(", "), near.rfind(" y "), near.rfind(" e "))
    near = near[cut + 1:] if cut >= 0 else near
    concept = concept or concept_in(near, kind) or concept_in(right, kind) or concept_in(left, kind) or concept_in(folded, kind)
    words = sorted(set(stems(left + " " + right)))[:4]
    if not concept and kind == "amount" and not words:
        concept = "price"
    label = _clean_label(original[max(0, start - len(left)):start]) or _clean_label(original[end:end + len(right)]) or _clean_label(label_hint)
    return {"kind": kind, "value": value, "unit": unit, "concept": concept, "subject": concept or ("w:" + "-".join(words)),
            "claim": label or original.strip()[:140], "raw": raw or original[start:end], "date": line_date}


def _numbers_unit_hit(folded: str) -> list[tuple[int, int, float, str]]:
    out = []
    for m in _QTY.finditer(folded):
        value = parse_qty(m.group(1))
        if value is None:
            continue
        unit = _UNIT[m.group(2)]
        if unit == "weeks":
            value, unit = value * 7, "days"
        out.append((m.start(), m.end(), value, unit))
    return out


def claims_from_text(original: str, label_hint: str = "") -> list[dict[str, Any]]:
    """Dates, amounts, quantities (days, months, percent...) and obligations in one line or cell, with no citation yet."""
    folded = fold(original)
    claims: list[dict[str, Any]] = []
    taken: list[tuple[int, int]] = []
    date_hits = dates_mod.find_dates(folded)
    only_date = date_hits[0].date.isoformat() if len(date_hits) == 1 else ""
    for h in date_hits:
        claims.append(_mk("date", folded, original, h.start, h.end, h.date.isoformat(), "", label_hint, h.raw))
        taken.append((h.start, h.end))
    for h in amounts_mod.money_hits(folded, original):
        if any(h.start < b and a < h.end for a, b in taken):
            continue
        claims.append(_mk("amount", folded, original, h.start, h.end, round(h.value, 2), h.currency, label_hint, h.raw, only_date))
        taken.append((h.start, h.end))
    for a, b, value, unit in _numbers_unit_hit(folded):
        if any(a < y and x < b for x, y in taken):
            continue
        claims.append(_mk("quantity", folded, original, a, b, round(value, 4), unit, label_hint, original[a:b], only_date))
        taken.append((a, b))
    neg, pos = _MODAL_NEG.search(folded), _MODAL_POS.search(folded)
    if neg or pos:
        claims.append(_requirement(original, folded, bool(neg), label_hint, only_date))
    return claims


def _requirement(original: str, folded: str, negative: bool, label_hint: str, line_date: str) -> dict[str, Any]:
    body = _MODAL_NEG.sub(" ", _MODAL_POS.sub(" ", re.sub(r"\d[\d./,-]*", " ", folded)))
    st = sorted(set(stems(body + " " + fold(label_hint))))[:8]
    return {"kind": "requirement", "value": "not_required_or_forbidden" if negative else "required", "unit": "",
            "concept": concept_in(folded, "requirement"), "subject": "r:" + "-".join(st), "stems": st,
            "claim": _clean_label(original)[:200], "raw": original.strip()[:200], "date": line_date}


# ------------------------------------------------------------------------------------------------------------ tables
def _split_cells(line: str, delim: str) -> list[str]:
    if delim == "ws":
        return [c.strip() for c in re.split(r"\t+|\s{2,}", line.strip()) if c.strip()]
    return [c.strip() for c in next(csv.reader([line], delimiter=delim, quotechar='"'), [])]


def table_rows(lines: list[dict[str, Any]], is_csv: bool) -> dict[int, dict[str, Any]]:
    """Index (position in ``lines``) -> {cells, header, row}: lines that belong to a table. A table is a run of lines with the same
    number (>= 2) of cells split by ``;`` ``|`` tab, a comma (csv files only) or two spaces (typed or OCR-read tables); its first line is the header
    when it has no digits."""
    delims = [";", "|", "\t", "ws"] + ([","] if is_csv else [])
    out: dict[int, dict[str, Any]] = {}
    i = 0
    while i < len(lines):
        best: Optional[tuple[str, int, int]] = None
        for d in delims:
            try:
                n = len(_split_cells(lines[i]["text"], d))
            except csv.Error:
                continue
            if n < 2:
                continue
            j = i + 1
            while j < len(lines):
                try:
                    if len(_split_cells(lines[j]["text"], d)) != n:
                        break
                except csv.Error:
                    break
                j += 1
            if j - i >= (2 if is_csv else 3) and (best is None or (j - i) > (best[2] - i)):
                best = (d, n, j)
        if best is None:
            i += 1
            continue
        d, n, j = best
        header = _split_cells(lines[i]["text"], d)
        has_header = not any(ch.isdigit() for c in header for ch in c)
        start = i + 1 if has_header else i
        for k in range(start, j):
            out[k] = {"cells": _split_cells(lines[k]["text"], d), "header": header if has_header else [], "row": k - start + 1}
        i = j
    return out


def claims_from_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    cells, header = row["cells"], row["header"]
    label = cells[0] if cells and not _NUMBER_ONLY.match(fold(cells[0]).strip()) and not dates_mod.find_dates(fold(cells[0])) else ""
    out: list[dict[str, Any]] = []
    for idx, cell in enumerate(cells):
        head = header[idx] if idx < len(header) else ""
        if not cell or (idx == 0 and label):
            continue
        hint = f"{label} {head}".strip()
        found = claims_from_text(cell, hint)
        folded_cell = fold(cell).strip()
        if not any(c["kind"] == "amount" for c in found) and _AMOUNT_HEAD.search(fold(head)) and _NUMBER_ONLY.match(folded_cell):
            value = amounts_mod.parse_number(re.sub(r"[^\d.,\s-]", "", folded_cell).strip())
            if value is not None:
                unit = "USD" if re.search(r"usd|\$", fold(head) + folded_cell) else "EUR" if re.search(r"eur|€", fold(head) + folded_cell) else ""
                found = [c for c in found if c["kind"] != "quantity"]
                found.append({"kind": "amount", "value": round(value, 2), "unit": unit, "concept": concept_in(fold(hint), "amount") or "price",
                              "subject": concept_in(fold(hint), "amount") or "price", "claim": _clean_label(hint) or cell, "raw": cell, "date": ""})
        if not found and head and folded_cell and _NUMBER_ONLY.match(folded_cell) and concept_in(fold(hint), "quantity"):
            value = parse_qty(re.sub(r"[^\d.,\s-]", "", folded_cell).strip())
            if value is not None:
                found = [{"kind": "quantity", "value": value, "unit": "", "concept": concept_in(fold(hint), "quantity"),
                          "subject": concept_in(fold(hint), "quantity"), "claim": _clean_label(hint), "raw": cell, "date": ""}]
        for c in found:
            if label and c["kind"] != "requirement":
                lc = concept_in(fold(label), c["kind"])
                c["concept"] = lc
                c["subject"] = lc or "w:" + "-".join(sorted(set(stems(label)))[:4])
            c["table"] = {"row": row["row"], "column": head or str(idx + 1), "row_label": label}
            if label and c["kind"] != "requirement":
                c["claim"] = _clean_label(f"{label} · {head}") if head else _clean_label(label)
        out.extend(found)
    return out


# ------------------------------------------------------------------------------------------------------------ one source
def keywords(question: str) -> list[str]:
    seen: list[str] = []
    for w in re.findall(r"[a-z0-9]{3,}", fold(question)):
        if w in STOP:
            continue
        k = w if len(w) <= 5 else w[:5]
        if k not in seen:
            seen.append(k)
    return seen[:24]


def _matches(folded: str, kws: list[str]) -> list[str]:
    words = re.findall(r"[a-z0-9]+", folded)
    return [k for k in kws if any(w.startswith(k) for w in words)]


def question_concepts(question: str) -> set[str]:
    folded = fold(question)
    return {name for name, rx in CONCEPTS if rx.search(folded)}


def claim_id(doc_id: str, claim: dict[str, Any]) -> str:
    key = f"{doc_id}|{claim['page']}|{claim['line']}|{claim['kind']}|{claim['value']}|{claim.get('raw', '')}"
    return "c_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]


def extract_source(doc: dict[str, Any], pages: list[dict[str, Any]], question: str, *, max_passages: int = 6, now: float = 0.0) -> dict[str, Any]:
    """The section of one source: relevant passages and every claim read from its text, each with a page/line citation."""
    doc_id = doc["id"]
    lines = source_lines(pages)
    is_csv = str(doc.get("file_ext") or "").lower() == "csv"
    tables = table_rows(lines, is_csv)
    kws, qcon = keywords(question), question_concepts(question)
    passages_all: list[dict[str, Any]] = []
    claims: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    for idx, ln in enumerate(lines):
        row = tables.get(idx)
        found = claims_from_row(row) if row else claims_from_text(ln["text"])
        header_text = " ".join(row["header"]) if row and row["header"] else ""
        hit = _matches(fold(ln["text"] + " " + header_text), kws)
        for c in found:
            ident = (c["kind"], c["subject"], str(c["value"]), c["unit"], ln["page"], ln["line"], c.get("raw", ""))
            if ident in seen or len(claims) >= MAX_CLAIMS_PER_SOURCE:
                continue
            seen.add(ident)
            c.update(page=ln["page"], line=ln["line"], text=ln["text"][:240], cite=cite(doc_id, ln["page"], ln["line"]), doc_id=doc_id,
                     relevant=bool(hit) or c["concept"] in qcon, from_ocr=bool(doc.get("ocr")), derived_by="rules")
            c["id"] = claim_id(doc_id, c)
            claims.append(c)
        if hit:
            text = ln["text"] if not row else " | ".join(f"{(row['header'][i] + '=') if i < len(row['header']) else ''}{v}" for i, v in enumerate(row["cells"]))
            passages_all.append({"page": ln["page"], "line": ln["line"], "text": text[:300], "cite": cite(doc_id, ln["page"], ln["line"]),
                                 "matched": hit, "score": len(hit), "table_row": row["row"] if row else None})
    ranked = sorted(passages_all, key=lambda p: (-p["score"], p["page"], p["line"]))[:max(1, min(max_passages, MAX_PASSAGES))]
    passages = sorted(ranked, key=lambda p: (p["page"], p["line"]))
    empty = [int(p["page"]) for p in pages if not str(p.get("text") or "").strip()]
    return {"doc_id": doc_id, "title": doc.get("title", ""), "kind": doc.get("kind", ""), "ocr": bool(doc.get("ocr")),
            "file_ext": doc.get("file_ext", ""), "text_sha": text_sha(pages), "file_sha": doc.get("file_sha", ""), "page_count": len(pages),
            "empty_pages": empty, "table_rows": len(tables), "passages": passages, "passages_found": len(passages_all),
            "claims": claims, "computed_ts": now, "derived_by": "rules"}


# ------------------------------------------------------------------------------------------------------------ synthesis
def fmt_value(kind: str, value: Any, unit: str, lang: str) -> str:
    if kind == "date":
        try:
            d = date.fromisoformat(str(value))
            return d.strftime("%d/%m/%Y") if lang == "es" else d.isoformat()
        except ValueError:
            return str(value)
    if kind in ("amount", "quantity"):
        num = f"{float(value):,.2f}" if kind == "amount" else f"{float(value):,.2f}".rstrip("0").rstrip(".")
        if lang == "es":
            num = num.replace(",", "§").replace(".", ",").replace("§", ".")
        label = {"percent": "%", "working_days": "días hábiles" if lang == "es" else "working days", "days": "días" if lang == "es" else "days",
                 "months": "meses" if lang == "es" else "months", "years": "años" if lang == "es" else "years",
                 "hours": "horas" if lang == "es" else "hours"}.get(unit, unit)
        if label == "%":
            return f"{num} %" if lang == "es" else f"{num}%"
        return f"{num} {label}".strip()
    if kind == "requirement":
        return {"required": "obligatorio" if lang == "es" else "required",
                "not_required_or_forbidden": "no exigido / prohibido" if lang == "es" else "not required / forbidden"}.get(str(value), str(value))
    return str(value)


def _group_key(c: dict[str, Any]) -> tuple:
    if c["kind"] == "amount":
        return ("amount", c["concept"] or c["subject"])
    if c["kind"] == "quantity":
        return ("quantity", c["subject"], c["unit"])
    return (c["kind"], c["subject"])


def _cluster_requirements(claims: list[dict[str, Any]]) -> dict[int, str]:
    """Requirement claims about the same thing: greedy clusters by overlap of their content stems (Jaccard >= 0.5, two shared at least)."""
    clusters: list[tuple[set, str]] = []
    assign: dict[int, str] = {}
    for i, c in enumerate(claims):
        s = set(c.get("stems") or [])
        for cs, name in clusters:
            inter = len(s & cs)
            if s and cs and inter >= min(2, len(s), len(cs)) and inter / len(s | cs) >= 0.5:
                assign[i] = name
                cs |= s
                break
        else:
            name = "r" + str(len(clusters) + 1)
            clusters.append((set(s), name))
            assign[i] = name
    return assign


def _ident(c: dict[str, Any], dominant_unit: str) -> str:
    if c["kind"] == "amount":
        return f"{float(c['value']):.2f} {c['unit'] or dominant_unit}".strip()
    if c["kind"] == "quantity":
        return f"{float(c['value']):g} {c['unit']}"
    return str(c["value"])


def synthesize(sections: dict[str, dict[str, Any]], lang: str = "es") -> dict[str, Any]:
    """Group claims by subject across sources. Groups where the sources hold different values are contradictions."""
    every = [c for sec in sections.values() for c in sec["claims"]]
    req = [c for c in every if c["kind"] == "requirement"]
    req_cluster = _cluster_requirements(req)
    req_name = {id(c): req_cluster[i] for i, c in enumerate(req)}
    buckets: dict[tuple, list[dict[str, Any]]] = {}
    for c in every:
        if c["kind"] == "requirement":
            key = ("requirement", req_name[id(c)])
        elif c["kind"] != "requirement" and not c["subject"]:
            continue
        else:
            key = _group_key(c)
        buckets.setdefault(key, []).append(c)
    groups, contradictions, agreements, single = [], [], [], []
    for key, members in buckets.items():
        units = [m["unit"] for m in members if m["unit"]]
        dominant = max(set(units), key=units.count) if units else ""
        by_value: dict[str, list[dict[str, Any]]] = {}
        for m in members:
            by_value.setdefault(_ident(m, dominant), []).append(m)
        docs = list(dict.fromkeys(m["doc_id"] for m in members))
        gid = "g_" + hashlib.sha1("|".join(map(str, key)).encode("utf-8")).hexdigest()[:8]
        first = members[0]
        named = T[lang]["subject"].get(first["concept"], "")
        label = first["claim"] if first["kind"] == "requirement" else (named[:1].upper() + named[1:] if named else first["claim"])
        positions = []
        for ident, ms in sorted(by_value.items()):
            positions.append({"value": ms[0]["value"], "unit": ms[0]["unit"] or dominant, "display": fmt_value(ms[0]["kind"], ms[0]["value"], ms[0]["unit"] or dominant, lang),
                              "sources": [{"doc_id": m["doc_id"], "title": sections[m["doc_id"]]["title"], "cite": m["cite"], "text": m["text"],
                                           "claim": m["claim"], "claim_id": m["id"], "from_ocr": m["from_ocr"], "date": m.get("date", "")} for m in ms]})
        per_doc = {d: {_ident(m, dominant) for m in members if m["doc_id"] == d} for d in docs}
        sets = list(per_doc.values())
        relevant = any(m["relevant"] for m in members)
        if len(docs) < 2:
            status, nature = "single", ""
            if len(by_value) > 1:
                status, nature = "conflict", "internal"
        elif all(s == sets[0] for s in sets):
            status, nature = "agree", ""
        else:
            disjoint = any(not (a & b) for i, a in enumerate(sets) for b in sets[i + 1:])
            status, nature = "conflict", "disjoint" if disjoint else "partial"
        group = {"id": gid, "kind": first["kind"], "subject": label, "concept": first["concept"], "relevant": relevant, "status": status,
                 "nature": nature, "sources": docs, "positions": positions, "resolved": False if status == "conflict" else None}
        groups.append(group)
        {"conflict": contradictions, "agree": agreements, "single": single}[status].append(group)
    order = lambda g: (not g["relevant"], CONCEPT_ORDER.index(g["concept"]) if g["concept"] in CONCEPT_ORDER else 99, g["id"])  # noqa: E731
    for lst in (groups, contradictions, agreements, single):
        lst.sort(key=order)
    return {"groups": groups, "contradictions": contradictions, "agreements": agreements, "single_source": single,
            "counts": {"claims": len(every), "groups": len(groups), "contradictions": len(contradictions), "agreements": len(agreements),
                       "single_source": len(single)},
            "note": T[lang]["synth_note"], "derived_by": "rules"}


# ------------------------------------------------------------------------------------------------------------ conclusion
T = {
    "es": {
        "synth_note": "Las contradicciones se listan con todas sus fuentes; Kafka no elige cuál es la vigente.",
        "agree": "Todas las fuentes coinciden en {subject}: {value}.",
        "conflict": "Las fuentes no coinciden en {subject}.",
        "ask_conflict": "¿Cuál es el valor vigente de «{subject}»? {options}",
        "ask_single": "Solo {doc} habla de «{subject}» ({value}); ¿lo confirma otra fuente?",
        "ask_empty": "La fuente {doc} no tiene pasajes sobre la pregunta; ¿es el documento correcto?",
        "ask_notext": "La fuente {doc} tiene páginas sin texto ({pages}); ¿hay que leerlas con OCR?",
        "ask_none": "No se ha encontrado ningún dato fechado, importe ni requisito relacionado con la pregunta.",
        "ask_internal": "La fuente {doc} da valores distintos para «{subject}»; ¿cuál se aplica?",
        "ask_ocr": "Hay datos leídos con OCR en {doc}; conviene comprobarlos en el original.",
        "subject": {"deadline": "el plazo", "delivery": "la entrega", "payment": "el pago", "price": "el importe", "start": "el inicio",
                    "end": "el fin", "renewal": "la renovación", "warranty": "la garantía", "notice": "el preaviso", "penalty": "la penalización",
                    "signature": "la firma", "deposit": "la fianza", "cancellation": "la cancelación", "discount": "el descuento", "tax": "los impuestos"},
    },
    "en": {
        "synth_note": "Contradictions are listed with all their sources; Kafka does not pick the one in force.",
        "agree": "All sources agree on {subject}: {value}.",
        "conflict": "The sources disagree on {subject}.",
        "ask_conflict": "Which value of «{subject}» is in force? {options}",
        "ask_single": "Only {doc} mentions «{subject}» ({value}); does another source confirm it?",
        "ask_empty": "Source {doc} has no passages about the question; is it the right document?",
        "ask_notext": "Source {doc} has pages without text ({pages}); do they need OCR?",
        "ask_none": "No dated item, amount or requirement related to the question was found.",
        "ask_internal": "Source {doc} gives different values for «{subject}»; which one applies?",
        "ask_ocr": "Some data in {doc} was read with OCR; check it against the original.",
        "subject": {"deadline": "the deadline", "delivery": "delivery", "payment": "payment", "price": "the amount", "start": "the start",
                    "end": "the end", "renewal": "renewal", "warranty": "the warranty", "notice": "the notice period", "penalty": "the penalty",
                    "signature": "signing", "deposit": "the deposit", "cancellation": "cancellation", "discount": "the discount", "tax": "taxes"},
    },
}


def _name(group: dict[str, Any], lang: str) -> str:
    """A requirement is named by its own clause: "entregar los planos" is about the plans, not about the delivery date."""
    if group["kind"] == "requirement":
        return group["subject"]
    return T[lang]["subject"].get(group["concept"]) or group["subject"]


def _sources_text(position: dict[str, Any]) -> str:
    return ", ".join(dict.fromkeys(s["cite"] for s in position["sources"]))


def conclude(sections: dict[str, dict[str, Any]], synthesis: dict[str, Any], lang: str = "es") -> dict[str, Any]:
    """Separate what every source says the same from what conflicts, and list what to ask. Never picks a side."""
    t = T[lang]
    agreed = [{"group": g["id"], "subject": _name(g, lang), "statement": t["agree"].format(subject=_name(g, lang), value=g["positions"][0]["display"]),
               "value": g["positions"][0]["display"], "relevant": g["relevant"], "sources": [s["cite"] for s in g["positions"][0]["sources"]]}
              for g in synthesis["agreements"]]
    conflicts = [{"group": g["id"], "subject": _name(g, lang), "statement": t["conflict"].format(subject=_name(g, lang)), "nature": g["nature"],
                  "relevant": g["relevant"], "positions": [{"value": p["display"], "sources": [s["cite"] for s in p["sources"]]} for p in g["positions"]],
                  "resolved": False} for g in synthesis["contradictions"]]
    questions: list[str] = []
    for g in synthesis["contradictions"]:
        if g["nature"] == "internal":
            doc = g["sources"][0]
            questions.append(t["ask_internal"].format(doc=sections[doc]["title"] or doc, subject=_name(g, lang)))
        else:
            options = " / ".join(f"{p['display']} ({_sources_text(p)})" for p in g["positions"])
            questions.append(t["ask_conflict"].format(subject=_name(g, lang), options=options))
    for g in synthesis["single_source"]:
        if g["relevant"]:
            p = g["positions"][0]
            questions.append(t["ask_single"].format(doc=sections[g["sources"][0]]["title"] or g["sources"][0], subject=_name(g, lang), value=p["display"]))
    for doc_id, sec in sections.items():
        name = sec["title"] or doc_id
        if not sec["passages"]:
            questions.append(t["ask_empty"].format(doc=name))
        if sec["empty_pages"]:
            questions.append(t["ask_notext"].format(doc=name, pages=", ".join(map(str, sec["empty_pages"][:10]))))
        if sec["ocr"] and sec["claims"]:
            questions.append(t["ask_ocr"].format(doc=name))
    if not synthesis["counts"]["claims"]:
        questions.append(t["ask_none"])
    return {"agreed": agreed, "conflicts": conflicts, "open_questions": questions, "derived_by": "rules",
            "summary": {"agreed": len(agreed), "conflicts": len(conflicts), "open_questions": len(questions)}}


def build_sections(question: str, sources: list[tuple[dict[str, Any], list[dict[str, Any]]]], *, lang: str = "es", max_passages: int = 6,
                   now: float = 0.0) -> dict[str, Any]:
    sections = {f"source:{d['id']}": extract_source(d, pages, question, max_passages=max_passages, now=now) for d, pages in sources}
    return assemble(sections, lang, now)


def assemble(sections: dict[str, dict[str, Any]], lang: str, now: float) -> dict[str, Any]:
    by_doc = {s["doc_id"]: s for s in sections.values()}
    synthesis = synthesize(by_doc, lang)
    synthesis["computed_ts"] = now
    conclusion = conclude(by_doc, synthesis, lang)
    conclusion["computed_ts"] = now
    return {"sections": sections, "synthesis": synthesis, "conclusion": conclusion}


# ------------------------------------------------------------------------------------------------------------ requirements diff
_ORD = {"primera": "1", "segunda": "2", "tercera": "3", "cuarta": "4", "quinta": "5", "sexta": "6", "septima": "7", "octava": "8",
        "novena": "9", "decima": "10", "first": "1", "second": "2", "third": "3", "fourth": "4", "fifth": "5"}
_HEAD = re.compile(r"^\s*(?:(?:clausula|articulo|art\.?|seccion|apartado|punto|clause|article|section)\s+([0-9]+(?:\.[0-9]+)*|[ivxl]+\b|[a-z]+)\b[\s.:)\-–]*|"
                   r"(\d+(?:\.\d+)*)[.)]\s+|([a-z])\)\s+)")


def _clause_key(line: str) -> Optional[str]:
    m = _HEAD.match(fold(line))
    if not m:
        return None
    key = (m.group(1) or m.group(2) or m.group(3) or "").strip(".")
    return _ORD.get(key, key) or None


def segment_clauses(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Clauses of a document: a heading line (clause/article/section N, ``N.``, ``N)``) plus the lines under it. Without headings,
    each line is a clause."""
    lines = source_lines(pages)
    clauses: list[dict[str, Any]] = []
    cur: Optional[dict[str, Any]] = None
    for ln in lines:
        key = _clause_key(ln["text"])
        if key is not None:
            cur = {"key": key, "lines": [ln], "page": ln["page"], "line": ln["line"]}
            clauses.append(cur)
        elif cur is not None:
            cur["lines"].append(ln)
        else:
            clauses.append({"key": "", "lines": [ln], "page": ln["page"], "line": ln["line"]})
    for c in clauses:
        c["text"] = " ".join(x["text"] for x in c["lines"])
        c["norm"] = " ".join(c["text"].split())
        c["pages"] = sorted({x["page"] for x in c["lines"]})
    return clauses[:MAX_CLAUSES]


def _word_diff(a: str, b: str) -> list[dict[str, str]]:
    x, y = a.split(), b.split()
    out = []
    for op, i, j, k, l in SequenceMatcher(None, x, y, autojunk=False).get_opcodes():
        if op != "equal":
            out.append({"op": {"replace": "modified", "delete": "removed", "insert": "added"}[op], "before": " ".join(x[i:j]), "after": " ".join(y[k:l])})
    return out


def _clause_claims(clause: dict[str, Any], doc_id: str) -> list[dict[str, Any]]:
    out = []
    for ln in clause["lines"]:
        for c in claims_from_text(ln["text"]):
            c.update(page=ln["page"], line=ln["line"], cite=cite(doc_id, ln["page"], ln["line"]), doc_id=doc_id, text=ln["text"][:240])
            out.append(c)
    return out


def _claim_changes(before: list[dict[str, Any]], after: list[dict[str, Any]], lang: str) -> list[dict[str, Any]]:
    def keyed(cs):
        out: dict[tuple, list[dict[str, Any]]] = {}
        for c in cs:
            out.setdefault(("requirement",) if c["kind"] == "requirement" else _group_key(c), []).append(c)
        return out
    kb, ka = keyed(before), keyed(after)
    changes = []
    for key in list(dict.fromkeys([*kb, *ka])):
        b, a = kb.get(key, []), ka.get(key, [])
        vb = {_ident(c, "") for c in b}
        va = {_ident(c, "") for c in a}
        if vb == va:
            continue
        sample = (a or b)[0]
        named = T[lang]["subject"].get(sample["concept"], "") if sample["kind"] != "requirement" else ""
        row = {"kind": sample["kind"], "subject": sample["concept"] or sample["claim"] if sample["kind"] != "requirement" else sample["claim"],
               "label": (named[:1].upper() + named[1:]) if named else sample["claim"],
               "change": "modified" if b and a else "added" if a else "removed",
               "before": [{"display": fmt_value(c["kind"], c["value"], c["unit"], lang), "cite": c["cite"]} for c in b if _ident(c, "") not in va],
               "after": [{"display": fmt_value(c["kind"], c["value"], c["unit"], lang), "cite": c["cite"]} for c in a if _ident(c, "") not in vb]}
        changes.append(row)
    return changes


def requirements_diff(before_doc: dict[str, Any], before_pages: list[dict[str, Any]], after_doc: dict[str, Any], after_pages: list[dict[str, Any]],
                      *, lang: str = "es") -> dict[str, Any]:
    """Clauses added, removed and modified between two versions (or two sources), and the claims each one affects."""
    cb, ca = segment_clauses(before_pages), segment_clauses(after_pages)
    used_b: set[int] = set()
    pairs: dict[int, int] = {}                     # index in ca -> index in cb
    keys_b = {}
    for i, c in enumerate(cb):
        if c["key"]:
            keys_b.setdefault(c["key"], []).append(i)
    keys_a = {}
    for j, c in enumerate(ca):
        if c["key"]:
            keys_a.setdefault(c["key"], []).append(j)
    for key, js in keys_a.items():
        if len(js) == 1 and len(keys_b.get(key, [])) == 1:
            pairs[js[0]] = keys_b[key][0]
            used_b.add(keys_b[key][0])
    for j, c in enumerate(ca):
        if j in pairs:
            continue
        best, best_r = None, 0.0
        for i, o in enumerate(cb):
            if i in used_b:
                continue
            if o["norm"] == c["norm"]:
                best, best_r = i, 1.0
                break
            r = SequenceMatcher(None, o["norm"], c["norm"], autojunk=False).ratio() if abs(len(o["norm"]) - len(c["norm"])) < max(len(o["norm"]), len(c["norm"])) * 0.6 else 0.0
            if r > best_r:
                best, best_r = i, r
        if best is not None and best_r >= 0.6:
            pairs[j] = best
            used_b.add(best)
    changes: list[dict[str, Any]] = []
    unchanged = 0
    for j, c in enumerate(ca):
        if j in pairs:
            o = cb[pairs[j]]
            if o["norm"] == c["norm"]:
                unchanged += 1
                continue
            changes.append({"change": "modified", "clause": c["key"] or o["key"], "before": _side(o, before_doc["id"]), "after": _side(c, after_doc["id"]),
                            "words": _word_diff(o["norm"], c["norm"]),
                            "claims": _claim_changes(_clause_claims(o, before_doc["id"]), _clause_claims(c, after_doc["id"]), lang), "_pos": c["page"] * 10000 + c["line"]})
        else:
            changes.append({"change": "added", "clause": c["key"], "before": None, "after": _side(c, after_doc["id"]), "words": [],
                            "claims": _claim_changes([], _clause_claims(c, after_doc["id"]), lang), "_pos": c["page"] * 10000 + c["line"]})
    for i, o in enumerate(cb):
        if i not in used_b:
            changes.append({"change": "removed", "clause": o["key"], "before": _side(o, before_doc["id"]), "after": None, "words": [],
                            "claims": _claim_changes(_clause_claims(o, before_doc["id"]), [], lang), "_pos": o["page"] * 10000 + o["line"]})
    changes.sort(key=lambda x: (x["_pos"], x["change"]))
    for n, ch in enumerate(changes):
        ch.pop("_pos")
        ch["index"] = n
    count = {k: sum(1 for c in changes if c["change"] == k) for k in ("added", "removed", "modified")}
    return {"before": {"doc_id": before_doc["id"], "title": before_doc.get("title", ""), "clauses": len(cb)},
            "after": {"doc_id": after_doc["id"], "title": after_doc.get("title", ""), "clauses": len(ca)},
            "summary": {**count, "unchanged": unchanged}, "changes": changes,
            "claims_affected": sum(len(c["claims"]) for c in changes),
            "method": "clause_alignment_by_number_then_similarity", "derived_by": "rules",
            "note": ("Compara texto archivado por cláusulas; no interpreta el sentido jurídico." if lang == "es"
                     else "Compares archived text clause by clause; no legal interpretation.")}


def _side(clause: dict[str, Any], doc_id: str) -> dict[str, Any]:
    return {"text": clause["text"][:600], "pages": clause["pages"], "cite": cite(doc_id, clause["page"], clause["line"])}
