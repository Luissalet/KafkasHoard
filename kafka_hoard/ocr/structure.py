"""The structured side of an OCR result: Markdown, typed blocks in reading order, tables as rows and columns, the language.

Indra gives a page as Markdown (headings ``##``, paragraphs, lists, tables as HTML ``<table>``) and as layout blocks in reading order
(``{label, bbox, text}``). This module turns that into what the family asks for: Markdown with GitHub tables, blocks with a kind and their
lines, tables as rows (CSV, XLSX), a plain text that keeps table rows on one line (what Kafka stores and searches) and a language guess
(Indra reads the script of each line but does not name the language). Engines without layout give their lines as one text block.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from html.parser import HTMLParser
from typing import Any, Iterable, Optional, Sequence
from xml.sax.saxutils import escape as _xml

KINDS = {"Text": "paragraph", "Section-Header": "heading", "Page-Header": "page_header", "Page-Footer": "page_footer", "List-Group": "list",
         "Table": "table", "Caption": "caption", "Footnote": "footnote", "Table-Of-Contents": "toc", "Form": "form", "Picture": "picture"}
FURNITURE = ("page_header", "page_footer")
TABLE_RE = re.compile(r"<table>.*?</table>", re.S | re.I)
CELL_SEP = "  "                       # cells of a table row in the plain text: the two spaces Kafka's line joiner already uses
MATH_LINE = re.compile(r"^\s*\$\$?.+\$\$?\s*$")


# ------------------------------------------------------------------------------------------------------------ tables
class _TableParser(HTMLParser):
    """``<table><tr><th colspan=2>a</th>…`` -> ``[[rows of cells]]`` per table; a cell spanning n columns keeps its text in the first one."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self.header: list[bool] = []
        self._row: Optional[list[str]] = None
        self._cell: Optional[list[str]] = None
        self._span = 1
        self._th = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag == "table":
            self.tables.append([])
            self.header.append(False)
        elif tag == "tr" and self.tables:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []
            self._th = tag == "th"
            try:
                self._span = max(1, min(50, int(dict(attrs).get("colspan") or 1)))
            except ValueError:
                self._span = 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._row.extend([""] * (self._span - 1))
            if self._th and len(self.tables[-1]) == 0:
                self.header[-1] = True
            self._cell = None
        elif tag == "tr" and self._row is not None and self.tables:
            self.tables[-1].append(self._row)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def parse_tables(markdown: str) -> list[dict[str, Any]]:
    """Every ``<table>`` of an Indra Markdown page: ``[{rows, header, n_rows, n_cols}]`` with every row padded to the widest one."""
    out: list[dict[str, Any]] = []
    for chunk in TABLE_RE.findall(markdown or ""):
        parser = _TableParser()
        try:
            parser.feed(chunk)
            parser.close()
        except Exception:  # noqa: BLE001 - a broken table is skipped, never fatal
            continue
        for rows, header in zip(parser.tables, parser.header):
            width = max((len(r) for r in rows), default=0)
            if not rows or not width:
                continue
            rows = [r + [""] * (width - len(r)) for r in rows]
            out.append({"rows": rows, "header": bool(header), "n_rows": len(rows), "n_cols": width})
    return out


def _pipe(cell: str) -> str:
    return (cell or "").replace("|", "\\|").replace("\n", " ").strip()


def pipe_table(rows: Sequence[Sequence[str]]) -> str:
    """A GitHub Markdown table; the first row is the header (Markdown has no table without one)."""
    if not rows:
        return ""
    width = max(len(r) for r in rows)
    lines = []
    for i, row in enumerate(rows):
        cells = [_pipe(c) for c in row] + [""] * (width - len(row))
        lines.append("| " + " | ".join(cells) + " |")
        if i == 0:
            lines.append("|" + "|".join(" --- " for _ in range(width)) + "|")
    return "\n".join(lines)


def markdown_with_pipes(markdown: str) -> str:
    """Indra's Markdown with each HTML table written as a GitHub table (merged cells become empty cells)."""
    def repl(match: re.Match) -> str:
        tables = parse_tables(match.group(0))
        return "\n\n".join(pipe_table(t["rows"]) for t in tables) or match.group(0)

    return TABLE_RE.sub(repl, markdown or "")


def to_csv(rows: Sequence[Sequence[str]], *, delimiter: str = ",") -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=delimiter, lineterminator="\r\n")
    for row in rows:
        writer.writerow(list(row))
    return buf.getvalue()


_ES_NUM = re.compile(r"^-?(\d{1,3}(\.\d{3})+|\d+),\d+$")          # 1.234,56 and 45,90
_THOUSANDS = re.compile(r"^-?\d{1,3}(\.\d{3})+$")                  # 1.234 or 12.345.678: Spanish thousands, ambiguous -> text
_PLAIN = re.compile(r"^-?(0|[1-9]\d{0,14})(\.\d+)?$")              # 7, 45.90 (no leading zeros: codes keep them)


def number_value(text: str) -> Optional[str]:
    """The number a cell holds, written for a spreadsheet («45,90» -> «45.90»); None for text, codes with leading zeros and «1.234»."""
    value = (text or "").strip()
    if not value or len(value) > 18 or _THOUSANDS.match(value):
        return None
    if _ES_NUM.match(value):
        return value.replace(".", "").replace(",", ".")
    if _PLAIN.match(value):
        return value
    return None


def _cell_xml(ref: str, value: str) -> str:
    number = number_value(value)
    if number is not None:
        return f'<c r="{ref}"><v>{number}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{_xml((value or "").strip())}</t></is></c>'


def _col(n: int) -> str:
    name = ""
    n += 1
    while n:
        n, rem = divmod(n - 1, 26)
        name = chr(65 + rem) + name
    return name


def xlsx_bytes(sheets: Iterable[tuple[str, Sequence[Sequence[str]]]]) -> bytes:
    """A minimal .xlsx (one sheet per table, inline strings, numbers written as numbers with the Spanish 1.234,56 read too)."""
    sheets = list(sheets) or [("Tabla 1", [])]
    names: list[str] = []
    for i, (name, _rows) in enumerate(sheets, start=1):
        clean = re.sub(r"[\[\]:*?/\\]", " ", str(name or f"Tabla {i}")).strip()[:31] or f"Tabla {i}"
        while clean in names:
            clean = (clean[:27] + f" {i}")[:31]
        names.append(clean)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                   '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                   '<Default Extension="xml" ContentType="application/xml"/>'
                   '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                   + "".join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                             for i in range(1, len(sheets) + 1)) + "</Types>")
        z.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
                   "</Relationships>")
        z.writestr("xl/workbook.xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                   'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>'
                   + "".join(f'<sheet name="{_xml(n, {chr(34): "&quot;"})}" sheetId="{i}" r:id="rId{i}"/>' for i, n in enumerate(names, start=1))
                   + "</sheets></workbook>")
        z.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   + "".join(f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                             f'Target="worksheets/sheet{i}.xml"/>' for i in range(1, len(sheets) + 1)) + "</Relationships>")
        for i, (_name, rows) in enumerate(sheets, start=1):
            body = []
            for r, row in enumerate(rows, start=1):
                body.append(f'<row r="{r}">' + "".join(_cell_xml(f"{_col(c)}{r}", v) for c, v in enumerate(row)) + "</row>")
            z.writestr(f"xl/worksheets/sheet{i}.xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                       '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
                       + "".join(body) + "</sheetData></worksheet>")
    return buf.getvalue()


# ------------------------------------------------------------------------------------------------------------ language
STOPWORDS = {
    "es": "de la que el en y a los se del las un por con no una su para es al lo como más pero sus le ya o este sí porque esta entre cuando muy sin sobre también me hasta hay donde quien desde todo nos durante todos uno les ni contra otros ese eso ante ellos e esto mí antes algunos qué unos yo otro otras otra él tanto esa estos mucho quienes nada muchos cual poco ella estar estas algunas algo nosotros mi mis tú te ti tu tus usted ustedes su factura importe fecha plazo día",
    "en": "the of and to in is that for it as was with be by on not he this are or his from at which but have an they you were her she there been has their one all would will we if can more when so no what up out who its about into than them only other new some could these then do any like said my me how our us your may should must please here also amount date invoice",
    "fr": "de la le et les des en un une du est que pour qui dans sur au pas par ne plus se ce il avec sont ou mais nous vous leur cette son ses aux été comme être fait tout bien elle ont deux",
    "pt": "de a o que e do da em um para é com não uma os no se na por mais as dos como mas foi ao ele das tem à seu sua ou ser quando muito há nos já está eu também só pelo pela até isso ela entre era depois sem mesmo aos ter seus quem nas me esse eles estão você",
    "it": "di e il la che in un per è non una sono del le della con si da al dei i gli lo ma come anche più nel alla ha questo questa ci delle essere sul se ne o hanno era molto",
    "de": "der die und in den von zu das mit sich des auf für ist im dem nicht ein eine als auch es an werden aus er hat dass sie nach wird bei einer um am sind noch wie einem über einen so zum war haben nur oder aber vor zur bis mehr durch man sehr",
    "ca": "de la el i a que en les els per un una amb del no es és al ha seu seva dels pel però més com ho són aquest aquesta també quan hi va ser tot molt tots fins sobre entre sense",
}
_STOP = {lang: set(words.split()) for lang, words in STOPWORDS.items()}
_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
SCRIPTS = (("cjk", 0x4E00, 0x9FFF), ("ja", 0x3040, 0x30FF), ("ko", 0xAC00, 0xD7AF), ("cyrillic", 0x0400, 0x04FF), ("greek", 0x0370, 0x03FF),
           ("arabic", 0x0600, 0x06FF), ("hebrew", 0x0590, 0x05FF), ("devanagari", 0x0900, 0x097F), ("thai", 0x0E00, 0x0E7F))
SCRIPT_LANG = {"ja": "ja", "ko": "ko", "cjk": "zh", "greek": "el", "hebrew": "he", "thai": "th", "devanagari": "hi"}


def script_of(text: str) -> str:
    """The writing system most letters of ``text`` use: latin, cyrillic, greek, arabic, hebrew, cjk, ja, ko, devanagari, thai."""
    counts: dict[str, int] = {}
    for ch in text or "":
        if not ch.isalpha():
            continue
        code = ord(ch)
        name = next((n for n, lo, hi in SCRIPTS if lo <= code <= hi), "latin" if code < 0x0250 or 0x1E00 <= code <= 0x1EFF else "other")
        counts[name] = counts.get(name, 0) + 1
    if not counts:
        return ""
    if counts.get("ja"):                     # kana among kanji is Japanese
        counts["ja"] += counts.pop("cjk", 0)
    return max(counts, key=lambda k: counts[k])


def detect_language(text: str) -> dict[str, Any]:
    """``{code, confidence, script}``: the language by its most frequent function words (es, en, fr, pt, it, de, ca), or by its script
    (zh, ja, ko, el, he, th, hi). ``code`` is "" when there is too little text to tell."""
    script = script_of(text)
    if script and script != "latin":
        return {"code": SCRIPT_LANG.get(script, ""), "confidence": 0.9 if script in SCRIPT_LANG else 0.0, "script": script}
    words = [w.lower() for w in _WORD.findall(text or "")]
    if len(words) < 3:
        return {"code": "", "confidence": 0.0, "script": script}
    scores = {lang: sum(1 for w in words if w in stop) for lang, stop in _STOP.items()}
    total = sum(scores.values())
    best = max(scores, key=lambda k: scores[k])
    if not scores[best]:
        return {"code": "", "confidence": 0.0, "script": script}
    share = scores[best] / total
    coverage = min(1.0, scores[best] / max(3.0, len(words) * 0.25))
    return {"code": best, "confidence": round(share * coverage, 2), "script": script}


def languages_of(parts: Iterable[str]) -> list[dict[str, Any]]:
    """The languages of several pieces of text (blocks), weighted by letters: ``[{code, share}]``, largest first."""
    weight: dict[str, int] = {}
    for part in parts:
        guess = detect_language(part)
        if guess["code"] and guess["confidence"] >= 0.2:
            weight[guess["code"]] = weight.get(guess["code"], 0) + sum(ch.isalpha() for ch in part)
    total = sum(weight.values())
    return [{"code": k, "share": round(v / total, 2)} for k, v in sorted(weight.items(), key=lambda kv: -kv[1])] if total else []


# ------------------------------------------------------------------------------------------------------------ one page
def _inside(box: Sequence[Sequence[float]], bbox: Sequence[float]) -> bool:
    xs = [p[0] for p in box]
    ys = [p[1] for p in box]
    cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
    return bbox[0] - 2 <= cx <= bbox[2] + 2 and bbox[1] - 2 <= cy <= bbox[3] + 2


def _kind(label: str, text: str) -> str:
    kind = KINDS.get(label, "paragraph")
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    if lines and all(MATH_LINE.match(ln) for ln in lines):
        return "equation"
    return kind


def build(result: Any, *, page: Optional[int] = None, tables_as: str = "markdown", lines: bool = True) -> dict[str, Any]:
    """The structure of one recognised page (``OcrResult``): ``{markdown, blocks, tables, text, language, languages, size?}``.

    ``blocks`` are in reading order (``order`` 0..n), each ``{order, kind, label, bbox, text, lines?, table?}``; a table block points at its
    entry in ``tables`` (``{index, page, rows, n_rows, n_cols, header, bbox, csv}``). ``markdown`` writes tables as GitHub tables unless
    ``tables_as="html"`` (Indra's own HTML, which keeps merged cells). ``text`` is the plain text in reading order with each table row
    on one line (cells separated by two spaces) and page headers and footers included."""
    raw_md = str(getattr(result, "markdown", "") or "")
    layout = list(getattr(result, "layout", None) or [])
    ocr_lines = list(getattr(result, "blocks", None) or [])
    parsed = parse_tables(raw_md)
    tables: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []
    queue = list(parsed)
    for order, item in enumerate(layout):
        label = str(item.get("label") or "Text")
        text = str(item.get("text") or "")
        bbox = [round(float(v), 1) for v in (item.get("bbox") or [0, 0, 0, 0])]
        block: dict[str, Any] = {"order": order, "kind": _kind(label, text), "label": label, "bbox": bbox, "text": text}
        if label == "Table" and "\n" in text and queue:
            table = dict(queue.pop(0))
            table.update(index=len(tables), bbox=bbox, csv=to_csv(table["rows"]))
            if page is not None:
                table["page"] = page
            block["table"] = table["index"]
            tables.append(table)
        if lines and ocr_lines:
            block["lines"] = [b.public() for b in ocr_lines if _inside(b.box, bbox)]
        blocks.append(block)
    for extra in queue:                       # tables the blocks did not claim (should not happen): kept, without a box
        table = dict(extra)
        table.update(index=len(tables), bbox=None, csv=to_csv(table["rows"]))
        if page is not None:
            table["page"] = page
        tables.append(table)
    if not layout:                            # an engine without layout: its lines are one text block
        text = str(getattr(result, "text", "") or "")
        if text.strip():
            block = {"order": 0, "kind": "paragraph", "label": "Text", "bbox": None, "text": text}
            if lines:
                block["lines"] = [b.public() for b in ocr_lines]
            blocks.append(block)
    markdown = raw_md if tables_as == "html" else markdown_with_pipes(raw_md)
    if not markdown.strip():
        markdown = str(getattr(result, "text", "") or "")
    plain = plain_text(blocks, tables) or str(getattr(result, "text", "") or "")
    out: dict[str, Any] = {"markdown": markdown.strip(), "blocks": blocks, "tables": tables, "text": plain,
                           "language": detect_language(plain), "languages": languages_of(b["text"] for b in blocks if b["kind"] not in ("picture", "table"))}
    meta = getattr(result, "meta", None) or {}
    for key in ("rotation", "skew", "vertical", "source", "size"):
        if key in meta:
            out[key] = meta[key]
    if page is not None:
        out["page"] = page
    return out


def plain_text(blocks: Sequence[dict[str, Any]], tables: Sequence[dict[str, Any]]) -> str:
    """Blocks in reading order as text; a table is one line per row, its cells separated by two spaces; pictures leave nothing."""
    parts: list[str] = []
    for block in blocks:
        if block.get("kind") == "picture":
            continue
        if block.get("table") is not None and 0 <= block["table"] < len(tables):
            parts.append("\n".join(CELL_SEP.join(c for c in row if c) for row in tables[block["table"]]["rows"] if any(row)))
        elif str(block.get("text") or "").strip():
            parts.append(str(block["text"]).strip())
    return "\n".join(p for p in parts if p)


def merge_pages(pages: Sequence[dict[str, Any]], *, separator: str = "\n\n") -> dict[str, Any]:
    """Several page structures as one document: Markdown joined with a page rule, tables numbered across pages, the language of all."""
    markdown = separator.join(p.get("markdown", "") for p in pages if p.get("markdown"))
    tables: list[dict[str, Any]] = []
    for p in pages:
        for t in p.get("tables") or []:
            tables.append({**t, "index": len(tables)})
    text = "\n\n".join(p.get("text", "") for p in pages if p.get("text"))
    return {"markdown": markdown, "tables": tables, "text": text, "language": detect_language(text),
            "languages": languages_of(b["text"] for p in pages for b in p.get("blocks") or [] if b.get("kind") not in ("picture", "table"))}
