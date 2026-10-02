"""The income-tax pack: the documents of one fiscal year that matter for the return, in one folder (and a zip), with an index and a CSV.

The fiscal year in Spain is the calendar year. A document belongs to a year by what it says («ejercicio 2025», a 31/12 date), else by
its issue date, its period or the day it arrived. Kafka has no «tax» flag on documents, so the pack uses what it does know: the kind
(tax office letters, payslips, bank documents), words in the title, issuer, item, tags and first page (donations, rent, mortgage and
loan certificates), and the tag «deducible» / «deductible» that the user puts on an invoice or receipt of a deductible expense.
The pack also lists the usual certificates that were not found; the Ledger summary of the year is added when Ledger answers.
Originals are copied, never moved or changed; nothing is overwritten.
"""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from datetime import date
from pathlib import Path
from typing import Any, Optional

from . import model as M
from .errors import KafkaError
from .util import fold, money_text, parse_iso
from .workshop import names

CATEGORIES = ("hacienda", "payroll", "donation", "mortgage", "rent", "bank", "deductible")
LABELS = {
    "es": {"hacienda": "Hacienda", "payroll": "Nóminas y retenciones", "donation": "Donaciones", "mortgage": "Hipoteca y préstamos",
           "rent": "Alquiler", "bank": "Bancos", "deductible": "Gastos deducibles"},
    "en": {"hacienda": "Tax office", "payroll": "Payroll and withholdings", "donation": "Donations", "mortgage": "Mortgage and loans",
           "rent": "Rent", "bank": "Banks", "deductible": "Deductible expenses"},
}
MISSING_LABELS = {
    "es": {"payroll_certificate": "Certificado de retenciones e ingresos a cuenta (la empresa lo da a principios de año)",
           "bank_certificate": "Certificado de intereses y saldos del banco a 31 de diciembre",
           "mortgage_certificate": "Certificado de intereses y capital amortizado de la hipoteca o el préstamo",
           "donation_certificate": "Certificado de donativos",
           "rent_receipts": "Recibos o justificantes del alquiler pagado"},
    "en": {"payroll_certificate": "Certificate of withholdings and income (the employer issues it early in the year)",
           "bank_certificate": "Bank certificate of interest and balances at 31 December",
           "mortgage_certificate": "Certificate of interest and capital repaid on the mortgage or loan",
           "donation_certificate": "Donation certificate",
           "rent_receipts": "Receipts or proof of the rent paid"},
}
CATEGORY_DIRS = {"es": dict(zip(CATEGORIES, ("1 Hacienda", "2 Nominas", "3 Donaciones", "4 Hipoteca y prestamos", "5 Alquiler", "6 Bancos", "7 Gastos deducibles"))),
                 "en": dict(zip(CATEGORIES, ("1 Tax office", "2 Payroll", "3 Donations", "4 Mortgage and loans", "5 Rent", "6 Banks", "7 Deductible expenses")))}

RE_FISCAL = re.compile(r"(?:ejercicio|a[nñ]o\s+fiscal|campa[nñ]a|renta|fiscal\s+year|tax\s+year)\s*(?:fiscal\s*)?(20\d{2})|31[/\-. ]12[/\-. ](20\d{2})")
RE_AEAT = re.compile(r"agencia\s+tributaria|\baeat\b|ministerio\s+de\s+hacienda|\bhacienda\b|tax\s+agency")
RE_PAYROLL_CERT = re.compile(r"certificado\s+de\s+(?:ingresos\s+y\s+)?retenciones|modelo\s*190|certificado\s+de\s+rendimientos|certificate\s+of\s+(?:income|withholding)")
RE_BANK_CERT = re.compile(r"certificado\s+(?:de\s+)?(?:intereses|saldos?|retenciones|posicion)|resumen\s+anual|extracto\s+anual|saldo\s+a\s+31|intereses\s+(?:abonados|devengados)|year[- ]end\s+statement")
RE_DONATION = re.compile(r"donativo|donaci[oó]n|donaciones|certificado\s+de\s+donaci|\bong\b|fundaci[oó]n|donation")
RE_MORTGAGE = re.compile(r"hipotec|prestamo|pr[eé]stamo|amortizaci|capital\s+pendiente|cuadro\s+de\s+amortiz|mortgage|\bloan\b")
RE_RENT = re.compile(r"alquiler|arrendamiento|arrendador|inquilin|renta\s+mensual|\brent\b|landlord|tenant")
DEDUCTIBLE_TAGS = ("deducible", "deductible", "desgravable")
TEXT_KINDS = (M.CONTRACT, M.BANK, M.RECEIPT, M.INVOICE, M.BILL, M.OTHER, M.OFFICIAL, M.TAX, M.PAYSLIP, M.INSURANCE)
SKIP_KINDS = (M.MANUAL, M.IDENTITY, M.FINE, M.VEHICLE, M.WARRANTY)


def _fold_join(*parts: Any) -> str:
    return fold(" ".join(str(p or "") for p in parts))


def _head(svc: Any, doc: dict[str, Any]) -> str:
    """Title, issuer, item, tags, subject and file name plus the first page: what says what the document is."""
    text = ""
    if doc["kind"] in TEXT_KINDS:
        pages = svc.store.pages(doc["id"])
        text = pages[0]["text"][:1500] if pages else ""
    return _fold_join(doc.get("title"), doc.get("issuer"), doc.get("item"), " ".join(doc.get("tags") or []), doc.get("mail_subject"), doc.get("file_name"), text)


def fiscal_year(doc: dict[str, Any], head: str) -> Optional[int]:
    """The year a document counts for."""
    found = {int(g) for m in RE_FISCAL.finditer(head) for g in m.groups() if g}
    if len(found) == 1:
        return found.pop()
    for name in ("issue_date", "period_to", "period_from"):
        d = parse_iso(doc.get(name))
        if d:
            return d.year
    ts = doc.get("received_ts") or doc.get("created_ts")
    return date.fromtimestamp(float(ts)).year if ts else None


def categorize(doc: dict[str, Any], head: str) -> str:
    """The tax category of a document ('' when it is not tax paperwork)."""
    kind = doc["kind"]
    if kind in SKIP_KINDS:
        return ""
    tags = {fold(t) for t in doc.get("tags") or []}
    if kind == M.TAX or RE_AEAT.search(fold(doc.get("issuer") or "")) or (kind == M.OFFICIAL and RE_AEAT.search(head)):
        return "hacienda"
    if kind == M.PAYSLIP or RE_PAYROLL_CERT.search(head):
        return "payroll"
    if RE_DONATION.search(head) and kind != M.INSURANCE:
        return "donation"
    if RE_MORTGAGE.search(head) and kind in (M.CONTRACT, M.BANK, M.BILL, M.RECEIPT, M.OTHER, M.OFFICIAL):
        return "mortgage"
    if RE_RENT.search(head) and kind in (M.CONTRACT, M.RECEIPT, M.BILL, M.INVOICE, M.OTHER):
        return "rent"
    if kind == M.BANK or RE_BANK_CERT.search(head):
        return "bank"
    if tags & set(DEDUCTIBLE_TAGS) and kind in (M.INVOICE, M.RECEIPT, M.BILL, M.INSURANCE, M.SUBSCRIPTION, M.OTHER, M.CONTRACT):
        return "deductible"
    return ""


def classify_all(svc: Any) -> list[dict[str, Any]]:
    out = []
    for doc in svc.store.documents(limit=2000, exclude_archived=True):
        head = _head(svc, doc)
        cat = categorize(doc, head)
        if not cat:
            continue
        out.append({"doc": doc, "category": cat, "year": fiscal_year(doc, head), "head": head})
    return out


def missing_certificates(rows: list[dict[str, Any]], year: int, lang: str) -> list[dict[str, str]]:
    """The usual certificates that are not in the year: what exists in the year decides what is expected, and so does what was
    filed in earlier years."""
    labels = MISSING_LABELS[lang]
    this = [r for r in rows if r["year"] == year]
    earlier = [r for r in rows if r["year"] is not None and r["year"] < year]
    has = lambda items, cat, rx=None: any(r["category"] == cat and (rx is None or rx.search(r["head"])) for r in items)  # noqa: E731
    es = lang == "es"
    out: list[dict[str, str]] = []

    def add(key: str, reason: str) -> None:
        out.append({"key": key, "label": labels[key], "reason": reason})

    if (has(this, "payroll") or has(earlier, "payroll")) and not has(this, "payroll", RE_PAYROLL_CERT):
        if has(this, "payroll"):
            add("payroll_certificate", "hay nóminas y no el certificado del año" if es else "payslips are filed and the year's certificate is not")
        else:
            add("payroll_certificate", "había nóminas en años anteriores" if es else "payslips were filed in earlier years")
    if (has(this, "bank") or has(earlier, "bank")) and not has(this, "bank", RE_BANK_CERT):
        add("bank_certificate", "hay documentos del banco y ninguno es un certificado anual" if es else "bank documents are filed and none is a yearly certificate")
    if (has(this, "mortgage") or has(earlier, "mortgage")) and not has(this, "mortgage", re.compile(r"certificado|certificate|intereses|interest")):
        add("mortgage_certificate", "hay una hipoteca o un préstamo y falta el certificado del año" if es else "a mortgage or loan is filed and the year's certificate is missing")
    if has(earlier, "donation") and not has(this, "donation"):
        add("donation_certificate", "hubo donativos en años anteriores" if es else "donations were filed in earlier years")
    if has(earlier, "rent") and not has(this, "rent"):
        add("rent_receipts", "hubo alquiler en años anteriores" if es else "rent was filed in earlier years")
    return out


def _unique_file(folder: Path, stem: str, ext: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    for n in range(1, 10_000):
        target = folder / (f"{stem}.{ext}" if n == 1 else f"{stem} ({n}).{ext}")
        if not target.exists():
            return target
    raise OSError("no free file name")


def _csv_text(rows: list[list[Any]]) -> str:
    buf = io.StringIO()
    csv.writer(buf, delimiter=";", lineterminator="\r\n").writerows(rows)
    return buf.getvalue()


def _write_text(folder: Path, name: str, text: str, *, bom: bool = False) -> Path:
    target = _unique_file(folder, Path(name).stem, Path(name).suffix.lstrip("."))
    target.write_bytes((b"\xef\xbb\xbf" if bom else b"") + text.encode("utf-8"))
    return target


def _ledger_files(svc: Any, year: int, folder: Path) -> dict[str, Any]:
    res = svc.engine._unwrap(svc.engine._fcall("ledger", "report_year", {"year": year}, timeout=45.0))
    if not res.get("ok"):
        return {"ok": False, "error": res.get("error") or "Ledger did not answer", "files": []}
    body = {k: v for k, v in res.items() if k != "ok"}
    files = [_write_text(folder, f"ledger_{year}.json", json.dumps(body, ensure_ascii=False, indent=2, default=str)).name]
    for key, header in (("by_category", ("category", "income", "expense", "net")), ("by_month", ("month", "income", "expense", "net"))):
        rows = body.get(key)
        if isinstance(rows, list) and rows and all(isinstance(r, dict) for r in rows):
            cols = list(header) if any(c in rows[0] for c in header[1:]) else list(rows[0].keys())
            files.append(_write_text(folder, f"ledger_{year}_{key}.csv", _csv_text([cols, *[[r.get(c, "") for c in cols] for r in rows]]), bom=True).name)
    return {"ok": True, "files": files, "totals": body.get("totals")}


def build(svc: Any, year: int, out_dir: str = "", make_zip: bool = True) -> dict[str, Any]:
    if not 1990 <= year <= 2100:
        raise KafkaError("invalid", "year must be a four-digit year.")
    lang = svc.engine.lang()
    es = lang == "es"
    rows = classify_all(svc)
    mine = sorted((r for r in rows if r["year"] == year), key=lambda r: (CATEGORIES.index(r["category"]), r["doc"].get("issue_date") or "", r["doc"]["id"]))
    base = Path(out_dir).expanduser() if out_dir else svc.workshop.default_dir()
    base = svc.workshop._check_out(base)
    pack = names.unique_dir(base, f"Renta {year}" if es else f"Tax return {year}")
    written: list[str] = []
    index_rows: list[list[Any]] = [["category", "doc_id", "date", "kind", "issuer", "title", "amount", "currency", "reference", "file"]]
    per_cat: dict[str, list[dict[str, Any]]] = {c: [] for c in CATEGORIES}
    copied = missing_files = 0
    for r in mine:
        doc, cat = r["doc"], r["category"]
        entry = {"doc": doc, "file": ""}
        if doc.get("file_sha") and svc.files.exists(doc["file_sha"], doc.get("file_ext") or ""):
            stem = names.safe_stem(" ".join(x for x in (doc.get("issue_date") or "", doc.get("issuer") or "", doc.get("title") or "", doc["id"]) if x), doc["id"])
            ext = (doc.get("file_ext") or "bin").lstrip(".")
            target = _unique_file(pack / CATEGORY_DIRS[lang][cat], stem, ext)
            target.write_bytes(svc.files.read(doc["file_sha"], doc.get("file_ext") or ""))
            entry["file"] = f"{CATEGORY_DIRS[lang][cat]}/{target.name}"
            written.append(entry["file"])
            copied += 1
        else:
            missing_files += 1
        per_cat[cat].append(entry)
        index_rows.append([cat, doc["id"], doc.get("issue_date") or "", doc["kind"], doc.get("issuer") or "", doc.get("title") or "",
                           "" if doc.get("amount") is None else doc["amount"], doc.get("currency") or "", doc.get("ref") or "", entry["file"]])
    missing = missing_certificates(rows, year, lang)
    ledger = _ledger_files(svc, year, pack)
    written.extend(ledger["files"])
    # index.md
    title = f"Renta {year}: documentos de Kafka" if es else f"Tax return {year}: documents from Kafka"
    lines = [f"# {title}", "",
             ("Ejercicio fiscal = año natural. Lista hecha con los tipos de documento de Kafka, las palabras del título y de la primera página y la etiqueta «deducible»; "
              "revísala antes de usarla." if es else
              "Fiscal year = calendar year. Built from Kafka's document kinds, the words of the title and first page and the «deductible» tag; check it before use."), ""]
    for cat in CATEGORIES:
        items = per_cat[cat]
        lines.append(f"## {LABELS[lang][cat]} ({len(items)})")
        lines.append("")
        if not items:
            lines += [("Nada archivado." if es else "Nothing filed."), ""]
            continue
        lines += [("| Fecha | Emisor | Documento | Importe | Fichero |" if es else "| Date | Issuer | Document | Amount | File |"), "|---|---|---|---|---|"]
        for e in items:
            d = e["doc"]
            lines.append(f"| {d.get('issue_date') or ''} | {d.get('issuer') or ''} | {d.get('title') or ''} ({d['id']}) | "
                         f"{money_text(d.get('amount'), d.get('currency') or 'EUR', es) if d.get('amount') is not None else ''} | {e['file'] or ('sin fichero' if es else 'no file')} |")
        lines.append("")
    lines += [("## Que falta" if es else "## Missing"), ""]
    lines += [f"- {m['label']} — {m['reason']}" for m in missing] or [("Nada de lo habitual falta." if es else "None of the usual certificates is missing.")]
    lines += ["", "## Ledger", ""]
    if ledger["ok"]:
        totals = ledger.get("totals")
        lines.append((f"Resumen del año en {', '.join(f for f in ledger['files'])}." if es else f"Yearly summary in {', '.join(ledger['files'])}.")
                     + (f" Totales: {json.dumps(totals, ensure_ascii=False)}." if totals else ""))
    else:
        lines.append((f"Ledger no respondió: {ledger.get('error')}." if es else f"Ledger did not answer: {ledger.get('error')}."))
    lines.append("")
    written.append(_write_text(pack, "index.md", "\n".join(lines)).name)
    written.append(_write_text(pack, "documentos.csv", _csv_text(index_rows), bom=True).name)
    zip_path = ""
    if make_zip:
        target = _unique_file(pack.parent, pack.name, "zip")
        with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(pack.rglob("*")):
                if f.is_file():
                    zf.write(f, f.relative_to(pack.parent).as_posix())
        zip_path = str(target)
    counts = {c: len(per_cat[c]) for c in CATEGORIES}
    return {"ok": True, "year": year, "path": str(pack), "zip": zip_path, "files": written, "documents": len(mine), "copied": copied,
            "without_file": missing_files, "counts": counts, "missing": missing,
            "ledger": {k: ledger[k] for k in ("ok", "error", "files") if k in ledger}}
