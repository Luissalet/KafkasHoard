"""The work: read documents into the store, turn their dates into deadlines, remind the user and keep the books tidy.

Everything that changes a document or a deadline goes through ``Engine`` so the rules live in one place:

* **Ingestion**: a file (upload, watched folder, mail attachment, pasted text) is stored once by its SHA-256; text comes from the PDF
  text layer, OCR or the format's reader; the extraction (rules, optionally a local model) fills the fields and proposes deadlines.
* **Deadlines** created by the extraction are *auto*: re-processing a document replaces them, except the ones the user touched
  (``edited``) and the states the user chose.
* **Reminders**: for each open deadline and each lead day not yet notified whose day has come, one notification (the most urgent
  lead), once; overdue once. Nothing is sent at night; it waits for the morning.
* **History is quiet**: old mails and old files are filed without notifications and their past deadlines are created as done.
"""

from __future__ import annotations

import re

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Callable, Optional

from . import model as M
from . import paths, readers
from .bizdays import Calendar
from .errors import KafkaError
from .extract import issuers as issuers_mod
from .extract import refs as refs_mod
from .extract import texts
from .extract.pipeline import extract
from .extract.rules import DEFAULT_WARRANTY_YEARS, warranty_end
from .extract.types import Ctx, Extraction, Hints, Meta
from .files import FileStore, sha256_of
from .mail.classify import MailClass, classify_mail
from .util import add_months, clamp_text, fold, human_day, issuer_key, money_text, parse_iso, ts_date
from .workshop import jobs as workshop_jobs

log = logging.getLogger("kafka.engine")

DAY = 86400.0
QUIET_AGE_S = 2 * DAY                 # mails and files older than this are history: no notifications
ARCHIVE_DONE_DAYS = 30
DEADLINE_CHANNELS = ("toast", "ntfy", "telegram", "email")   # the bus event of a deadline is emitted by the engine itself
ROLL_AFTER_DAYS = 3                   # an open recurring deadline this late rolls to its next occurrence
OVERDUE_NOTIFY_MAX_DAYS = 14          # older than this and it is history, not news
PRICE_RECENT_DAYS = 120
MAX_NEW_FILES_PER_SCAN = 200
SKIP_NAMES = ("~$", ".~lock", ".part", ".tmp", ".crdownload", ".download", "thumbs.db", "desktop.ini", ".ds_store")
SERIES_KINDS = (M.INSURANCE, M.SUBSCRIPTION, M.BILL)
PRICE_KINDS = (M.INSURANCE, M.SUBSCRIPTION)
LEDGER_KINDS = (M.INVOICE, M.RECEIPT)   # documents that may be a payment seen in Ledger
LEDGER_STRONG = 0.8                      # tx_find score that counts as a match
LEDGER_RETRY_DAYS = 45                   # keep looking for the payment of a filed invoice this long
LEDGER_RETRY_EVERY_S = 20 * 3600.0
EDITABLE = ("title", "kind", "issuer", "ref", "amount", "issue_date", "period_from", "period_to", "item", "state", "tags", "notes")
OVERRIDE_FIELDS = ("kind", "issuer", "ref", "amount", "issue_date", "item")


def _bad_date(value: Any) -> bool:
    return bool(value) and parse_iso(str(value)) is None


SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,39}$")


def _check_external(source: str, external_key: str) -> tuple[str, str]:
    source, external_key = (source or "").strip().lower(), (external_key or "").strip()
    if not SOURCE_RE.match(source):
        raise KafkaError("invalid", "source must be an app id (lowercase letters, digits, '-', '_' or '.').")
    if not 1 <= len(external_key) <= 160:
        raise KafkaError("invalid", "external_key must have 1 to 160 characters.")
    return source, external_key


class Engine:
    def __init__(self, store: Any, files: FileStore, notifier: Any, ocr: Any, *, settings_get: Callable[[str, Optional[str]], Optional[str]],
                 settings_set: Callable[[str, str], None], emit: Callable[[str, dict], None] = lambda t, d: None,
                 clock: Callable[[], float] = time.time, mail_source: Any = None, data_dir: Optional[Path] = None,
                 mail_cache_dir: Optional[Path] = None, inbox_dir: Optional[Path] = None, page_cache_dir: Optional[Path] = None,
                 llm: Any = None, family_call: Optional[Callable[..., dict]] = None, base_url: Callable[[], str] = lambda: "",
                 submit: Optional[Callable[[str, str], Any]] = None, mail_claim: Optional[Callable[..., dict]] = None,
                 refs_link: Optional[Callable[..., dict]] = None, app_url: Optional[Callable[[str], str]] = None):
        self.store = store
        self.files = files
        self.notifier = notifier
        self.ocr = ocr
        self.get = settings_get
        self.set = settings_set
        self.emit = emit
        self.clock = clock
        self.mail = mail_source
        self.data_dir = Path(data_dir) if data_dir else None
        self.mail_cache_dir = Path(mail_cache_dir) if mail_cache_dir else None
        self.inbox_dir = Path(inbox_dir) if inbox_dir else None
        self.page_cache_dir = Path(page_cache_dir) if page_cache_dir else None
        self.llm = llm
        self.family_call = family_call
        self.base_url = base_url
        self.submit = submit
        self.mail_claim = mail_claim      # (hub mail ids, kind, hoard:// ref) -> the hub's answer; None without a hub
        self.refs_link = refs_link        # (from_uri, to_uri, rel, from_label=, to_label=) -> the hub's answer
        self.app_url = app_url            # app id -> its base url as the hub knows it ('' when unknown)

    # ------------------------------------------------------------------ settings
    def setting(self, key: str, default: str = "") -> str:
        try:
            value = self.get(key, None)
        except Exception:  # noqa: BLE001
            value = None
        return default if value in (None, "") else str(value)

    def lang(self) -> str:
        return "en" if self.setting("ui.language", "es") == "en" else "es"

    def calendar(self) -> Calendar:
        extra = []
        for chunk in self.setting("calendar.extra_holidays", "").replace(";", ",").split(","):
            d = parse_iso(chunk.strip()) if chunk.strip() else None
            if d:
                extra.append(d)
        return Calendar(self.setting("calendar.region", "ES-MD"), extra)

    def today(self) -> date:
        return datetime.fromtimestamp(self.clock()).date()

    def warranty_years(self) -> int:
        try:
            return max(1, min(10, int(float(self.setting("warranty.years", str(DEFAULT_WARRANTY_YEARS))))))
        except ValueError:
            return DEFAULT_WARRANTY_YEARS

    def leads(self) -> dict[str, list[int]]:
        out: dict[str, list[int]] = {}
        for kind in M.DEADLINE_KINDS:
            raw = self.setting(f"remind.{kind}", "")
            if raw:
                try:
                    values = sorted({int(x) for x in raw.replace(";", ",").split(",") if x.strip()}, reverse=True)
                except ValueError:
                    continue
                out[kind] = [v for v in values if 0 <= v <= 365] or list(M.DEFAULT_LEADS[kind])
        return out

    def lead_days(self, kind: str) -> list[int]:
        return self.leads().get(kind) or list(M.DEFAULT_LEADS.get(kind, [7, 1]))

    def ctx(self, doc: Optional[dict[str, Any]] = None) -> Ctx:
        facts = (doc or {}).get("facts") or {}
        overrides: dict[str, Any] = {}
        if doc:
            for name in facts.get("edited") or []:
                if name in OVERRIDE_FIELDS and doc.get(name) not in (None, ""):
                    overrides[name] = doc[name]
        months = facts.get("warranty_months")
        return Ctx(today=self.today(), cal=self.calendar(), lang=self.lang(), warranty_years=self.warranty_years(),
                   warranty_months=int(months) if months else None, leads=self.leads(), overrides=overrides)

    def link_to(self, doc_id: str) -> str:
        base = self.base_url()
        return f"{base}/#/documentos/{doc_id}" if base else ""


    # ------------------------------------------------------------------ family: events, calls, references
    def _emit_event(self, type_: str, data: dict[str, Any]) -> None:
        """A fact for the family bus (ids and short titles only). Events are hints: they never fail the work."""
        try:
            self.emit(type_, data)
        except Exception:  # noqa: BLE001
            log.info("event %s not sent", type_)

    def _fcall(self, app: str, tool: str, args: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
        """Call a tool of another app through the hub; always a dict, ``{"ok": False, "error": ...}`` when nothing answered."""
        if self.family_call is None:
            return {"ok": False, "error": "no hub connection"}
        try:
            try:
                answer = self.family_call(app, tool, args, timeout=timeout)
            except TypeError:
                answer = self.family_call(app, tool, args)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:120]}"}
        return answer if isinstance(answer, dict) else {"ok": False, "error": "unexpected answer"}

    @staticmethod
    def _unwrap(answer: dict[str, Any]) -> dict[str, Any]:
        """The tool's own result inside the hub's ``{ok, app, tool, result|error}`` envelope."""
        if not answer.get("ok"):
            return {"ok": False, "error": str(answer.get("error") or "the app did not answer")[:200]}
        inner = answer.get("result")
        if isinstance(inner, dict):
            if inner.get("ok") is False:
                return {"ok": False, "error": str(inner.get("error") or "the tool failed")[:200], **{k: v for k, v in inner.items() if k not in ("ok", "error")}}
            return {"ok": True, **inner}
        return {"ok": True, "result": inner}

    @staticmethod
    def doc_ref(doc_id: str) -> str:
        return f"hoard://kafka/document/{doc_id}"

    def order_ref_of(self, doc: dict[str, Any]) -> str:
        """The order number of a purchase document: the shipment's for a parcel, else the one found in the text."""
        facts = doc.get("facts") or {}
        if doc.get("source") == "phileas":
            return str(doc.get("ref") or "")
        if doc.get("ref") and facts.get("ref_what") == "order":
            return str(doc["ref"])
        return str(facts.get("order_ref") or "")

    def _emit_archived(self, doc: dict[str, Any]) -> None:
        self._emit_event("kafka.document.archived", {
            "doc_id": doc["id"], "kind": doc["kind"], "merchant": doc.get("issuer") or "", "amount": doc.get("amount"),
            "currency": (doc.get("currency") or "") if doc.get("amount") is not None else "", "date": doc.get("issue_date") or "",
            "order_ref": self.order_ref_of(doc), "message_id": doc.get("source_ref") if doc.get("source") == "mail" else ""})

    def _emit_deadline_created(self, t: dict[str, Any]) -> None:
        if t.get("state") == M.OPEN:
            self._emit_event("kafka.deadline.created", {"deadline_id": t["id"], "title": t["title"], "due": t["date"], "kind": t["kind"]})

    # ------------------------------------------------------------------ Ledger: an invoice and the payment that paid it
    def _ledger_enabled(self) -> bool:
        return self.setting("links.ledger", "1") == "1"

    def _ledger_eligible(self, doc: dict[str, Any]) -> str:
        """'' when the document can be looked up in Ledger, else why not."""
        if doc.get("kind") not in LEDGER_KINDS:
            return "only invoices and receipts are looked up in Ledger"
        if doc.get("amount") is None or not parse_iso(doc.get("issue_date")):
            return "the document needs an amount and an issue date"
        return ""

    def ledger_candidates(self, doc: dict[str, Any]) -> dict[str, Any]:
        args: dict[str, Any] = {"amount": doc["amount"], "date": doc["issue_date"], "days": 5}
        if doc.get("issuer"):
            args["merchant"] = doc["issuer"]
        if doc.get("currency"):
            args["currency"] = doc["currency"]
        res = self._unwrap(self._fcall("ledger", "tx_find", args))
        if not res.get("ok"):
            return {"ok": False, "error": res.get("error") or "Ledger did not answer", "matches": []}
        matches = []
        for m in res.get("matches") or []:
            if not isinstance(m, dict) or m.get("tx_id") in (None, ""):
                continue
            try:
                score = float(m.get("score") or 0)
            except (TypeError, ValueError):
                score = 0.0
            matches.append({"tx_id": str(m["tx_id"]), "date": str(m.get("date") or ""), "amount": m.get("amount"),
                            "merchant": str(m.get("merchant") or "")[:80], "score": round(score, 3)})
        matches.sort(key=lambda m: -m["score"])
        return {"ok": True, "matches": matches}

    def link_ledger(self, did: str, *, tx_id: str = "") -> dict[str, Any]:
        """Find the Ledger transaction of an invoice or receipt and link both ways.

        Without ``tx_id``: exactly one candidate with a score of 0.8 or more is linked, otherwise the candidates come back.
        With ``tx_id`` the chosen transaction is linked (the manual choice)."""
        doc = self.store.document(did)
        facts = dict(doc.get("facts") or {})
        current = facts.get("ledger_tx")
        if current and not tx_id:
            return {"ok": True, "linked": True, "tx_id": current["tx_id"], "ref": current["ref"], "candidates": [], "reason": "already linked"}
        why = self._ledger_eligible(doc)
        if why:
            return {"ok": False, "linked": False, "candidates": [], "reason": why}
        found = self.ledger_candidates(doc)
        facts["ledger_checked_ts"] = self.clock()
        self.store.update_document(did, facts=facts)
        if not found["ok"]:
            return {"ok": False, "linked": False, "candidates": [], "reason": found["error"]}
        matches = found["matches"]
        chosen: Optional[dict[str, Any]] = None
        if tx_id:
            chosen = next((m for m in matches if m["tx_id"] == str(tx_id)), {"tx_id": str(tx_id), "date": "", "amount": None, "merchant": "", "score": 0.0})
        else:
            strong = [m for m in matches if m["score"] >= LEDGER_STRONG]
            if len(strong) == 1:
                chosen = strong[0]
        if chosen is None:
            reason = "no transaction matches" if not matches else ("several transactions match: pick one" if len([m for m in matches if m["score"] >= LEDGER_STRONG]) > 1
                                                                       else "no transaction is a strong match: pick one")
            return {"ok": True, "linked": False, "candidates": matches, "reason": reason}
        return self._attach_ledger(doc, chosen, matches)

    def _attach_ledger(self, doc: dict[str, Any], tx: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
        ref = f"hoard://ledger/tx/{tx['tx_id']}"
        label = clamp_text(doc.get("title") or doc.get("issuer") or doc["id"], 120)
        res = self._unwrap(self._fcall("ledger", "tx_attach_doc", {"tx_id": tx["tx_id"], "doc_ref": self.doc_ref(doc["id"]), "label": label}))
        if not res.get("ok"):
            return {"ok": False, "linked": False, "candidates": candidates, "reason": f"Ledger did not attach the document: {res.get('error')}"}
        if self.refs_link is not None:
            try:
                self.refs_link(ref, self.doc_ref(doc["id"]), "invoice", from_label=" ".join(x for x in (tx.get("merchant"), str(tx.get("amount") or "")) if x),
                               to_label=label)
            except Exception:  # noqa: BLE001 — references are hints
                log.info("reference to %s not recorded", ref)
        base = ""
        if self.app_url is not None:
            try:
                base = (self.app_url("ledger") or "").rstrip("/")
            except Exception:  # noqa: BLE001
                base = ""
        facts = dict(self.store.document(doc["id"]).get("facts") or {})
        facts["ledger_tx"] = {"tx_id": tx["tx_id"], "ref": ref, "score": tx.get("score"), "merchant": tx.get("merchant") or "",
                              "amount": tx.get("amount"), "date": tx.get("date") or "", "linked_ts": self.clock(),
                              "url": f"{base}/#/movimientos?tx={tx['tx_id']}" if base else ""}
        self.store.update_document(doc["id"], facts=facts)
        return {"ok": True, "linked": True, "tx_id": tx["tx_id"], "ref": ref, "candidates": candidates, "reason": ""}

    def _auto_link_ledger(self, doc: dict[str, Any]) -> None:
        if not self._ledger_enabled() or self.family_call is None or self._ledger_eligible(doc):
            return
        try:
            self.link_ledger(doc["id"])
        except Exception:  # noqa: BLE001 — a missing Ledger never stops filing
            log.info("ledger lookup for %s failed", doc["id"])

    # ------------------------------------------------------------------ minutes: the dated action items become deadlines
    MINUTES_ME = ("yo", "me", "i", "mi", "mí", "myself")

    def _mine(self, owner: str) -> bool:
        """An action item is the user's when it names nobody or names the user (the minutes.me setting, or «yo»/«me»)."""
        owner = fold(owner or "").strip()
        if not owner:
            return True
        mine = {fold(x).strip() for x in self.setting("minutes.me", "").replace(";", ",").split(",") if x.strip()} | {fold(x) for x in self.MINUTES_ME}
        return owner in mine or any(part.strip() in mine for part in re.split(r"[,/&]|\by\b|\band\b", owner) if part.strip())

    def deadlines_from_minutes(self, minutes_id: str) -> dict[str, Any]:
        """Read the minutes from Funes through the hub; one deadline per action item that has a date and is not someone else's."""
        res = self._unwrap(self._fcall("funes", "minutes_get", {"minutes_id": minutes_id}, timeout=30.0))
        if not res.get("ok"):
            return {"ok": False, "error": res.get("error") or "Funes did not answer", "added": [], "skipped": []}
        title = str(res.get("title") or "").strip()
        when = parse_iso(str(res.get("date") or ""))
        es = self.lang() == "es"
        ref = f"hoard://funes/minutes/{minutes_id}"
        added: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for item in res.get("action_items") or []:
            if isinstance(item, str):
                item = {"text": item}
            if not isinstance(item, dict):
                continue
            text = clamp_text(str(item.get("text") or ""), 160)
            if not text:
                continue
            due = parse_iso(str(item.get("due") or ""))
            owner = str(item.get("owner") or "").strip()
            if due is None or len(str(item.get("due") or "").strip()) < 10:
                skipped.append({"text": text, "reason": "no date"})
                continue
            if not self._mine(owner):
                skipped.append({"text": text, "reason": f"belongs to {owner}"})
                continue
            key = f"minutes:{minutes_id}:{hashlib.sha1(fold(text).encode('utf-8')).hexdigest()[:12]}"
            basis = ((f"Acción acordada en la reunión «{title}»" + (f" del {human_day(when, True)}" if when else "") + ".") if es else
                     (f"Action item from the meeting «{title}»" + (f" of {human_day(when, False)}" if when else "") + "."))
            r = self.upsert_external(source="funes", external_key=key, title=text, date_=due.isoformat(), kind=M.CUSTOM, basis=basis,
                                     rule="Funes", source_ref=ref)
            row = {"deadline_id": r["deadline"]["id"], "title": text, "due": due.isoformat(), "action": r["action"]}
            if r["action"] == "created":
                added.append(row)
            else:
                skipped.append({"text": text, "reason": f"already added ({r['action']})", "deadline_id": row["deadline_id"]})
        return {"ok": True, "minutes": {"id": minutes_id, "title": title, "date": res.get("date") or ""}, "added": added, "skipped": skipped}

    def retry_ledger_links(self, limit: int = 20) -> int:
        """The payment often shows up in Ledger after the invoice was filed: look again for recent unlinked invoices, once a day."""
        if not self._ledger_enabled() or self.family_call is None:
            return 0
        now, tried = self.clock(), 0
        for doc in self.store.documents(limit=400, exclude_archived=True):
            if tried >= limit:
                break
            facts = doc.get("facts") or {}
            if facts.get("ledger_tx") or self._ledger_eligible(doc) or doc.get("source") == "phileas":
                continue
            if now - float(doc.get("created_ts") or 0) > LEDGER_RETRY_DAYS * DAY or now - float(facts.get("ledger_checked_ts") or 0) < LEDGER_RETRY_EVERY_S:
                continue
            tried += 1
            self._auto_link_ledger(doc)
        return tried

    # ================================================================== ingestion
    def ingest_bytes(self, data: bytes, name: str, *, source: str = "upload", source_ref: str = "", mail: Optional[dict[str, Any]] = None,
                     received_ts: Optional[float] = None, quiet: bool = False) -> dict[str, Any]:
        """Store and read one file. ``created`` is False when the same content is already filed (``document`` is then the existing one)."""
        if not data:
            raise KafkaError("invalid", "The file is empty.")
        if len(data) > readers.MAX_FILE_BYTES:
            raise KafkaError("too_large", f"{name} is larger than {readers.MAX_FILE_BYTES // (1024 * 1024)} MB.")
        sha = sha256_of(data)
        existing = self.store.document_by_sha(sha)
        if existing is not None:
            return {"created": False, "document": existing, "documents": [existing], "duplicate_of": existing["id"]}
        name = os.path.basename(str(name or "")) or "document"
        notes: list[str] = []
        try:
            read = readers.read_bytes(name, data, self.ocr)
        except readers.ReadError as exc:
            read = readers.ReadResult("unsupported", readers.sniff(name, data) or "bin", "application/octet-stream", [], 0, [f"unreadable: {exc}"])
        notes.extend(read.notes)
        if read.kind == "eml":
            return self._ingest_eml(data, name, sha, read, source=source, source_ref=source_ref, received_ts=received_ts, quiet=quiet)
        self.files.put(data, read.ext, sha)
        mail = dict(mail or {})
        doc = self.store.create_document(
            title=Path(name).stem[:160], file_sha=sha, file_ext=read.ext, file_name=name, mime=read.mime, size=len(data), pages=len(read.pages),
            ocr=bool(read.ocr_pages), source=source, source_ref=source_ref, mail_subject=str(mail.get("subject") or "")[:300],
            mail_from=_format_sender(mail), received_ts=received_ts if received_ts is not None else mail.get("ts"),
            state=M.REVIEW, facts={"notes": notes, "edited": []})
        if read.kind == "unsupported":
            self.store.update_document(doc["id"], title=Path(name).stem[:160] or name, state=M.REVIEW,
                                       facts={"notes": notes or ["unsupported type"], "edited": [], "unsupported": True})
            self.store.set_pages(doc["id"], doc["title"], "", [])
            return {"created": True, "document": self.store.document(doc["id"]), "documents": [self.store.document(doc["id"])],
                    "notes": notes or ["unsupported type"]}
        self.store.set_pages(doc["id"], doc["title"], "", read.pages)
        doc = self.process_document(doc["id"], quiet=quiet, announce=True)
        return {"created": True, "document": doc, "documents": [doc], "notes": notes}

    def _ingest_eml(self, data: bytes, name: str, sha: str, read: readers.ReadResult, *, source: str, source_ref: str,
                    received_ts: Optional[float], quiet: bool) -> dict[str, Any]:
        mail = read.mail
        ref = source_ref or mail.get("message_id") or f"eml:{sha[:16]}"
        out: list[dict[str, Any]] = []
        created_any = False
        body = (read.pages[0] if read.pages else "").strip()
        if body:
            probe = self._probe_text(body, mail)
            if probe.kind != M.OTHER and probe.kind_score >= 10:
                self.files.put(data, "eml", sha)
                doc = self.store.create_document(
                    title=clamp_text(str(mail.get("subject") or Path(name).stem), 160), file_sha=sha, file_ext="eml", file_name=name,
                    mime=readers.MIME["eml"], size=len(data), pages=1, source=source, source_ref=ref, mail_subject=str(mail.get("subject") or "")[:300],
                    mail_from=_format_sender(mail), received_ts=mail.get("ts") or received_ts, state=M.REVIEW, facts={"notes": [], "edited": []})
                self.store.set_pages(doc["id"], doc["title"], "", read.pages)
                out.append(self.process_document(doc["id"], quiet=quiet, announce=True))
                created_any = True
        for child in read.children:
            result = self.ingest_bytes(child.data, child.name, source=source, source_ref=ref, mail=mail, received_ts=mail.get("ts") or received_ts, quiet=quiet)
            created_any = created_any or result["created"]
            out.extend(result["documents"])
        if not out:
            return {"created": False, "document": None, "documents": [], "notes": ["no_documents_in_mail"]}
        return {"created": created_any, "document": out[0], "documents": out, "notes": []}

    def ingest_text(self, title: str, text: str, *, source: str = "paste", source_ref: str = "", mail: Optional[dict[str, Any]] = None,
                    received_ts: Optional[float] = None, quiet: bool = False) -> dict[str, Any]:
        text = readers.clean_text(text or "")
        if len(text.strip()) < 10:
            raise KafkaError("invalid", "The text is too short to be a document.")
        data = text.encode("utf-8")
        name = (title.strip() or "text")[:80] + ".txt"
        existing = self.store.document_by_sha(sha256_of(data))
        if existing is not None:
            return {"created": False, "document": existing, "documents": [existing], "duplicate_of": existing["id"]}
        sha = sha256_of(data)
        self.files.put(data, "txt", sha)
        mail = dict(mail or {})
        doc = self.store.create_document(
            title=clamp_text(title.strip() or text.strip().splitlines()[0], 160), file_sha=sha, file_ext="txt", file_name=name, mime="text/plain",
            size=len(data), pages=0, source=source, source_ref=source_ref, mail_subject=str(mail.get("subject") or "")[:300],
            mail_from=_format_sender(mail), received_ts=received_ts if received_ts is not None else mail.get("ts"), state=M.REVIEW,
            facts={"notes": [], "edited": [], "title_given": bool(title.strip())})
        pages = readers.split_pages(text[:readers.MAX_TEXT_CHARS])
        self.store.set_pages(doc["id"], doc["title"], "", pages)
        self.store.update_document(doc["id"], pages=len(pages))
        doc = self.process_document(doc["id"], quiet=quiet, announce=True)
        return {"created": True, "document": doc, "documents": [doc]}

    def ingest_path(self, path: str, *, source: str = "upload", source_ref: str = "", quiet: bool = False) -> dict[str, Any]:
        reason = paths.unsafe_file(path, self.data_dir)
        if reason:
            raise KafkaError("forbidden", f"Cannot read {path}: {reason}.", "Give the path of a document (PDF, image, .eml, .txt, .html, .docx) outside system folders.")
        p = Path(path).expanduser().resolve()
        if p.suffix.lower().lstrip(".") not in readers.SUPPORTED_EXT:
            raise KafkaError("unsupported", f"{p.suffix or 'That'} files are not documents Kafka can read.", f"Supported: {', '.join(sorted(readers.SUPPORTED_EXT))}.")
        try:
            size = p.stat().st_size
        except OSError as exc:
            raise KafkaError("not_found", f"Cannot read {path}.") from exc
        if size > readers.MAX_FILE_BYTES:
            raise KafkaError("too_large", f"{p.name} is larger than {readers.MAX_FILE_BYTES // (1024 * 1024)} MB.")
        return self.ingest_bytes(p.read_bytes(), p.name, source=source, source_ref=source_ref or str(p), quiet=quiet)

    def apply_given(self, result: dict[str, Any], *, kind: str = "", item: str = "") -> dict[str, Any]:
        """Apply a kind or item stated by the caller (another app, the assistant) to what was just filed, like a user edit.
        A document already on file keeps a kind or item the user corrected before."""
        values = {k: v for k, v in (("kind", kind), ("item", item.strip())) if v}
        if not values:
            return result
        docs = []
        for doc in result.get("documents") or []:
            if doc is None:
                continue
            edited = set((doc.get("facts") or {}).get("edited") or [])
            todo = values if result.get("created") else {k: v for k, v in values.items() if k not in edited}
            docs.append(self.update_document(doc["id"], todo) if todo else doc)
        return {**result, "documents": docs, "document": docs[0] if docs else result.get("document")}

    def _probe_text(self, text: str, mail: dict[str, Any]) -> Extraction:
        meta = Meta(from_name=str(mail.get("from_name") or ""), from_address=str(mail.get("from_address") or ""), subject=str(mail.get("subject") or ""))
        return extract([text], meta, self.ctx())

    # ================================================================== processing
    def _meta(self, doc: dict[str, Any]) -> Meta:
        raw = str(doc.get("mail_from") or "")
        m = re.match(r"^\s*\"?(.*?)\"?\s*<([^<>@\s]+@[^<>\s]+)>\s*$", raw)
        # «Ejemplo, PBC <x@y>»: parseaddr splits at the comma and loses the name
        name, address = (m.group(1).strip(), m.group(2).strip()) if m else parseaddr(raw)
        received = ts_date(doc.get("received_ts"))
        return Meta(from_name=name, from_address=address, subject=doc.get("mail_subject") or "", received=received,
                    source=doc.get("source") or "upload", file_name=doc.get("file_name") or "")

    def process_document(self, did: str, *, quiet: bool = False, announce: bool = False, llm: bool = False,
                         hints: Optional[Hints] = None) -> dict[str, Any]:
        """(Re)run the extraction for a stored document and apply it, keeping what the user edited."""
        doc = self.store.document(did)
        facts = dict(doc.get("facts") or {})
        facts.setdefault("edited", [])
        pages = [p["text"] for p in self.store.pages(did)]
        if doc["source"] == "phileas" and not pages:
            return self._refresh_phileas_doc(doc)
        ex = extract(pages, self._meta(doc), self.ctx(doc), hints)
        if llm and hints is None and self.llm is not None and ex.state == M.REVIEW:
            got = self.llm.hints("\n\n".join(pages))
            facts["llm"] = {"tried_ts": self.clock(), "used": got is not None, "error": getattr(self.llm, "last_error", "") if got is None else ""}
            if got is not None:
                ex = extract(pages, self._meta(doc), self.ctx(doc), got)
        edited = set(facts["edited"])
        fields: dict[str, Any] = {}
        keep = lambda name: name in edited  # noqa: E731
        if not keep("kind"):
            fields["kind"] = ex.kind
        fields["issuer"] = ex.issuer.name if not keep("issuer") else doc["issuer"]
        fields["issuer_key"] = issuer_key(fields["issuer"]) or ex.issuer.key
        if not keep("ref"):
            fields["ref"] = ex.ref
        if not keep("amount"):
            fields["amount"] = ex.amount
            fields["currency"] = (ex.currency or "EUR") if ex.amount is not None else ""
        if not keep("issue_date"):
            fields["issue_date"] = ex.issue_date
        if not keep("period_from"):
            fields["period_from"] = ex.period_from
        if not keep("period_to"):
            fields["period_to"] = ex.period_to
        if not keep("item"):
            fields["item"] = ex.item
        if not keep("title") and not facts.get("title_given"):
            fields["title"] = ex.title or doc["title"]
        fields["confidence"] = ex.confidence
        if not keep("state"):
            fields["state"] = M.REVIEW if ex.state == M.REVIEW else M.OK
        elif doc["state"] == M.OK and ex.state == M.REVIEW:
            fields["state"] = M.OK
        joined = "\n".join(pages)[:60_000]
        facts.update({"ref_what": ex.ref_what, "order_ref": refs_mod.find_order_ref(joined, fold(joined)),
                      "extracted": ex.facts, "notes": sorted(set((facts.get("notes") or []) + ex.notes)), "kind_reasons": ex.kind_reasons,
                      "amount_reduced": ex.amount_reduced, "period_hint": ex.period_hint, "processed": self.clock(),
                      "relative": [{"n": p.n, "unit": p.unit, "working": p.working, "purpose": p.purpose, "raw": p.raw} for p in ex.periods]})
        facts["notes"] = [n for n in facts["notes"] if n not in ("no_deadline_found", "kind_unknown", "no_dates") or n in ex.notes]
        doc = self.store.update_document(did, **fields, facts=facts)
        old_past_quiet = quiet or self._is_history(doc)
        self._merge_deadlines(doc, ex, quiet=old_past_quiet)
        self._apply_links(doc)
        self._join_series(doc, ex, quiet=old_past_quiet)
        doc = self.store.document(did)
        if announce:
            self._announce_document(doc, quiet=old_past_quiet)
            if not old_past_quiet:
                self._emit_archived(doc)
            self._auto_link_ledger(doc)
            doc = self.store.document(did)
            if (doc["state"] == M.REVIEW and self.setting("extract.llm", "auto") == "auto" and self.llm is not None and self.submit is not None
                    and "llm" not in (doc.get("facts") or {})):
                try:
                    self.submit("llm", did)
                except Exception:  # noqa: BLE001
                    log.info("could not queue the model pass for %s", did)
        return doc

    def _is_history(self, doc: dict[str, Any]) -> bool:
        ts = doc.get("received_ts")
        return bool(ts) and self.clock() - float(ts) > QUIET_AGE_S

    def llm_pass(self, did: str) -> dict[str, Any]:
        """Ask the local model about a document that stayed in review. Skips silently when no model answers."""
        if self.llm is None:
            return {"llm": "unavailable", "reason": "no model pass configured"}
        ok, why = self.llm.available()
        if not ok:
            doc = self.store.document(did)
            facts = dict(doc.get("facts") or {})
            facts["llm"] = {"tried_ts": self.clock(), "used": False, "error": why}
            self.store.update_document(did, facts=facts)
            return {"llm": "unavailable", "reason": why}
        before = self.store.document(did)
        doc = self.process_document(did, llm=True)
        return {"llm": "used" if (doc.get("facts") or {}).get("llm", {}).get("used") else "no_answer", "state": doc["state"],
                "was": before["state"], "document": doc["id"]}

    def reprocess(self, did: str, *, llm: bool = False) -> dict[str, Any]:
        doc = self.store.document(did)
        if llm and self.llm is not None:
            ok, why = self.llm.available()
            if not ok:
                result = self.process_document(did)
                return {"document": result, "llm": "unavailable", "reason": why}
        result = self.process_document(did, llm=llm)
        return {"document": result, "llm": ("used" if (result.get("facts") or {}).get("llm", {}).get("used") else "off") if llm else "off",
                "unchanged_edited": (doc.get("facts") or {}).get("edited") or []}

    # ------------------------------------------------------------------ deadlines from an extraction
    def _merge_deadlines(self, doc: dict[str, Any], ex: Extraction, *, quiet: bool) -> None:
        today = self.today()
        existing = [d for d in self.store.doc_deadlines(doc["id"]) if d["auto"]]
        pool = list(existing)
        pending = []
        for draft in ex.deadlines:
            match = next((e for e in pool if e["key"] == draft.key and e["date"] == draft.date.isoformat()), None)
            if match is not None:
                pool.remove(match)
                self._refresh_deadline(match, draft)
            else:
                pending.append(draft)
        for draft in pending:
            same_key = next((e for e in pool if e["key"] == draft.key), None)
            if same_key is not None:
                pool.remove(same_key)
                if same_key["edited"]:
                    continue
                self._refresh_deadline(same_key, draft, date_changed=True)
                continue
            past = draft.date < today
            state = M.DONE if (past and quiet) else M.OPEN
            notified = [] if state == M.OPEN else [*draft.remind, "overdue"]
            created = self.store.create_deadline(doc_id=doc["id"], kind=draft.kind, title=draft.title, date=draft.date.isoformat(), basis=draft.basis,
                                                 evidence=draft.evidence, page=draft.page, confidence=draft.confidence, state=state,
                                                 remind=draft.remind, notified=notified, amount=draft.amount, recurring=draft.recurring, key=draft.key,
                                                 auto=True, done_ts=self.clock() if state == M.DONE else None)
            if not quiet:
                self._emit_deadline_created(created)
        for stale in pool:
            if stale["state"] == M.OPEN and not stale["edited"]:
                self.store.delete_deadline(stale["id"])

    def _refresh_deadline(self, existing: dict[str, Any], draft: Any, date_changed: bool = False) -> None:
        fields: dict[str, Any] = {"title": draft.title, "basis": draft.basis, "evidence": draft.evidence, "page": draft.page,
                                  "confidence": draft.confidence, "amount": draft.amount}
        if not existing["edited"]:
            fields["recurring"] = draft.recurring
            fields["remind"] = draft.remind
            if date_changed:
                fields.update(date=draft.date.isoformat(), notified=[])
        elif date_changed:
            return
        self.store.update_deadline(existing["id"], **fields)

    # ------------------------------------------------------------------ announcements, series and prices
    def _announce_document(self, doc: dict[str, Any], *, quiet: bool) -> None:
        if quiet:
            return
        es = self.lang() == "es"
        kind = M.kind_label(doc["kind"], self.lang())
        title = (f"Nuevo documento: {doc['title']}" if es else f"New document: {doc['title']}")
        body = " · ".join(x for x in (kind, doc.get("issuer"), money_text(doc.get("amount"), doc.get("currency") or "EUR", es)) if x)
        event = {"id": f"doc:{doc['id']}", "type": M.N_DOC, "severity": "low", "title": title, "summary": body, "url": self.link_to(doc["id"]),
                 "bus": "kafka.document.added", "data": {"doc_id": doc["id"], "title": doc["title"], "kind": doc["kind"], "issuer": doc["issuer"]}}
        self._send(event, ["hub"], doc_id=doc["id"], deadline_id=None, dedupe=f"doc:{doc['id']}")

    def _join_series(self, doc: dict[str, Any], ex: Extraction, *, quiet: bool) -> None:
        key = doc.get("issuer_key") or ""
        series_id: Optional[str] = None
        if key and doc["kind"] in M.KINDS:
            ref = ex.series_ref if (ex.series_ref and doc["kind"] != M.OTHER) else ""
            if doc["ref"] and "ref" in ((doc.get("facts") or {}).get("edited") or []):
                ref = doc["ref"]
            if ref or doc["kind"] in SERIES_KINDS:
                found = self.store.find_series(key, ref, "" if ref else doc["kind"])
                if found is None:
                    label = " · ".join(x for x in (doc["issuer"] or key, ref or M.kind_label(doc["kind"], self.lang())) if x)
                    found = self.store.create_series(doc["kind"], key, ref, label)
                series_id = found["id"]
        if (doc.get("series_id") or None) != series_id:
            self.store.update_document(doc["id"], series_id=series_id)
            self.store.drop_empty_series()
        if series_id and not quiet:
            self.check_price(self.store.document(doc["id"]))

    def price_history(self, series_id: str) -> list[dict[str, Any]]:
        rows, previous = [], None
        for d in self.store.series_documents(series_id):
            amount = d.get("amount")
            change = pct = None
            if previous is not None and amount is not None and previous > 0:
                change = round(amount - previous, 2)
                pct = round((amount - previous) / previous * 100, 1)
            rows.append({"doc_id": d["id"], "title": d["title"], "date": d.get("period_to") or d.get("period_from") or d.get("issue_date") or "",
                         "issue_date": d.get("issue_date") or "", "amount": amount, "currency": d.get("currency") or "EUR", "change": change, "pct": pct})
            if amount is not None:
                previous = amount
        return rows

    def check_price(self, doc: dict[str, Any]) -> bool:
        """Notify once when this (newest) document of a series costs ``prices.alert_pct`` % more than the one before."""
        kinds = PRICE_KINDS + ((M.BILL,) if self.setting("prices.bills", "0") == "1" else ())
        if doc["kind"] not in kinds or not doc.get("series_id") or doc.get("amount") is None:
            return False
        try:
            threshold = float(self.setting("prices.alert_pct", "5"))
        except ValueError:
            threshold = 5.0
        docs = [d for d in self.store.series_documents(doc["series_id"]) if d.get("amount") is not None]
        if len(docs) < 2 or docs[-1]["id"] != doc["id"]:
            return False
        old = docs[-2]["amount"]
        if not old or old <= 0:
            return False
        pct = (doc["amount"] - old) / old * 100
        if pct < threshold:
            return False
        stamp = ts_date(doc.get("received_ts")) or parse_iso(doc.get("issue_date")) or self.today()
        if (self.today() - stamp).days > PRICE_RECENT_DAYS:
            return False
        dedupe = f"price:{doc['series_id']}:{doc['id']}"
        if self.store.notified(dedupe):
            return False
        es = self.lang() == "es"
        noun = texts.tr(self.lang(), "noun_insurance" if doc["kind"] == M.INSURANCE else "noun_subscription" if doc["kind"] == M.SUBSCRIPTION else "noun_other")
        pct_text = f"{pct:.0f}"
        title = texts.tr(self.lang(), "price_change", issuer=doc["issuer"] or "?", noun=noun, pct=pct_text,
                         old=money_text(old, doc.get("currency") or "EUR", es), new=money_text(doc["amount"], doc.get("currency") or "EUR", es))
        event = {"id": dedupe, "type": M.N_PRICE, "severity": "medium", "title": title, "summary": doc["title"], "url": self.link_to(doc["id"]),
                 "bus": "kafka.price.change", "data": {"series": doc["series_id"], "issuer": doc["issuer"], "old": old, "new": doc["amount"], "pct": round(pct, 1)}}
        self._send(event, ["toast", "hub", "ntfy", "telegram", "email"], doc_id=doc["id"], deadline_id=None, dedupe=dedupe)
        return True

    # ================================================================== editing
    def update_document(self, did: str, values: dict[str, Any]) -> dict[str, Any]:
        doc = self.store.document(did)
        facts = dict(doc.get("facts") or {})
        edited = set(facts.get("edited") or [])
        fields: dict[str, Any] = {}
        recompute = False
        for name, value in values.items():
            if value is None:
                continue
            if name == "warranty_years":
                try:
                    years = float(value)
                except (TypeError, ValueError) as exc:
                    raise KafkaError("invalid", "warranty_years must be a number.") from exc
                if not 0 < years <= 10:
                    raise KafkaError("invalid", "warranty_years must be between 0 and 10.")
                facts["warranty_months"] = int(round(years * 12))
                recompute = True
                continue
            if name not in EDITABLE:
                continue
            if name == "kind" and value not in M.KINDS:
                raise KafkaError("invalid", f"Unknown kind {value}.", f"Known: {', '.join(M.KINDS)}.")
            if name == "state" and value not in M.DOC_STATES:
                raise KafkaError("invalid", f"Unknown state {value}.", f"Known: {', '.join(M.DOC_STATES)}.")
            if name in ("issue_date", "period_from", "period_to") and _bad_date(value):
                raise KafkaError("invalid", f"{name} must be YYYY-MM-DD.")
            if name == "amount":
                value = None if value == "" else float(value)
                if value is not None and value < 0:
                    raise KafkaError("invalid", "amount cannot be negative.")
            if name == "tags":
                value = [str(t).strip()[:40] for t in (value if isinstance(value, list) else str(value).split(",")) if str(t).strip()][:20]
            if name in ("title", "issuer", "ref", "item", "notes"):
                value = str(value).strip()[: 4000 if name == "notes" else 160]
            fields[name] = value
            if name not in ("tags", "notes") and doc.get(name) != value:
                edited.add(name)
            if name in OVERRIDE_FIELDS:
                recompute = True
        if "issuer" in fields:
            fields["issuer_key"] = issuer_key(fields["issuer"])
        if "amount" in fields:
            fields["currency"] = (doc.get("currency") or "EUR") if fields["amount"] is not None else ""
        if "title" in fields and not fields["title"]:
            raise KafkaError("invalid", "The title cannot be empty.")
        facts["edited"] = sorted(edited)
        self.store.update_document(did, **fields, facts=facts)
        if recompute:
            self.process_document(did)
        return self.store.document(did)

    def delete_document(self, did: str) -> dict[str, Any]:
        doc = self.store.document(did)
        sha, ext = doc.get("file_sha") or "", doc.get("file_ext") or ""
        shared = bool(sha) and self.store.sha_users(sha, excluding=did) > 0
        self.store.delete_document(did)
        removed = False
        if sha and not shared:
            removed = self.files.remove(sha, ext)
        if self.page_cache_dir:
            for png in self.page_cache_dir.glob(f"{did}-*.png"):
                try:
                    png.unlink()
                except OSError:
                    pass
        self.store.drop_empty_series()
        return {"deleted": did, "file_removed": removed, "file_kept_for_other_documents": shared}

    # ------------------------------------------------------------------ deadline actions
    def add_deadline(self, *, title: str, date_: str, kind: str = M.CUSTOM, remind: Optional[list[int]] = None, doc_id: str = "",
                     recurring: str = "none", notes: str = "", amount: Optional[float] = None) -> dict[str, Any]:
        day = parse_iso(date_)
        if day is None or len(str(date_).strip()) < 10:
            raise KafkaError("invalid", "date must be YYYY-MM-DD.")
        if kind not in M.DEADLINE_KINDS:
            raise KafkaError("invalid", f"Unknown kind {kind}.", f"Known: {', '.join(M.DEADLINE_KINDS)}.")
        if recurring not in M.RECURRING:
            raise KafkaError("invalid", f"recurring must be one of {', '.join(M.RECURRING)}.")
        if doc_id:
            self.store.document(doc_id)
        leads = sorted({int(x) for x in remind}, reverse=True) if remind else self.lead_days(kind)
        title = (title or "").strip()
        if not title:
            raise KafkaError("invalid", "The deadline needs a title.")
        notified = ["overdue"] if day < self.today() else []
        created = self.store.create_deadline(doc_id=doc_id or None, kind=kind, title=title[:160], date=day.isoformat(),
                                             basis=texts.tr(self.lang(), "custom_basis"), confidence=100, state=M.OPEN, remind=leads,
                                             notified=notified, amount=amount, recurring=recurring, key="manual", auto=False, edited=True, notes=notes[:2000])
        self._emit_deadline_created(created)
        return created

    # ------------------------------------------------------------------ deadlines kept for another app (source + external key)
    def upsert_external(self, *, source: str, external_key: str, title: str, date_: str, kind: str = M.CUSTOM, remind: Optional[list[int]] = None,
                        recurring: str = "none", notes: str = "", amount: Optional[float] = None, basis: str = "", rule: str = "",
                        url: str = "", doc_id: str = "", source_ref: str = "") -> dict[str, Any]:
        """Add or update the deadline another app keeps here under (source, external_key); never a duplicate.

        The owner app moves the occurrence (a new date reopens it); what the user changed here wins for the current occurrence:
        an edited title, reminders or notes stay, a date the user moved stays until the app sends a new one, and a deadline the
        user dismissed stays dismissed."""
        source, external_key = _check_external(source, external_key)
        day = parse_iso(date_)
        if day is None or len(str(date_).strip()) < 10:
            raise KafkaError("invalid", "date must be YYYY-MM-DD.")
        if kind not in M.DEADLINE_KINDS:
            raise KafkaError("invalid", f"Unknown kind {kind}.", f"Known: {', '.join(M.DEADLINE_KINDS)}.")
        if recurring not in M.RECURRING:
            raise KafkaError("invalid", f"recurring must be one of {', '.join(M.RECURRING)}.")
        if doc_id:
            self.store.document(doc_id)
        title = (title or "").strip()[:160]
        if not title:
            raise KafkaError("invalid", "The deadline needs a title.")
        leads = sorted({int(x) for x in remind}, reverse=True) if remind else self.lead_days(kind)
        basis = (basis or "").strip()[:1500] or texts.tr(self.lang(), "external_basis", source=source)
        rule, url = (rule or "").strip()[:120], (url or "").strip()[:500]
        iso_day = day.isoformat()
        past = ["overdue"] if day < self.today() else []
        existing = self.store.deadline_by_key(source, external_key)
        if existing is None:
            t = self.store.create_deadline(doc_id=doc_id or None, kind=kind, title=title, date=iso_day, basis=basis, confidence=100, state=M.OPEN,
                                           remind=leads, notified=past, amount=amount, recurring=recurring, key="external", auto=False, edited=False,
                                           notes=notes[:2000], source=source, external_key=external_key, ext_date=iso_day, rule=rule, url=url,
                                           source_ref=(source_ref or "").strip()[:300])
            self._emit_deadline_created(t)
            return {"deadline": t, "action": "created"}
        if existing["state"] == M.DISMISSED and existing["edited"]:
            t = self.store.update_deadline(existing["id"], basis=basis, rule=rule, url=url)
            return {"deadline": t, "action": "kept_user_dismissed"}
        fields: dict[str, Any] = {"basis": basis, "rule": rule, "url": url, "kind": kind, "amount": amount, "doc_id": doc_id or existing["doc_id"]}
        if source_ref:
            fields["source_ref"] = source_ref.strip()[:300]
        if not existing["edited"]:
            fields.update(title=title, remind=leads, recurring=recurring, notes=notes[:2000])
        action = "updated"
        if iso_day != existing["ext_date"]:
            fields.update(date=iso_day, ext_date=iso_day, state=M.OPEN, done_ts=None, notified=past)
            action = "rescheduled"
        elif existing["state"] != M.OPEN and not existing["edited"]:
            fields.update(state=M.OPEN, done_ts=None, notified=past if existing["date"] == iso_day else existing["notified"])
            action = "reopened"
        elif existing["edited"]:
            action = "kept_user_edits"
        return {"deadline": self.store.update_deadline(existing["id"], **fields), "action": action}

    def update_external(self, *, source: str, external_key: str, values: dict[str, Any]) -> dict[str, Any]:
        """The owner app closes, reopens or reschedules its deadline. A deadline the user dismissed here stays dismissed."""
        source, external_key = _check_external(source, external_key)
        d = self.store.deadline_by_key(source, external_key)
        if d is None:
            raise KafkaError("not_found", f"No deadline from {source} with key {external_key}.", "Create it with deadline_add (source, external_key).")
        if d["state"] == M.DISMISSED and d["edited"]:
            return {"deadline": d, "action": "kept_user_dismissed"}
        fields: dict[str, Any] = {}
        action = "unchanged"
        if values.get("date"):
            day = parse_iso(values["date"])
            if day is None:
                raise KafkaError("invalid", "date must be YYYY-MM-DD.")
            if day.isoformat() != d["ext_date"] or day.isoformat() != d["date"]:
                fields.update(date=day.isoformat(), ext_date=day.isoformat(), state=M.OPEN, done_ts=None,
                              notified=["overdue"] if day < self.today() else [])
                action = "rescheduled"
        if values.get("title") and not d["edited"]:
            fields["title"] = str(values["title"]).strip()[:160]
            action = "updated" if action == "unchanged" else action
        if values.get("notes") is not None and not d["edited"]:
            fields["notes"] = str(values["notes"])[:2000]
            action = "updated" if action == "unchanged" else action
        state = values.get("state")
        if state:
            if state not in M.DEADLINE_STATES:
                raise KafkaError("invalid", f"state must be one of {', '.join(M.DEADLINE_STATES)}.")
            fields["state"] = state
            fields["done_ts"] = self.clock() if state in (M.DONE, M.DISMISSED) else None
            if state == M.OPEN and "date" not in fields:
                fields["notified"] = ["overdue"] if (parse_iso(d["date"]) or self.today()) < self.today() else []
            action = {"done": "closed", "dismissed": "closed", "open": "reopened"}[state] if "date" not in fields or state != M.OPEN else action
        if not fields:
            return {"deadline": d, "action": action}
        return {"deadline": self.store.update_deadline(d["id"], **fields), "action": action}

    def update_deadline(self, tid: str, values: dict[str, Any]) -> dict[str, Any]:
        d = self.store.deadline(tid)
        fields: dict[str, Any] = {"edited": True}
        if values.get("title"):
            fields["title"] = str(values["title"]).strip()[:160]
        if values.get("date"):
            day = parse_iso(values["date"])
            if day is None:
                raise KafkaError("invalid", "date must be YYYY-MM-DD.")
            if day.isoformat() != d["date"]:
                fields.update(date=day.isoformat(), notified=["overdue"] if day < self.today() else [])
        if values.get("remind") is not None:
            fields["remind"] = sorted({int(x) for x in values["remind"]}, reverse=True)
        if values.get("notes") is not None:
            fields["notes"] = str(values["notes"])[:2000]
        if values.get("recurring"):
            if values["recurring"] not in M.RECURRING:
                raise KafkaError("invalid", f"recurring must be one of {', '.join(M.RECURRING)}.")
            fields["recurring"] = values["recurring"]
        if values.get("snooze_days"):
            days = int(values["snooze_days"])
            if not 1 <= days <= 365:
                raise KafkaError("invalid", "snooze_days must be between 1 and 365.")
            base = max(parse_iso(d["date"]) or self.today(), self.today())
            new_day = base + timedelta(days=days)
            fields.update(date=new_day.isoformat(), notified=[], state=M.OPEN, done_ts=None)
        state = values.get("state")
        if state:
            if state not in M.DEADLINE_STATES:
                raise KafkaError("invalid", f"state must be one of {', '.join(M.DEADLINE_STATES)}.")
            if state == M.DONE and d["recurring"] != "none" and d["state"] != M.DONE and "date" not in fields:
                return self._roll(d, fields)
            fields["state"] = state
            fields["done_ts"] = self.clock() if state in (M.DONE, M.DISMISSED) else None
            if state == M.OPEN and "date" not in fields:
                fields["notified"] = ["overdue"] if (parse_iso(d["date"]) or self.today()) < self.today() else []
        return self.store.update_deadline(tid, **fields)

    def _roll(self, d: dict[str, Any], fields: dict[str, Any]) -> dict[str, Any]:
        """Done on a recurring deadline: it moves to its next occurrence and stays open."""
        current = parse_iso(d["date"]) or self.today()
        step = 1 if d["recurring"] == "monthly" else 12
        nxt = add_months(current, step)
        while nxt <= self.today():
            nxt = add_months(nxt, step)
        return self.store.update_deadline(d["id"], **{**fields, "date": nxt.isoformat(), "notified": [], "state": M.OPEN, "done_ts": self.clock(), "edited": True})

    # ================================================================== folders
    def watched_folders(self) -> list[str]:
        raw = self.setting("folders.watch", "")
        if not raw:
            return [str(self.inbox_dir)] if self.inbox_dir else []
        try:
            data = json.loads(raw)
        except ValueError:
            return []
        return [str(x) for x in data if isinstance(x, str)] if isinstance(data, list) else []

    def _save_folders(self, folders: list[str]) -> None:
        self.set("folders.watch", json.dumps(folders, ensure_ascii=False))

    def add_folder(self, path: str) -> dict[str, Any]:
        reason = paths.unsafe_folder(path, self.data_dir)
        if reason:
            raise KafkaError("forbidden", f"Cannot watch {path}: {reason}.", "Pick an existing folder that is not a drive root, the profile folder or a system folder.")
        resolved = str(Path(path).expanduser().resolve())
        folders = self.watched_folders()
        if resolved in folders:
            return {"added": False, "folders": folders}
        self._save_folders(folders + [resolved])
        return {"added": True, "folders": self.watched_folders()}

    def remove_folder(self, path: str) -> dict[str, Any]:
        folders = self.watched_folders()
        try:
            resolved = str(Path(path).expanduser().resolve())
        except OSError:
            resolved = path
        target = next((f for f in folders if f in (path, resolved)), None)
        if target is None:
            raise KafkaError("not_found", f"{path} is not a watched folder.", "List them with folders_list.")
        self._save_folders([f for f in folders if f != target])
        self.store.forget_folder(target)
        return {"removed": target, "folders": self.watched_folders()}

    def scan_folders(self, folder: str = "") -> dict[str, Any]:
        t0 = time.monotonic()
        folders = [folder] if folder else self.watched_folders()
        out = {"folders": len(folders), "files": 0, "new_documents": 0, "duplicates": 0, "skipped": 0, "errors": [], "documents": []}
        budget = MAX_NEW_FILES_PER_SCAN
        for root in folders:
            base = Path(root)
            if not base.is_dir():
                out["errors"].append(f"{root}: folder not found")
                continue
            if paths.unsafe_folder(root, self.data_dir):
                out["errors"].append(f"{root}: {paths.unsafe_folder(root, self.data_dir)}")
                continue
            for file in self._walk(base):
                if budget <= 0:
                    break
                try:
                    stat = file.stat()
                except OSError:
                    continue
                key = str(file)
                seen = self.store.file_seen(key)
                if seen and abs(seen["mtime"] - stat.st_mtime) < 0.001 and seen["size"] == stat.st_size:
                    continue
                if self.clock() - stat.st_mtime < 2.0:        # still being written: next scan
                    out["skipped"] += 1
                    continue
                out["files"] += 1
                budget -= 1
                if stat.st_size > readers.MAX_FILE_BYTES or stat.st_size == 0:
                    self.store.mark_file_seen(key, stat.st_mtime, stat.st_size, "")
                    out["skipped"] += 1
                    continue
                try:
                    data = file.read_bytes()
                    quiet = self.clock() - stat.st_mtime > QUIET_AGE_S
                    result = self.ingest_bytes(data, file.name, source="folder", source_ref=key, quiet=quiet)
                except KafkaError as exc:
                    out["errors"].append(f"{file.name}: {exc.message}")
                    self.store.mark_file_seen(key, stat.st_mtime, stat.st_size, "")
                    continue
                except Exception as exc:  # noqa: BLE001 — one bad file must not stop the scan
                    log.exception("folder scan: %s failed", file)
                    out["errors"].append(f"{file.name}: {type(exc).__name__}")
                    continue
                self.store.mark_file_seen(key, stat.st_mtime, stat.st_size, sha256_of(data))
                if result["created"]:
                    out["new_documents"] += len(result["documents"])
                    out["documents"].extend(d["id"] for d in result["documents"])
                else:
                    out["duplicates"] += 1
        self.set("folders.last_scan_ts", str(self.clock()))
        self.store.add_run("folders", "", not out["errors"] or out["new_documents"] > 0, int((time.monotonic() - t0) * 1000),
                           f"{out['files']} files, {out['new_documents']} new, {out['duplicates']} known, {len(out['errors'])} errors")
        return out

    def _walk(self, base: Path, depth: int = 3):
        try:
            entries = sorted(base.iterdir(), key=lambda p: p.name.lower())
        except OSError:
            return
        for entry in entries:
            name = entry.name.lower()
            if name.startswith(".") or any(name.startswith(s) or name.endswith(s) for s in SKIP_NAMES):
                continue
            if entry.is_symlink():
                continue
            if entry.is_dir():
                if depth > 0:
                    yield from self._walk(entry, depth - 1)
            elif entry.is_file():
                yield entry

    # ================================================================== mail
    def scan_mail(self, *, since_days: Optional[int] = None, query: str = "", limit: int = 0) -> dict[str, Any]:
        if self.mail is None:
            return {"ok": False, "error": "no mail source"}
        first = self.setting("mail.first_scan_done", "0") != "1"
        days = since_days or int(self.setting("mail.first_days", "180") if first else self.setting("mail.window_days", "14"))
        limit = limit or (800 if first else 150)
        t0 = time.monotonic()
        attachments_dir = str(self.mail_cache_dir) if self.mail_cache_dir else ""
        if attachments_dir:
            Path(attachments_dir).mkdir(parents=True, exist_ok=True)
        answer = self.mail.scan(since_days=days, limit=limit, skip=self.store.known_message_ids(), query=query, attachments_dir=attachments_dir)
        if not answer.get("ok"):
            error = str(answer.get("error") or "mail failed")
            self.store.add_run("mail", "", False, int((time.monotonic() - t0) * 1000), error)
            self.set("mail.last_error", error[:300])
            return {"ok": False, "error": error, "accounts": answer.get("accounts") or []}
        messages = answer.get("messages") or []
        summary = self.ingest_messages(messages, bootstrap=first)
        if first and not since_days and not query:
            self.set("mail.first_scan_done", "1")
        if answer.get("hub_last_id") is not None:     # what the hub's gateway handed over is now filed: resume after it
            self.set("mail.hub_since_id", str(int(answer["hub_last_id"])))
        self.set("mail.last_scan_ts", str(self.clock()))
        self.set("mail.last_error", "")
        self.store.add_run("mail", "", True, int((time.monotonic() - t0) * 1000),
                           f"{len(messages)} mails, {summary['docs']} with documents, {summary['maybe']} to review, {summary['documents_created']} new documents")
        return {"ok": True, "accounts": answer.get("accounts") or [], "messages": len(messages), "error": answer.get("error") or "", **summary}

    def ingest_messages(self, messages: list[dict[str, Any]], *, bootstrap: bool = False) -> dict[str, Any]:
        out = {"docs": 0, "maybe": 0, "noise": 0, "documents_created": 0, "documents": []}
        known = set(self.store.known_message_ids())
        for message in sorted(messages, key=lambda m: m.get("ts") or 0):
            mid = str(message.get("message_id") or "")
            if not mid or mid in known:
                continue
            known.add(mid)
            result = classify_mail(message)
            quiet = bootstrap or (bool(message.get("ts")) and self.clock() - float(message["ts"]) > QUIET_AGE_S)
            doc_ids: list[str] = []
            kind, state = result.kind, "new"
            if result.kind == "doc":
                doc_ids = self._file_mail(message, result, quiet=quiet)
                if doc_ids:
                    state = "filed"
                    out["docs"] += 1
                else:
                    kind = "maybe"
                    out["maybe"] += 1
            elif result.kind == "maybe":
                out["maybe"] += 1
            else:
                state = "ignored"
                out["noise"] += 1
            if doc_ids and message.get("hub_id"):
                self._claim_mail(message["hub_id"], doc_ids[0])
            created = [d for d in doc_ids if d not in out["documents"]]
            out["documents"].extend(created)
            out["documents_created"] += len(created)
            self.store.save_mail(message, kind=kind, score=result.score, state=state, doc_ids=doc_ids, reasons=result.reasons)
            self._drop_cached_attachments(message)
        return out

    def _claim_mail(self, hub_id: Any, doc_id: str) -> None:
        """Tell the hub this mail became a document, so it leaves the person's «sin dueño» tray."""
        if self.mail_claim is None:
            return
        try:
            self.mail_claim([int(hub_id)], "document", self.doc_ref(doc_id))
        except Exception:  # noqa: BLE001 — a claim is a hint
            log.info("mail claim for %s failed", doc_id)

    def _file_mail(self, message: dict[str, Any], result: MailClass, *, quiet: bool, force: bool = False) -> list[str]:
        ids: list[str] = []
        mid = str(message.get("message_id") or "")
        meta = {"subject": message.get("subject"), "from_name": message.get("from_name"), "from_address": message.get("from_address"), "ts": message.get("ts")}
        attachments = result.attachments if not force else [a for a in (message.get("attachments") or []) if a.get("path") or a.get("sha")]
        for att in attachments:
            data = self._read_attachment(att)
            if not data:
                continue
            try:
                got = self.ingest_bytes(data, str(att.get("name") or "attachment"), source="mail", source_ref=mid, mail=meta,
                                        received_ts=message.get("ts"), quiet=quiet)
            except KafkaError as exc:
                log.info("mail attachment skipped: %s", exc.message)
                continue
            ids.extend(d["id"] for d in got["documents"])
        if not ids and (not attachments or force):
            text = str(message.get("text") or "")
            probe = self._probe_text(text, meta) if text.strip() else None
            if probe is not None and (force or (probe.kind != M.OTHER and probe.kind_score >= 10)):
                try:
                    got = self.ingest_text(str(message.get("subject") or ""), text, source="mail", source_ref=mid, mail=meta,
                                           received_ts=message.get("ts"), quiet=quiet)
                    ids.extend(d["id"] for d in got["documents"])
                except KafkaError as exc:
                    log.info("mail body skipped: %s", exc.message)
        ids = list(dict.fromkeys(ids))
        self._fold_copies(ids)
        return ids

    _COPY_RANK = re.compile(r"(?i)factura|invoice")

    def _fold_copies(self, ids: list[str]) -> None:
        """One payment, two PDFs (shops on card processors send «Invoice-123.pdf» and «Receipt-123.pdf»): the invoice
        stays as the document, the receipt is archived beside it with a link, so the payment and its deadlines count once."""
        docs = [d for d in (self.store.document(i) for i in ids) if d is not None]
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for d in docs:
            if d.get("amount") is None or not d.get("issue_date"):
                continue
            groups.setdefault((round(float(d["amount"]), 2), d["issue_date"], d.get("issuer_key") or ""), []).append(d)
        for same in groups.values():
            if len(same) < 2:
                continue
            same.sort(key=lambda d: (0 if self._COPY_RANK.search(str(d.get("file_name") or "")) else 1, str(d.get("created_ts") or "")))
            keep = same[0]
            for copy in same[1:]:
                facts = dict(copy.get("facts") or {})
                facts["copy_of"] = keep["id"]
                note = texts.tr(self.lang(), "copy_note", name=str(keep.get("file_name") or keep.get("title") or keep["id"]))
                facts["notes"] = [*(facts.get("notes") or []), note]
                self.store.update_document(copy["id"], state=M.ARCHIVED, facts=facts)
                for t in self.store.doc_deadlines(copy["id"]):
                    if t["state"] == M.OPEN and not t["edited"]:
                        self.store.update_deadline(t["id"], state=M.DISMISSED, done_ts=self.clock())
                kfacts = dict((self.store.document(keep["id"]) or {}).get("facts") or {})
                kfacts["copies"] = sorted({*(kfacts.get("copies") or []), copy["id"]})
                self.store.update_document(keep["id"], facts=kfacts)

    def _read_attachment(self, att: dict[str, Any]) -> bytes:
        path = att.get("path")
        if not path:
            return b""
        try:
            p = Path(str(path)).resolve()
            if self.mail_cache_dir is not None and self.mail_cache_dir.resolve() not in p.parents:
                return b""                         # only what the mail helper wrote into the cache folder
            if p.stat().st_size > 15 * 1024 * 1024:
                return b""
            return p.read_bytes()
        except OSError:
            return b""

    def _drop_cached_attachments(self, message: dict[str, Any]) -> None:
        for att in message.get("attachments") or []:
            try:
                p = Path(str(att.get("path") or ""))
                if self.mail_cache_dir is not None and p.is_file() and self.mail_cache_dir.resolve() in p.resolve().parents:
                    p.unlink()
            except OSError:
                continue

    def accept_mail(self, message_id: str) -> dict[str, Any]:
        mail = self.store.mail(message_id)
        if mail is None:
            raise KafkaError("not_found", "No such mail.", "List mails with mail_list.")
        message = {"message_id": message_id, "subject": mail["subject"], "from_name": mail["from_name"], "from_address": mail["from_address"],
                   "ts": mail["ts"], "text": mail.get("body") or mail.get("snippet") or "", "attachments": mail.get("attachments") or []}
        ids = self._file_mail(message, MailClass("doc", mail["score"], mail.get("reasons") or []), quiet=False, force=True)
        if not ids:
            raise KafkaError("invalid", "Nothing in that mail could be filed as a document.",
                             "Its attachments are gone (they are kept only while a scan runs) and the text is too short.")
        self.store.set_mail_state(message_id, "filed", ids)
        if mail.get("hub_id"):
            self._claim_mail(mail["hub_id"], ids[0])
        return {"message_id": message_id, "documents": ids}

    def ignore_mail(self, message_id: str) -> dict[str, Any]:
        if self.store.mail(message_id) is None:
            raise KafkaError("not_found", "No such mail.")
        self.store.set_mail_state(message_id, "ignored")
        return {"ignored": message_id}

    # ================================================================== Phileas (delivered parcels -> warranties)
    def sync_phileas(self) -> dict[str, Any]:
        if self.setting("links.phileas", "1") != "1":
            return {"ok": False, "skipped": True, "reason": "links.phileas is off"}
        if self.family_call is None:
            return {"ok": False, "skipped": True, "reason": "no hub connection"}
        t0 = time.monotonic()
        answer = self.family_call("phileas", "shipments_list", {"filter": "delivered", "limit": 200})
        if not isinstance(answer, dict) or not answer.get("ok"):
            error = str((answer or {}).get("error") or "Phileas did not answer")[:200]
            self.store.add_run("phileas", "", False, int((time.monotonic() - t0) * 1000), error)
            self.set("links.phileas.last_error", error)
            return {"ok": False, "error": error}
        result = answer.get("result") or {}
        shipments = result.get("shipments") if isinstance(result, dict) else None
        created = linked = skipped = 0
        for s in shipments or []:
            sid = str(s.get("id") or "")
            label = str(s.get("label") or s.get("item") or "").strip()
            delivered = ts_date(s.get("delivered_ts"))
            known = self.store.document_by_source("phileas", sid) if sid else None
            if known is not None:
                self._refresh_phileas_doc(known, quiet=True)  # applies newer rules (second-hand, unnamed parcels) to old ones
                skipped += 1
                continue
            if not sid or not label or delivered is None or not self._names_a_product(s):
                skipped += 1
                continue
            twin = self._find_purchase(s, delivered)
            if twin is not None:
                facts = dict(twin.get("facts") or {})
                links = [x for x in facts.get("links", []) if x.get("phileas") != sid]
                links.append({"phileas": sid, "label": label, "delivered": delivered.isoformat()})
                facts["links"] = links
                self.store.update_document(twin["id"], facts=facts)
                self._apply_links(self.store.document(twin["id"]))
                linked += 1
                continue
            doc = self.store.create_document(
                title=label[:160], kind=M.WARRANTY, issuer=str(s.get("merchant") or "")[:80], issuer_key=issuer_key(str(s.get("merchant") or "")),
                ref=str(s.get("order_ref") or "")[:60], amount=s.get("price") if isinstance(s.get("price"), (int, float)) else None,
                currency=str(s.get("currency") or "EUR") if s.get("price") else "", issue_date=delivered.isoformat(), item=label[:120],
                source="phileas", source_ref=sid, state=M.OK, confidence=90,
                facts={"edited": [], "notes": [], "delivered": delivered.isoformat(), "from": "phileas"})
            self._refresh_phileas_doc(doc, quiet=self._is_history_day(delivered))
            created += 1
        self.set("links.phileas.last_sync_ts", str(self.clock()))
        self.set("links.phileas.last_error", "")
        self.store.add_run("phileas", "", True, int((time.monotonic() - t0) * 1000), f"{created} new warranties, {linked} linked, {skipped} skipped")
        return {"ok": True, "created": created, "linked": linked, "skipped": skipped}

    def _is_history_day(self, day: date) -> bool:
        return (self.today() - day).days > 2

    def _find_purchase(self, shipment: dict[str, Any], delivered: date) -> Optional[dict[str, Any]]:
        key = issuer_key(str(shipment.get("merchant") or ""))
        if not key:
            return None
        order = str(shipment.get("order_ref") or "").strip()
        price = shipment.get("price") if isinstance(shipment.get("price"), (int, float)) else None
        for d in self.store.documents(limit=500):
            if d["source"] == "phileas" or d["issuer_key"] != key or d["kind"] not in (M.INVOICE, M.RECEIPT, M.WARRANTY):
                continue
            if order and d.get("ref") and order == d["ref"]:
                return d
            when = parse_iso(d.get("issue_date"))
            if when and abs((when - delivered).days) <= 10 and price is not None and d.get("amount") is not None and abs(d["amount"] - price) < 0.01:
                return d
        return None

    # Marketplaces between private people: the consumer guarantee (RDL 7/2021) binds a business seller, not a neighbour.
    SECOND_HAND_C2C = ("wallapop", "vinted", "milanuncios", "todocoleccion", "segundamano", "leboncoin", "kleinanzeigen")
    _UNNAMED_PARCEL = re.compile(r"^\S+(?:\s+\S+)?\s+·\s+(?:pedido|order|env[ií]o|paquete)\b", re.I)

    def _second_hand(self, merchant: str) -> bool:
        key = issuer_key(merchant or "")
        return any(name in key.replace(" ", "") for name in self.SECOND_HAND_C2C)

    def _names_a_product(self, shipment: dict[str, Any]) -> bool:
        """A parcel Phileas only knows as «InPost · pedido 123» says nothing about what was bought: no warranty to keep."""
        if str(shipment.get("item") or "").strip():
            return True
        return not self._UNNAMED_PARCEL.match(str(shipment.get("label") or "").strip())

    def _refresh_phileas_doc(self, doc: dict[str, Any], quiet: bool = False) -> dict[str, Any]:
        facts = dict(doc.get("facts") or {})
        if doc.get("source") == "phileas" and not self._names_a_product({"item": "", "label": doc.get("title") or ""}):
            self._drop_auto_warranty(doc)
            if doc.get("state") != M.ARCHIVED:
                self.store.update_document(doc["id"], state=M.ARCHIVED)
            return self.store.document(doc["id"])
        if self._second_hand(doc.get("issuer") or "") and not facts.get("warranty_months"):
            note = texts.tr(self.lang(), "c2c_note")
            if note not in (facts.get("notes") or []):
                facts["notes"] = [*(facts.get("notes") or []), note]
                self.store.update_document(doc["id"], facts=facts)
            self._drop_auto_warranty(doc)
            return self.store.document(doc["id"])
        delivered = parse_iso(facts.get("delivered") or doc.get("issue_date"))
        if delivered is not None:
            self._set_delivery_warranty(doc, delivered, quiet=quiet, shipment_id=str(doc.get("source_ref") or "") if doc.get("source") == "phileas" else "")
        return self.store.document(doc["id"])

    def _drop_auto_warranty(self, doc: dict[str, Any]) -> None:
        for d in self.store.doc_deadlines(doc["id"]):
            if d["key"] == "warranty" and not d["edited"] and d["state"] == M.OPEN:
                self.store.update_deadline(d["id"], state=M.DISMISSED, done_ts=self.clock())

    def _apply_links(self, doc: dict[str, Any]) -> None:
        links = (doc.get("facts") or {}).get("links") or []
        for link in links:
            delivered = parse_iso(link.get("delivered"))
            if delivered is not None:
                self._set_delivery_warranty(doc, delivered, quiet=self._is_history_day(delivered), shipment_id=str(link.get("phileas") or ""))

    def _set_delivery_warranty(self, doc: dict[str, Any], delivered: date, *, quiet: bool, shipment_id: str = "") -> None:
        lang = self.lang()
        months = (doc.get("facts") or {}).get("warranty_months") or self.warranty_years() * 12
        end = warranty_end(delivered, months=int(months))
        what = doc.get("item") or doc.get("title") or doc.get("issuer") or ""
        title = texts.tr(lang, "warranty_title", what=what)
        basis = texts.tr(lang, "phileas_basis", years=texts.years_text(lang, int(months)), date=human_day(delivered, lang != "en"))
        existing = next((d for d in self.store.doc_deadlines(doc["id"]) if d["key"] == "warranty"), None)
        evidence = f"Phileas: {doc.get('title') or what}"
        if existing is not None:
            if existing["edited"]:
                return
            changed = existing["date"] != end.isoformat()
            self.store.update_deadline(existing["id"], title=title, basis=basis, evidence=evidence, auto=True,
                                       **({"date": end.isoformat(), "notified": []} if changed else {}))
            return
        past = end < self.today()
        state = M.DONE if (past and quiet) else M.OPEN
        leads = self.lead_days(M.WARRANTY_END)
        created = self.store.create_deadline(doc_id=doc["id"], kind=M.WARRANTY_END, title=title, date=end.isoformat(), basis=basis, evidence=evidence,
                                             confidence=90, state=state, remind=leads, notified=[] if state == M.OPEN else [*leads, "overdue"],
                                             key="warranty", auto=True, done_ts=self.clock() if state == M.DONE else None)
        if not quiet:
            self._emit_deadline_created(created)
            self._emit_event("kafka.warranty.created", {"doc_id": doc["id"], "shipment_id": shipment_id, "merchant": doc.get("issuer") or "",
                                                         "until": end.isoformat()})

    # ================================================================== reminders
    def _in_night(self) -> bool:
        try:
            start, end = int(float(self.setting("notify.night_from", "23"))), int(float(self.setting("notify.night_to", "7")))
        except ValueError:
            start, end = 23, 7
        hour = datetime.fromtimestamp(self.clock()).hour
        if start == end:
            return False
        return (hour >= start or hour < end) if start > end else (start <= hour < end)

    def run_reminders(self) -> dict[str, Any]:
        """Notify the deadlines whose lead day has come (once each) and the ones that just became overdue."""
        today = self.today()
        sent = overdue = deferred = 0
        night = self._in_night()
        night_high = self.setting("notify.night_high", "0") == "1"
        horizon = (today + timedelta(days=400)).isoformat()
        for d in self.store.deadlines(states=[M.OPEN], date_to=horizon, limit=2000):
            day = parse_iso(d["date"])
            if day is None:
                continue
            days_left = (day - today).days
            notified = list(d["notified"])
            doc = self.store.find_document(d["doc_id"]) if d["doc_id"] else None
            if days_left < 0:
                if "overdue" in notified:
                    continue
                if night and not night_high:
                    deferred += 1
                    continue
                if -days_left <= OVERDUE_NOTIFY_MAX_DAYS:
                    self._notify_deadline(d, doc, days_left, overdue=True)
                    overdue += 1
                self.store.update_deadline(d["id"], notified=[*notified, "overdue"])
                continue
            due = sorted(l for l in d["remind"] if l not in notified and days_left <= l)
            if not due:
                continue
            severity = M.severity_for(d["kind"], days_left)
            if night and not (night_high and severity == "high"):
                deferred += 1
                continue
            self._notify_deadline(d, doc, days_left)
            sent += 1
            self.store.update_deadline(d["id"], notified=sorted({*[x for x in notified], *due}, key=lambda v: (isinstance(v, str), v)))
        return {"sent": sent, "overdue": overdue, "deferred": deferred, "night": night}

    def _notify_deadline(self, d: dict[str, Any], doc: Optional[dict[str, Any]], days_left: int, overdue: bool = False) -> None:
        es = self.lang() == "es"
        day = parse_iso(d["date"]) or self.today()
        pretty = human_day(day, es)
        if overdue:
            n = -days_left
            title = (f"Vencido hace {n} {'día' if n == 1 else 'días'}: {d['title']}" if es else f"Overdue by {n} day{'s' if n != 1 else ''}: {d['title']}")
            type_, severity, bus = M.N_OVERDUE, "high", "kafka.deadline.overdue"
            dedupe = f"overdue:{d['id']}:{d['date']}"
        else:
            when = (("hoy" if days_left == 0 else "mañana" if days_left == 1 else f"en {days_left} días") if es
                    else ("today" if days_left == 0 else "tomorrow" if days_left == 1 else f"in {days_left} days"))
            title = f"{when.capitalize()}: {d['title']}"
            type_, severity, bus = M.N_SOON, M.severity_for(d["kind"], days_left), "kafka.deadline.soon"
            dedupe = f"soon:{d['id']}:{d['date']}:{days_left}"
        parts = [f"{pretty}", clamp_text(d.get("basis") or "", 220)]
        if d.get("amount") is not None:
            parts.insert(1, money_text(d["amount"], (doc or {}).get("currency") or "EUR", es))
        if doc:
            parts.append(f"«{doc['title']}»")
        data = {"deadline_id": d["id"], "title": d["title"], "due": d["date"], "date": d["date"], "days_left": days_left, "kind": d["kind"],
                "severity": severity, "doc_id": doc["id"] if doc else None, "source": d.get("source") or None,
                "external_key": d.get("external_key") or None}
        event = {"id": dedupe, "type": type_, "severity": severity, "title": title[:200], "summary": " · ".join(p for p in parts if p),
                 "url": self.link_to(doc["id"]) if doc else (d.get("url") or ""), "bus": bus, "data": data}
        self._send(event, list(DEADLINE_CHANNELS), doc_id=doc["id"] if doc else None, deadline_id=d["id"], dedupe=dedupe)
        # the bus event of the state (soon, overdue): once per deadline, date and state, whatever the lead day that triggered it
        marker = f"evt.{'overdue' if overdue else 'soon'}.{d['id']}.{d['date']}"      # a new date is a new state
        if self.setting(marker) == "":
            self.set(marker, "1")
            if self.setting("notify.hub.enabled", "1") != "0":
                self._emit_event(bus, {k: data[k] for k in ("deadline_id", "title", "due", "days_left", "kind", "doc_id")})

    def _send(self, event: dict[str, Any], channels: list[str], *, doc_id: Optional[str], deadline_id: Optional[str], dedupe: str) -> bool:
        if self.store.notified(dedupe):
            return False
        try:
            results = self.notifier.send(event, channels)
        except Exception as exc:  # noqa: BLE001
            results = [{"channel": "all", "ok": False, "error": type(exc).__name__}]
        self.store.add_notification(doc_id=doc_id, deadline_id=deadline_id, type_=event["type"], severity=event["severity"], title=event["title"],
                                    body=event.get("summary") or "", results=results, dedupe=dedupe)
        return True

    # ================================================================== housekeeping
    def housekeeping(self) -> dict[str, Any]:
        now, today = self.clock(), self.today()
        archived = rolled = relinked = purged = 0
        for d in self.store.deadlines(states=[M.DONE, M.DISMISSED], limit=5000):
            stamp = d.get("done_ts") or d.get("updated_ts") or now
            if now - float(stamp) > ARCHIVE_DONE_DAYS * DAY:
                self.store.update_deadline(d["id"], archived=True)
                archived += 1
        for d in self.store.deadlines(states=[M.OPEN], limit=5000):
            if d["recurring"] == "none":
                continue
            day = parse_iso(d["date"])
            if day is None or (today - day).days < ROLL_AFTER_DAYS:
                continue
            step = 1 if d["recurring"] == "monthly" else 12
            nxt = add_months(day, step)
            while nxt <= today:
                nxt = add_months(nxt, step)
            self.store.update_deadline(d["id"], date=nxt.isoformat(), notified=[], edited=True)
            rolled += 1
        by_mail: dict[str, list[str]] = {}
        for doc in self.store.documents(limit=2000, exclude_archived=True):
            if doc.get("source") == "mail" and doc.get("source_ref"):
                by_mail.setdefault(doc["source_ref"], []).append(doc["id"])
        for ids in by_mail.values():
            if len(ids) > 1:
                self._fold_copies(ids)
        for doc in self.store.documents(limit=2000, exclude_archived=True):
            if doc.get("series_id") is None and doc.get("issuer_key") and doc["kind"] in SERIES_KINDS:
                ex = Extraction(kind=doc["kind"], series_ref="")
                self._join_series(doc, ex, quiet=True)
                relinked += 1
        self.store.drop_empty_series()
        cutoffs = [(self.page_cache_dir, 30 * DAY), (self.mail_cache_dir, DAY)]
        for folder, age in cutoffs:
            if folder and folder.is_dir():
                for file in folder.iterdir():
                    try:
                        if file.is_file() and now - file.stat().st_mtime > age:
                            file.unlink()
                            purged += 1
                    except OSError:
                        continue
        jobs_purged = workshop_jobs.purge(self.data_dir / "workshop", now) if self.data_dir else 0
        return {"archived_deadlines": archived, "rolled_recurring": rolled, "series_relinked": relinked, "cache_files_purged": purged,
                "workshop_jobs_purged": jobs_purged, "ledger_lookups": self.retry_ledger_links()}


def _format_sender(mail: dict[str, Any]) -> str:
    name, address = str(mail.get("from_name") or "").strip(), str(mail.get("from_address") or "").strip()
    if name and address:
        return f"{name} <{address}>"
    return address or name


__all__ = ["Engine", "issuers_mod"]
