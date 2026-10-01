"""Wiring: database, file store, OCR, mail source, notifier, engine and scheduler behind one object that the API routers and
the agent tools share. Also the views (cards, dashboard, detail, status) built from the store."""

from __future__ import annotations

import logging
import os
import secrets as _secrets
import time
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from . import SERVICE, __version__
from . import model as M
from .bizdays import REGIONS
from .config import Config
from .db import Database
from .engine import Engine
from .errors import KafkaError
from .extract.llm import LlmPass
from .files import FileStore
from .mail.source import FaustusMail
from .notify import CHANNELS, EMAIL_BACKENDS, Notifier
from .ocr import Ocr
from .scheduler import Scheduler
from .store import Store
from .util import parse_iso

log = logging.getLogger("kafka")

SECRET_NAMES = ("FAUSTUS_DIR", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "NTFY_TOPIC", "NTFY_TOKEN", "SMTP_HOST", "SMTP_PORT", "SMTP_USER",
                "SMTP_PASSWORD", "SMTP_FROM", "SMTP_TO")
SHOWN_SECRETS = ("SMTP_HOST", "SMTP_PORT", "SMTP_FROM", "SMTP_TO", "TELEGRAM_CHAT_ID", "FAUSTUS_DIR")
UI_SETTINGS: dict[str, Optional[tuple[str, ...]]] = {
    "ui.language": ("es", "en"),
    "scheduler.paused": ("0", "1"),
    "folders.interval_min": None,
    "mail.enabled": ("1", "0"),
    "mail.interval_min": None,
    "mail.window_days": None,
    "mail.first_days": None,
    "mail.faustus_dir": None,
    "mail.faustus_owner": None,
    "extract.llm": ("auto", "off"),
    "warranty.years": None,
    "calendar.region": REGIONS,
    "calendar.extra_holidays": None,
    "prices.alert_pct": None,
    "prices.bills": ("0", "1"),
    "links.phileas": ("1", "0"),
    "notify.night_from": None,
    "notify.night_to": None,
    "notify.night_high": ("0", "1"),
    "notify.ntfy.server": None,
    "notify.email.backend": EMAIL_BACKENDS,
    **{f"remind.{k}": None for k in M.DEADLINE_KINDS},
    **{f"notify.{c}.enabled": ("1", "0") for c in CHANNELS},
    **{f"notify.{c}.min_severity": ("low", "medium", "high") for c in CHANNELS},
}
DEFAULTS = {"ui.language": "es", "scheduler.paused": "0", "folders.interval_min": "5", "mail.enabled": "1", "mail.interval_min": "15",
            "mail.window_days": "14", "mail.first_days": "180", "extract.llm": "auto", "warranty.years": "3", "calendar.region": "ES-MD",
            "prices.alert_pct": "5", "prices.bills": "0", "links.phileas": "1", "notify.night_from": "23", "notify.night_to": "7",
            "notify.night_high": "0", "notify.ntfy.server": "https://ntfy.sh", "notify.email.backend": "auto",
            **{f"remind.{k}": ",".join(str(x) for x in v) for k, v in M.DEFAULT_LEADS.items()}}
NUMERIC = {"folders.interval_min": (1, 1440), "mail.interval_min": (5, 1440), "mail.window_days": (1, 365), "mail.first_days": (1, 730),
           "warranty.years": (1, 10), "prices.alert_pct": (1, 100), "notify.night_from": (0, 24), "notify.night_to": (0, 24)}
SOON_DAYS = 7
MONTH_DAYS = 30


def write_token(config: Config) -> str:
    """The MCP token is persistent: created once, reused on every later start."""
    config.data_dir.mkdir(parents=True, exist_ok=True)
    try:
        existing = config.token_path.read_text(encoding="utf-8").strip()
    except OSError:
        existing = ""
    if len(existing) >= 32:
        return existing
    token = _secrets.token_hex(32)
    config.token_path.write_text(token, encoding="utf-8")
    try:
        config.token_path.chmod(0o600)
    except OSError:
        pass
    return token


def write_url(config: Config) -> None:
    try:
        config.url_path.write_text(f"http://127.0.0.1:{config.port}", encoding="utf-8")
    except OSError:
        pass


def _family_call(app: str, tool: str, arguments: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    from .hoard_link import family
    return family.call(app, tool, arguments, timeout=60.0)


class Services:
    def __init__(self, config: Config, *, http_transport: Optional[httpx.BaseTransport] = None, clock_fn: Callable[[], float] = time.time,
                 notifier: Any = None, mail_runner: Optional[Callable[..., Any]] = None, mail_source: Any = None, ocr: Any = None,
                 llm: Any = None, family_call: Optional[Callable[..., dict]] = None):
        self.config = config
        self.clock = clock_fn
        self.started_at = time.time()
        for d in (config.data_dir, config.cache_dir, config.logs_dir, config.files_dir, config.inbox_dir, config.mail_cache_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.token = write_token(config)
        write_url(config)
        self.db = Database(config.db_path)
        self.store = Store(self.db, clock_fn)
        self._load_secrets()
        self.files = FileStore(config.files_dir)
        self.notifier = notifier or Notifier(config, self.db.get_setting, transport=http_transport, clock=clock_fn)
        self.mail = mail_source or FaustusMail(self.setting, config.secret, runner=mail_runner, clock=clock_fn)
        self.ocr = ocr or Ocr(config.ocr)
        self.llm = llm if llm is not None else LlmPass(config.backend_json_path)
        self.engine = Engine(self.store, self.files, self.notifier, self.ocr, settings_get=self.db.get_setting, settings_set=self.db.set_setting,
                             emit=self._emit, clock=clock_fn, mail_source=self.mail, data_dir=config.data_dir, mail_cache_dir=config.mail_cache_dir,
                             inbox_dir=config.inbox_dir, page_cache_dir=config.cache_dir / "pages", llm=self.llm,
                             family_call=family_call if family_call is not None else (None if config.offline else _family_call),
                             base_url=lambda: f"http://127.0.0.1:{config.port}", submit=self._submit_job)
        self.scheduler = Scheduler(self.engine, self.store, clock=clock_fn, enabled=config.scheduler,
                                   paused=lambda: self.setting("scheduler.paused") == "1",
                                   folders_interval_min=lambda: float(self.setting("folders.interval_min") or 5),
                                   mail_interval_min=lambda: float(self.setting("mail.interval_min") or 15),
                                   mail_enabled=lambda: self.setting("mail.enabled") == "1" and not config.offline,
                                   phileas_enabled=lambda: self.setting("links.phileas") == "1" and not config.offline)

    def _submit_job(self, kind: str, ref: str) -> Any:
        if self.config.scheduler and self.scheduler._alive():
            return self.scheduler.submit(kind, ref, "ingest")
        return None

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self.config.scheduler:
            self.scheduler.start()

    def stop(self) -> None:
        self.scheduler.stop()
        try:
            self.llm.close()
        except Exception:  # noqa: BLE001
            pass
        self.db.close()

    def _emit(self, type_: str, data: dict[str, Any]) -> None:
        try:
            from .hoard_link import family
            family.emit(type_, data)
        except Exception:  # noqa: BLE001 — events are hints; the database is the truth
            pass

    # ------------------------------------------------------------------ settings and secrets
    def setting(self, key: str, default: Optional[str] = None) -> str:
        value = self.db.get_setting(key, None)
        if value in (None, ""):
            if default is not None:
                return default
            if key.endswith(".enabled") and key.startswith("notify."):
                from .notify import DEFAULT_ENABLED
                return "1" if DEFAULT_ENABLED.get(key.split(".")[1]) else "0"
            if key.endswith(".min_severity"):
                return "medium" if key == "notify.email.min_severity" else "low"
            return DEFAULTS.get(key, "")
        return str(value)

    def settings(self) -> dict[str, str]:
        return {key: self.setting(key) for key in UI_SETTINGS}

    def set_settings(self, values: dict[str, Any]) -> dict[str, str]:
        for key, value in values.items():
            if key not in UI_SETTINGS:
                raise KafkaError("invalid", f"Unknown setting {key}.", f"Known: {', '.join(UI_SETTINGS)}.")
            value = ("1" if value else "0") if isinstance(value, bool) else str(value).strip()
            allowed = UI_SETTINGS[key]
            if allowed and value not in allowed:
                raise KafkaError("invalid", f"{key} must be one of {', '.join(a or '(none)' for a in allowed)}.")
            if key in NUMERIC and value:
                try:
                    number = int(float(value))
                except ValueError as exc:
                    raise KafkaError("invalid", f"{key} must be a number.") from exc
                lo, hi = NUMERIC[key]
                if not lo <= number <= hi:
                    raise KafkaError("invalid", f"{key} must be between {lo} and {hi}.")
                value = str(number)
            if key == "calendar.extra_holidays" and value:
                for chunk in value.replace(";", ",").split(","):
                    if parse_iso(chunk.strip()) is None or len(chunk.strip()) != 10:
                        raise KafkaError("invalid", f"Holiday {chunk.strip()!r} is not YYYY-MM-DD.")
            if key.startswith("remind.") and value:
                try:
                    leads = sorted({int(x) for x in value.replace(";", ",").split(",") if x.strip()}, reverse=True)
                except ValueError as exc:
                    raise KafkaError("invalid", f"{key} must be days separated by commas, e.g. 30,7,0.") from exc
                if not leads or any(not 0 <= x <= 365 for x in leads):
                    raise KafkaError("invalid", f"{key}: days must be between 0 and 365.")
                value = ",".join(str(x) for x in leads)
            self.db.set_setting(key, value)
        return self.settings()

    def _load_secrets(self) -> None:
        for name in SECRET_NAMES:
            value = self.db.get_setting(f"secret.{name}")
            key = f"KAFKA_{name}"
            if value and key not in self.config.secrets:
                self.config.secrets[key] = value

    def set_secret(self, name: str, value: str) -> dict[str, Any]:
        name = name.upper().removeprefix("KAFKA_")
        if name not in SECRET_NAMES:
            raise KafkaError("invalid", f"Unknown secret {name}.", f"Known: {', '.join(SECRET_NAMES)}.")
        key = f"KAFKA_{name}"
        value = (value or "").strip()
        if value:
            self.db.set_setting(f"secret.{name}", value)
            self.config.secrets[key] = value
        else:
            self.db.execute("DELETE FROM settings WHERE key = ?", (f"secret.{name}",))
            self.config.secrets.pop(key, None)
        return self.secrets_status()[name]

    def secrets_status(self) -> dict[str, dict[str, Any]]:
        out = {}
        for name in SECRET_NAMES:
            key = f"KAFKA_{name}"
            value = self.config.secret(name)
            env = bool(os.environ.get(key))
            db = self.db.get_setting(f"secret.{name}") is not None
            source = "env" if env else ("settings" if db else (".env" if value else ""))
            shown = value if name in SHOWN_SECRETS else (("…" + value[-4:]) if len(value) >= 8 else ("****" if value else ""))
            out[name] = {"configured": bool(value), "source": source, "value": shown}
        return out

    # ------------------------------------------------------------------ views
    def doc_card(self, d: dict[str, Any], *, deadline: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        lang = self.setting("ui.language")
        facts = d.get("facts") or {}
        if deadline is None and d.get("id"):
            nxt = self.store.deadlines(doc_id=d["id"], states=[M.OPEN], limit=1)
            deadline = nxt[0] if nxt else None
        return {**{k: d.get(k) for k in ("id", "title", "kind", "issuer", "ref", "amount", "currency", "issue_date", "period_from", "period_to",
                                         "item", "file_name", "mime", "size", "pages", "ocr", "source", "mail_subject", "mail_from", "received_ts",
                                         "state", "confidence", "series_id", "tags", "notes", "created_ts", "updated_ts")},
                "kind_label": M.kind_label(d.get("kind") or M.OTHER, lang), "has_file": bool(d.get("file_sha")),
                "notes_extraction": facts.get("notes") or [], "edited": facts.get("edited") or [],
                "warranty_months": facts.get("warranty_months"), "links": facts.get("links") or [],
                "next_deadline": ({"date": deadline["date"], "title": deadline["title"]} if deadline else None)}

    def deadline_card(self, t: dict[str, Any], doc: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        lang = self.setting("ui.language")
        today = self.engine.today()
        day = parse_iso(t["date"])
        days_left = (day - today).days if day else None
        if doc is None and t.get("doc_id"):
            doc = self.store.find_document(t["doc_id"])
        return {**{k: t.get(k) for k in ("id", "doc_id", "kind", "title", "date", "basis", "evidence", "page", "confidence", "state", "remind",
                                         "notified", "amount", "recurring", "auto", "edited", "archived", "notes", "created_ts", "done_ts")},
                "kind_label": M.deadline_label(t["kind"], lang), "days_left": days_left,
                "severity": M.severity_for(t["kind"], days_left) if days_left is not None and t["state"] == M.OPEN else "low",
                "document": ({"id": doc["id"], "title": doc["title"], "issuer": doc["issuer"], "kind": doc["kind"], "kind_label": M.kind_label(doc["kind"], lang),
                              "currency": doc.get("currency")} if doc else None)}

    def deadline_cards(self, rows: list[dict[str, Any]], doc: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        docs: dict[str, Any] = {doc["id"]: doc} if doc else {}
        out = []
        for t in rows:
            did = t.get("doc_id")
            if did and did not in docs:
                docs[did] = self.store.find_document(did)
            out.append(self.deadline_card(t, docs.get(did) if did else None))
        return out

    def dashboard(self) -> dict[str, Any]:
        now = self.clock()
        today = self.engine.today()
        open_rows = self.deadline_cards(self.store.deadlines(states=[M.OPEN], limit=1500))
        overdue = [c for c in open_rows if (c["days_left"] or 0) < 0]
        week = [c for c in open_rows if c["days_left"] is not None and 0 <= c["days_left"] <= SOON_DAYS]
        month = [c for c in open_rows if c["days_left"] is not None and SOON_DAYS < c["days_left"] <= MONTH_DAYS]
        later = [c for c in open_rows if c["days_left"] is not None and c["days_left"] > MONTH_DAYS]
        done = self.deadline_cards(self.store.deadlines(states=[M.DONE, M.DISMISSED], limit=10, newest_first=True))
        last_visit = float(self.db.get_setting("dashboard.last_visit_ts", "0") or 0)
        news = [n for n in self.store.notifications(limit=30) if n["ts"] > last_visit and n["type"] != M.N_DOC]
        recent = [self.doc_card(d) for d in self.store.documents(limit=8, exclude_archived=True)]
        review_mails = [{k: m.get(k) for k in ("message_id", "ts", "from_address", "subject", "snippet", "score", "reasons")}
                        for m in self.store.mails(kind=["maybe"], state=["new"], limit=10)]
        return {"now": now, "today": today.isoformat(), "overdue": overdue, "week": week, "month": month, "later": later, "recently_closed": done,
                "recent_documents": recent, "news": news, "last_visit_ts": last_visit or None, "counts": self.store.counts(),
                "review_mails": review_mails, "folders": self.folders_view(),
                "mail": {"last_scan_ts": float(self.setting("mail.last_scan_ts") or 0) or None, "last_error": self.setting("mail.last_error"),
                         "enabled": self.setting("mail.enabled") == "1"},
                "scheduler": self.scheduler.status()}

    def mark_visit(self) -> dict[str, Any]:
        now = self.clock()
        n = self.store.mark_notifications_seen(now)
        self.db.set_setting("dashboard.last_visit_ts", str(now))
        return {"marked_seen": n, "last_visit_ts": now}

    def detail(self, did: str, *, text_limit: int = 80_000) -> dict[str, Any]:
        d = self.store.document(did)
        deadlines = self.deadline_cards(self.store.doc_deadlines(did), d)
        series: Optional[dict[str, Any]] = None
        neighbours: list[dict[str, Any]] = []
        if d.get("series_id"):
            s = self.store.series(d["series_id"])
            history = self.engine.price_history(s["id"])
            last = [h for h in history if h["pct"] is not None]
            series = {**s, "history": history, "latest_pct": last[-1]["pct"] if last else None}
            neighbours = [self.doc_card(x) for x in self.store.series_documents(s["id"]) if x["id"] != did][:12]
        pages, used = [], 0
        for p in self.store.pages(did):
            text = p["text"][: max(0, text_limit - used)]
            used += len(text)
            pages.append({"page": p["page"], "text": text, "chars": len(p["text"])})
        mails = []
        if d.get("source") == "mail" and d.get("source_ref"):
            m = self.store.mail(d["source_ref"])
            if m:
                mails.append({k: m.get(k) for k in ("message_id", "ts", "from_address", "from_name", "subject", "kind", "snippet", "state")})
        extracted = (d.get("facts") or {}).get("extracted") or []
        return {"document": self.doc_card(d), "facts": extracted, "deadlines": deadlines, "series": series, "neighbours": neighbours,
                "pages": pages, "mails": mails, "extraction_notes": (d.get("facts") or {}).get("notes") or [],
                "kind_reasons": (d.get("facts") or {}).get("kind_reasons") or [], "llm": (d.get("facts") or {}).get("llm"),
                "amount_reduced": (d.get("facts") or {}).get("amount_reduced"),
                "notifications": [n for n in self.store.notifications(limit=200) if n.get("doc_id") == did][:20]}

    # ------------------------------------------------------------------ status
    def counts(self) -> dict[str, int]:
        return self.store.counts()

    def folders_view(self) -> list[dict[str, Any]]:
        out = []
        for f in self.engine.watched_folders():
            p = Path(f)
            out.append({"path": f, "exists": p.is_dir(), "inbox": str(p) == str(self.config.inbox_dir)})
        return out

    def status(self) -> dict[str, Any]:
        ocr_ok, ocr_note = self.ocr.available()
        return {"service": SERVICE, "version": __version__, "data_dir": str(self.config.data_dir), "uptime_s": int(time.time() - self.started_at),
                "counts": self.counts(), "scheduler": self.scheduler.status(), "channels": self.notifier.channels_status(),
                "folders": self.folders_view(), "last_folder_scan_ts": float(self.setting("folders.last_scan_ts") or 0) or None,
                "mail": {"faustus_dir": str(self.mail.faustus_dir() or ""), "last_scan_ts": float(self.setting("mail.last_scan_ts") or 0) or None,
                         "last_error": self.setting("mail.last_error"), "first_scan_done": self.setting("mail.first_scan_done") == "1"},
                "ocr": {"available": ocr_ok, "detail": ocr_note},
                "llm": {"mode": self.setting("extract.llm"), "available": self._llm_available()},
                "phileas": {"enabled": self.setting("links.phileas") == "1", "last_sync_ts": float(self.setting("links.phileas.last_sync_ts") or 0) or None,
                            "last_error": self.setting("links.phileas.last_error")},
                "offline": self.config.offline, "recent_runs": self.store.runs(limit=12)}

    def _llm_available(self) -> Optional[dict[str, Any]]:
        if self.config.offline or self.setting("extract.llm") == "off":
            return None
        cached = getattr(self, "_llm_cache", None)
        if cached and time.time() - cached[0] < 60:
            return cached[1]
        try:
            ok, why = self.llm.available()
        except Exception as exc:  # noqa: BLE001
            ok, why = False, type(exc).__name__
        value = {"ok": ok, "detail": why}
        self._llm_cache = (time.time(), value)
        return value

