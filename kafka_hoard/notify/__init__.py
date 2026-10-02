"""Notification channels: Windows toast, family hub, ntfy, Telegram and e-mail.

The senders themselves (toast, ntfy, Telegram, SMTP, the Faustus helper) are the family's (``hoard_link.notify_channels`` and
``hoard_link.fam_mail``); this module decides which channel gets which event: the per-channel switches and severity floors, the
``notify.via`` choice between the hub's notification service and Kafka's own channels (``fam_notify.Router``) and the wording.

E-mail has two backends: plain SMTP with Kafka's own credentials, or the account configured in Faustus (the helper runs under
Faustus's Python, so that password never reaches Kafka). ``auto`` uses SMTP when Kafka has its own user and password and Faustus
otherwise.

``Notifier.send(event, channels)`` returns one ``{channel, ok, error}`` per channel and never raises.
Secrets come only from ``config.secret(...)`` (environment / .env); the database holds only the
``notify.<channel>.enabled`` / ``min_severity`` / non-secret settings. Nothing secret is logged or
put in an error string.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from ..hoard_link import fam_mail, notify_channels as nc
from ..hoard_link.fam_notify import Router
from .labels import LABELS, TYPE_TAGS, WORDS, compose, format_price, label

log = logging.getLogger("kafka.notify")

CHANNELS = ("toast", "hub", "ntfy", "telegram", "email")
OWN_CHANNELS = ("toast", "ntfy", "telegram", "email")     # what the hub's notification service replaces when notify.via is hub or auto
VIA_MODES = ("auto", "hub", "own")
HUB_NOTIFY = "hub_notify"                                  # the channel name recorded for a notification handed to the hub
URGENT_KINDS = ("appeal", "fine_discount", "cancel_by")    # on their last day these are urgent for the hub (it bypasses quiet hours)
GROUPS = {"deadline_soon": "deadline", "deadline_overdue": "deadline", "price_change": "price", "document_added": "document"}
SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}
HUB_PRIORITY = {"low": "low", "medium": "normal", "high": "high"}
APP_ID = "Kafka's Hoard"
DEFAULT_ENABLED = {"toast": True, "hub": True, "ntfy": False, "telegram": False, "email": False}
HTTP_TIMEOUT_S = nc.HTTP_TIMEOUT_S
EMAIL_BACKENDS = ("auto", "faustus", "smtp")
xml_escape = nc.xml_escape
build_toast_ps1 = nc.build_toast_ps1
_http_url = nc.http_url
telegram_discover_chat_id = nc.telegram_discover_chat_id


class _Hub:
    """The hub's notification client as ``fam_notify.Router`` wants it, with Kafka's offline switch in front."""

    def __init__(self, notifier: "Notifier"):
        self.n = notifier
        self.last: Optional[dict[str, Any]] = None       # what the hub answered, for the result list

    def hub_available(self) -> bool:
        return self.n.hub_available()

    def notify(self, title: str, body: str = "", **kw: Any) -> dict[str, Any]:
        if getattr(self.n.config, "offline", False):
            self.last = {"ok": False, "error": "offline"}
        else:
            try:
                answer = self.n.hub_api.notify(title, body, **kw)
                self.last = answer if isinstance(answer, dict) else {"ok": False, "error": "unexpected answer"}
            except Exception as exc:  # noqa: BLE001 — never raise out of the engine
                self.last = {"ok": False, "error": type(exc).__name__}
        return self.last


class Notifier:
    def __init__(self, config: Any, db_settings_get: Callable[[str, Optional[str]], Optional[str]], *, transport: Any = None,
                 clock: Callable[[], float] = time.time, smtp_factory: Optional[Callable[..., Any]] = None,
                 toast_backend: Optional[Callable[[str, str, str, bool], None]] = None,
                 powershell_runner: Optional[Callable[..., Any]] = None, platform: Optional[str] = None,
                 faustus_runner: Optional[Callable[..., Any]] = None, hub_api: Any = None):
        self.config = config
        self.get = db_settings_get
        self.transport = transport
        self.clock = clock
        self.smtp_factory = smtp_factory                 # (host, port, use_ssl) -> client; the default is the family's
        self.toast_backend = toast_backend
        self.powershell_runner = powershell_runner
        self.platform = platform or sys.platform
        self._hub_api = hub_api            # ``notify(...)`` and ``hub_available()`` of the family library; tests inject a fake
        self.helper = fam_mail.FaustusHelper(lambda: self._setting("mail.faustus_dir") or self._secret("FAUSTUS_DIR"),
                                             owner=lambda: self._setting("mail.faustus_owner"), env_drop_prefixes=("KAFKA_",),
                                             runner=faustus_runner, clock=clock)

    # ------------------------------------------------------------------ the hub's notification service
    @property
    def hub_api(self) -> Any:
        if self._hub_api is None:
            from ..hoard_link import fam_notify
            self._hub_api = fam_notify
        return self._hub_api

    def via(self) -> str:
        value = self._setting("notify.via", "auto").lower()
        return value if value in VIA_MODES else "auto"

    def hub_available(self) -> bool:
        if getattr(self.config, "offline", False):
            return False
        try:
            return bool(self.hub_api.hub_available())
        except Exception:  # noqa: BLE001
            return False

    def via_status(self) -> dict[str, Any]:
        return Router(self.via, None, APP_ID, hub=_Hub(self)).via_status()

    @staticmethod
    def hub_priority(event: dict[str, Any]) -> str:
        """Kafka's severities to the hub's priorities: low, medium to normal, high to high; the last day of an appeal, a fine
        discount or a cancellation window is urgent."""
        severity = str(event.get("severity") or "medium").lower()
        data = event.get("data") or {}
        if severity == "high" and data.get("days_left") == 0 and str(data.get("kind") or "") in URGENT_KINDS and event.get("type") != "deadline_overdue":
            return "urgent"
        return HUB_PRIORITY.get(severity, "normal")

    def _hub_result(self, answer: dict[str, Any]) -> dict[str, Any]:
        ok = bool(answer.get("ok"))
        out: dict[str, Any] = {"channel": HUB_NOTIFY, "ok": ok, "error": "" if ok else str(answer.get("error") or "the hub refused")[:160], "ts": self.clock()}
        if answer.get("held"):
            out["held"] = str(answer["held"])
        if ok:
            out["delivered"] = answer.get("delivered")
        return out

    # ------------------------------------------------------------------ settings and status
    def _setting(self, key: str, default: str = "") -> str:
        try:
            value = self.get(key, None)
        except Exception:  # noqa: BLE001
            value = None
        return default if value in (None, "") else str(value)

    def _lang(self) -> str:
        lang = (self._setting("notify.language") or self._setting("ui.language") or "es").lower()[:2]
        return lang if lang in LABELS else "es"

    def _secret(self, name: str) -> str:
        try:
            return self.config.secret(name)
        except Exception:  # noqa: BLE001
            return ""

    def _enabled(self, channel: str) -> bool:
        raw = self._setting(f"notify.{channel}.enabled", "")
        return DEFAULT_ENABLED.get(channel, False) if raw == "" else raw.strip() == "1"

    def _min_severity(self, channel: str) -> str:
        value = self._setting(f"notify.{channel}.min_severity", "low").lower()
        return value if value in SEVERITY_RANK else "low"

    def _is_windows(self) -> bool:
        return self.platform.startswith("win")

    def _ntfy_topic(self) -> str:
        return self._secret("NTFY_TOPIC") or self._setting("notify.ntfy.topic")

    def _ntfy_server(self) -> str:
        return self._setting("notify.ntfy.server", "https://ntfy.sh").rstrip("/")

    def _email_settings(self) -> dict[str, Any]:
        user = self._secret("SMTP_USER")
        try:
            port = int(self._secret("SMTP_PORT") or 465)
        except ValueError:
            port = 465
        to = [a.strip() for a in (self._secret("SMTP_TO") or user).split(",") if a.strip()]
        return {"host": self._secret("SMTP_HOST") or "smtp.gmail.com", "port": port, "user": user, "password": self._secret("SMTP_PASSWORD"),
                "from": self._secret("SMTP_FROM") or user, "to": to}

    def _configured(self, channel: str) -> tuple[bool, str]:
        if channel == "toast":
            return (True, "Windows toast") if self._is_windows() else (False, "not windows")
        if channel == "hub":
            return True, "family event bus"
        if channel == "ntfy":
            return (True, f"{self._ntfy_server()}") if self._ntfy_topic() else (False, "missing topic (KAFKA_NTFY_TOPIC or notify.ntfy.topic)")
        if channel == "telegram":
            missing = [f"KAFKA_{n}" for n in ("TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID") if not self._secret(n)]
            return (not missing), ("bot + chat id set" if not missing else "missing " + ", ".join(missing))
        if channel == "email":
            if self.email_backend() == "faustus":
                return self._faustus_configured()
            e = self._email_settings()
            missing = [n for n, v in (("KAFKA_SMTP_USER", e["user"]), ("KAFKA_SMTP_PASSWORD", e["password"]), ("KAFKA_SMTP_TO", e["to"])) if not v]
            return (not missing), (f"SMTP {e['host']}:{e['port']}" if not missing else "missing " + ", ".join(missing))
        return False, "unknown channel"

    def channels_status(self) -> dict[str, dict[str, Any]]:
        status = {}
        for channel in CHANNELS:
            configured, detail = self._configured(channel)
            status[channel] = {"configured": configured, "enabled": self._enabled(channel), "detail": detail,
                               "min_severity": self._min_severity(channel)}
        status["email"]["backend"] = self.email_backend()
        status["email"]["backend_setting"] = self._email_backend_setting()
        status["email"]["faustus_dir"] = str(self.faustus_dir() or "")
        return status

    # ------------------------------------------------------------------ e-mail through Faustus
    def _email_backend_setting(self) -> str:
        value = self._setting("notify.email.backend", "auto").lower()
        return value if value in EMAIL_BACKENDS else "auto"

    def email_backend(self) -> str:
        """``faustus`` or ``smtp``: the backend a mail would use now."""
        mode = self._email_backend_setting()
        if mode != "auto":
            return mode
        if self._secret("SMTP_USER") and self._secret("SMTP_PASSWORD"):
            return "smtp"
        return "faustus" if self.faustus_dir() else "smtp"

    def faustus_dir(self) -> Optional[Path]:
        """The Faustus folder: setting, KAFKA_FAUSTUS_DIR, FAUSTUS_DIR, then a ``faustus`` folder next to this app."""
        return self.helper.faustus_dir()

    def faustus_status(self, refresh: bool = False) -> dict[str, Any]:
        return self.helper.status(refresh)

    def _faustus_configured(self) -> tuple[bool, str]:
        if self.faustus_dir() is None:
            return False, "Faustus folder not found (set mail.faustus_dir or KAFKA_FAUSTUS_DIR)"
        st = self.faustus_status()
        if not st.get("ok"):
            return False, "Faustus: " + str(st.get("error") or "unavailable")[:200]
        name = f"{st.get('account')} " if st.get("account") else ""
        return True, f"Faustus account {name}<{st.get('from')}> → {', '.join(st.get('to') or [])}"

    # ------------------------------------------------------------------ sending
    def send(self, event: dict[str, Any], channels: list[str]) -> list[dict[str, Any]]:
        """Deliver through the hub's notification service (it decides channels, quiet hours and the sphere) when ``notify.via`` says
        so, else through the app's own channels. ``auto`` falls back to the own channels when the hub does not take it."""
        asked = [c for c in channels if c in OWN_CHANNELS]
        if not asked or self.via() == "own":
            return self._send_own(event, channels)
        rest = [c for c in channels if c not in OWN_CHANNELS]
        own: list[dict[str, Any]] = []

        def own_send(*_args: Any, **_kw: Any) -> dict[str, Any]:       # the router's fallback: every channel asked for, with its own gates
            own.extend(self._send_own(event, channels))
            return {"ok": any(r["ok"] for r in own)}

        hub = _Hub(self)
        router = Router(self.via, own_send, APP_ID, hub=hub)
        title, body = compose(event, self._lang())
        res = router.send(title, body, priority=self.hub_priority(event), url=_http_url(event.get("url")),
                          group=GROUPS.get(str(event.get("type")), ""), dedupe_key=str(event.get("id") or ""))
        tried = [self._hub_result(hub.last)] if hub.last is not None else []
        if res["via"] == "hub":
            return [*self._send_own(event, rest), *(tried or [{"channel": HUB_NOTIFY, "ok": False, "error": "hub unavailable", "ts": self.clock()}])]
        return [*own, *tried]      # the hub was not asked (auto and it is down) or refused (auto falls back): the own channels carry it

    def _send_own(self, event: dict[str, Any], channels: list[str]) -> list[dict[str, Any]]:
        results = []
        rank = SEVERITY_RANK.get(str(event.get("severity") or "medium").lower(), 1)
        for channel in channels:
            if channel not in CHANNELS:
                results.append({"channel": channel, "ok": False, "error": "unknown channel"})
                continue
            if not self._enabled(channel):
                results.append({"channel": channel, "ok": False, "error": "disabled", "skipped": True})
                continue
            if rank < SEVERITY_RANK[self._min_severity(channel)]:
                results.append({"channel": channel, "ok": False, "error": "below minimum severity", "skipped": True})
                continue
            results.append(self._deliver(channel, event))
        return results

    def test(self, channel: str) -> dict[str, Any]:
        """Send a sample notification through one channel, ignoring its enabled flag and severity floor."""
        if channel not in CHANNELS:
            return {"channel": channel, "ok": False, "error": "unknown channel"}
        words = WORDS[self._lang()]
        sample = {"id": "test", "type": "test", "severity": "medium", "title": words["test_title"], "summary": words["test_body"], "url": ""}
        return self._deliver(channel, sample)

    def _deliver(self, channel: str, event: dict[str, Any]) -> dict[str, Any]:
        configured, detail = self._configured(channel)
        if not configured:
            return {"channel": channel, "ok": False, "error": detail}
        try:
            error = getattr(self, "_send_" + channel)(event, *compose(event, self._lang()))
        except Exception as exc:  # noqa: BLE001 — a channel must never raise out of the engine
            error = f"{type(exc).__name__}"
            log.info("notify %s failed: %s", channel, type(exc).__name__)
        return {"channel": channel, "ok": not error, "error": error or "", "ts": self.clock()}

    @staticmethod
    def _error(answer: dict[str, Any]) -> str:
        return "" if answer.get("ok") else str(answer.get("error") or "failed")[:200]

    def _http(self, url: str, payload: Optional[dict[str, Any]] = None, headers: Optional[dict[str, str]] = None,
              timeout: float = HTTP_TIMEOUT_S) -> tuple[Optional[int], Any]:
        """The transport the shared senders use: ``(status, json | None)`` or ``(None, "<ExceptionName>")``. Tests inject an httpx transport."""
        try:
            with httpx.Client(transport=self.transport, timeout=timeout, follow_redirects=False) as client:
                response = client.post(url, json=payload, headers=headers) if payload is not None else client.get(url, headers=headers)
        except httpx.HTTPError as exc:
            return None, type(exc).__name__
        try:
            return response.status_code, response.json()
        except ValueError:
            return response.status_code, None

    # ------------------------------------------------------------------ channels (each returns '' on success or an error)
    def _send_toast(self, event: dict[str, Any], title: str, body: str) -> str:
        if not self._is_windows():
            return "not windows"
        url = _http_url(event.get("url"))
        if self.toast_backend is not None:
            self.toast_backend(title, body, url, str(event.get("severity")) == "high")
            return ""
        return self._error(nc.send_toast(title, body, url, app_name=APP_ID, runner=self.powershell_runner, platform=self.platform))

    def _send_hub(self, event: dict[str, Any], title: str, body: str) -> str:
        from ..hoard_link import family

        payload = {"event_id": event.get("id"), "type": event.get("type"), "severity": event.get("severity"), "title": title,
                   "summary": event.get("summary") or body, "url": event.get("url"), **(event.get("data") or {})}
        try:
            accepted = family.emit(str(event.get("bus") or "kafka.update"), payload)
        except Exception:  # noqa: BLE001 — emit is documented not to raise; be safe anyway
            accepted = False
        return "" if accepted else "hub not configured"

    def _send_ntfy(self, event: dict[str, Any], title: str, body: str) -> str:
        priority = {"high": "high", "medium": "normal"}.get(str(event.get("severity") or "medium"), "low")
        return self._error(nc.send_ntfy(self._ntfy_server(), self._ntfy_topic(), title, body, priority=priority, url=_http_url(event.get("url")),
                                        token=self._secret("NTFY_TOKEN") or None, tags=[TYPE_TAGS.get(str(event.get("type")), "bell")], http=self._http))

    def _send_telegram(self, event: dict[str, Any], title: str, body: str) -> str:
        text = nc.telegram_text(title, body, event.get("url"), link_label=WORDS[self._lang()]["open"])
        return self._error(nc.send_telegram(self._secret("TELEGRAM_TOKEN"), self._secret("TELEGRAM_CHAT_ID"), text, http=self._http))

    def telegram_discover_chat_id(self) -> dict[str, Any]:
        """Find the chat id after the user has written to the bot (getUpdates). Nothing is stored here."""
        return nc.telegram_discover_chat_id(self._secret("TELEGRAM_TOKEN"), http=self._http)

    def _send_email(self, event: dict[str, Any], title: str, body: str) -> str:
        subject, text, html_body = nc.email_parts(title, body, event.get("url"))
        if self.email_backend() == "faustus":
            to = [a.strip() for a in self._secret("SMTP_TO").split(",") if a.strip()]
            return self._error(nc.send_via_helper(self.helper.run, subject, text, html=html_body, to=to or None))
        e = self._email_settings()
        factory = (lambda cfg: self.smtp_factory(cfg["host"], cfg["port"], cfg["port"] == 465)) if self.smtp_factory else None
        return self._error(nc.send_smtp({"host": e["host"], "port": e["port"], "user": e["user"], "password": e["password"], "from": e["from"]},
                                        e["to"], subject, text, html=html_body, smtp_factory=factory))


__all__ = ["Notifier", "CHANNELS", "OWN_CHANNELS", "VIA_MODES", "HUB_NOTIFY", "EMAIL_BACKENDS", "telegram_discover_chat_id", "build_toast_ps1", "compose", "label",
           "LABELS", "format_price", "xml_escape"]
