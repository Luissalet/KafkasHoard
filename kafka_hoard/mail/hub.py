"""Paperwork mail through the family hub's mail gateway, shaped like the answer of the Faustus helper.

The hub reads the inbox once for the whole family and hands each app the mail it asked for. Kafka registers what it
searches today (the paperwork words of ``faustus_mail.SUBJECT_TERMS`` and any mail with an attachment), reads the messages
after a stored watermark and feeds them to the same classification and filing code as the helper's output. ``MailRouter``
chooses between the two sources by the ``mail.source`` setting: ``auto`` (the hub when its gateway is on, else the helper),
``hub`` or ``faustus``.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Optional

from .faustus_mail import ATTACHMENT_EXT, MAX_ATTACHMENT_BYTES, MAX_ATTACHMENTS, SUBJECT_TERMS
from .source import FaustusMail

log = logging.getLogger("kafka.mail")

SOURCES = ("auto", "hub", "faustus")
INTEREST = {"subject_terms": list(SUBJECT_TERMS), "has_attachment": True}
PAGE = 100
REGISTER_EVERY_S = 6 * 3600.0


class HubMail:
    """Reads Kafka's mail from the hub (``fam_mail``). ``api`` is the family library's ``fam_mail`` module; tests inject a fake."""

    def __init__(self, setting: Callable[[str, str], str], api: Any = None, clock: Callable[[], float] = time.time):
        self.setting = setting
        self._api = api
        self.clock = clock
        self._registered_at = 0.0

    @property
    def api(self) -> Any:
        if self._api is None:
            from ..hoard_link import fam_mail
            self._api = fam_mail
        return self._api

    def available(self) -> bool:
        try:
            return bool(self.api.available())
        except Exception:  # noqa: BLE001
            return False

    def forget_interest(self) -> None:
        self._registered_at = 0.0

    def register_interest(self, force: bool = False) -> dict[str, Any]:
        """Tell the hub which mail Kafka wants (again after a few hours or when the mail settings change)."""
        if not force and self.clock() - self._registered_at < REGISTER_EVERY_S:
            return {"ok": True, "cached": True}
        try:
            res = self.api.register_interest(dict(INTEREST))
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": type(exc).__name__}
        if res.get("ok"):
            self._registered_at = self.clock()
        return res

    def status(self) -> dict[str, Any]:
        up = self.available()
        out: dict[str, Any] = {"ok": up, "source": "hub", "accounts": [{"account": "hub gateway"}] if up else []}
        if not up:
            out["error"] = "the hub's mail gateway is not on (or the hub is not running)"
        return out

    def _filter(self, m: dict[str, Any], *, since_ts: float, query: str) -> bool:
        ts = m.get("ts")
        if since_ts and ts and float(ts) < since_ts:
            return False
        if query:
            hay = " ".join(str(m.get(k) or "") for k in ("subject", "text", "from_address", "from_name")).lower()
            return all(word in hay for word in query.lower().split())
        return True

    def _copy_attachments(self, m: dict[str, Any], dest_dir: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not dest_dir:
            return out
        for att in (m.get("attachments") or [])[:MAX_ATTACHMENTS]:
            name = str(att.get("name") or "attachment")
            ext = os.path.splitext(name)[1].lstrip(".").lower()
            if ext not in ATTACHMENT_EXT or int(att.get("size") or 0) > MAX_ATTACHMENT_BYTES:
                continue
            try:
                path = self.api.copy_attachment(att, dest_dir)
            except Exception:  # noqa: BLE001
                path = ""
            if path:
                out.append({"name": name, "mime": att.get("mime") or "", "size": att.get("size") or 0, "sha": att.get("sha") or "", "path": path})
        return out

    def scan(self, *, since_days: int, limit: int, skip: list[str], query: str = "", attachments_dir: str = "") -> dict[str, Any]:
        """Messages after the stored watermark (``mail.hub_since_id``); ``hub_last_id`` is what the engine stores once they are filed.
        A search with a query, or looking back further than the regular window, re-reads from the start of what the hub holds."""
        window = int(float(self.setting("mail.window_days", "14") or 14))
        deep = bool(query) or since_days > window
        watermark = 0 if deep else int(float(self.setting("mail.hub_since_id", "0") or 0))
        since_ts = self.clock() - since_days * 86400.0 if deep or watermark == 0 else 0.0
        known, collected, last_id, got_any = set(skip), [], watermark, False
        while len(collected) < limit:
            page = self.api.messages(since_id=last_id, limit=PAGE, full=True)
            if not page.get("ok"):
                return {"ok": False, "error": str(page.get("error") or "the hub did not answer")[:200]}
            rows = page.get("messages") or []
            if not rows:
                last_id = max(last_id, int(page.get("last_id") or last_id))
                break
            got_any = True
            for m in rows:
                last_id = max(last_id, int(m.get("id") or 0))
                if str(m.get("message_id") or "") in known or not self._filter(m, since_ts=since_ts, query=query):
                    continue
                collected.append(m)
                if len(collected) >= limit:
                    break
            if len(rows) < PAGE:
                break
        messages = []
        for m in collected:
            messages.append({"message_id": m.get("message_id"), "subject": m.get("subject") or "", "from_name": m.get("from_name") or "",
                             "from_address": m.get("from_address") or "", "text": m.get("text") or "", "ts": m.get("ts"),
                             "account": m.get("account") or m.get("source") or "", "from_self": bool(m.get("from_self")),
                             "hub_id": str(m.get("id") or ""), "attachments": self._copy_attachments(m, attachments_dir)})
        return {"ok": True, "accounts": [{"account": "hub gateway", "folder": "all", "matches": len(messages)}], "messages": messages,
                "hub_last_id": None if deep else (last_id if (got_any or last_id != watermark) else None), "source": "hub"}


class MailRouter:
    """The mail source of the engine: the hub's gateway or the Faustus helper, by ``mail.source``."""

    def __init__(self, setting: Callable[[str, str], str], secret: Callable[[str], str], *, runner: Optional[Callable[..., Any]] = None,
                 clock: Callable[[], float] = time.time, hub_api: Any = None, offline: bool = False):
        self.setting = setting
        self.own = FaustusMail(setting, secret, runner=runner, clock=clock)
        self.hub = HubMail(setting, hub_api, clock)
        self.offline = offline

    def mode(self) -> str:
        value = (self.setting("mail.source", "auto") or "auto").lower()
        return value if value in SOURCES else "auto"

    def source_now(self) -> str:
        """``hub`` or ``faustus``: where a scan would read now."""
        mode = self.mode()
        if self.offline or mode == "faustus":
            return "faustus"
        if mode == "hub":
            return "hub"
        return "hub" if self.hub.available() else "faustus"

    # the engine and the status view use the helper's interface
    def faustus_dir(self):
        return self.own.faustus_dir()

    def forget_interest(self) -> None:
        self.hub.forget_interest()

    def register_interest(self, force: bool = False) -> dict[str, Any]:
        if self.source_now() != "hub":
            return {"ok": False, "skipped": True, "reason": "not reading from the hub"}
        return self.hub.register_interest(force)

    def status(self, refresh: bool = False) -> dict[str, Any]:
        mode, now = self.mode(), self.source_now()
        base = self.hub.status() if now == "hub" else self.own.status(refresh)
        return {**base, "source": now, "mode": mode}

    def scan(self, *, since_days: int, limit: int, skip: list[str], query: str = "", attachments_dir: str = "") -> dict[str, Any]:
        if self.source_now() == "hub":
            self.hub.register_interest()
            answer = self.hub.scan(since_days=since_days, limit=limit, skip=skip, query=query, attachments_dir=attachments_dir)
            if answer.get("ok") or self.mode() == "hub":
                return answer
            log.info("hub mail failed (%s); using the Faustus helper", answer.get("error"))
        return self.own.scan(since_days=since_days, limit=limit, skip=skip, query=query, attachments_dir=attachments_dir)
