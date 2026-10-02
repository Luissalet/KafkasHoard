"""Paperwork mail for Kafka: the family's mail router with Kafka's words and filing rules.

The family reads the inbox once. ``fam_mail.MailRouter`` takes the mail from the hub's gateway when it is on and from the Faustus
helper (run with Faustus's own Python, so the password never leaves Faustus) when it is not, chosen by the ``mail.source`` setting:
``auto``, ``hub`` or ``faustus``. This module says which mail Kafka wants (the paperwork words below and any mail with an attachment),
keeps the hub's watermark in ``mail.hub_since_id`` and hands each message to the engine in one shape, whichever source it came from.
"""

from __future__ import annotations

import os
import time
from typing import Any, Callable, Optional

from ..hoard_link import fam_mail

SOURCES = fam_mail.SOURCES

# Words that paperwork mail carries in its subject (ES, EN). Matched by the helper's IMAP SUBJECT search and registered at the hub.
SUBJECT_TERMS = [
    "factura", "recibo", "póliza", "poliza", "contrato", "garantía", "garantia", "notificación", "notificacion", "requerimiento",
    "liquidación", "liquidacion", "multa", "sanción", "sancion", "renovación", "renovacion", "vencimiento", "aviso de cargo", "extracto",
    "invoice", "receipt", "policy", "renewal", "statement", "warranty",
]
GMAIL_QUERY = ("(factura OR recibo OR póliza OR contrato OR garantía OR notificación OR requerimiento OR liquidación OR multa OR sanción "
               "OR renovación OR vencimiento OR \"aviso de cargo\" OR invoice OR receipt OR policy OR renewal OR statement "
               "OR (has:attachment filename:pdf))")
INTEREST = {"subject_terms": list(SUBJECT_TERMS), "has_attachment": True}
MAX_ATTACHMENT_BYTES = 15 * 1024 * 1024
MAX_ATTACHMENTS = 10
ATTACHMENT_EXT = {"pdf", "png", "jpg", "jpeg", "webp", "heic", "tif", "tiff"}


class MailSource:
    """What the engine scans: ``scan`` / ``commit`` / ``status`` / ``source_now`` over a ``fam_mail.MailRouter``."""

    def __init__(self, setting: Callable[[str, str], str], secret: Callable[[str], str], *, runner: Optional[Callable[..., Any]] = None,
                 clock: Callable[[], float] = time.time, hub_api: Any = None, offline: bool = False,
                 save: Optional[Callable[[str, str], Any]] = None):
        self.setting = setting
        self.offline = offline
        self.helper = fam_mail.FaustusHelper(lambda: setting("mail.faustus_dir", "") or secret("FAUSTUS_DIR"),
                                             owner=lambda: setting("mail.faustus_owner", ""), env_drop_prefixes=("KAFKA_",), runner=runner, clock=clock)
        self.router = fam_mail.MailRouter(self.helper, source_getter=lambda: "faustus" if offline else setting("mail.source", "auto"),
                                          interest=INTEREST, claim_kind="document", hub=hub_api, clock=clock,
                                          watermark_get=lambda: int(float(setting("mail.hub_since_id", "0") or 0)),
                                          watermark_set=(lambda v: save("mail.hub_since_id", str(int(v)))) if save else None)

    # ------------------------------------------------------------------ which source
    def mode(self) -> str:
        return self.router.mode()

    def source_now(self) -> str:
        """``hub`` or ``faustus``: where a scan would read now."""
        return self.router.source_now()

    def faustus_dir(self):
        return self.helper.faustus_dir()

    def forget_interest(self) -> None:
        self.router.forget_interest()

    def register_interest(self, force: bool = False) -> dict[str, Any]:
        if self.source_now() != "hub":
            return {"ok": False, "skipped": True, "reason": "not reading from the hub"}
        return self.router.ensure_interest(force=force)

    def status(self, refresh: bool = False) -> dict[str, Any]:
        mode, now = self.mode(), self.source_now()
        if now == "hub":
            up = self.router.hub_up()
            base: dict[str, Any] = {"ok": up, "accounts": [{"account": "hub gateway"}] if up else []}
            if not up:
                base["error"] = "the hub's mail gateway is not on (or the hub is not running)"
        else:
            base = self.helper.status(refresh)
        return {**base, "source": now, "mode": mode}

    # ------------------------------------------------------------------ scanning
    def scan(self, *, since_days: int, limit: int, skip: list[str], query: str = "", attachments_dir: str = "") -> dict[str, Any]:
        """Paperwork mail of the last ``since_days`` days. A search, or looking back further than the regular window, re-reads from the
        start of what the hub holds and leaves its watermark alone. PDF and image attachments are saved in ``attachments_dir``."""
        window = int(float(self.setting("mail.window_days", "14") or 14))
        answer = self.router.scan_ex(since_days=since_days, limit=limit, skip=list(skip), query=query, deep=bool(query) or since_days > window,
                                     subject_terms=SUBJECT_TERMS, gmail_query=GMAIL_QUERY, attachments_dir=attachments_dir)
        if not answer["ok"]:
            return {"ok": False, "error": str(answer.get("error") or "mail failed"), "accounts": [], "source": answer.get("source", "")}
        messages = answer["messages"]
        if answer["source"] == "hub":
            messages = [self._shape(m, attachments_dir) for m in messages]
        return {"ok": True, "source": answer["source"], "accounts": answer.get("accounts") or [], "messages": messages, "more": bool(answer.get("more"))}

    def commit(self) -> None:
        """Move the hub watermark to where the last scan read: call it once the messages are filed."""
        self.router.commit()

    def _shape(self, m: dict[str, Any], dest_dir: str) -> dict[str, Any]:
        return {"message_id": m.get("message_id"), "subject": m.get("subject") or "", "from_name": m.get("from_name") or "",
                "from_address": m.get("from_address") or "", "text": m.get("text") or "", "ts": m.get("ts"),
                "account": m.get("account") or m.get("source") or "", "from_self": bool(m.get("from_self")),
                "hub_id": str(m.get("hub_id") or m.get("id") or ""), "attachments": self._copy_attachments(m, dest_dir)}

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
                path = self.router.hub.copy_attachment(att, dest_dir)
            except Exception:  # noqa: BLE001 — an attachment that cannot be had is skipped, the mail is still filed
                path = ""
            if path:
                out.append({"name": name, "mime": att.get("mime") or "", "size": att.get("size") or 0, "sha": att.get("sha") or "", "path": path})
        return out
