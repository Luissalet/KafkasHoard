"""Read paperwork e-mails through the account configured in Faustus.

Kafka runs this file with Faustus's own Python, inside the Faustus folder, so the mail password never leaves
Faustus: Kafka only receives the messages that look like paperwork (and their PDF / image attachments). It reads one JSON request on stdin:

    {"action": "status"}                                       -> which account would be read, nothing is fetched
    {"action": "scan", "since_days": 30, "max": 60,
     "skip": ["<message-id>", ...], "query": "optional",
     "gmail_query": "...", "subject_terms": ["factura", ...],   -> what a mail must match (defaults below)
     "attachments_dir": "/abs/folder"}                          -> candidate messages, newest first; every PDF / image
                                                                   attachment (<= 15 MB) is written there as <sha256>.<ext>
    {"action": "read", "message_id": "<...>"}                  -> one message (any folder searched)
    {"action": "send", "subject": "...", "text": "...", "html": "...", "to": [...]}  -> a notification mail

and prints one JSON line. ``owner`` (optional) picks the Faustus user whose accounts are used; without it the only
owner that has accounts is used. Every account of that owner is scanned (``account`` narrows it to one).
The file imports nothing from Kafka: it must run under another interpreter, so it is stdlib-only.
"""

from __future__ import annotations

import contextlib
import email
import email.header
import email.utils
import html as _html
import io
import json
import os
import re
import smtplib
import ssl
import sys
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid
from datetime import datetime, timedelta, timezone

MAX_TEXT = 24_000
MAX_LINKS = 60

# Words that paperwork mail carries in its subject (ES, EN). Matched by IMAP SUBJECT search when the request gives no terms.
SUBJECT_TERMS = [
    "factura", "recibo", "póliza", "poliza", "contrato", "garantía", "garantia", "notificación", "notificacion", "requerimiento",
    "liquidación", "liquidacion", "multa", "sanción", "sancion", "renovación", "renovacion", "vencimiento", "aviso de cargo", "extracto",
    "invoice", "receipt", "policy", "renewal", "statement", "warranty",
]
GMAIL_QUERY = ("(factura OR recibo OR póliza OR contrato OR garantía OR notificación OR requerimiento OR liquidación OR multa OR sanción "
               "OR renovación OR vencimiento OR \"aviso de cargo\" OR invoice OR receipt OR policy OR renewal OR statement "
               "OR (has:attachment filename:pdf))")
MAX_ATTACHMENT_BYTES = 15 * 1024 * 1024
MAX_ATTACHMENTS = 10
ATTACHMENT_EXT = {"pdf", "png", "jpg", "jpeg", "webp", "heic", "tif", "tiff"}


def _mask(address: str) -> str:
    address = str(address or "")
    if "@" not in address:
        return "***" if address else ""
    local, _, domain = address.partition("@")
    return (local[:3] + "***@" + domain) if local else "***@" + domain


def _load_server(root: str):
    sys.path.insert(0, root)
    os.chdir(root)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from mcp_servers import email_server  # type: ignore
    return email_server


def _pick_owner(server, requested: str) -> str:
    if requested:
        return requested
    for key in ("ODYSSEUS_MCP_EMAIL_OWNER", "ODYSSEUS_EMAIL_OWNER"):
        if os.environ.get(key, "").strip():
            return os.environ[key].strip()
    rows = [r for r in server._read_accounts_from_db() if r.get("enabled", 1)]
    owners = sorted({str(r.get("owner") or "").strip() for r in rows} - {""})
    return owners[0] if len(owners) == 1 else ""


def _accounts(server, wanted: str | None) -> list[dict]:
    rows = [r for r in server._read_accounts_from_db() if r.get("enabled", 1)]
    try:
        rows = server._filter_accounts_for_owner(rows)
    except Exception:  # noqa: BLE001 — older Faustus builds
        pass
    if wanted:
        rows = [r for r in rows if wanted in (str(r.get("id")), str(r.get("account_name") or ""), str(r.get("imap_user") or ""))]
    return rows


def _selector(row: dict) -> str | None:
    for key in ("account_name", "imap_user", "id"):
        if row.get(key):
            return str(row[key])
    return None


# ------------------------------------------------------------------ message → text
class _Text:
    BLOCK = re.compile(r"<\s*(br|/p|/div|/tr|/li|/h\d|/table|p|div|tr|li|h\d)\b[^>]*>", re.I)
    DROP = re.compile(r"<\s*(style|script|head|title)\b.*?<\s*/\s*\1\s*>", re.I | re.S)
    HREF = re.compile(r"""<a\b[^>]*?href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a\s*>""", re.I | re.S)
    TAG = re.compile(r"<[^>]+>")

    @classmethod
    def from_html(cls, raw: str) -> tuple[str, list[dict]]:
        raw = cls.DROP.sub(" ", raw)
        raw = re.sub(r"<!--.*?-->", " ", raw, flags=re.S)
        links = []
        for href, label in cls.HREF.findall(raw):
            label = re.sub(r"\s+", " ", _html.unescape(cls.TAG.sub(" ", label))).strip()
            href = _html.unescape(href).strip()
            if href.lower().startswith(("http://", "https://")):
                links.append({"url": href[:1500], "label": label[:160]})
        text = cls.BLOCK.sub("\n", raw)
        text = _html.unescape(cls.TAG.sub(" ", text))
        return cls.tidy(text), links

    @staticmethod
    def tidy(text: str) -> str:
        text = text.replace("\r", "").replace(" ", " ").replace("​", "")
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
        out, blank = [], 0
        for line in lines:
            if not line:
                blank += 1
                if blank == 1 and out:
                    out.append("")
                continue
            blank = 0
            out.append(line)
        return "\n".join(out).strip()


def _decode(part) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        return ""
    for charset in (part.get_content_charset(), "utf-8", "latin-1"):
        if not charset:
            continue
        try:
            return payload.decode(charset, errors="replace" if charset == "latin-1" else "strict")
        except (LookupError, UnicodeDecodeError):
            continue
    return payload.decode("utf-8", errors="replace")


def _save_attachments(msg, directory: str, server=None) -> list[dict]:
    """Write every PDF / image attachment into ``directory`` as <sha256>.<ext> and describe it."""
    import hashlib
    out: list[dict] = []
    if not directory:
        return out
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError:
        return out
    for part in msg.walk():
        if part.is_multipart() or len(out) >= MAX_ATTACHMENTS:
            continue
        ctype = part.get_content_type()
        raw_name = part.get_filename() or ""
        try:
            name = str(server._decode_header(raw_name)) if (server is not None and raw_name) else str(email.header.make_header(email.header.decode_header(raw_name)))
        except Exception:  # noqa: BLE001
            name = raw_name
        name = os.path.basename(name.replace("\\", "/"))[:160]
        ext = os.path.splitext(name)[1].lower().lstrip(".")
        if ctype == "application/pdf":
            ext = "pdf"
        elif ctype.startswith("image/"):
            if not name and "attachment" not in str(part.get("Content-Disposition", "")).lower():
                continue                                    # inline pictures without a name are logos
            ext = ext or ctype.split("/", 1)[1].replace("jpeg", "jpg")
        elif ctype in ("application/octet-stream", "application/force-download") and ext in ATTACHMENT_EXT:
            pass
        else:
            continue
        if ext not in ATTACHMENT_EXT:
            continue
        try:
            payload = part.get_payload(decode=True) or b""
        except Exception:  # noqa: BLE001
            continue
        if not payload or len(payload) > MAX_ATTACHMENT_BYTES:
            continue
        sha = hashlib.sha256(payload).hexdigest()
        path = os.path.join(directory, f"{sha}.{ext}")
        try:
            if not os.path.exists(path):
                with open(path, "wb") as handle:
                    handle.write(payload)
        except OSError:
            continue
        out.append({"name": name or f"attachment.{ext}", "mime": ctype, "size": len(payload), "sha": sha, "path": path})
    return out


def message_to_record(msg, server=None, attachments_dir: str = "") -> dict:
    """Subject, sender, date, plain text (HTML converted), links and (when asked) the saved attachments of one parsed message."""
    def header(name: str) -> str:
        value = msg.get(name, "") or ""
        if server is not None:
            try:
                return str(server._decode_header(value))
            except Exception:  # noqa: BLE001
                pass
        try:
            return str(email.header.make_header(email.header.decode_header(value)))
        except Exception:  # noqa: BLE001
            return str(value)

    plain, html_text, links = "", "", []
    for part in (msg.walk() if msg.is_multipart() else [msg]):
        if part.is_multipart() or "attachment" in str(part.get("Content-Disposition", "")).lower():
            continue
        ctype = part.get_content_type()
        if ctype == "text/plain" and not plain:
            plain = _Text.tidy(_decode(part))
        elif ctype == "text/html" and not html_text:
            html_text, links = _Text.from_html(_decode(part))
    text = html_text if len(html_text) > len(plain) * 0.6 or not plain else plain
    if plain and html_text and plain not in text:
        text = text  # keep the richer one; links already come from the HTML part
    for url in re.findall(r"https?://[^\s<>\"')\]]+", plain):
        links.append({"url": url[:1500], "label": ""})
    seen, unique = set(), []
    for link in links:
        if link["url"] in seen:
            continue
        seen.add(link["url"])
        unique.append(link)
    sender = header("From")
    name, address = email.utils.parseaddr(sender)
    date_raw = msg.get("Date", "")
    try:
        when = email.utils.parsedate_to_datetime(date_raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        ts = when.timestamp()
    except (TypeError, ValueError, IndexError):
        ts = None
    return {"message_id": (msg.get("Message-ID", "") or "").strip()[:300], "subject": header("Subject")[:300],
            "from_name": name[:120], "from_address": address[:200], "date": date_raw[:80], "ts": ts,
            "text": text[:MAX_TEXT], "links": unique[:MAX_LINKS], "attachments": _save_attachments(msg, attachments_dir, server)}


# ------------------------------------------------------------------ IMAP search
def _uids(conn, criteria: list[str]) -> list[bytes]:
    try:
        status, data = conn.uid("SEARCH", None, *criteria)
    except Exception:  # noqa: BLE001
        return []
    if status != "OK" or not data or not data[0]:
        return []
    return data[0].split()


def _imap_since(days: int) -> str:
    when = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))
    return when.strftime("%d-%b-%Y")


def _search(conn, host: str, since_days: int, query: str, gmail_query: str = "", subject_terms=None) -> list[bytes]:
    since = _imap_since(since_days)
    if "gmail" in host or "googlemail" in host:
        raw = query or gmail_query or GMAIL_QUERY
        raw = f"{raw} newer_than:{max(1, int(since_days))}d"
        found = _uids(conn, ["X-GM-RAW", '"' + raw.replace("\\", "").replace('"', '\\"') + '"'])
        if found:
            return found
    if query:
        q = query.replace("\\", "").replace('"', "")
        return _uids(conn, ["SINCE", since, "TEXT", f'"{q}"'])
    seen: dict[bytes, None] = {}
    for term in (subject_terms or SUBJECT_TERMS):
        for uid in _uids(conn, ["SINCE", since, "SUBJECT", f'"{term}"']):
            seen[uid] = None
    return list(seen)


def _folders_for(conn, host: str) -> list[str]:
    if "gmail" in host or "googlemail" in host:
        return ["[Gmail]/All Mail", "[Gmail]/Todos", "INBOX"]
    return ["INBOX"]


def _select_first(conn, server, folders: list[str]) -> str:
    for folder in folders:
        try:
            status, _ = conn.select(server._q(folder) if hasattr(server, "_q") else folder, readonly=True)
        except Exception:  # noqa: BLE001
            continue
        if status == "OK":
            return folder
    # localized Gmail "All Mail": look it up by its \All flag
    try:
        status, lines = conn.list()
        for line in lines or []:
            text = line.decode("utf-8", "replace") if isinstance(line, bytes) else str(line)
            if "\\All" in text:
                name = text.rsplit(' "', 1)[-1].rstrip('"') if '"' in text else text.split()[-1]
                if conn.select(f'"{name}"', readonly=True)[0] == "OK":
                    return name
    except Exception:  # noqa: BLE001
        pass
    conn.select("INBOX", readonly=True)
    return "INBOX"


def scan(server, request: dict) -> dict:
    since_days = max(1, min(int(request.get("since_days") or 30), 365))
    limit = max(1, min(int(request.get("max") or 60), 1000))
    skip = set(str(s) for s in (request.get("skip") or []))
    query = str(request.get("query") or "").strip()[:200]
    gmail_query = str(request.get("gmail_query") or "").strip()[:1500]
    terms = [re.sub(r'["\\]', "", str(t)).strip()[:60] for t in (request.get("subject_terms") or []) if str(t).strip()][:60]
    attachments_dir = str(request.get("attachments_dir") or "").strip()
    out, errors, scanned_accounts = [], [], []
    for row in _accounts(server, request.get("account")):
        selector = _selector(row)
        host = str(row.get("imap_host") or "").lower()
        conn = None
        try:
            conn = server._imap_connect(selector)
            folder = _select_first(conn, server, _folders_for(conn, host))
            uids = _uids_newest(_search(conn, host, since_days, query, gmail_query, terms))
            scanned_accounts.append({"account": row.get("account_name") or _mask(row.get("imap_user") or ""),
                                     "folder": folder, "matches": len(uids)})
            for uid in uids:
                if len(out) >= limit:
                    break
                status, head = conn.uid("FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
                mid = ""
                if status == "OK" and head and isinstance(head[0], tuple):
                    mid = (email.message_from_bytes(head[0][1]).get("Message-ID", "") or "").strip()
                if mid and mid in skip:
                    continue
                status, data = conn.uid("FETCH", uid, "(BODY.PEEK[])")
                if status != "OK" or not data or not isinstance(data[0], tuple):
                    continue
                record = message_to_record(email.message_from_bytes(data[0][1]), server, attachments_dir)
                record["account"] = row.get("account_name") or _mask(row.get("imap_user") or "")
                own = {str(row.get(k) or "").lower() for k in ("imap_user", "from_address", "smtp_user")} - {""}
                record["from_self"] = record["from_address"].lower() in own
                out.append(record)
        except Exception as exc:  # noqa: BLE001 — one bad account must not hide the others
            errors.append(f"{row.get('account_name') or _mask(row.get('imap_user') or '')}: {type(exc).__name__}: {str(exc)[:160]}")
        finally:
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.logout()
    out.sort(key=lambda r: r.get("ts") or 0, reverse=True)
    return {"ok": not errors or bool(out) or bool(scanned_accounts), "error": "; ".join(errors), "accounts": scanned_accounts,
            "messages": out}


def _uids_newest(uids: list[bytes]) -> list[bytes]:
    try:
        return sorted(set(uids), key=lambda u: int(u), reverse=True)
    except ValueError:
        return list(reversed(uids))


def read_one(server, request: dict) -> dict:
    mid = str(request.get("message_id") or "").strip()
    if not mid:
        return {"ok": False, "error": "message_id required"}
    for row in _accounts(server, request.get("account")):
        conn = None
        try:
            conn = server._imap_connect(_selector(row))
            _select_first(conn, server, _folders_for(conn, str(row.get("imap_host") or "").lower()))
            uids = _uids(conn, ["HEADER", "Message-ID", f'"{mid}"'])
            if not uids:
                continue
            status, data = conn.uid("FETCH", uids[-1], "(BODY.PEEK[])")
            if status == "OK" and data and isinstance(data[0], tuple):
                record = message_to_record(email.message_from_bytes(data[0][1]), server)
                record["account"] = row.get("account_name") or _mask(row.get("imap_user") or "")
                return {"ok": True, "error": "", "message": record}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:160]}"}
        finally:
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.logout()
    return {"ok": False, "error": "message not found"}


SEND_TIMEOUT_S = 25


def _connect(cfg: dict):
    host, port = cfg["smtp_host"], int(cfg.get("smtp_port") or 465)
    security = str(cfg.get("smtp_security") or "").strip().lower()
    # A stored mode that contradicts the well-known port (implicit TLS on 587, STARTTLS on 465) cannot work: trust the port.
    if port == 587 and security == "ssl":
        security = "starttls"
    elif port == 465 and security == "starttls":
        security = "ssl"
    if security not in ("ssl", "starttls", "none"):
        security = "starttls" if port == 587 else "ssl"
    context = ssl.create_default_context()
    if security == "ssl":
        client = smtplib.SMTP_SSL(host, port, timeout=SEND_TIMEOUT_S, context=context)
    else:
        client = smtplib.SMTP(host, port, timeout=SEND_TIMEOUT_S)
        if security == "starttls":
            client.starttls(context=context)
    client.login(cfg["smtp_user"], cfg["smtp_password"])
    return client


def _addresses(value) -> list[str]:
    if isinstance(value, str):
        value = value.split(",")
    out = []
    for item in value or []:
        item = re.sub(r"[\r\n]+", "", str(item)).strip()
        if item and re.fullmatch(r"[^@\s,;<>]+@[^@\s,;<>]+\.[^@\s,;<>]+", item):
            out.append(item)
    return out[:10]


def send_info(server, request: dict) -> dict:
    _selector, cfg = server._resolve_send_config(request.get("account") or None)
    sender = str(cfg.get("from_address") or cfg.get("smtp_user") or "")
    to = _addresses(request.get("to")) or _addresses(sender)
    return {"cfg": cfg, "sender": sender, "to": to,
            "info": {"account": cfg.get("account_name") or "", "from": _mask(sender), "to": [_mask(a) for a in to],
                     "server": f"{cfg.get('smtp_host')}:{cfg.get('smtp_port')}"}}


def send(server, request: dict) -> dict:
    try:
        meta = send_info(server, request)
    except Exception as exc:  # noqa: BLE001 — Faustus's messages carry no secrets
        return {"ok": False, "error": str(exc)[:200] or type(exc).__name__}
    cfg, sender, to, info = meta["cfg"], meta["sender"], meta["to"], meta["info"]
    if not to:
        return {"ok": False, "error": "no recipient", **info}
    msg = EmailMessage()
    msg["Subject"] = re.sub(r"[\r\n]+", " ", str(request.get("subject") or "Kafka's Hoard"))[:200]
    msg["From"] = formataddr((str(request.get("from_name") or "Kafka's Hoard"), sender))
    msg["To"] = ", ".join(to)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=sender.partition("@")[2] or None)
    msg.set_content(str(request.get("text") or request.get("subject") or ""))
    if request.get("html"):
        msg.add_alternative(str(request["html"]), subtype="html")
    client = None
    try:
        client = _connect(cfg)
        client.send_message(msg)
    except smtplib.SMTPAuthenticationError:
        return {"ok": False, "error": "authentication failed", **info}
    except (smtplib.SMTPException, OSError) as exc:
        return {"ok": False, "error": type(exc).__name__, **info}
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.quit()
    return {"ok": True, "error": "", **info}


def handle(request: dict, root: str) -> dict:
    try:
        server = _load_server(root)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"Faustus mail module not loadable ({type(exc).__name__})"}
    owner = _pick_owner(server, str(request.get("owner") or "").strip())
    if owner:
        os.environ["ODYSSEUS_MCP_EMAIL_OWNER"] = owner
    try:
        rows = _accounts(server, request.get("account"))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"accounts not readable ({type(exc).__name__})"}
    if not rows:
        return {"ok": False, "error": "Faustus has no enabled mail account" + (f" for {owner}" if owner else "")}
    info = {"accounts": [{"account": r.get("account_name") or "", "user": _mask(r.get("imap_user") or ""),
                          "server": f"{r.get('imap_host')}:{r.get('imap_port')}"} for r in rows]}
    try:
        info.update(send_info(server, request)["info"])
    except Exception:  # noqa: BLE001 — reading may work even when sending is not configured
        pass
    action = request.get("action")
    if action == "send":
        return send(server, request)
    if action == "scan":
        return scan(server, request)
    if action == "read":
        return read_one(server, request)
    return {"ok": True, "error": "", **info}


def main() -> int:
    try:
        request = json.loads(sys.stdin.read() or "{}")
    except ValueError:
        request = {}
    root = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.getcwd())
    real_stdout = sys.stdout
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        answer = handle(request if isinstance(request, dict) else {}, root)
    real_stdout.write(json.dumps(answer, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
