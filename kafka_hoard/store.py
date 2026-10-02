"""Typed access to the database: documents and their pages (with full-text index), deadlines, series, mails, watched files,
notifications and runs."""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime
from typing import Any, Callable, Iterable, Optional

from .db import Database
from .errors import KafkaError
from .util import fold, new_id

HIT_OPEN, HIT_CLOSE = "⟦", "⟧"

DOC_JSON = ("tags", "facts")
DOC_FIELDS = (
    "title", "kind", "issuer", "issuer_key", "ref", "amount", "currency", "issue_date", "period_from", "period_to", "item", "file_sha",
    "file_ext", "file_name", "mime", "size", "pages", "ocr", "source", "source_ref", "mail_subject", "mail_from", "received_ts", "state",
    "confidence", "series_id", "tags", "notes", "facts")
DL_JSON = ("remind", "notified")
DL_FIELDS = ("doc_id", "kind", "title", "date", "basis", "evidence", "page", "confidence", "state", "remind", "notified", "amount",
             "recurring", "key", "auto", "edited", "archived", "notes", "done_ts", "source", "external_key", "ext_date", "rule", "url", "source_ref")
MAIL_JSON = ("doc_ids", "attachments", "reasons")


def _loads(value: Any, default: Any) -> Any:
    if not isinstance(value, str):
        return value if value is not None else default
    try:
        return json.loads(value or json.dumps(default))
    except ValueError:
        return default


def _doc(row: Any) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    data = dict(row)
    data["tags"] = _loads(data.get("tags"), [])
    data["facts"] = _loads(data.get("facts"), {})
    data["ocr"] = bool(data.get("ocr"))
    return data


def _dl(row: Any) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    data = dict(row)
    data["remind"] = _loads(data.get("remind"), [])
    data["notified"] = _loads(data.get("notified"), [])
    for key in ("auto", "edited", "archived"):
        data[key] = bool(data.get(key))
    return data


def _mail(row: Any) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    data = dict(row)
    for key, default in (("doc_ids", []), ("attachments", []), ("reasons", [])):
        data[key] = _loads(data.get(key), default)
    return data


def _enc(value: Any) -> Any:
    return value if isinstance(value, (str, int, float)) or value is None else json.dumps(value, ensure_ascii=False, default=str)


def fts_query(text: str) -> str:
    """User text -> an FTS5 query: every word is a prefix term, all must match. Quotes and operators are neutralised."""
    words = [w for w in re.findall(r"[\w]+", fold(text or ""), flags=re.UNICODE) if w]
    return " ".join(f'"{w}"*' for w in words[:12])


class Store:
    def __init__(self, db: Database, clock: Callable[[], float] = time.time):
        self.db = db
        self.clock = clock

    def today(self) -> date:
        return datetime.fromtimestamp(self.clock()).date()

    # ------------------------------------------------------------------ documents
    def create_document(self, **fields: Any) -> dict[str, Any]:
        now = self.clock()
        did = fields.pop("id", None) or new_id("d", now)
        data = {k: _enc(v) for k, v in fields.items() if k in DOC_FIELDS}
        if "ocr" in data:
            data["ocr"] = int(bool(data["ocr"]))
        cols = ["id", "created_ts", "updated_ts", *data.keys()]
        self.db.execute(f"INSERT INTO documents({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})", [did, now, now, *data.values()])
        return self.document(did)

    def update_document(self, did: str, **fields: Any) -> dict[str, Any]:
        data = {k: _enc(v) for k, v in fields.items() if k in DOC_FIELDS}
        if "ocr" in data:
            data["ocr"] = int(bool(data["ocr"]))
        if data:
            sets = ", ".join(f"{k} = ?" for k in data)
            self.db.execute(f"UPDATE documents SET {sets}, updated_ts = ? WHERE id = ?", [*data.values(), self.clock(), did])
            if "title" in data or "issuer" in data:
                self._reindex_meta(did)
        return self.document(did)

    def document(self, did: str) -> dict[str, Any]:
        row = _doc(self.db.one("SELECT * FROM documents WHERE id = ?", (did,)))
        if row is None:
            raise KafkaError("not_found", f"No document {did}.", "List documents with docs_list.")
        return row

    def find_document(self, did: str) -> Optional[dict[str, Any]]:
        return _doc(self.db.one("SELECT * FROM documents WHERE id = ?", ((did or "").strip(),)))

    def document_by_sha(self, sha: str) -> Optional[dict[str, Any]]:
        return _doc(self.db.one("SELECT * FROM documents WHERE file_sha = ? AND file_sha != '' ORDER BY created_ts LIMIT 1", (sha,)))

    def document_by_source(self, source: str, source_ref: str) -> Optional[dict[str, Any]]:
        return _doc(self.db.one("SELECT * FROM documents WHERE source = ? AND source_ref = ? ORDER BY created_ts LIMIT 1", (source, source_ref)))

    def sha_users(self, sha: str, excluding: str = "") -> int:
        row = self.db.one("SELECT COUNT(*) FROM documents WHERE file_sha = ? AND id != ?", (sha, excluding))
        return int(row[0]) if row else 0

    def documents(self, *, kind: str = "", issuer: str = "", year: str = "", state: str = "", text: str = "", series_id: str = "",
                  source: str = "", exclude_archived: bool = False, doc_ids: Optional[list[str]] = None, limit: int = 100,
                  offset: int = 0) -> list[dict[str, Any]]:
        sql, params = "SELECT * FROM documents WHERE 1=1", []
        if doc_ids is not None:
            if not doc_ids:
                return []
            sql += f" AND id IN ({', '.join('?' for _ in doc_ids)})"
            params.extend(doc_ids)
        if kind:
            sql += " AND kind = ?"
            params.append(kind)
        if issuer:
            sql += " AND (issuer_key LIKE ? OR issuer LIKE ?)"
            params.extend([f"%{fold(issuer)}%", f"%{issuer}%"])
        if year:
            sql += " AND (substr(issue_date, 1, 4) = ? OR (issue_date = '' AND strftime('%Y', created_ts, 'unixepoch') = ?))"
            params.extend([str(year), str(year)])
        if state:
            sql += " AND state = ?"
            params.append(state)
        elif exclude_archived:
            sql += " AND state != 'archived'"
        if series_id:
            sql += " AND series_id = ?"
            params.append(series_id)
        if source:
            sql += " AND source = ?"
            params.append(source)
        if text:
            like = f"%{text}%"
            sql += " AND (title LIKE ? OR issuer LIKE ? OR ref LIKE ? OR item LIKE ? OR file_name LIKE ? OR mail_subject LIKE ?)"
            params.extend([like] * 6)
        sql += " ORDER BY COALESCE(NULLIF(issue_date, ''), strftime('%Y-%m-%d', created_ts, 'unixepoch')) DESC, created_ts DESC LIMIT ? OFFSET ?"
        params.extend([max(1, min(int(limit), 2000)), max(0, int(offset))])
        return [_doc(r) for r in self.db.query(sql, params)]

    def count_documents(self, **kw: Any) -> int:
        return len(self.documents(limit=2000, **kw))

    def delete_document(self, did: str) -> None:
        self.db.execute("DELETE FROM pages_fts WHERE doc_id = ?", (did,))
        self.db.execute("DELETE FROM documents WHERE id = ?", (did,))     # pages and deadlines cascade

    # ------------------------------------------------------------------ pages and full-text index
    def set_pages(self, did: str, title: str, issuer: str, pages: list[str]) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM pages WHERE doc_id = ?", (did,))
            conn.execute("DELETE FROM pages_fts WHERE doc_id = ?", (did,))
            for number, text in enumerate(pages, start=1):
                conn.execute("INSERT INTO pages(doc_id, page, text) VALUES (?, ?, ?)", (did, number, text or ""))
                conn.execute("INSERT INTO pages_fts(title, issuer, text, doc_id, page) VALUES (?, ?, ?, ?, ?)",
                             (title or "", issuer or "", text or "", did, number))

    def _reindex_meta(self, did: str) -> None:
        d = self.db.one("SELECT title, issuer FROM documents WHERE id = ?", (did,))
        if d is None:
            return
        self.db.execute("UPDATE pages_fts SET title = ?, issuer = ? WHERE doc_id = ?", (d["title"], d["issuer"], did))

    def pages(self, did: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self.db.query("SELECT page, text FROM pages WHERE doc_id = ? ORDER BY page", (did,))]

    def page_text(self, did: str, page: int) -> str:
        row = self.db.one("SELECT text FROM pages WHERE doc_id = ? AND page = ?", (did, page))
        return row["text"] if row else ""

    def search(self, text: str, *, kind: str = "", issuer: str = "", state: str = "", year: str = "", doc_ids: Optional[list[str]] = None,
               limit: int = 20) -> list[dict[str, Any]]:
        query = fts_query(text)
        if not query or (doc_ids is not None and not doc_ids):
            return []
        sql = ("SELECT f.doc_id AS doc_id, f.page AS page, snippet(pages_fts, 2, ?, ?, '…', 14) AS snippet, bm25(pages_fts, 4.0, 2.0, 1.0) AS rank "
               "FROM pages_fts f JOIN documents d ON d.id = f.doc_id WHERE pages_fts MATCH ?")
        params: list[Any] = [HIT_OPEN, HIT_CLOSE, query]
        if doc_ids is not None:
            sql += f" AND d.id IN ({', '.join('?' for _ in doc_ids)})"
            params.extend(doc_ids)
        if kind:
            sql += " AND d.kind = ?"
            params.append(kind)
        if issuer:
            sql += " AND (d.issuer_key LIKE ? OR d.issuer LIKE ?)"
            params.extend([f"%{fold(issuer)}%", f"%{issuer}%"])
        if state:
            sql += " AND d.state = ?"
            params.append(state)
        elif doc_ids is None:
            sql += " AND d.state != 'archived'"
        if year:
            sql += " AND substr(d.issue_date, 1, 4) = ?"
            params.append(str(year))
        sql += " ORDER BY rank LIMIT ?"
        params.append(max(1, min(int(limit), 200)))
        try:
            return [{"doc_id": r["doc_id"], "page": int(r["page"]), "snippet": r["snippet"], "rank": r["rank"]} for r in self.db.query(sql, params)]
        except Exception:  # noqa: BLE001 — a malformed query must never break the caller
            return []

    # ------------------------------------------------------------------ series
    def find_series(self, issuer_key: str, ref: str = "", kind: str = "") -> Optional[dict[str, Any]]:
        if ref:
            row = self.db.one("SELECT * FROM series WHERE issuer_key = ? AND ref = ? ORDER BY created_ts LIMIT 1", (issuer_key, ref))
        else:
            row = self.db.one("SELECT * FROM series WHERE issuer_key = ? AND ref = '' AND kind = ? ORDER BY created_ts LIMIT 1", (issuer_key, kind))
        return dict(row) if row else None

    def create_series(self, kind: str, issuer_key: str, ref: str, label: str) -> dict[str, Any]:
        sid = new_id("r", self.clock())
        self.db.execute("INSERT INTO series(id, kind, issuer_key, ref, label, created_ts) VALUES (?, ?, ?, ?, ?, ?)",
                        (sid, kind, issuer_key, ref, label, self.clock()))
        return self.series(sid)

    def series(self, sid: str) -> dict[str, Any]:
        row = self.db.one("SELECT * FROM series WHERE id = ?", (sid,))
        if row is None:
            raise KafkaError("not_found", f"No series {sid}.", "List series with series_list.")
        return dict(row)

    def all_series(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.db.query("SELECT * FROM series ORDER BY created_ts DESC")]

    def series_documents(self, sid: str) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM documents WHERE series_id = ? AND state != 'archived' "
                             "ORDER BY COALESCE(NULLIF(period_to, ''), NULLIF(period_from, ''), NULLIF(issue_date, ''), strftime('%Y-%m-%d', created_ts, 'unixepoch')), created_ts",
                             (sid,))
        return [_doc(r) for r in rows]

    def drop_empty_series(self) -> int:
        return self.db.execute("DELETE FROM series WHERE id NOT IN (SELECT series_id FROM documents WHERE series_id IS NOT NULL)").rowcount

    # ------------------------------------------------------------------ deadlines
    def create_deadline(self, **fields: Any) -> dict[str, Any]:
        now = self.clock()
        tid = fields.pop("id", None) or new_id("t", now)
        data = {k: _enc(v) for k, v in fields.items() if k in DL_FIELDS}
        for key in ("auto", "edited", "archived"):
            if key in data:
                data[key] = int(bool(data[key]))
        cols = ["id", "created_ts", "updated_ts", *data.keys()]
        self.db.execute(f"INSERT INTO deadlines({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})", [tid, now, now, *data.values()])
        return self.deadline(tid)

    def update_deadline(self, tid: str, **fields: Any) -> dict[str, Any]:
        data = {k: _enc(v) for k, v in fields.items() if k in DL_FIELDS}
        for key in ("auto", "edited", "archived"):
            if key in data:
                data[key] = int(bool(data[key]))
        if data:
            sets = ", ".join(f"{k} = ?" for k in data)
            self.db.execute(f"UPDATE deadlines SET {sets}, updated_ts = ? WHERE id = ?", [*data.values(), self.clock(), tid])
        return self.deadline(tid)

    def deadline(self, tid: str) -> dict[str, Any]:
        row = _dl(self.db.one("SELECT * FROM deadlines WHERE id = ?", ((tid or "").strip(),)))
        if row is None:
            raise KafkaError("not_found", f"No deadline {tid}.", "List deadlines with deadlines_list.")
        return row

    def deadlines(self, *, states: Optional[Iterable[str]] = None, date_from: str = "", date_to: str = "", kind: str = "", text: str = "",
                  doc_id: str = "", source: str = "", include_archived: bool = False, limit: int = 500,
                  newest_first: bool = False) -> list[dict[str, Any]]:
        sql, params = "SELECT * FROM deadlines WHERE 1=1", []
        if source:
            sql += " AND source = ?"
            params.append(source)
        if states:
            states = list(states)
            sql += f" AND state IN ({', '.join('?' for _ in states)})"
            params.extend(states)
        if date_from:
            sql += " AND date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND date <= ?"
            params.append(date_to)
        if kind:
            sql += " AND kind = ?"
            params.append(kind)
        if text:
            sql += " AND (title LIKE ? OR basis LIKE ? OR notes LIKE ?)"
            params.extend([f"%{text}%"] * 3)
        if doc_id:
            sql += " AND doc_id = ?"
            params.append(doc_id)
        if not include_archived:
            sql += " AND archived = 0"
        sql += f" ORDER BY date {'DESC' if newest_first else 'ASC'}, created_ts LIMIT ?"
        params.append(max(1, min(int(limit), 5000)))
        return [_dl(r) for r in self.db.query(sql, params)]

    def deadline_by_key(self, source: str, external_key: str) -> Optional[dict[str, Any]]:
        if not source or not external_key:
            return None
        return _dl(self.db.one("SELECT * FROM deadlines WHERE source = ? AND external_key = ?", (source, external_key)))

    def doc_deadlines(self, did: str) -> list[dict[str, Any]]:
        return self.deadlines(doc_id=did, include_archived=True, limit=500)

    def delete_deadline(self, tid: str) -> None:
        self.db.execute("DELETE FROM deadlines WHERE id = ?", (tid,))

    # ------------------------------------------------------------------ mails
    def known_message_ids(self, limit: int = 20000) -> list[str]:
        return [r["message_id"] for r in self.db.query("SELECT message_id FROM mails ORDER BY ts DESC LIMIT ?", (limit,))]

    def mail(self, message_id: str) -> Optional[dict[str, Any]]:
        return _mail(self.db.one("SELECT * FROM mails WHERE message_id = ?", (message_id,)))

    def save_mail(self, message: dict[str, Any], *, kind: str, score: int, state: str, doc_ids: list[str], reasons: list[str]) -> None:
        text = " ".join(str(message.get("text") or "").split())
        snippet = text[:280]
        attachments = [{k: a.get(k) for k in ("name", "mime", "size", "sha", "path")} for a in (message.get("attachments") or [])]
        self.db.execute(
            "INSERT INTO mails(message_id, ts, from_address, from_name, subject, account, kind, score, state, doc_ids, snippet, body, attachments, reasons, created_ts, hub_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(message_id) DO UPDATE SET kind = excluded.kind, score = excluded.score, "
            "state = excluded.state, doc_ids = excluded.doc_ids, reasons = excluded.reasons, attachments = excluded.attachments, "
            "hub_id = CASE WHEN excluded.hub_id != '' THEN excluded.hub_id ELSE mails.hub_id END",
            (message.get("message_id"), message.get("ts"), message.get("from_address") or "", message.get("from_name") or "",
             message.get("subject") or "", message.get("account") or "", kind, int(score), state, json.dumps(doc_ids), snippet,
             str(message.get("text") or "")[:24000], json.dumps(attachments, ensure_ascii=False), json.dumps(reasons, ensure_ascii=False), self.clock(),
             str(message.get("hub_id") or "")))

    def set_mail_state(self, message_id: str, state: str, doc_ids: Optional[list[str]] = None) -> None:
        if doc_ids is None:
            self.db.execute("UPDATE mails SET state = ? WHERE message_id = ?", (state, message_id))
        else:
            self.db.execute("UPDATE mails SET state = ?, doc_ids = ? WHERE message_id = ?", (state, json.dumps(doc_ids), message_id))

    def mails(self, *, kind: Optional[Iterable[str]] = None, state: Optional[Iterable[str]] = None, limit: int = 100) -> list[dict[str, Any]]:
        sql, params = "SELECT * FROM mails WHERE 1=1", []
        if kind:
            kind = list(kind)
            sql += f" AND kind IN ({', '.join('?' for _ in kind)})"
            params.extend(kind)
        if state:
            state = list(state)
            sql += f" AND state IN ({', '.join('?' for _ in state)})"
            params.extend(state)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))
        return [_mail(r) for r in self.db.query(sql, params)]

    # ------------------------------------------------------------------ watched files
    def file_seen(self, path: str) -> Optional[dict[str, Any]]:
        row = self.db.one("SELECT * FROM files_seen WHERE path = ?", (path,))
        return dict(row) if row else None

    def mark_file_seen(self, path: str, mtime: float, size: int, sha: str) -> None:
        self.db.execute("INSERT INTO files_seen(path, mtime, size, sha, ts) VALUES (?, ?, ?, ?, ?) ON CONFLICT(path) DO UPDATE SET "
                        "mtime = excluded.mtime, size = excluded.size, sha = excluded.sha, ts = excluded.ts", (path, mtime, size, sha, self.clock()))

    def forget_folder(self, folder: str) -> int:
        prefix = folder.rstrip("/\\")
        return self.db.execute("DELETE FROM files_seen WHERE path LIKE ? OR path LIKE ?", (prefix + "/%", prefix + "\\%")).rowcount

    # ------------------------------------------------------------------ notifications and runs
    def add_notification(self, *, doc_id: Optional[str], deadline_id: Optional[str], type_: str, severity: str, title: str, body: str,
                         results: list[dict[str, Any]], dedupe: str) -> int:
        cur = self.db.execute("INSERT INTO notifications(ts, doc_id, deadline_id, type, severity, title, body, results, dedupe) "
                              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              (self.clock(), doc_id, deadline_id, type_, severity, title, body, json.dumps(results, default=str), dedupe))
        return int(cur.lastrowid)

    def notified(self, dedupe: str) -> bool:
        return self.db.one("SELECT 1 FROM notifications WHERE dedupe = ? LIMIT 1", (dedupe,)) is not None

    def notifications(self, *, unseen: bool = False, limit: int = 50) -> list[dict[str, Any]]:
        sql = "SELECT * FROM notifications" + (" WHERE seen = 0" if unseen else "") + " ORDER BY ts DESC, id DESC LIMIT ?"
        out = []
        for r in self.db.query(sql, (max(1, min(limit, 500)),)):
            data = dict(r)
            data["results"] = _loads(data.get("results"), [])
            out.append(data)
        return out

    def mark_notifications_seen(self, before: float) -> int:
        return self.db.execute("UPDATE notifications SET seen = 1 WHERE seen = 0 AND ts <= ?", (before,)).rowcount

    def add_run(self, kind: str, ref: str, ok: bool, duration_ms: int, detail: str) -> None:
        self.db.execute("INSERT INTO runs(ts, kind, ref, ok, duration_ms, detail) VALUES (?, ?, ?, ?, ?, ?)",
                        (self.clock(), kind, ref, int(ok), duration_ms, detail[:500]))
        self.db.execute("DELETE FROM runs WHERE id IN (SELECT id FROM runs ORDER BY id DESC LIMIT -1 OFFSET 2000)")

    def runs(self, kind: str = "", limit: int = 50) -> list[dict[str, Any]]:
        if kind:
            rows = self.db.query("SELECT * FROM runs WHERE kind = ? ORDER BY id DESC LIMIT ?", (kind, limit))
        else:
            rows = self.db.query("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ counts
    def counts(self) -> dict[str, int]:
        one = lambda sql, p=(): int((self.db.one(sql, p) or [0])[0] or 0)  # noqa: E731
        today = self.today().isoformat()
        return {
            "documents": one("SELECT COUNT(*) FROM documents WHERE state != 'archived'"),
            "review": one("SELECT COUNT(*) FROM documents WHERE state = 'review'"),
            "archived": one("SELECT COUNT(*) FROM documents WHERE state = 'archived'"),
            "deadlines_open": one("SELECT COUNT(*) FROM deadlines WHERE state = 'open' AND archived = 0"),
            "overdue": one("SELECT COUNT(*) FROM deadlines WHERE state = 'open' AND archived = 0 AND date < ?", (today,)),
            "series": one("SELECT COUNT(*) FROM series"),
            "mails": one("SELECT COUNT(*) FROM mails"),
            "mails_review": one("SELECT COUNT(*) FROM mails WHERE kind = 'maybe' AND state = 'new'"),
            "unseen_notifications": one("SELECT COUNT(*) FROM notifications WHERE seen = 0"),
        }
