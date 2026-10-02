"""SQLite connection (WAL) and ordered schema migrations."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

MIGRATIONS: list[str] = [
    # 1: settings, documents, pages (+ FTS5), deadlines, series, mails, watched files, notifications, runs
    """
    CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE series (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL DEFAULT '',
      issuer_key TEXT NOT NULL DEFAULT '',
      ref TEXT NOT NULL DEFAULT '',
      label TEXT NOT NULL DEFAULT '',
      created_ts REAL NOT NULL
    );
    CREATE INDEX series_key ON series(issuer_key, ref, kind);
    CREATE TABLE documents (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL DEFAULT '',
      kind TEXT NOT NULL DEFAULT 'other',
      issuer TEXT NOT NULL DEFAULT '',
      issuer_key TEXT NOT NULL DEFAULT '',
      ref TEXT NOT NULL DEFAULT '',
      amount REAL,
      currency TEXT NOT NULL DEFAULT '',
      issue_date TEXT NOT NULL DEFAULT '',
      period_from TEXT NOT NULL DEFAULT '',
      period_to TEXT NOT NULL DEFAULT '',
      item TEXT NOT NULL DEFAULT '',
      file_sha TEXT NOT NULL DEFAULT '',
      file_ext TEXT NOT NULL DEFAULT '',
      file_name TEXT NOT NULL DEFAULT '',
      mime TEXT NOT NULL DEFAULT '',
      size INTEGER NOT NULL DEFAULT 0,
      pages INTEGER NOT NULL DEFAULT 0,
      ocr INTEGER NOT NULL DEFAULT 0,
      source TEXT NOT NULL DEFAULT 'upload',
      source_ref TEXT NOT NULL DEFAULT '',
      mail_subject TEXT NOT NULL DEFAULT '',
      mail_from TEXT NOT NULL DEFAULT '',
      received_ts REAL,
      state TEXT NOT NULL DEFAULT 'review',
      confidence INTEGER NOT NULL DEFAULT 0,
      series_id TEXT REFERENCES series(id) ON DELETE SET NULL,
      tags TEXT NOT NULL DEFAULT '[]',
      notes TEXT NOT NULL DEFAULT '',
      facts TEXT NOT NULL DEFAULT '{}',
      created_ts REAL NOT NULL,
      updated_ts REAL NOT NULL
    );
    CREATE INDEX documents_sha ON documents(file_sha);
    CREATE INDEX documents_issuer ON documents(issuer_key, kind);
    CREATE INDEX documents_state ON documents(state, created_ts);
    CREATE INDEX documents_series ON documents(series_id);
    CREATE INDEX documents_source ON documents(source, source_ref);
    CREATE TABLE pages (
      doc_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      page INTEGER NOT NULL,
      text TEXT NOT NULL DEFAULT '',
      PRIMARY KEY (doc_id, page)
    );
    CREATE VIRTUAL TABLE pages_fts USING fts5(title, issuer, text, doc_id UNINDEXED, page UNINDEXED, tokenize = 'unicode61 remove_diacritics 2');
    CREATE TABLE deadlines (
      id TEXT PRIMARY KEY,
      doc_id TEXT REFERENCES documents(id) ON DELETE CASCADE,
      kind TEXT NOT NULL DEFAULT 'custom',
      title TEXT NOT NULL DEFAULT '',
      date TEXT NOT NULL,
      basis TEXT NOT NULL DEFAULT '',
      evidence TEXT NOT NULL DEFAULT '',
      page INTEGER,
      confidence INTEGER NOT NULL DEFAULT 70,
      state TEXT NOT NULL DEFAULT 'open',
      remind TEXT NOT NULL DEFAULT '[]',
      notified TEXT NOT NULL DEFAULT '[]',
      amount REAL,
      recurring TEXT NOT NULL DEFAULT 'none',
      key TEXT NOT NULL DEFAULT '',
      auto INTEGER NOT NULL DEFAULT 0,
      edited INTEGER NOT NULL DEFAULT 0,
      archived INTEGER NOT NULL DEFAULT 0,
      notes TEXT NOT NULL DEFAULT '',
      created_ts REAL NOT NULL,
      updated_ts REAL NOT NULL,
      done_ts REAL
    );
    CREATE INDEX deadlines_open ON deadlines(state, date);
    CREATE INDEX deadlines_doc ON deadlines(doc_id);
    CREATE TABLE mails (
      message_id TEXT PRIMARY KEY,
      ts REAL,
      from_address TEXT NOT NULL DEFAULT '',
      from_name TEXT NOT NULL DEFAULT '',
      subject TEXT NOT NULL DEFAULT '',
      account TEXT NOT NULL DEFAULT '',
      kind TEXT NOT NULL DEFAULT 'noise',
      score INTEGER NOT NULL DEFAULT 0,
      state TEXT NOT NULL DEFAULT 'new',
      doc_ids TEXT NOT NULL DEFAULT '[]',
      snippet TEXT NOT NULL DEFAULT '',
      body TEXT NOT NULL DEFAULT '',
      attachments TEXT NOT NULL DEFAULT '[]',
      reasons TEXT NOT NULL DEFAULT '[]',
      created_ts REAL NOT NULL
    );
    CREATE INDEX mails_kind ON mails(kind, state, ts);
    CREATE TABLE files_seen (
      path TEXT PRIMARY KEY,
      mtime REAL NOT NULL,
      size INTEGER NOT NULL,
      sha TEXT NOT NULL DEFAULT '',
      ts REAL NOT NULL
    );
    CREATE TABLE notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts REAL NOT NULL,
      doc_id TEXT,
      deadline_id TEXT,
      type TEXT NOT NULL,
      severity TEXT NOT NULL DEFAULT 'medium',
      title TEXT NOT NULL DEFAULT '',
      body TEXT NOT NULL DEFAULT '',
      results TEXT NOT NULL DEFAULT '[]',
      dedupe TEXT NOT NULL DEFAULT '',
      seen INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX notifications_ts ON notifications(ts);
    CREATE INDEX notifications_dedupe ON notifications(dedupe);
    CREATE TABLE runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts REAL NOT NULL,
      kind TEXT NOT NULL,
      ref TEXT NOT NULL DEFAULT '',
      ok INTEGER NOT NULL DEFAULT 1,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      detail TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX runs_ts ON runs(ts);
    """,
    # 2: deadlines kept for another app of the family (source + external_key), with the basis and rule it sends
    """
    ALTER TABLE deadlines ADD COLUMN source TEXT NOT NULL DEFAULT '';
    ALTER TABLE deadlines ADD COLUMN external_key TEXT NOT NULL DEFAULT '';
    ALTER TABLE deadlines ADD COLUMN ext_date TEXT NOT NULL DEFAULT '';
    ALTER TABLE deadlines ADD COLUMN rule TEXT NOT NULL DEFAULT '';
    ALTER TABLE deadlines ADD COLUMN url TEXT NOT NULL DEFAULT '';
    CREATE UNIQUE INDEX deadlines_external ON deadlines(source, external_key) WHERE external_key != '';
    """,
    # 3: the record a deadline came from (hoard:// uri) and the hub's id of a mail that was read through the gateway
    """
    ALTER TABLE deadlines ADD COLUMN source_ref TEXT NOT NULL DEFAULT '';
    ALTER TABLE mails ADD COLUMN hub_id TEXT NOT NULL DEFAULT '';
    """,
]


class Database:
    """One connection shared by every thread, guarded by a re-entrant lock.

    The app is the only writer; the MCP bridge never opens this file.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.migrate()

    def migrate(self) -> None:
        with self.lock:
            self.conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
            row = self.conn.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()
            current = row["v"] or 0
            for index, sql in enumerate(MIGRATIONS, start=1):
                if index <= current:
                    continue
                script = f"BEGIN;\n{sql}\nINSERT INTO schema_version(version) VALUES ({index});\nCOMMIT;"
                try:
                    self.conn.executescript(script)
                except Exception:
                    if self.conn.in_transaction:
                        self.conn.execute("ROLLBACK")
                    raise

    def version(self) -> int:
        row = self.one("SELECT MAX(version) AS v FROM schema_version")
        return int(row["v"] or 0)

    def query(self, sql: str, params: tuple | list = ()) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, params).fetchall()

    def one(self, sql: str, params: tuple | list = ()) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute(sql, params).fetchone()

    def execute(self, sql: str, params: tuple | list = ()) -> sqlite3.Cursor:
        with self.lock:
            return self.conn.execute(sql, params)

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self.one("SELECT value FROM settings WHERE key = ?", (key,))
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        self.execute("INSERT INTO settings(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))

    def transaction(self):
        """`with db.transaction():` — BEGIN IMMEDIATE / COMMIT (ROLLBACK on error) under the lock."""
        return _Transaction(self)

    def close(self) -> None:
        with self.lock:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error:
                pass
            self.conn.close()


class _Transaction:
    def __init__(self, db: Database):
        self.db = db

    def __enter__(self):
        self.db.lock.acquire()
        self.db.conn.execute("BEGIN IMMEDIATE")
        return self.db.conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.db.conn.execute("COMMIT")
            else:
                self.db.conn.execute("ROLLBACK")
        finally:
            self.db.lock.release()
        return False
