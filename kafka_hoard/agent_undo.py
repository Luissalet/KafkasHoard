"""Accountable agents: what each write tool captures before it runs, what it reports after it, and how to take it back.

Three hooks per tool (see ``hoard_link.agentkit.Tool``):

* ``capture(svc, args)`` runs just before the write and returns the state that would be lost (a document as it was with its
  deadlines, a deadline row, a setting). It is kept in the agent journal as ``before``.
* ``track(svc, args, result)`` runs just after and says which objects the write touched and a fingerprint (``etag``) of what they
  look like now. It reads the database, not the result: the result an agent gets is masked and cut to ~20 KB.
* ``undo(svc, record, dry_run=False)`` puts ``before`` back. It first compares the object with ``etag``; when it differs (the person
  edited it in the web interface, which the journal never sees) it raises a ``conflict`` and nothing changes.

Objects are slash-separated paths, and two writes conflict when the paths are equal or one contains the other::

    doc:D                  a whole document (filing it, deleting it)
    doc:D/meta             its fields (title, kind, issuer, reference, amount, dates, item, tags, notes, state)
    doc:D/deadlines        every deadline of the document (a correction recomputes them)
    doc:D/deadlines/T      one deadline that belongs to a document
    deadline:T             one deadline that belongs to no document (added by hand or by another app)
    mail:M                 the state of one mail (ignored or not)
    settings:KEY           one setting
    settings:folders.watch the list of watched folders

What the fingerprint leaves out on purpose: ``notified`` (the reminders mark it by themselves), ``updated_ts`` and the model or
extraction timestamps, so a reminder that fired meanwhile is not a conflict.

Not undoable (no handler, reported as such): doc_delete (the stored original goes with the document and Kafka keeps no trash, so
nothing could bring it back), mail_accept, mail_scan, folder_scan, phileas_sync (they ingest from outside), document_link_tx (writes
to another app), tax_pack, every pdf_* tool, images_compress and every PrintCraft tool that is not read-only (they write new files
and never touch an original; removing them is for the person), notify_test, telegram_find_chat_id, secret_set (a secret is never
copied into the journal), housekeeping_run.
"""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from typing import Any, Callable

from .hoard_link.agentkit import AppError
from .store import DL_FIELDS

#: the document fields an undo puts back (everything a correction or a re-read can change)
DOC_SNAP = ("title", "kind", "issuer", "issuer_key", "ref", "amount", "currency", "issue_date", "period_from", "period_to", "item", "state",
            "confidence", "series_id", "tags", "notes", "facts")
#: the part of that the fingerprint looks at (what a person can change; not the confidence or the extraction timestamps)
DOC_SIG = ("title", "kind", "issuer", "issuer_key", "ref", "amount", "currency", "issue_date", "period_from", "period_to", "item", "state",
           "series_id", "tags", "notes")
FACTS_SIG = ("edited", "warranty_months", "title_given")


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()[:16]


def _conflict(message: str) -> AppError:
    return AppError("conflict", message, hint="Look at the record as it is now before undoing anything else.")


def _id_of(record: dict[str, Any], key: str) -> str:
    for item in record.get("ids") or []:
        name, _, value = str(item).partition("=")
        if name == key:
            return value
    return ""


def _ids_of(record: dict[str, Any], key: str) -> list[str]:
    out = []
    for item in record.get("ids") or []:
        name, _, value = str(item).partition("=")
        if name == key and value:
            out.append(value)
    return list(dict.fromkeys(out))


def _before(record: dict[str, Any]) -> dict[str, Any]:
    before = record.get("before")
    return before if isinstance(before, dict) else {}


# ------------------------------------------------------------------------------------------------ deadline rows

def _dl_path(row: dict[str, Any]) -> str:
    return f"doc:{row['doc_id']}/deadlines/{row['id']}" if row.get("doc_id") else f"deadline:{row['id']}"


def _dl_row(row: dict[str, Any]) -> dict[str, Any]:
    """Everything needed to write the deadline back exactly as it was."""
    return {"id": row["id"], **{k: row.get(k) for k in DL_FIELDS}}


def _dl_sig(row: dict[str, Any]) -> dict[str, Any]:
    return {"id": row["id"], **{k: row.get(k) for k in DL_FIELDS if k != "notified"}}


def _live_deadline(svc: Any, tid: str) -> dict[str, Any] | None:
    from .store import _dl
    return _dl(svc.db.one("SELECT * FROM deadlines WHERE id = ?", (tid,)))


def _write_deadline(svc: Any, snap: dict[str, Any]) -> None:
    fields = {k: snap.get(k) for k in DL_FIELDS}
    if svc.db.one("SELECT 1 FROM deadlines WHERE id = ?", (snap["id"],)) is not None:
        svc.store.update_deadline(snap["id"], **fields)
    else:
        svc.store.create_deadline(id=snap["id"], **fields)


# ------------------------------------------------------------------------------------------------ whole documents

def _doc_snapshot(svc: Any, did: str) -> dict[str, Any]:
    """The document (its editable and recomputed fields), all its deadlines and its series, as they are now."""
    doc = svc.store.document(did)
    series = svc.db.one("SELECT * FROM series WHERE id = ?", (doc["series_id"],)) if doc.get("series_id") else None
    return {"id": did, "doc": {k: doc.get(k) for k in DOC_SNAP}, "deadlines": [_dl_row(d) for d in svc.store.doc_deadlines(did)],
            "series": dict(series) if series is not None else None}


def _sig(doc: dict[str, Any], deadlines: list[dict[str, Any]]) -> str:
    facts = doc.get("facts") or {}
    return _digest({"doc": {k: doc.get(k) for k in DOC_SIG}, "facts": {k: facts.get(k) for k in FACTS_SIG},
                    "deadlines": sorted((_dl_sig(d) for d in deadlines), key=lambda d: d["id"])})


def _doc_sig(svc: Any, did: str) -> str | None:
    doc = svc.store.find_document(did)
    return None if doc is None else _sig(doc, svc.store.doc_deadlines(did))


def _snap_sig(snap: dict[str, Any]) -> str:
    return _sig(snap["doc"], snap["deadlines"])


def _born_sig(svc: Any, did: str) -> str | None:
    """A freshly filed document: has a person touched it (a correction, a deadline closed or added)? Extraction work that runs
    afterwards (the model pass, a re-read) is not a touch."""
    doc = svc.store.find_document(did)
    if doc is None:
        return None
    facts = doc.get("facts") or {}
    return _digest({"edited": facts.get("edited") or [], "notes": doc.get("notes"), "tags": doc.get("tags"),
                    "touched": sorted(d["id"] for d in svc.store.doc_deadlines(did) if d["edited"])})


def _doc_track(svc: Any, did: str) -> dict[str, Any]:
    return {"objects": [f"doc:{did}/meta", f"doc:{did}/deadlines"], "ids": [f"doc_id={did}"], "etag": _doc_sig(svc, did) or ""}


def _restore_document(svc: Any, snap: dict[str, Any]) -> dict[str, int]:
    did = snap["id"]
    with svc.db.transaction():
        series = snap.get("series")
        if series and svc.db.one("SELECT 1 FROM series WHERE id = ?", (series["id"],)) is None:
            svc.db.execute("INSERT INTO series(id, kind, issuer_key, ref, label, created_ts) VALUES (?, ?, ?, ?, ?, ?)",
                           (series["id"], series["kind"], series["issuer_key"], series["ref"], series["label"], series["created_ts"]))
        svc.store.update_document(did, **snap["doc"])
        wanted = {d["id"]: d for d in snap["deadlines"]}
        current = {d["id"]: d for d in svc.store.doc_deadlines(did)}
        removed = [tid for tid in current if tid not in wanted]
        for tid in removed:
            svc.store.delete_deadline(tid)
        brought_back = restored = 0
        for tid, row in wanted.items():
            if tid not in current:
                brought_back += 1
            elif _dl_row(current[tid]) != row:
                restored += 1
            else:
                continue
            _write_deadline(svc, row)
        svc.store.drop_empty_series()
    return {"restored_deadlines": restored, "brought_back_deadlines": brought_back, "removed_deadlines": len(removed)}


def _plan_document(svc: Any, snap: dict[str, Any]) -> dict[str, Any]:
    doc = svc.store.document(snap["id"])
    changed = [k for k in DOC_SNAP if k != "facts" and doc.get(k) != snap["doc"].get(k)]
    current = {d["id"]: d for d in svc.store.doc_deadlines(snap["id"])}
    wanted = {d["id"]: d for d in snap["deadlines"]}
    return {"fields": changed, "restore_deadlines": sum(1 for t in wanted if t in current and _dl_row(current[t]) != wanted[t]),
            "bring_back_deadlines": sum(1 for t in wanted if t not in current), "remove_deadlines": sum(1 for t in current if t not in wanted)}


# ------------------------------------------------------------------------------------------------ doc_update, doc_reprocess

def capture_doc(svc: Any, args: Any) -> dict:
    return {"snapshot": _doc_snapshot(svc, args.doc)}


def track_doc(svc: Any, args: Any, result: dict) -> dict:
    return _doc_track(svc, args.doc)


def undo_doc(svc: Any, record: dict, dry_run: bool = False) -> dict:
    snap = _before(record).get("snapshot")
    if not snap:
        return {"unchanged": True}
    did = snap["id"]
    if svc.store.find_document(did) is None:
        raise _conflict("The document no longer exists.")
    if _doc_sig(svc, did) != record.get("etag"):
        raise _conflict("The document or its deadlines were changed after this write (in the web interface or by another session).")
    if _snap_sig(snap) == record.get("etag") and svc.store.document(did).get("facts") == snap["doc"].get("facts"):
        return {"unchanged": True}
    if dry_run:
        return {"would_restore_document": did, **_plan_document(svc, snap)}
    return {"restored_document": did, **_restore_document(svc, snap)}


# ------------------------------------------------------------------------------------------------ doc_add_file, doc_add_text

def _existing_by_content(svc: Any, args: Any) -> dict[str, Any] | None:
    """The document already filed with the same content, when the call also gives a kind or an item (those are applied to it)."""
    if not (args.kind or args.item.strip()):
        return None
    from . import paths, readers
    from .files import sha256_of
    try:
        if hasattr(args, "path"):
            if paths.unsafe_file(args.path, svc.config.data_dir):
                return None
            from pathlib import Path
            data = Path(args.path).expanduser().resolve().read_bytes()
        else:
            data = readers.clean_text(args.text or "").encode("utf-8")
        return svc.store.document_by_sha(sha256_of(data))
    except Exception:  # noqa: BLE001 - the call reports a bad path itself
        return None


def _born_ids(svc: Any, result: dict, max_rowid: int) -> list[str]:
    ids = []
    for item in result.get("documents") or []:
        did = item.get("id") if isinstance(item, dict) else None
        row = svc.db.one("SELECT rowid AS r FROM documents WHERE id = ?", (did,)) if did else None
        if row is not None and int(row["r"]) > max_rowid:
            ids.append(did)
    return ids


# ------------------------------------------------------------------------------------------------ deadlines

def _external_key(args: Any) -> tuple[str, str]:
    from .agent_tools import _ref_key
    source, key = args.source, args.external_key
    if args.source_ref.strip() and not (source or key):
        source, key = _ref_key(args.source_ref, args.title)
    return source, key


def capture_deadline_add(svc: Any, args: Any) -> dict:
    source, key = _external_key(args)
    row = svc.store.deadline_by_key(source, key) if (source and key) else None
    return {"row": _dl_row(row) if row else None}


def track_deadline_add(svc: Any, args: Any, result: dict) -> dict:
    row = _live_deadline(svc, result["deadline_id"])
    return {"objects": [_dl_path(row)] if row else [], "ids": [f"deadline_id={result['deadline_id']}"],
            "etag": _digest(_dl_sig(row)) if row else ""}


def undo_deadline_add(svc: Any, record: dict, dry_run: bool = False) -> dict:
    tid = _id_of(record, "deadline_id")
    snap = _before(record).get("row")
    row = _live_deadline(svc, tid)
    if row is None:
        if snap:
            raise _conflict("The deadline no longer exists.")
        return {"already_gone": True}
    if _digest(_dl_sig(row)) != record.get("etag"):
        raise _conflict("The deadline was changed after this write (in the web interface or by another session).")
    if snap is None:
        if dry_run:
            return {"would_delete_deadline": tid}
        svc.store.delete_deadline(tid)
        return {"deleted_deadline": tid}
    if _dl_sig(snap) == _dl_sig(row):
        return {"unchanged": True}
    if dry_run:
        return {"would_restore_deadline": tid}
    _write_deadline(svc, snap)
    return {"restored_deadline": tid}


def capture_deadline(svc: Any, args: Any) -> dict:
    return {"row": _dl_row(svc.store.deadline(args.deadline))}


def capture_deadline_by_key(svc: Any, args: Any) -> dict:
    row = svc.store.deadline_by_key(args.source, args.external_key)
    return {"row": _dl_row(row) if row else None}


def track_deadline(svc: Any, args: Any, result: dict) -> dict:
    return _track_row(svc, args.deadline)


def track_deadline_by_key(svc: Any, args: Any, result: dict) -> dict:
    row = svc.store.deadline_by_key(args.source, args.external_key)
    return _track_row(svc, row["id"] if row else "")


def _track_row(svc: Any, tid: str) -> dict:
    row = _live_deadline(svc, tid.strip()) if tid else None
    if row is None:
        return {"objects": [], "ids": [], "etag": ""}
    return {"objects": [_dl_path(row)], "ids": [f"deadline_id={row['id']}"], "etag": _digest(_dl_sig(row))}


def undo_deadline(svc: Any, record: dict, dry_run: bool = False) -> dict:
    snap = _before(record).get("row")
    if not snap:
        return {"unchanged": True}
    row = _live_deadline(svc, snap["id"])
    if row is None:
        raise _conflict("The deadline no longer exists.")
    if _digest(_dl_sig(row)) != record.get("etag"):
        raise _conflict("The deadline was changed after this write (in the web interface or by another session).")
    if _dl_sig(snap) == _dl_sig(row):
        return {"unchanged": True}
    if dry_run:
        return {"would_restore_deadline": snap["id"], "title": snap["title"], "date": snap["date"], "state": snap["state"]}
    _write_deadline(svc, snap)
    return {"restored_deadline": snap["id"]}


def track_deadline_delete(svc: Any, args: Any, result: dict) -> dict:
    return {"objects": [], "ids": [f"deadline_id={args.deadline}"], "etag": "deleted"}


def undo_deadline_delete(svc: Any, record: dict, dry_run: bool = False) -> dict:
    snap = _before(record).get("row")
    if not snap:
        return {"unchanged": True}
    if svc.db.one("SELECT 1 FROM deadlines WHERE id = ?", (snap["id"],)) is not None:
        raise _conflict("A deadline with that id exists again.")
    if snap.get("doc_id") and svc.store.find_document(snap["doc_id"]) is None:
        raise _conflict("The document the deadline belonged to no longer exists.")
    if snap.get("source") and snap.get("external_key") and svc.store.deadline_by_key(snap["source"], snap["external_key"]) is not None:
        raise _conflict(f"{snap['source']} has created that deadline again.")
    if dry_run:
        return {"would_restore_deadline": snap["id"], "title": snap["title"], "date": snap["date"]}
    _write_deadline(svc, snap)
    return {"restored_deadline": snap["id"]}


def track_minutes(svc: Any, args: Any, result: dict) -> dict:
    ids = [r["deadline_id"] for r in result.get("added") or [] if r.get("deadline_id")]
    rows = [r for r in (_live_deadline(svc, i) for i in ids) if r]
    return {"objects": [_dl_path(r) for r in rows], "ids": [f"deadline_id={r['id']}" for r in rows],
            "etag": _digest(sorted((_dl_sig(r) for r in rows), key=lambda d: d["id"])) if rows else ""}


def undo_minutes(svc: Any, record: dict, dry_run: bool = False) -> dict:
    ids = _ids_of(record, "deadline_id")
    if not ids:
        return {"unchanged": True}
    rows = [r for r in (_live_deadline(svc, i) for i in ids) if r]
    if not rows:
        return {"already_gone": True}
    if len(rows) != len(ids) or _digest(sorted((_dl_sig(r) for r in rows), key=lambda d: d["id"])) != record.get("etag"):
        raise _conflict("Some of the deadlines made from these minutes were changed or removed afterwards.")
    if dry_run:
        return {"would_delete_deadlines": len(rows)}
    with svc.db.transaction():
        for r in rows:
            svc.store.delete_deadline(r["id"])
    return {"deleted_deadlines": len(rows)}


# ------------------------------------------------------------------------------------------------ doc_add_file / doc_add_text
# ``track`` does not get what ``capture`` returned, and it must tell the documents this call made from the ones it found already filed
# (a mail with several attachments can hold both). Capture leaves the high-water mark of the document rowids in a small note keyed by
# the validated arguments object, which the router hands to both hooks of one call; if the note is missing nothing is claimed.

_MARKS: "OrderedDict[int, dict[str, Any]]" = OrderedDict()


def capture_add(svc: Any, args: Any) -> dict:
    row = svc.db.one("SELECT MAX(rowid) AS m FROM documents")
    existing = _existing_by_content(svc, args)
    before = {"max_rowid": int(row["m"] or 0) if row is not None else 0, "existing": _doc_snapshot(svc, existing["id"]) if existing else None}
    _MARKS[id(args)] = before
    while len(_MARKS) > 64:
        _MARKS.popitem(last=False)
    return before


def track_add(svc: Any, args: Any, result: dict) -> dict:
    before = _MARKS.pop(id(args), None) or {"max_rowid": 1 << 62, "existing": None}
    born = _born_ids(svc, result, int(before["max_rowid"]))
    existing = before.get("existing")
    objects = [f"doc:{d}" for d in born]
    ids = [f"doc_id={d}" for d in born]
    sigs = {d: _born_sig(svc, d) for d in born}
    edited: dict[str, str | None] = {}
    if existing and existing["id"] not in born:
        objects += [f"doc:{existing['id']}/meta", f"doc:{existing['id']}/deadlines"]
        ids.append(f"edited_doc_id={existing['id']}")
        edited[existing["id"]] = _doc_sig(svc, existing["id"])
    return {"objects": objects, "ids": ids, "etag": _digest({"born": sigs, "edited": edited}) if (born or edited) else ""}


def undo_add(svc: Any, record: dict, dry_run: bool = False) -> dict:
    born = _ids_of(record, "doc_id")
    edited_ids = _ids_of(record, "edited_doc_id")
    if not born and not edited_ids:
        return {"unchanged": True}
    present = [d for d in born if svc.store.find_document(d) is not None]
    snap = (_before(record).get("existing") or None) if edited_ids else None
    sigs = {d: _born_sig(svc, d) for d in born}
    edited = {snap["id"]: _doc_sig(svc, snap["id"])} if snap else {}
    if not present and not snap:
        return {"already_gone": True}
    if len(present) != len(born) or _digest({"born": sigs, "edited": edited}) != record.get("etag"):
        raise _conflict("The documents were corrected, or their deadlines changed, after they were filed.")
    if dry_run:
        return {"would_delete_documents": len(present), **({"would_restore_document": snap["id"]} if snap else {})}
    for did in present:
        svc.engine.delete_document(did)
    out: dict[str, Any] = {"deleted_documents": len(present),
                           "note": "Notifications and links to other apps made when it was filed are not taken back."}
    if snap:
        out["restored_document"] = snap["id"]
        out.update(_restore_document(svc, snap))
    return out


# ------------------------------------------------------------------------------------------------ mail, folders and settings

def capture_mail(svc: Any, args: Any) -> dict:
    mail = svc.store.mail(args.message_id)
    return {"message_id": args.message_id, "state": mail["state"] if mail else None}


def track_mail(svc: Any, args: Any, result: dict) -> dict:
    mail = svc.store.mail(args.message_id)
    return {"objects": [f"mail:{args.message_id}"], "ids": [f"message_id={args.message_id}"], "etag": mail["state"] if mail else ""}


def undo_mail(svc: Any, record: dict, dry_run: bool = False) -> dict:
    before = _before(record)
    mail = svc.store.mail(before.get("message_id") or "")
    if mail is None or not before.get("state"):
        raise _conflict("The mail is no longer there.")
    if mail["state"] != record.get("etag"):
        raise _conflict("The mail was filed, ignored or reviewed again after this write.")
    if mail["state"] == before["state"]:
        return {"unchanged": True}
    if dry_run:
        return {"would_restore_mail_state": before["state"]}
    svc.store.set_mail_state(before["message_id"], before["state"])
    return {"restored_mail_state": before["state"]}


def _seen_rows(svc: Any, folder: str) -> list[dict[str, Any]]:
    prefix = folder.rstrip("/\\")
    return [dict(r) for r in svc.db.query("SELECT * FROM files_seen WHERE path LIKE ? OR path LIKE ?", (prefix + "/%", prefix + "\\%"))]


def capture_folders(svc: Any, args: Any) -> dict:
    from pathlib import Path
    out: dict[str, Any] = {"raw": svc.db.get_setting("folders.watch", None), "watched": svc.engine.watched_folders()}
    try:
        resolved = str(Path(args.path).expanduser().resolve())
    except OSError:
        resolved = args.path
    out["seen"] = _seen_rows(svc, resolved) if resolved in out["watched"] or args.path in out["watched"] else []
    return out


def track_folders(svc: Any, args: Any, result: dict) -> dict:
    return {"objects": ["settings:folders.watch"], "ids": [], "etag": _digest(svc.db.get_setting("folders.watch", None))}


def undo_folders(svc: Any, record: dict, dry_run: bool = False) -> dict:
    before = _before(record)
    raw = svc.db.get_setting("folders.watch", None)
    if _digest(raw) != record.get("etag"):
        raise _conflict("The list of watched folders changed after this write.")
    if raw == before.get("raw"):
        return {"unchanged": True}
    added = [f for f in svc.engine.watched_folders() if f not in (before.get("watched") or [])]
    if dry_run:
        return {"would_restore_folders": before.get("watched"), "would_stop_watching": added}
    with svc.db.transaction():
        if before.get("raw") is None:
            svc.db.execute("DELETE FROM settings WHERE key = 'folders.watch'")
        else:
            svc.db.set_setting("folders.watch", before["raw"])
        for folder in added:
            svc.store.forget_folder(folder)
        for r in before.get("seen") or []:
            svc.db.execute("INSERT OR REPLACE INTO files_seen(path, mtime, size, sha, ts) VALUES (?, ?, ?, ?, ?)",
                           (r["path"], r["mtime"], r["size"], r["sha"], r["ts"]))
    return {"restored_folders": svc.engine.watched_folders(), "note": "Documents already filed from a folder stay."}


def capture_settings(svc: Any, args: Any) -> dict:
    return {"values": {k: svc.db.get_setting(k, None) for k in args.values}}


def track_settings(svc: Any, args: Any, result: dict) -> dict:
    return {"objects": [f"settings:{k}" for k in args.values], "ids": [],
            "etag": _digest({k: svc.db.get_setting(k, None) for k in args.values})}


def undo_settings(svc: Any, record: dict, dry_run: bool = False) -> dict:
    values = _before(record).get("values") or {}
    now = {k: svc.db.get_setting(k, None) for k in values}
    if _digest(now) != record.get("etag"):
        raise _conflict("One of those settings was changed after this write.")
    todo = {k: v for k, v in values.items() if now.get(k) != v}
    if not todo:
        return {"unchanged": True}
    if dry_run:
        return {"would_restore_settings": sorted(todo)}
    with svc.db.transaction():
        for key, old in todo.items():
            if old is None:
                svc.db.execute("DELETE FROM settings WHERE key = ?", (key,))
            else:
                svc.db.set_setting(key, old)
    if any(k.startswith("mail.") for k in todo):
        forget = getattr(getattr(svc, "mail", None), "forget_interest", None)
        if forget:
            forget()
    return {"restored_settings": sorted(todo)}


# ------------------------------------------------------------------------------------------------ the table

#: tool name -> the hooks to attach (``dataclasses.replace(tool, **HOOKS[name])``)
HOOKS: dict[str, dict[str, Callable[..., Any]]] = {
    "doc_update": {"capture": capture_doc, "track": track_doc, "undo": undo_doc},
    "doc_reprocess": {"capture": capture_doc, "track": track_doc, "undo": undo_doc},
    "doc_add_file": {"capture": capture_add, "track": track_add, "undo": undo_add},
    "doc_add_text": {"capture": capture_add, "track": track_add, "undo": undo_add},
    "deadline_add": {"capture": capture_deadline_add, "track": track_deadline_add, "undo": undo_deadline_add},
    "deadline_update": {"capture": capture_deadline, "track": track_deadline, "undo": undo_deadline},
    "deadline_update_by_key": {"capture": capture_deadline_by_key, "track": track_deadline_by_key, "undo": undo_deadline},
    "deadline_delete": {"capture": capture_deadline, "track": track_deadline_delete, "undo": undo_deadline_delete},
    "deadlines_from_minutes": {"track": track_minutes, "undo": undo_minutes},
    "mail_ignore": {"capture": capture_mail, "track": track_mail, "undo": undo_mail},
    "folder_add": {"capture": capture_folders, "track": track_folders, "undo": undo_folders},
    "folder_remove": {"capture": capture_folders, "track": track_folders, "undo": undo_folders},
    "settings_set": {"capture": capture_settings, "track": track_settings, "undo": undo_settings},
}

#: writes that create or edit drafts and never delete, send or read from outside: what a token with the ``drafts`` profile may call
DRAFT_SAFE = {"doc_add_file", "doc_add_text", "doc_update", "doc_reprocess", "deadline_add", "deadline_update", "deadline_update_by_key",
              "deadlines_from_minutes", "mail_ignore", "pdf_merge", "pdf_split", "pdf_pages", "pdf_compress", "pdf_watermark", "pdf_metadata_set",
              "pdf_from_images", "pdf_from_office", "pdf_to_images", "images_compress"}
