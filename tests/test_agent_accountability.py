"""Agent sessions: mandatory reasons, the write journal, undoing a whole session and the token profiles, on the real app."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import docs
from kafka_hoard.hoard_link import tokens as link_tokens
from test_family import MINUTES, minutes_call
from test_workshop import nothing_installed, write_pdf


def agent_call(client, name, arguments=None, *, agent="agent-a", session="s1", reason="Because the person asked for it", token=None, **extra):
    headers = {"Authorization": f"Bearer {token or client.svc.token}", "X-Agent-Id": agent, "X-Agent-Session": session}
    body = {"name": name, "arguments": arguments or {}}
    if reason is not None:
        body["reason"] = reason
    body.update(extra)
    return client.post("/api/agent/call", json=body, headers=headers)


def ok(response):
    assert response.status_code == 200, response.text
    return response.json()


def undo(client, session, *, agent="agent-a", token=None, **body):
    headers = {"Authorization": f"Bearer {token or client.svc.token}", "X-Agent-Id": agent}
    if body.get("confirm"):
        body.setdefault("reason", "Taking the session back")
    return client.post("/api/agent/undo", json={"session": session, **body}, headers=headers)


def journal(client, **params):
    return client.get("/api/agent/journal", params=params, headers={"Authorization": f"Bearer {client.svc.token}"}).json()


def doc_of(client, did):
    return client.svc.store.document(did)


def state_of(client, did):
    """Everything an undo is supposed to put back: the document fields and every deadline of it."""
    d = doc_of(client, did)
    keys = ("title", "kind", "issuer", "ref", "amount", "issue_date", "period_from", "period_to", "item", "state", "tags", "notes")
    return ({k: d[k] for k in keys}, (d["facts"] or {}).get("edited"),
            [{k: t[k] for k in ("id", "kind", "title", "date", "state", "edited", "recurring", "remind", "key")} for t in client.svc.store.doc_deadlines(did)])


@pytest.fixture
def ready(client):
    """An insurance policy and a fine filed by the services (not by an agent): documents with deadlines, and a deadline kept for another app."""
    out = {}
    for name, text in (("policy", docs.INSURANCE), ("fine", docs.FINE)):
        r = client.svc.engine.ingest_text(name, text)
        out[name] = r["document"]["id"]
    return out


# ---------------------------------------------------------------- reasons

def test_a_write_without_a_reason_is_refused_with_a_hint(client):
    r = agent_call(client, "doc_add_text", {"title": "Nota", "text": docs.RECEIPT}, reason=None)
    assert r.status_code == 400 and r.json()["code"] == "reason_required" and r.json()["hint"]
    assert agent_call(client, "doc_add_text", {"title": "Nota", "text": docs.RECEIPT}, reason="no").status_code == 400      # shorter than 3 characters
    assert ok(agent_call(client, "kafka_overview", reason=None))["counts"]["documents"] == 0                              # reads need no reason
    assert client.svc.store.documents() == []                                                                              # and nothing was filed


def test_the_web_ui_is_exempt_from_reasons(client):
    r = client.post("/api/ui/call", json={"name": "doc_add_text", "arguments": {"title": "Desde la interfaz", "text": docs.RECEIPT}})
    assert r.status_code == 200 and r.json()["created"] is True
    assert journal(client)["entries"] == []                       # the interface is not an agent: nothing in the agent journal


def test_the_catalogue_tells_agents_about_reasons_and_draft_safe_tools(client):
    tools = {t["name"]: t for t in client.get("/api/agent/tools").json()["tools"]}
    assert "reason" in tools["deadline_add"]["inputSchema"]["properties"] and "reason" in tools["deadline_add"]["inputSchema"]["required"]
    assert "reason" not in tools["deadlines_list"]["inputSchema"]["properties"]
    assert "reason" in tools["pdf_pages"]["inputSchema"]["properties"]
    for name in ("doc_update", "doc_add_text", "deadline_add", "deadline_update", "pdf_merge", "mail_ignore"):
        assert tools[name]["annotations"]["draftSafeHint"] is True, name
    for name in ("doc_delete", "deadline_delete", "secret_set", "settings_set", "folder_add", "folder_remove", "mail_scan", "mail_accept",
                 "notify_test", "pdf_protect", "tax_pack", "document_link_tx", "housekeeping_run", "phileas_sync"):
        assert "draftSafeHint" not in tools[name]["annotations"], name
    assert client.get("/api/agent/tools").json()["reasons_required"] is True


# ---------------------------------------------------------------- a session files, corrects and closes, then undoes it all

def test_undoing_a_session_restores_the_state_before_it(client, ready):
    policy = ready["policy"]
    before = state_of(client, policy)
    filed = ok(agent_call(client, "doc_add_text", {"title": "Ticket", "text": docs.RECEIPT}))
    new_doc = filed["documents"][0]["id"]
    ok(agent_call(client, "doc_update", {"doc": policy, "title": "Seguro del hogar corregido", "issuer": "Otra Aseguradora", "amount": 111.0, "state": "ok"}))
    manual = ok(agent_call(client, "deadline_add", {"title": "Llamar al corredor", "date": "2026-11-05", "kind": "custom"}))["deadline_id"]
    target = client.svc.store.doc_deadlines(policy)[0]["id"]
    ok(agent_call(client, "deadline_update", {"deadline": target, "date": "2026-12-31", "state": "done"}))
    assert doc_of(client, policy)["title"] == "Seguro del hogar corregido"

    entries = journal(client, session="s1")["entries"]
    assert sorted(e["tool"] for e in entries) == ["deadline_add", "deadline_update", "doc_add_text", "doc_update"]
    assert all(e["agent"] == "agent-a" and e["reason"] and e["ok"] and e["kind"] == "write" and e["undoable"] for e in entries)

    dry = ok(undo(client, "s1", dry_run=True))
    assert dry["complete"] is True and len(dry["would_undo"]) == 4 and doc_of(client, policy)["title"] == "Seguro del hogar corregido"
    refused = undo(client, "s1")                                    # no confirm: nothing happens
    assert refused.status_code == 400 and refused.json()["code"] == "confirm_required"
    assert client.svc.store.find_document(new_doc) is not None

    done = ok(undo(client, "s1", confirm=True, reason="The person did not want these changes"))
    assert len(done["undone"]) == 4 and not done["conflicts"] and not done["not_undoable"] and done["complete"] is True
    assert state_of(client, policy) == before
    assert client.svc.store.find_document(new_doc) is None          # the document the session filed is gone again, with its deadlines
    assert client.svc.store.deadlines(doc_id=new_doc, include_archived=True) == []
    assert client.svc.db.one("SELECT 1 FROM deadlines WHERE id = ?", (manual,)) is None
    kinds = [e["kind"] for e in journal(client, session="s1")["entries"]]
    assert kinds.count("undo") == 4 and kinds.count("write") == 4
    assert ok(undo(client, "s1", confirm=True))["undone"] == []     # a second go finds nothing left to do


def test_undo_puts_back_the_deadlines_a_correction_recomputed(client, ready):
    policy = ready["policy"]
    before = state_of(client, policy)
    assert before[2], "the policy has deadlines"
    ok(agent_call(client, "doc_update", {"doc": policy, "period_to": "2027-05-31", "kind": "contract"}))
    assert state_of(client, policy) != before
    ok(agent_call(client, "doc_reprocess", {"doc": policy}))
    done = ok(undo(client, "s1", confirm=True))
    assert len(done["undone"]) == 2 and done["complete"] is True
    assert state_of(client, policy) == before
    assert doc_of(client, policy)["facts"].get("edited") == before[1]


def test_undo_closes_nothing_it_did_not_open_deadlines_come_back_as_they_were(client, ready):
    fine = ready["fine"]
    rows = {t["id"]: t for t in client.svc.store.doc_deadlines(fine)}
    tid = next(iter(rows))
    ok(agent_call(client, "deadline_update", {"deadline": tid, "state": "dismissed", "notes": "No es mía"}))
    assert client.svc.store.deadline(tid)["state"] == "dismissed"
    ok(undo(client, "s1", confirm=True))
    back = client.svc.store.deadline(tid)
    assert (back["state"], back["notes"], back["edited"], back["date"], back["done_ts"]) == (rows[tid]["state"], rows[tid]["notes"], rows[tid]["edited"],
                                                                                             rows[tid]["date"], rows[tid]["done_ts"])


def test_a_deleted_deadline_comes_back_with_the_same_id(client, ready):
    tid = client.svc.store.doc_deadlines(ready["policy"])[0]["id"]
    row = client.svc.store.deadline(tid)
    refused = agent_call(client, "deadline_delete", {"deadline": tid})
    assert refused.status_code == 400 and refused.json()["code"] == "confirm_required"
    ok(agent_call(client, "deadline_delete", {"deadline": tid, "confirm": True}))
    assert client.svc.db.one("SELECT 1 FROM deadlines WHERE id = ?", (tid,)) is None
    assert ok(undo(client, "s1", dry_run=True))["complete"] is True
    ok(undo(client, "s1", confirm=True))
    back = client.svc.store.deadline(tid)
    assert {k: back[k] for k in ("title", "date", "kind", "state", "remind", "doc_id", "key", "basis", "evidence", "page")} == \
        {k: row[k] for k in ("title", "date", "kind", "state", "remind", "doc_id", "key", "basis", "evidence", "page")}


def test_deadlines_kept_for_another_app_go_back_to_their_previous_version(client):
    first = ok(agent_call(client, "deadline_add", {"title": "Revisar la caldera", "due": "2026-11-10", "source": "homehoard", "external_key": "caldera-1"}, session="a1"))
    assert first["action"] == "created"
    again = ok(agent_call(client, "deadline_add", {"title": "Revisar la caldera", "due": "2026-12-10", "source": "homehoard", "external_key": "caldera-1"}, session="a2"))
    assert again["action"] == "rescheduled" and again["deadline_id"] == first["deadline_id"]
    ok(agent_call(client, "deadline_update_by_key", {"source": "homehoard", "external_key": "caldera-1", "state": "done"}, session="a3"))
    tid = first["deadline_id"]
    assert client.svc.store.deadline(tid)["state"] == "done"
    ok(undo(client, "a3", confirm=True))
    assert client.svc.store.deadline(tid)["state"] == "open" and client.svc.store.deadline(tid)["date"] == "2026-12-10"
    ok(undo(client, "a2", confirm=True))
    assert client.svc.store.deadline(tid)["date"] == "2026-11-10"
    ok(undo(client, "a1", confirm=True))
    assert client.svc.db.one("SELECT 1 FROM deadlines WHERE id = ?", (tid,)) is None


def test_filing_a_document_that_is_already_there_changes_nothing_and_undo_keeps_it(client, ready):
    # the same text again: the document is found, not created, and applying a kind to it is an edit that undo takes back
    existing = ready["fine"]
    before = state_of(client, existing)
    r = ok(agent_call(client, "doc_add_text", {"title": "fine", "text": docs.FINE, "item": "Coche de Ana"}))
    assert r["created"] is False and r["documents"][0]["id"] == existing
    assert doc_of(client, existing)["item"] == "Coche de Ana"
    done = ok(undo(client, "s1", confirm=True))
    assert len(done["undone"]) == 1 and client.svc.store.find_document(existing) is not None
    assert state_of(client, existing) == before
    ok(agent_call(client, "doc_add_text", {"title": "fine", "text": docs.FINE}, session="s2"))      # nothing to apply: nothing to undo, nothing deleted
    ok(undo(client, "s2", confirm=True))
    assert client.svc.store.find_document(existing) is not None


def test_a_filed_file_is_removed_by_undo_with_its_stored_original(client, tmp_path):
    path = tmp_path / "recibo.txt"
    path.write_text(docs.RECEIPT, encoding="utf-8")
    r = ok(agent_call(client, "doc_add_file", {"path": str(path), "kind": "receipt", "item": "Aspirador"}))
    did = r["documents"][0]["id"]
    doc = doc_of(client, did)
    assert r["created"] is True and doc["item"] == "Aspirador" and client.svc.files.exists(doc["file_sha"], doc["file_ext"])
    refused = agent_call(client, "doc_add_file", {"path": "/etc/hostname"})
    assert refused.status_code in (400, 403)
    done = ok(undo(client, "s1", confirm=True))
    assert len(done["undone"]) == 1 and client.svc.store.find_document(did) is None
    assert not client.svc.files.exists(doc["file_sha"], doc["file_ext"]) and path.is_file()      # the copy Kafka kept goes, the person's file stays


def test_deadlines_from_minutes_are_removed_by_undo(client):
    client.svc.engine.family_call = minutes_call(MINUTES)
    client.svc.set_settings({"minutes.me": "Luis"})
    out = ok(agent_call(client, "deadlines_from_minutes", {"minutes_id": "mn1"}))
    assert len(out["added"]) == 3 and len(client.svc.store.deadlines(source="funes")) == 3
    done = ok(undo(client, "s1", confirm=True))
    assert len(done["undone"]) == 1 and client.svc.store.deadlines(source="funes") == []


def test_mail_folders_and_settings_go_back(client, tmp_path):
    client.svc.store.save_mail({"message_id": "m-1", "ts": 1.0, "subject": "Dudoso", "text": "Hola"}, kind="maybe", score=5, state="new", doc_ids=[], reasons=[])
    folder = tmp_path / "papeles"
    folder.mkdir()
    watched = client.svc.engine.watched_folders()
    ok(agent_call(client, "mail_ignore", {"message_id": "m-1"}))
    ok(agent_call(client, "folder_add", {"path": str(folder)}))
    ok(agent_call(client, "settings_set", {"values": {"warranty.years": 5, "remind.payment": "10,2"}}))
    assert client.svc.store.mail("m-1")["state"] == "ignored" and client.svc.setting("warranty.years") == "5"
    assert str(folder.resolve()) in client.svc.engine.watched_folders()
    plan = ok(undo(client, "s1", dry_run=True))
    assert len(plan["would_undo"]) == 3 and plan["complete"] is True
    done = ok(undo(client, "s1", confirm=True))
    assert len(done["undone"]) == 3
    assert client.svc.store.mail("m-1")["state"] == "new" and client.svc.setting("warranty.years") == "3"
    assert client.svc.setting("remind.payment") == ",".join(str(x) for x in __import__("kafka_hoard.model", fromlist=["x"]).DEFAULT_LEADS["payment"])
    assert client.svc.engine.watched_folders() == watched


def test_a_removed_folder_comes_back_with_the_files_it_had_seen(client, tmp_path):
    folder = tmp_path / "entrada"
    folder.mkdir()
    ok(agent_call(client, "folder_add", {"path": str(folder)}, session="add"))
    resolved = str(folder.resolve())
    client.svc.store.mark_file_seen(resolved + "/a.pdf", 1.0, 10, "ab" * 32)
    ok(agent_call(client, "folder_remove", {"path": str(folder)}))
    assert client.svc.store.file_seen(resolved + "/a.pdf") is None
    ok(undo(client, "s1", confirm=True))
    assert resolved in client.svc.engine.watched_folders() and client.svc.store.file_seen(resolved + "/a.pdf")["size"] == 10


# ---------------------------------------------------------------- sessions do not touch each other

def test_a_later_edit_by_another_session_is_a_conflict_and_is_left_alone(client, ready):
    policy = ready["policy"]
    original = doc_of(client, policy)["title"]
    ok(agent_call(client, "doc_update", {"doc": policy, "title": "Edición de A"}, session="sa", agent="agent-a"))
    ok(agent_call(client, "doc_update", {"doc": policy, "title": "Edición de B"}, session="sb", agent="agent-b"))

    plan = ok(undo(client, "sa", dry_run=True))
    assert plan["would_undo"] == [] and len(plan["conflicts"]) == 1 and plan["complete"] is False
    done = ok(undo(client, "sa", confirm=True))
    assert done["undone"] == [] and len(done["conflicts"]) == 1
    assert doc_of(client, policy)["title"] == "Edición de B"        # the other session's work survived
    assert [e["kind"] for e in journal(client, session="sb")["entries"]] == ["write"]

    ok(undo(client, "sb", agent="agent-b", confirm=True))           # once B is undone, A's edit can be taken back as well
    assert doc_of(client, policy)["title"] == "Edición de A"
    ok(undo(client, "sa", confirm=True))
    assert doc_of(client, policy)["title"] == original


def test_a_change_made_by_the_person_after_the_session_is_a_conflict_too(client, ready):
    policy = ready["policy"]
    tid = client.svc.store.doc_deadlines(policy)[0]["id"]
    ok(agent_call(client, "doc_update", {"doc": policy, "title": "Del agente"}))
    ok(agent_call(client, "deadline_update", {"deadline": tid, "notes": "Del agente"}))
    r = client.post("/api/ui/call", json={"name": "doc_update", "arguments": {"doc": policy, "notes": "De la persona"}})     # in the web interface
    assert r.status_code == 200
    done = ok(undo(client, "s1", confirm=True))
    assert [u["tool"] for u in done["undone"]] == ["deadline_update"]          # nobody touched that deadline: it goes back
    assert [c["tool"] for c in done["conflicts"]] == ["doc_update"]            # the person edited the document: it stays as the person left it
    assert client.svc.store.deadline(tid)["notes"] == ""
    assert doc_of(client, policy)["title"] == "Del agente" and doc_of(client, policy)["notes"] == "De la persona"


def test_a_document_edited_after_it_was_filed_is_not_deleted_by_undo(client):
    did = ok(agent_call(client, "doc_add_text", {"title": "Ticket", "text": docs.RECEIPT}))["documents"][0]["id"]
    client.svc.engine.update_document(did, {"title": "Mi ticket del aspirador"})            # the person corrects it
    done = ok(undo(client, "s1", confirm=True))
    assert done["undone"] == [] and len(done["conflicts"]) == 1
    assert client.svc.store.find_document(did)["title"] == "Mi ticket del aspirador"


def test_a_reminder_that_fired_meanwhile_is_not_a_conflict(client, ready):
    tid = client.svc.store.doc_deadlines(ready["policy"])[0]["id"]
    ok(agent_call(client, "deadline_update", {"deadline": tid, "notes": "Nota"}))
    client.svc.store.update_deadline(tid, notified=["30", "7"])                             # the scheduler marks reminders by itself
    assert len(ok(undo(client, "s1", confirm=True))["undone"]) == 1
    assert client.svc.store.deadline(tid)["notes"] == ""


def test_tools_without_a_way_back_are_reported_not_undone(client, ready, tmp_path):
    client.svc.workshop.env = nothing_installed()
    src = write_pdf(tmp_path / "in", "agente.pdf", 4)
    ok(agent_call(client, "doc_update", {"doc": ready["policy"], "notes": "Reclamada"}))
    made = ok(agent_call(client, "pdf_pages", {"action": "delete", "file": str(src), "pages": "2-3"}))["outputs"][0]["path"]
    ok(agent_call(client, "secret_set", {"name": "NTFY_TOPIC", "value": "mi-tema-secreto-1234"}))
    ok(agent_call(client, "doc_delete", {"doc": ready["fine"], "confirm": True}))
    assert client.svc.store.find_document(ready["fine"]) is None
    plan = ok(undo(client, "s1", dry_run=True))
    assert sorted(n["tool"] for n in plan["not_undoable"]) == ["doc_delete", "pdf_pages", "secret_set"] and plan["complete"] is False
    done = ok(undo(client, "s1", confirm=True))
    assert [u["tool"] for u in done["undone"]] == ["doc_update"] and doc_of(client, ready["policy"])["notes"] == ""
    assert client.svc.store.find_document(ready["fine"]) is None and done["complete"] is False
    assert Path(made).is_file() and src.is_file()          # the workshop output stays for the person, and the original was never touched


# ---------------------------------------------------------------- tokens and profiles

def mint(client, agent, profile):
    return link_tokens.mint_agent_token(client.svc.config.data_dir, agent, profile)["token"]


def test_a_read_only_token_cannot_write(client, ready):
    token = mint(client, "reader", "read_only")
    assert ok(agent_call(client, "docs_list", token=token, agent="spoofed"))["documents"]
    r = agent_call(client, "doc_update", {"doc": ready["policy"], "title": "No"}, token=token)
    assert r.status_code == 403 and r.json()["code"] == "profile_forbidden"
    assert doc_of(client, ready["policy"])["title"] != "No"


def test_a_drafts_token_may_file_and_correct_but_not_delete_send_or_read_from_outside(client, ready, tmp_path):
    client.svc.workshop.env = nothing_installed()
    src = write_pdf(tmp_path / "in", "borrador.pdf", 3)
    token = mint(client, "drafter", "drafts")
    policy = ready["policy"]
    ok(agent_call(client, "doc_update", {"doc": policy, "notes": "Borrador"}, token=token))
    ok(agent_call(client, "deadline_add", {"title": "Nuevo", "date": "2026-11-20"}, token=token))
    ok(agent_call(client, "doc_add_text", {"title": "Ticket", "text": docs.RECEIPT}, token=token))
    ok(agent_call(client, "pdf_pages", {"action": "extract", "file": str(src), "pages": "1"}, token=token))
    for name, args in (("doc_delete", {"doc": policy, "confirm": True}),
                       ("deadline_delete", {"deadline": client.svc.store.doc_deadlines(policy)[0]["id"], "confirm": True}),
                       ("secret_set", {"name": "NTFY_TOPIC", "value": "otro-tema-1234"}),
                       ("settings_set", {"values": {"warranty.years": 4}}),
                       ("folder_add", {"path": str(tmp_path)}),
                       ("mail_scan", {}), ("mail_accept", {"message_id": "m-x"}), ("notify_test", {"channel": "toast"}),
                       ("pdf_protect", {"action": "protect", "file": str(src), "password": "clave-de-prueba"}),
                       ("tax_pack", {}), ("housekeeping_run", {})):
        r = agent_call(client, name, args, token=token)
        assert r.status_code == 403 and r.json()["code"] == "profile_forbidden", name
    assert client.svc.store.find_document(policy) is not None and client.svc.setting("warranty.years") == "3"
    assert {e["agent"] for e in journal(client)["entries"]} == {"drafter"}         # the token fixes the agent: the journal says "drafter"
    assert undo(client, "s1", token=token, confirm=True).status_code == 403          # a drafts token may not undo (only the "all" profile can)


def test_an_all_token_can_undo_its_own_session(client, ready):
    token = mint(client, "writer", "all")
    ok(agent_call(client, "doc_update", {"doc": ready["policy"], "title": "Con token"}, token=token))
    done = ok(undo(client, "s1", token=token, confirm=True))
    assert len(done["undone"]) == 1 and doc_of(client, ready["policy"])["title"] != "Con token"


# ---------------------------------------------------------------- the journal itself

def test_the_journal_records_who_why_and_never_a_secret(client):
    ok(agent_call(client, "secret_set", {"name": "NTFY_TOPIC", "value": "tema-privado-xyz-98765"}, reason="Set the topic; password=hunter2hunter2",
                  agent="agent-z", session="zz"))
    assert client.svc.config.secret("NTFY_TOPIC") == "tema-privado-xyz-98765"                # it was really stored
    ok(agent_call(client, "doc_add_text", {"title": "Con clave", "text": docs.RECEIPT + "\nClave: sk-abcdefghijklmnopqrstuvwxyz0123456789\n"},
                  reason="File it", agent="agent-z", session="zz"))
    entries = journal(client, agent="agent-z")["entries"]
    assert {e["tool"] for e in entries} == {"secret_set", "doc_add_text"} and all(e["session"] == "zz" and e["ok"] and e["ids"] is not None for e in entries)
    text = json.dumps(entries) + (client.svc.config.data_dir / "agent_journal.jsonl").read_text(encoding="utf-8")
    for secret in ("tema-privado-xyz-98765", "hunter2hunter2", "sk-abcdefghij"):
        assert secret not in text, secret
    assert all(len(e["args_summary"]) <= 300 for e in entries)
    assert client.get("/api/agent/journal").status_code == 401


def test_failed_writes_are_journaled_but_never_undone(client, ready):
    r = agent_call(client, "doc_update", {"doc": "d_nope", "title": "x"})
    assert r.status_code == 404
    entries = journal(client, session="s1")["entries"]
    assert entries and entries[0]["ok"] is False
    assert ok(undo(client, "s1", confirm=True))["undone"] == []
