"""The edit forms send only the fields the user changed. doc_update and deadline_update must therefore merge just those fields
and leave everything else (including an edit made meanwhile by the assistant) as it is."""
from __future__ import annotations

import docs
from conftest import tool


def _policy(svc):
    return svc.engine.ingest_text("policy", docs.INSURANCE)["document"]["id"]


def test_doc_update_merges_only_the_fields_sent(svc):
    did = _policy(svc)
    tool(svc, "doc_update", doc=did, title="Agent title", issuer="Agent issuer")        # the assistant edits while a form is open
    tool(svc, "doc_update", doc=did, notes="my note")                                      # the user saves only the notes
    d = svc.store.document(did)
    assert d["notes"] == "my note" and d["title"] == "Agent title" and d["issuer"] == "Agent issuer"
    tool(svc, "doc_update", doc=did, notes="")                                             # an explicit clear is a change too
    assert svc.store.document(did)["notes"] == "" and svc.store.document(did)["title"] == "Agent title"


def test_deadline_update_merges_only_the_fields_sent(svc):
    did = _policy(svc)
    target = svc.store.doc_deadlines(did)[0]
    tool(svc, "deadline_update", deadline=target["id"], title="Agent title", date="2031-03-04")
    tool(svc, "deadline_update", deadline=target["id"], notes="mine", remind=[7, 0])
    d = svc.store.deadline(target["id"])
    assert d["notes"] == "mine" and d["remind"] == [7, 0]
    assert d["title"] == "Agent title" and d["date"] == "2031-03-04" and d["recurring"] == target["recurring"]
