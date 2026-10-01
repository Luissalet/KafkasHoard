"""Persistence: schema, FTS5 search, series, deadlines filters, notifications and mails."""

from __future__ import annotations


import pytest

import docs
from kafka_hoard.db import Database
from kafka_hoard.errors import KafkaError
from kafka_hoard.store import HIT_CLOSE, HIT_OPEN, Store, fts_query


@pytest.fixture
def store(tmp_path, clock):
    db = Database(tmp_path / "t.db")
    yield Store(db, clock)
    db.close()


def new_doc(store, **kw):
    base = dict(title="Doc", kind="invoice", issuer="Demo", issuer_key="demo", state="ok")
    base.update(kw)
    return store.create_document(**base)


def test_schema_is_wal_with_fts_and_versioned(tmp_path):
    db = Database(tmp_path / "x.db")
    assert db.one("PRAGMA journal_mode")[0] == "wal" and db.version() >= 1
    assert db.one("SELECT name FROM sqlite_master WHERE name = 'pages_fts'") is not None
    db.close()
    again = Database(tmp_path / "x.db")           # reopening runs no migration twice
    assert again.version() >= 1
    again.close()


def test_settings_roundtrip(tmp_path):
    db = Database(tmp_path / "s.db")
    assert db.get_setting("a") is None and db.get_setting("a", "x") == "x"
    db.set_setting("a", "1")
    db.set_setting("a", "2")
    assert db.get_setting("a") == "2"
    db.close()


def test_documents_filters_and_ordering(store):
    a = new_doc(store, title="Seguro", kind="insurance", issuer="Mapfre", issuer_key="mapfre", issue_date="2025-12-01", ref="P-1")
    b = new_doc(store, title="Luz", kind="bill", issuer="Iberdrola", issuer_key="iberdrola", issue_date="2026-09-01")
    c = new_doc(store, title="Viejo", kind="bill", issuer="Iberdrola", issuer_key="iberdrola", issue_date="2024-01-01", state="archived")
    assert [d["id"] for d in store.documents()] == [b["id"], a["id"], c["id"]]
    assert [d["id"] for d in store.documents(exclude_archived=True)] == [b["id"], a["id"]]
    assert [d["id"] for d in store.documents(kind="insurance")] == [a["id"]]
    assert [d["id"] for d in store.documents(issuer="iberdrola", exclude_archived=True)] == [b["id"]]
    assert [d["id"] for d in store.documents(year="2025")] == [a["id"]]
    assert [d["id"] for d in store.documents(text="P-1")] == [a["id"]]
    assert store.documents(state="archived")[0]["id"] == c["id"]
    assert store.counts()["documents"] == 2 and store.counts()["archived"] == 1


def test_json_columns_roundtrip(store):
    d = new_doc(store, tags=["a", "b"], facts={"edited": ["kind"], "n": 1})
    got = store.document(d["id"])
    assert got["tags"] == ["a", "b"] and got["facts"]["edited"] == ["kind"]
    store.update_document(d["id"], tags=["z"])
    assert store.document(d["id"])["tags"] == ["z"]


def test_unknown_document_is_not_found(store):
    with pytest.raises(KafkaError) as e:
        store.document("d_nope")
    assert e.value.code == "not_found" and store.find_document("d_nope") is None


def test_fts_query_neutralises_operators():
    assert fts_query('seguro "hogar" OR drop*') == '"seguro"* "hogar"* "or"* "drop"*'
    assert fts_query("   ") == "" and fts_query("---") == ""


def test_search_is_accent_insensitive_prefix_and_marks_hits(store):
    d = new_doc(store, title="Póliza del hogar", issuer="Mapfre", issuer_key="mapfre")
    store.set_pages(d["id"], d["title"], "Mapfre", ["Primera página sin nada", "Prima total anual: 320,00 euros. Tomador Ana"])
    hits = store.search("prima total")
    assert len(hits) == 1 and hits[0]["page"] == 2 and HIT_OPEN in hits[0]["snippet"] and HIT_CLOSE in hits[0]["snippet"]
    assert store.search("poliza") and store.search("MAPFRE") and store.search("hog")        # title and issuer are indexed too
    assert store.search("zzzz") == [] and store.search("") == [] and store.search('"""') == []


def test_search_filters_and_archived_exclusion(store):
    a = new_doc(store, kind="insurance", issuer_key="mapfre", issuer="Mapfre", issue_date="2025-12-01")
    b = new_doc(store, kind="bill", issuer_key="iberdrola", issuer="Iberdrola", issue_date="2026-09-01")
    for d in (a, b):
        store.set_pages(d["id"], d["title"], d["issuer"], ["texto común del documento"])
    assert len(store.search("común")) == 2
    assert [h["doc_id"] for h in store.search("común", kind="bill")] == [b["id"]]
    assert [h["doc_id"] for h in store.search("común", year="2025")] == [a["id"]]
    store.update_document(a["id"], state="archived")
    assert [h["doc_id"] for h in store.search("común")] == [b["id"]]
    assert len(store.search("común", state="archived")) == 1


def test_reindexing_replaces_old_pages_and_delete_cleans_the_index(store):
    d = new_doc(store)
    store.set_pages(d["id"], "Doc", "", ["alfa"])
    store.set_pages(d["id"], "Doc", "", ["beta"])
    assert store.search("alfa") == [] and store.search("beta")
    store.delete_document(d["id"])
    assert store.search("beta") == []


def test_editing_title_or_issuer_updates_the_index(store):
    d = new_doc(store, title="Original")
    store.set_pages(d["id"], "Original", "", ["cuerpo"])
    store.update_document(d["id"], title="Renombrado único")
    store._reindex_meta(d["id"])
    assert store.search("renombrado")


def test_series_lookup_by_reference_or_kind(store):
    s1 = store.create_series("insurance", "mapfre", "P-1", "Mapfre · P-1")
    s2 = store.create_series("subscription", "streamdemo", "", "Streamdemo · Suscripción")
    assert store.find_series("mapfre", "P-1")["id"] == s1["id"]
    assert store.find_series("streamdemo", "", "subscription")["id"] == s2["id"]
    assert store.find_series("mapfre", "P-2") is None
    d = new_doc(store, series_id=s1["id"], issue_date="2026-01-01")
    assert [x["id"] for x in store.series_documents(s1["id"])] == [d["id"]]
    assert store.drop_empty_series() == 1 and store.find_series("streamdemo", "", "subscription") is None


def test_deadline_filters(store):
    d = new_doc(store)
    mk = lambda title, date, **kw: store.create_deadline(doc_id=d["id"], kind=kw.pop("kind", "payment"), title=title, date=date, basis="xx",  # noqa: E731
                                                         confidence=80, state=kw.pop("state", "open"), remind=[3], notified=[], key=title, auto=True, **kw)
    a, b, c = mk("A", "2026-10-02"), mk("B", "2026-11-01", kind="renewal"), mk("C", "2026-09-01", state="done")
    assert [x["title"] for x in store.deadlines(states=["open"])] == ["A", "B"]
    assert [x["title"] for x in store.deadlines(states=["open"], date_to="2026-10-15")] == ["A"]
    assert [x["title"] for x in store.deadlines(kind="renewal")] == ["B"]
    assert [x["title"] for x in store.deadlines(states=["done"], newest_first=True)] == ["C"]
    assert [x["title"] for x in store.deadlines(text="B")] == ["B"]
    store.update_deadline(a["id"], archived=True)
    assert [x["title"] for x in store.deadlines(states=["open"])] == ["B"]
    assert len(store.deadlines(states=["open"], include_archived=True)) == 2
    assert store.deadline(b["id"])["remind"] == [3] and store.counts()["deadlines_open"] == 1
    store.delete_document(d["id"])
    assert store.deadlines(include_archived=True) == []


def test_mails_and_known_ids(store):
    msg = {"message_id": "m1", "ts": 1.0, "from_address": "a@b.c", "from_name": "A", "subject": "S", "text": "hola " * 100, "attachments": [{"name": "x.pdf"}]}
    store.save_mail(msg, kind="maybe", score=5, state="new", doc_ids=[], reasons=["r"])
    got = store.mail("m1")
    assert got["kind"] == "maybe" and got["reasons"] == ["r"] and got["attachments"][0]["name"] == "x.pdf" and len(got["snippet"]) <= 400
    assert store.known_message_ids() == ["m1"] and store.counts()["mails_review"] == 1
    store.set_mail_state("m1", "filed", ["d_1"])
    assert store.mail("m1")["doc_ids"] == ["d_1"] and store.mails(state=["new"]) == [] and len(store.mails(kind=["maybe"])) == 1
    assert store.mail("none") is None


def test_notifications_dedupe_and_seen(store, clock):
    store.add_notification(doc_id=None, deadline_id=None, type_="deadline_soon", severity="low", title="T", body="", results=[{"ok": True}], dedupe="k1")
    assert store.notified("k1") and not store.notified("k2")
    assert store.notifications()[0]["results"] == [{"ok": True}] and store.counts()["unseen_notifications"] == 1
    assert store.mark_notifications_seen(clock() + 1) == 1 and store.notifications(unseen=True) == []


def test_files_seen_and_forget_folder(store):
    store.mark_file_seen("/a/b/x.pdf", 10.0, 5, "sha")
    store.mark_file_seen("/a/c/y.pdf", 10.0, 5, "sha2")
    assert store.file_seen("/a/b/x.pdf")["size"] == 5 and store.file_seen("/zzz") is None
    assert store.forget_folder("/a/b") == 1 and store.file_seen("/a/b/x.pdf") is None and store.file_seen("/a/c/y.pdf")


def test_runs_are_listed_newest_first(store):
    store.add_run("mail", "", True, 5, "one")
    store.add_run("folders", "", False, 7, "two")
    assert [r["detail"] for r in store.runs()] == ["two", "one"] and [r["detail"] for r in store.runs("mail")] == ["one"]


def test_sha_users_counts_other_documents(store):
    a = new_doc(store, file_sha="abc")
    b = new_doc(store, file_sha="abc")
    assert store.sha_users("abc") == 2 and store.sha_users("abc", excluding=a["id"]) == 1 and b
    assert store.document_by_sha("abc") and store.document_by_sha("zzz") is None
    assert docs.TODAY
