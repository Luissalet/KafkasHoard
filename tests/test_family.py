"""The family side: notifications through the hub, mail through the gateway, the agenda, bus events, the Ledger link and the
cross-app tools. The hub is never contacted: every client is a fake."""

from __future__ import annotations

import csv
import json
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
import pytest

import docs
from conftest import FakeMail, build, make_config
from kafka_hoard import agenda
from kafka_hoard import model as M
from kafka_hoard.mail.hub import INTEREST, HubMail, MailRouter
from kafka_hoard.notify import HUB_NOTIFY, Notifier
from pdfmaker import make_pdf

INVOICE_TEXT = ("MEDIAMARKT\nFactura nº F-2026-114\nNúmero de pedido: 402-1234567-7654321\nFecha de factura: 15/08/2026\n"
                "Total (IVA incluido): 249,90 €\n")


# ------------------------------------------------------------------ fakes
class FakeHub:
    """The family library's notification client."""

    def __init__(self, available=True, answer=None):
        self.up, self.answer, self.calls = available, answer, []

    def hub_available(self):
        return self.up

    def notify(self, title, body="", **kw):
        self.calls.append({"title": title, "body": body, **kw})
        return self.answer if self.answer is not None else {"ok": True, "id": 1, "held": "", "delivered": ["windows"]}


class FakeMailApi:
    """The family library's mail client over an in-memory gateway."""

    def __init__(self, rows=None, up=True):
        self.rows, self.up = list(rows or []), up
        self.interests, self.claims, self.pages = [], [], []

    def available(self, *a, **k):
        return self.up

    def register_interest(self, spec, sphere=None, timeout=10.0):
        self.interests.append(spec)
        return {"ok": True}

    def messages(self, since_id=0, limit=100, full=True, interest=True, timeout=20.0):
        self.pages.append(since_id)
        if not self.up:
            return {"ok": False, "error": "hub unreachable", "messages": [], "last_id": since_id}
        rows = [dict(r) for r in self.rows if r["id"] > since_id][:limit]
        return {"ok": True, "messages": rows, "last_id": rows[-1]["id"] if rows else since_id}

    def claim(self, ids, kind, ref, timeout=10.0):
        self.claims.append((ids, kind, ref))
        return {"ok": True}

    def copy_attachment(self, att, dest_dir, timeout=30.0):
        src = Path(att["path"])
        if not src.is_file():
            return ""
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        dest = Path(dest_dir) / (att.get("sha") or src.stem)
        dest = dest.with_suffix(src.suffix)
        shutil.copyfile(src, dest)
        return str(dest)


def hub_row(rid, mid, subject, text="", attachments=None, ts=None):
    return {"id": rid, "message_id": mid, "subject": subject, "from_name": "Banco Demo", "from_address": "avisos@bancodemo.example", "text": text,
            "ts": ts if ts is not None else docs.T0 - 3600, "account": "demo", "from_self": False, "attachments": attachments or []}


def make_notifier(tmp_path, settings=None, hub=None, **kw):
    cfg = make_config(tmp_path)
    store = dict(settings or {})
    toasts = []
    n = Notifier(cfg, lambda k, d=None: store.get(k, d), transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True})), platform="win32",
                 toast_backend=lambda title, body, url, high: toasts.append(title), hub_api=hub, **kw)
    return n, store, toasts


EVENT = {"id": "soon:t_1:2026-10-02:1", "type": "deadline_soon", "severity": "high", "title": "Mañana: Pago de Demo", "summary": "2 oct · 100,00 €",
         "url": "http://127.0.0.1:5200/#/documentos/d_1", "bus": "kafka.deadline.soon", "data": {"deadline_id": "t_1", "days_left": 1, "kind": "payment"}}


# ================================================================== notifications through the hub
def test_notify_via_hub_hands_the_notification_over_and_skips_the_own_channels(tmp_path):
    hub = FakeHub()
    n, _, toasts = make_notifier(tmp_path, {"notify.via": "hub"}, hub)
    results = n.send(EVENT, ["toast", "hub", "ntfy"])
    assert [c["title"] for c in hub.calls] == [EVENT["title"]]
    call = hub.calls[0]
    assert call["priority"] == "high" and call["group"] == "deadline" and call["dedupe_key"] == EVENT["id"] and call["url"] == EVENT["url"]
    assert toasts == []
    assert {r["channel"] for r in results} == {"hub", HUB_NOTIFY} and all(r["ok"] for r in results if r["channel"] == HUB_NOTIFY)


def test_notify_auto_uses_the_hub_when_it_is_up_and_the_own_channels_when_not(tmp_path):
    hub = FakeHub(available=False)
    n, _, toasts = make_notifier(tmp_path, {}, hub)
    n.send(EVENT, ["toast"])
    assert hub.calls == [] and toasts == [EVENT["title"]]
    hub.up = True
    n.send(EVENT, ["toast"])
    assert len(hub.calls) == 1 and toasts == [EVENT["title"]]          # the hub took it: no second toast


def test_notify_auto_falls_back_to_the_own_channels_when_the_hub_refuses(tmp_path):
    hub = FakeHub(answer={"ok": False, "error": "hub unreachable"})
    n, _, toasts = make_notifier(tmp_path, {}, hub)
    results = n.send(EVENT, ["toast"])
    assert toasts == [EVENT["title"]]
    by = {r["channel"]: r for r in results}
    assert by["toast"]["ok"] and by[HUB_NOTIFY]["ok"] is False and by[HUB_NOTIFY]["error"] == "hub unreachable"


def test_notify_hub_mode_never_falls_back_and_own_mode_never_asks_the_hub(tmp_path):
    hub = FakeHub(answer={"ok": False, "error": "refused"})
    n, store, toasts = make_notifier(tmp_path, {"notify.via": "hub"}, hub)
    results = n.send(EVENT, ["toast"])
    assert toasts == [] and results[-1]["channel"] == HUB_NOTIFY and not results[-1]["ok"]
    store["notify.via"] = "own"
    hub.calls.clear()
    n.send(EVENT, ["toast"])
    assert hub.calls == [] and toasts == [EVENT["title"]]


def test_a_held_notification_is_a_success_and_says_why(tmp_path):
    hub = FakeHub(answer={"ok": True, "held": "quiet", "delivered": []})
    n, _, toasts = make_notifier(tmp_path, {}, hub)
    results = n.send(EVENT, ["toast"])
    assert toasts == [] and results[-1]["held"] == "quiet" and results[-1]["ok"]


def test_hub_priority_maps_severities_and_makes_a_last_day_urgent():
    p = Notifier.hub_priority
    assert p({"severity": "low"}) == "low" and p({"severity": "medium"}) == "normal" and p({"severity": "high"}) == "high"
    last_day = {"severity": "high", "type": "deadline_soon", "data": {"days_left": 0, "kind": "fine_discount"}}
    assert p(last_day) == "urgent"
    assert p({**last_day, "data": {"days_left": 0, "kind": "payment"}}) == "high"
    assert p({**last_day, "type": "deadline_overdue"}) == "high"


def test_the_status_says_which_way_notifications_go(tmp_path):
    n, store, _ = make_notifier(tmp_path, {}, FakeHub())
    assert n.via_status() == {"setting": "auto", "hub_available": True, "effective": "hub"}
    store["notify.via"] = "own"
    assert n.via_status()["effective"] == "own"


def test_notify_via_is_a_setting_with_three_values(svc):
    assert svc.set_settings({"notify.via": "hub"})["notify.via"] == "hub"
    with pytest.raises(Exception):
        svc.set_settings({"notify.via": "carrier-pigeon"})


# ================================================================== mail through the hub
def make_router(svc_or_cfg, api, mode="auto", settings=None):
    store = {"mail.source": mode, **(settings or {})}
    return MailRouter(lambda k, d="": store.get(k, d), lambda name: "", hub_api=api), store


def test_hub_mail_returns_the_helpers_shape_copies_attachments_and_keeps_a_watermark(tmp_path):
    src = tmp_path / "gateway"
    src.mkdir()
    pdf = src / "abc.pdf"
    pdf.write_bytes(make_pdf(docs.UTILITY))
    api = FakeMailApi([hub_row(5, "<a@x>", "Su factura", attachments=[{"name": "factura.pdf", "size": 90_000, "path": str(pdf), "sha": "abc"},
                                                                    {"name": "logo.exe", "size": 10, "path": str(pdf)}]),
                       hub_row(7, "<b@x>", "Otra")])
    hub = HubMail(lambda k, d="": {}.get(k, d), api)
    out = hub.scan(since_days=14, limit=100, skip=[], attachments_dir=str(tmp_path / "cache"))
    assert out["ok"] and [m["message_id"] for m in out["messages"]] == ["<a@x>", "<b@x>"] and out["hub_last_id"] == 7
    first = out["messages"][0]
    assert first["hub_id"] == "5" and first["from_address"] == "avisos@bancodemo.example" and first["account"] == "demo"
    assert [a["name"] for a in first["attachments"]] == ["factura.pdf"] and Path(first["attachments"][0]["path"]).parent == tmp_path / "cache"


def test_hub_mail_resumes_after_the_stored_watermark_and_skips_known_mail(tmp_path):
    api = FakeMailApi([hub_row(i, f"<m{i}@x>", "Factura") for i in range(1, 6)])
    settings = {"mail.hub_since_id": "3"}
    hub = HubMail(lambda k, d="": settings.get(k, d), api)
    out = hub.scan(since_days=14, limit=100, skip=["<m4@x>"])
    assert api.pages[0] == 3 and [m["message_id"] for m in out["messages"]] == ["<m5@x>"] and out["hub_last_id"] == 5
    settings["mail.hub_since_id"] = "5"
    assert hub.scan(since_days=14, limit=100, skip=[])["messages"] == []                    # nothing new after the watermark moves on


def test_hub_mail_stops_at_the_limit_and_resumes_from_the_last_message_it_returned():
    api = FakeMailApi([hub_row(i, f"<m{i}@x>", "Factura") for i in range(1, 8)])
    hub = HubMail(lambda k, d="": {}.get(k, d), api)
    out = hub.scan(since_days=14, limit=3, skip=[])
    assert len(out["messages"]) == 3 and out["hub_last_id"] == 3


def test_a_query_or_a_longer_look_back_rereads_from_the_start_without_moving_the_watermark():
    api = FakeMailApi([hub_row(1, "<a@x>", "Seguro del coche"), hub_row(2, "<b@x>", "Otra cosa")])
    hub = HubMail(lambda k, d="": {"mail.hub_since_id": "2"}.get(k, d), api)
    out = hub.scan(since_days=14, limit=50, skip=[], query="seguro")
    assert [m["message_id"] for m in out["messages"]] == ["<a@x>"] and out["hub_last_id"] is None
    deep = hub.scan(since_days=60, limit=50, skip=[])
    assert len(deep["messages"]) == 2 and deep["hub_last_id"] is None


def test_the_router_reads_the_hub_when_it_is_on_and_the_helper_otherwise(tmp_path):
    api = FakeMailApi([hub_row(1, "<a@x>", "Factura")])
    router, store = make_router(None, api)
    helper = FakeMail([{"message_id": "<own@x>", "subject": "Del helper"}])
    router.own = helper
    assert router.source_now() == "hub"
    assert router.scan(since_days=14, limit=10, skip=[])["messages"][0]["message_id"] == "<a@x>"
    assert api.interests == [INTEREST]
    api.up = False
    assert router.source_now() == "faustus"
    assert router.scan(since_days=14, limit=10, skip=[])["messages"][0]["message_id"] == "<own@x>"
    store["mail.source"] = "faustus"
    api.up = True
    assert router.source_now() == "faustus"
    store["mail.source"] = "hub"
    api.up = False
    assert router.source_now() == "hub" and router.scan(since_days=14, limit=10, skip=[])["ok"] is False


def test_the_router_falls_back_to_the_helper_when_the_hub_fails_in_auto_mode():
    api = FakeMailApi([hub_row(1, "<a@x>", "Factura")])
    router, _ = make_router(None, api)
    router.own = FakeMail([{"message_id": "<own@x>", "subject": "Del helper"}])
    api.messages = lambda *a, **k: {"ok": False, "error": "boom", "messages": [], "last_id": 0}
    assert router.scan(since_days=14, limit=10, skip=[])["messages"][0]["message_id"] == "<own@x>"


def test_interest_is_the_paperwork_words_and_attachments_and_is_registered_again_on_demand():
    assert "factura" in INTEREST["subject_terms"] and INTEREST["has_attachment"] is True
    api = FakeMailApi()
    hub = HubMail(lambda k, d="": d, api)
    hub.register_interest()
    hub.register_interest()
    assert len(api.interests) == 1
    hub.forget_interest()
    hub.register_interest()
    assert len(api.interests) == 2


def test_mail_from_the_hub_is_filed_claimed_and_the_watermark_stored(config, clock, tmp_path):
    src = tmp_path / "gateway"
    src.mkdir()
    pdf = src / "luz.pdf"
    pdf.write_bytes(make_pdf(docs.UTILITY))
    api = FakeMailApi([hub_row(11, "<luz@x>", "Su factura de la luz", attachments=[{"name": "factura.pdf", "size": 90_000, "path": str(pdf), "sha": "luz"}]),
                       hub_row(12, "<ruido@x>", "Newsletter: ofertas 30% de descuento", text="Ofertas y rebajas. Darte de baja aquí.")])
    router, _ = make_router(None, api)
    s = build(config, clock, router)
    claims = []
    s.engine.mail_claim = lambda ids, kind, ref: claims.append((ids, kind, ref)) or {"ok": True}
    try:
        s.db.set_setting("mail.first_scan_done", "1")
        out = s.engine.scan_mail()
        assert out["ok"] and out["docs"] == 1
        doc = s.store.documents()[0]
        assert claims == [([11], "document", f"hoard://kafka/document/{doc['id']}")]
        assert s.setting("mail.hub_since_id") == "12" and s.store.mail("<luz@x>")["hub_id"] == "11"
        assert s.engine.scan_mail()["messages"] == 0
        assert len(claims) == 1
    finally:
        s.stop()


def test_accepting_a_doubtful_mail_claims_it_too(config, clock):
    api = FakeMailApi([hub_row(21, "<dudoso@x>", "Justificante de pago", text=docs.UTILITY)])
    router, _ = make_router(None, api)
    s = build(config, clock, router)
    claims = []
    s.engine.mail_claim = lambda ids, kind, ref: claims.append((ids, kind, ref)) or {"ok": True}
    try:
        s.db.set_setting("mail.first_scan_done", "1")
        s.engine.scan_mail()
        if s.store.mail("<dudoso@x>")["state"] == "new":
            s.engine.accept_mail("<dudoso@x>")
        assert claims and claims[0][0] == [21] and claims[0][1] == "document"
    finally:
        s.stop()


def test_mail_source_is_a_setting_and_changing_mail_settings_asks_for_the_interest_again(svc):
    seen = []
    svc.mail = type("M", (), {"forget_interest": lambda self: seen.append("forget"), "faustus_dir": lambda self: None})()
    assert svc.set_settings({"mail.source": "hub"})["mail.source"] == "hub"
    assert seen == ["forget"]
    with pytest.raises(Exception):
        svc.set_settings({"mail.source": "pigeon"})


# ================================================================== the agenda
def test_agenda_items_carry_kind_priority_and_a_link(svc):
    svc.engine.add_deadline(title="Pagar la multa", date_="2026-10-02", kind=M.FINE_DISCOUNT)
    svc.engine.add_deadline(title="Renovar el seguro", date_="2026-12-01", kind=M.RENEWAL)
    svc.engine.upsert_external(source="homehoard", external_key="k1", title="Limpiar la caldera", date_="2026-10-20", kind=M.CUSTOM)
    svc.engine.upsert_external(source="people", external_key="k2", title="Llamar a Marta", date_="2026-10-05", kind=M.CUSTOM)
    items = {i["title"]: i for i in agenda.items(svc, datetime(2026, 9, 24).date(), datetime(2027, 1, 31).date())}
    assert items["Pagar la multa"]["kind"] == "deadline" and items["Pagar la multa"]["priority"] == "high"
    assert items["Renovar el seguro"]["kind"] == "renewal" and items["Renovar el seguro"]["priority"] == "low"
    assert items["Limpiar la caldera"]["kind"] == "maintenance" and items["Llamar a Marta"]["kind"] == "followup"
    assert all(i["id"].startswith("kafka:deadline:") and i["all_day"] for i in items.values())


def test_agenda_priority_grows_with_closeness_and_seriousness():
    assert agenda.agenda_priority(M.APPEAL, -2) == "urgent" and agenda.agenda_priority(M.WARRANTY_END, -2) == "high"
    assert agenda.agenda_priority(M.APPEAL, 0) == "urgent" and agenda.agenda_priority(M.APPEAL, 2) == "high"
    assert agenda.agenda_priority(M.CUSTOM, 2) == "normal" and agenda.agenda_priority(M.TAX_DUE, 10) == "normal"
    assert agenda.agenda_priority(M.RENEWAL, 10) == "low" and agenda.agenda_priority(M.RENEWAL, 90) == "low"


def test_agenda_only_lists_open_deadlines_inside_the_window(svc):
    done = svc.engine.add_deadline(title="Ya hecho", date_="2026-10-03")
    svc.engine.update_deadline(done["id"], {"state": "done"})
    svc.engine.add_deadline(title="Lejano", date_="2027-06-01")
    svc.engine.add_deadline(title="Cercano", date_="2026-10-04")
    titles = [i["title"] for i in agenda.items(svc, datetime(2026, 10, 1).date(), datetime(2026, 12, 31).date())]
    assert titles == ["Cercano"]


def test_the_agenda_route_needs_the_apps_own_token_and_never_500s(client):
    client.svc.engine.add_deadline(title="Cosa con fecha", date_="2026-10-10", kind=M.RENEWAL)
    assert client.get("/api/family/agenda").status_code == 401
    r = client.get("/api/family/agenda?from=2026-10-01&to=2026-10-31&sphere=personal", headers=client.bearer)
    body = r.json()
    assert r.status_code == 200 and body["ok"] and [i["title"] for i in body["items"]] == ["Cosa con fecha"]
    assert body["items"][0]["kind"] == "renewal" and body["items"][0]["url"].endswith("/#/plazos")
    assert client.get("/api/family/agenda?from=basura", headers=client.bearer).json()["ok"] is True


def test_the_manifest_announces_the_agenda():
    manifest = json.loads((Path(__file__).resolve().parent.parent / "faustus-plugin.json").read_text(encoding="utf-8"))
    assert manifest["x-family"]["agenda"] is True


# ================================================================== bus events
@pytest.fixture
def events(svc):
    got: list[tuple[str, dict]] = []
    svc.engine.emit = lambda t, d: got.append((t, d))
    return got


def of(events, type_):
    return [d for t, d in events if t == type_]


def test_a_new_deadline_emits_kafka_deadline_created(svc, events):
    t = svc.engine.add_deadline(title="Renovar DNI", date_="2026-12-01", kind=M.EXPIRY)
    assert of(events, "kafka.deadline.created") == [{"deadline_id": t["id"], "title": "Renovar DNI", "due": "2026-12-01", "kind": "expiry"}]


def test_deadlines_found_in_a_document_are_announced_but_quiet_history_is_not(svc, events):
    svc.engine.ingest_text("Multa", docs.FINE)
    assert of(events, "kafka.deadline.created")
    events.clear()
    svc.engine.ingest_text("Seguro viejo", docs.INSURANCE.replace("5550012345", "5550099999"), quiet=True)
    assert of(events, "kafka.deadline.created") == [] and of(events, "kafka.document.archived") == []


def test_a_filed_document_emits_kafka_document_archived_with_its_order_number(svc, events):
    doc = svc.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
    [e] = of(events, "kafka.document.archived")
    assert e == {"doc_id": doc["id"], "kind": "invoice", "merchant": "MediaMarkt", "amount": 249.9, "currency": "EUR", "date": "2026-08-15",
                 "order_ref": "402-1234567-7654321", "message_id": ""}


def test_a_document_without_an_order_number_has_an_empty_order_ref(svc, events):
    svc.engine.ingest_text("Ticket", docs.RECEIPT)
    assert of(events, "kafka.document.archived")[0]["order_ref"] == ""


def test_a_mail_document_carries_its_message_id(config, clock):
    s = build(config, clock, FakeMail())
    got = []
    s.engine.emit = lambda t, d: got.append((t, d))
    try:
        s.engine.ingest_text("Factura", INVOICE_TEXT, source="mail", source_ref="<m1@x>")
        assert [d["message_id"] for t, d in got if t == "kafka.document.archived"] == ["<m1@x>"]
    finally:
        s.stop()


def test_soon_and_overdue_events_go_out_once_per_deadline_and_state(svc, clock, events):
    t = svc.engine.add_deadline(title="Pago de Demo", date_="2026-10-11", remind=[7, 3, 1], kind=M.PAYMENT)
    clock.advance(4 * 86400)                       # 7 days left: first lead
    svc.engine.run_reminders()
    clock.advance(4 * 86400)                       # 3 days left: second lead, same state
    svc.engine.run_reminders()
    soon = of(events, "kafka.deadline.soon")
    assert len(soon) == 1 and soon[0]["deadline_id"] == t["id"] and soon[0]["due"] == "2026-10-11" and 0 < soon[0]["days_left"] <= 7
    assert len([e for e, _ in svc.notifier.sent if e["type"] == M.N_SOON]) == 2        # both reminders were still sent
    clock.advance(10 * 86400)
    svc.engine.run_reminders()
    svc.engine.run_reminders()
    assert len(of(events, "kafka.deadline.overdue")) == 1


def test_a_rescheduled_deadline_is_a_new_state(svc, clock, events):
    t = svc.engine.add_deadline(title="Cosa", date_="2026-10-03", remind=[3], kind=M.CUSTOM)
    svc.engine.run_reminders()
    svc.engine.update_deadline(t["id"], {"date": "2026-10-20"})
    clock.advance(16 * 86400)
    svc.engine.run_reminders()
    assert len(of(events, "kafka.deadline.soon")) == 2


def test_deadline_notifications_no_longer_go_through_the_notifiers_bus_channel(svc, events):
    svc.engine.add_deadline(title="Cosa", date_="2026-10-02", remind=[3], kind=M.CUSTOM)
    svc.engine.run_reminders()
    sent = [c for e, c in svc.notifier.sent if e["type"] == M.N_SOON]
    assert sent and "hub" not in sent[0] and of(events, "kafka.deadline.soon")


def test_the_bus_event_of_a_deadline_can_be_turned_off(svc, events):
    svc.set_settings({"notify.hub.enabled": "0"})
    svc.engine.add_deadline(title="Cosa", date_="2026-10-02", remind=[3], kind=M.CUSTOM)
    svc.engine.run_reminders()
    assert of(events, "kafka.deadline.soon") == []


def phileas(days_ago=0, sid="s1"):
    ts = datetime(2026, 10, 1, 9, 0).timestamp() - days_ago * 86400
    return {"id": sid, "label": "Aspirador Robot Demo X1", "merchant": "MediaMarkt", "order_ref": "A-1", "price": 249.9, "currency": "EUR", "delivered_ts": ts}


def test_a_new_warranty_from_a_delivered_parcel_emits_kafka_warranty_created(config, clock):
    s = build(config, clock, FakeMail(), family_call=lambda app, tool, args: {"ok": True, "result": {"shipments": [phileas(0)]}})
    got = []
    s.engine.emit = lambda t, d: got.append((t, d))
    try:
        assert s.engine.sync_phileas()["created"] == 1
        doc = s.store.documents()[0]
        assert [d for t, d in got if t == "kafka.warranty.created"] == [{"doc_id": doc["id"], "shipment_id": "s1", "merchant": "MediaMarkt", "until": "2029-10-01"}]
        s.engine.sync_phileas()
        assert len([1 for t, _ in got if t == "kafka.warranty.created"]) == 1
    finally:
        s.stop()


def test_old_deliveries_are_history_and_emit_no_warranty_event(config, clock):
    s = build(config, clock, FakeMail(), family_call=lambda app, tool, args: {"ok": True, "result": {"shipments": [phileas(30)]}})
    got = []
    s.engine.emit = lambda t, d: got.append((t, d))
    try:
        s.engine.sync_phileas()
        assert [t for t, _ in got if t == "kafka.warranty.created"] == []
    finally:
        s.stop()


# ================================================================== invoice and Ledger
def ledger_call(matches, attach_ok=True, record=None):
    def call(app, tool, args, timeout=None):
        if record is not None:
            record.append((app, tool, args))
        assert app == "ledger"
        if tool == "tx_find":
            return {"ok": True, "result": {"ok": True, "matches": matches}}
        if tool == "tx_attach_doc":
            return {"ok": True, "result": {"ok": True}} if attach_ok else {"ok": True, "result": {"ok": False, "error": "no such transaction"}}
        return {"ok": False, "error": "unknown tool"}
    return call


def ledger_services(config, clock, matches, **kw):
    calls: list = []
    s = build(config, clock, FakeMail(), family_call=ledger_call(matches, record=calls, **kw))
    links: list = []
    s.engine.refs_link = lambda a, b, rel, **k: links.append((a, b, rel, k)) or {"ok": True}
    s.engine.app_url = lambda app: "http://127.0.0.1:5199" if app == "ledger" else ""
    return s, calls, links


STRONG = {"tx_id": "42", "date": "2026-08-15", "amount": 249.9, "merchant": "MEDIAMARKT", "score": 0.93}


def test_an_invoice_with_one_strong_match_is_linked_both_ways_and_remembers_the_transaction(config, clock):
    s, calls, links = ledger_services(config, clock, [STRONG, {**STRONG, "tx_id": "43", "score": 0.4}])
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        find = next(a for _, t, a in calls if t == "tx_find")
        assert find == {"amount": 249.9, "date": "2026-08-15", "days": 5, "merchant": "MediaMarkt", "currency": "EUR"}
        attach = next(a for _, t, a in calls if t == "tx_attach_doc")
        assert attach["tx_id"] == "42" and attach["doc_ref"] == f"hoard://kafka/document/{doc['id']}"
        assert links[0][:3] == ("hoard://ledger/tx/42", f"hoard://kafka/document/{doc['id']}", "invoice")
        card = s.doc_card(s.store.document(doc["id"]))
        assert card["ledger_tx"]["tx_id"] == "42" and card["ledger_tx"]["ref"] == "hoard://ledger/tx/42"
        assert card["ledger_tx"]["url"] == "http://127.0.0.1:5199/#/movimientos?tx=42"
    finally:
        s.stop()


def test_two_strong_matches_or_none_leave_the_invoice_unlinked(config, clock):
    s, calls, links = ledger_services(config, clock, [STRONG, {**STRONG, "tx_id": "43", "score": 0.85}])
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        assert s.store.document(doc["id"])["facts"].get("ledger_tx") is None and links == []
        assert not any(t == "tx_attach_doc" for _, t, _ in calls)
    finally:
        s.stop()


def test_only_invoices_and_receipts_with_an_amount_and_a_date_are_looked_up(config, clock):
    s, calls, _ = ledger_services(config, clock, [STRONG])
    try:
        s.engine.ingest_text("Multa", docs.FINE)
        s.engine.ingest_text("Seguro", docs.INSURANCE)
        assert calls == []
    finally:
        s.stop()


def test_document_link_tx_returns_candidates_and_a_chosen_one_links(config, clock):
    s, calls, links = ledger_services(config, clock, [{**STRONG, "score": 0.6}, {**STRONG, "tx_id": "43", "score": 0.5}])
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        first = s.engine.link_ledger(doc["id"])
        assert first["ok"] and not first["linked"] and [c["tx_id"] for c in first["candidates"]] == ["42", "43"]
        picked = s.engine.link_ledger(doc["id"], tx_id="43")
        assert picked["linked"] and picked["tx_id"] == "43" and picked["ref"] == "hoard://ledger/tx/43"
        again = s.engine.link_ledger(doc["id"])
        assert again["linked"] and again["reason"] == "already linked"
    finally:
        s.stop()


def test_ledger_errors_are_reported_not_raised(config, clock):
    s = build(config, clock, FakeMail(), family_call=lambda app, tool, args: {"ok": False, "error": "Ledger is not running"})
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        out = s.engine.link_ledger(doc["id"])
        assert out == {"ok": False, "linked": False, "candidates": [], "reason": "Ledger is not running"}
        bad = s.engine.link_ledger(s.engine.ingest_text("Multa", docs.FINE)["document"]["id"])
        assert not bad["ok"] and "invoices and receipts" in bad["reason"]
    finally:
        s.stop()


def test_a_failed_attach_leaves_the_document_unlinked(config, clock):
    s, _, links = ledger_services(config, clock, [STRONG], attach_ok=False)
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        assert s.store.document(doc["id"])["facts"].get("ledger_tx") is None and links == []
        assert "no such transaction" in s.engine.link_ledger(doc["id"])["reason"]
    finally:
        s.stop()


def test_the_ledger_link_can_be_switched_off(config, clock):
    s, calls, _ = ledger_services(config, clock, [STRONG])
    try:
        s.set_settings({"links.ledger": "0"})
        s.engine.ingest_text("Factura", INVOICE_TEXT)
        assert calls == []
    finally:
        s.stop()


def test_housekeeping_looks_again_for_recent_unlinked_invoices_once_a_day(config, clock):
    s, calls, _ = ledger_services(config, clock, [])
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        n = len([1 for _, t, _ in calls if t == "tx_find"])
        assert s.engine.housekeeping()["ledger_lookups"] == 0          # tried a moment ago
        clock.advance(21 * 3600)
        s.family_call = None
        s.engine.family_call = ledger_call([STRONG], record=calls)
        assert s.engine.housekeeping()["ledger_lookups"] == 1
        assert s.store.document(doc["id"])["facts"]["ledger_tx"]["tx_id"] == "42"
        assert len([1 for _, t, _ in calls if t == "tx_find"]) == n + 1
    finally:
        s.stop()


# ================================================================== tools
def call(svc, name, **args):
    from conftest import tool
    return tool(svc, name, **args)


def test_deadline_add_takes_due_note_and_source_ref_and_is_idempotent(svc):
    a = call(svc, "deadline_add", title="Llamar a Marta", due="2026-10-09", note="por lo del viaje", source_ref="hoard://people/commitment/7")
    assert a["ok"] and a["deadline_id"] and a["action"] == "created"
    d = a["deadline"]
    assert d["source"] == "people" and d["source_ref"] == "hoard://people/commitment/7" and d["date"] == "2026-10-09"
    b = call(svc, "deadline_add", title="Llamar a Marta", due="2026-10-09", source_ref="hoard://people/commitment/7")
    assert b["deadline_id"] == a["deadline_id"] and b["action"] != "created"
    other = call(svc, "deadline_add", title="Llamar a Marta", due="2026-10-09", source_ref="hoard://people/commitment/8")
    assert other["deadline_id"] != a["deadline_id"]
    assert len(svc.store.deadlines(states=["open"])) == 2


def test_deadline_add_still_works_by_hand_and_needs_a_date(svc):
    a = call(svc, "deadline_add", title="Renovar el pasaporte", date="2026-12-01", kind="expiry")
    assert a["ok"] and a["deadline"]["kind"] == "expiry"
    with pytest.raises(Exception):
        call(svc, "deadline_add", title="Sin fecha")


def test_agenda_kinds_are_filed_as_custom_and_unknown_kinds_still_fail(svc):
    assert call(svc, "deadline_add", title="Tarea", due="2026-11-01", kind="followup")["deadline"]["kind"] == "custom"
    with pytest.raises(Exception):
        call(svc, "deadline_add", title="Otra", due="2026-11-01", kind="inventada")


def minutes_call(result, ok=True):
    def call_(app, tool, args, timeout=None):
        assert (app, tool) == ("funes", "minutes_get") and args == {"minutes_id": "mn1"}
        return {"ok": True, "result": result} if ok else {"ok": False, "error": "Funes is not running"}
    return call_


MINUTES = {"title": "Reunión de vecinos", "date": "2026-09-30", "attendees": ["Ana", "Luis"], "summary": "…",
           "action_items": [{"text": "Pagar la derrama", "owner": "yo", "due": "2026-10-15"},
                            {"text": "Pedir presupuesto del portal", "due": "2026-10-20"},
                            {"text": "Redactar el acta", "owner": "Marta", "due": "2026-10-10"},
                            {"text": "Pensar en lo del garaje"},
                            {"text": "Llamar al administrador", "owner": "Ana y Luis", "due": "2026-10-12"}]}


def test_deadlines_from_minutes_adds_my_dated_items_and_skips_the_rest(config, clock):
    s = build(config, clock, FakeMail(), family_call=minutes_call(MINUTES))
    try:
        s.set_settings({"minutes.me": "Luis"})
        out = call(s, "deadlines_from_minutes", minutes_id="mn1")
        assert out["ok"] and [a["title"] for a in out["added"]] == ["Pagar la derrama", "Pedir presupuesto del portal", "Llamar al administrador"]
        reasons = {x["text"]: x["reason"] for x in out["skipped"]}
        assert reasons["Redactar el acta"] == "belongs to Marta" and reasons["Pensar en lo del garaje"] == "no date"
        rows = s.store.deadlines(states=["open"], source="funes")
        row = next(r for r in rows if r["title"] == "Pagar la derrama")
        assert row["source_ref"] == "hoard://funes/minutes/mn1" and "Reunión de vecinos" in row["basis"] and row["date"] == "2026-10-15"
        again = call(s, "deadlines_from_minutes", minutes_id="mn1")
        assert again["added"] == [] and len(s.store.deadlines(states=["open"], source="funes")) == 3
    finally:
        s.stop()


def test_deadlines_from_minutes_reports_funes_being_away(config, clock):
    s = build(config, clock, FakeMail(), family_call=minutes_call(None, ok=False))
    try:
        out = call(s, "deadlines_from_minutes", minutes_id="mn1")
        assert out == {"ok": False, "error": "Funes is not running", "added": [], "skipped": []}
    finally:
        s.stop()


def test_document_link_tx_tool(config, clock):
    s, _, _ = ledger_services(config, clock, [{**STRONG, "score": 0.6}])
    try:
        doc = s.engine.ingest_text("Factura", INVOICE_TEXT)["document"]
        out = call(s, "document_link_tx", doc_id=doc["id"])
        assert out["ok"] and out["linked"] is False and out["candidates"][0]["tx_id"] == "42"
        linked = call(s, "document_link_tx", doc_id=doc["id"], tx_id="42")
        assert linked["linked"] and linked["document"]["ledger_tx"]["tx_id"] == "42"
    finally:
        s.stop()


# ------------------------------------------------------------------ tax pack
def file_doc(s, text, title, year="2025", tags=None, kind=None, issue=None):
    doc = s.engine.ingest_text(title, text, quiet=True)["document"]
    fields = {"issue_date": issue or f"{year}-03-15"}
    if kind:
        fields["kind"] = kind
    if tags:
        fields["tags"] = tags
    return s.engine.update_document(doc["id"], fields)


PAYSLIP = "NÓMINA\nEmpresa Demo S.L.\nRecibo de salarios\nPeriodo: marzo 2025\nLíquido a percibir: 1.800,00 €\n"
BANK_CERT = "BANCO DEMO\nCertificado de intereses y saldos a 31/12/2025\nIntereses abonados: 12,40 €\n"
RENT = "CONTRATO DE ARRENDAMIENTO\nArrendador: Casa Demo\nAlquiler de vivienda, renta mensual 700,00 €\n"
DONATION = "FUNDACIÓN EJEMPLO\nCertificado de donativo\nImporte donado: 60,00 €\n"
LEDGER_YEAR = {"ok": True, "year": 2025, "totals": {"income": 31000.0, "expense": 21000.0, "net": 10000.0},
               "by_category": [{"category": "Comida", "expense": 4200.0}], "by_month": [{"month": "2025-01", "income": 2500.0, "expense": 1800.0}]}


def tax_services(config, clock, ledger=LEDGER_YEAR):
    def family(app, tool, args, timeout=None):
        assert (app, tool, args) == ("ledger", "report_year", {"year": 2025})
        return {"ok": True, "result": ledger} if ledger else {"ok": False, "error": "Ledger is not running"}
    return build(config, clock, FakeMail(), family_call=family)


def test_tax_pack_gathers_the_years_tax_paperwork_with_an_index_csv_zip_and_what_is_missing(config, clock, tmp_path):
    s = tax_services(config, clock)
    try:
        file_doc(s, docs.TAX_NOTICE, "Requerimiento", issue="2025-05-10")
        file_doc(s, PAYSLIP, "Nómina marzo", kind="payslip")
        file_doc(s, DONATION, "Donativo")
        file_doc(s, INVOICE_TEXT, "Factura del portátil", tags=["deducible"], issue="2025-06-02", kind="invoice")
        file_doc(s, docs.RECEIPT, "Ticket sin marcar", issue="2025-06-02")
        file_doc(s, docs.FINE, "Multa", issue="2025-02-01")
        file_doc(s, BANK_CERT, "Certificado del banco 2024", issue="2025-01-20").get("id")
        old = s.engine.ingest_text("Alquiler 2024", RENT, quiet=True)["document"]
        s.engine.update_document(old["id"], {"issue_date": "2024-02-01"})
        out = call(s, "tax_pack", year=2025, out_dir=str(tmp_path / "salida"))
        assert out["ok"] and out["year"] == 2025
        counts = out["counts"]
        assert counts["hacienda"] == 1 and counts["payroll"] == 1 and counts["donation"] == 1 and counts["deductible"] == 1 and counts["bank"] == 1
        pack = Path(out["path"])
        assert pack.parent == tmp_path / "salida" and pack.name == "Renta 2025"
        assert (pack / "index.md").is_file() and (pack / "documentos.csv").is_file() and (pack / "ledger_2025.json").is_file()
        assert (pack / "ledger_2025_by_category.csv").is_file() and (pack / "ledger_2025_by_month.csv").is_file()
        rows = list(csv.reader((pack / "documentos.csv").read_text(encoding="utf-8-sig").splitlines(), delimiter=";"))
        assert rows[0][0] == "category" and len(rows) == 1 + out["documents"] == 6
        assert not any("Multa" in r[5] or "Ticket" in r[5] for r in rows[1:])
        copied = [p for p in pack.rglob("*") if p.is_file() and p.suffix == ".txt"]
        assert len(copied) == out["copied"] == 5
        zf = zipfile.ZipFile(out["zip"])
        assert any(n.endswith("index.md") for n in zf.namelist())
        keys = {m["key"] for m in out["missing"]}
        assert "payroll_certificate" in keys and "rent_receipts" in keys and "bank_certificate" not in keys
        assert out["ledger"]["ok"] and "ledger_2025.json" in out["ledger"]["files"]
        index = (pack / "index.md").read_text(encoding="utf-8")
        assert "Renta 2025" in index and "Certificado de retenciones" in index
    finally:
        s.stop()


def test_tax_pack_never_overwrites_and_reports_ledger_being_away(config, clock, tmp_path):
    s = tax_services(config, clock, ledger=None)
    try:
        file_doc(s, docs.TAX_NOTICE, "Requerimiento", issue="2025-05-10")
        first = call(s, "tax_pack", year=2025, out_dir=str(tmp_path / "x"))
        second = call(s, "tax_pack", year=2025, out_dir=str(tmp_path / "x"))
        assert first["path"] != second["path"] and Path(first["path"]).is_dir() and Path(second["path"]).name == "Renta 2025 (2)"
        assert first["ledger"]["ok"] is False and "Ledger is not running" in first["ledger"]["error"]
        assert "Ledger no respondió" in (Path(first["path"]) / "index.md").read_text(encoding="utf-8")
    finally:
        s.stop()


def test_tax_pack_belongs_by_what_the_document_says_about_its_year(config, clock, tmp_path):
    s = tax_services(config, clock)
    try:
        d = file_doc(s, "AGENCIA TRIBUTARIA\nCarta informativa\nEjercicio 2025\nDatos fiscales de la renta", "Datos fiscales", issue="2026-02-10", kind="tax")
        out = call(s, "tax_pack", year=2025, out_dir=str(tmp_path / "y"), zip=False)
        assert out["counts"]["hacienda"] == 1 and out["zip"] == "" and d["id"]
    finally:
        s.stop()


def test_tax_pack_defaults_to_the_previous_year_and_checks_the_year(svc, tmp_path):
    out = call(svc, "tax_pack", out_dir=str(tmp_path / "z"), zip=False)
    assert out["year"] == 2025 and out["documents"] == 0
    with pytest.raises(Exception):
        call(svc, "tax_pack", year=1800)


def test_the_new_tools_are_documented_with_short_first_lines():
    from kafka_hoard.agent_tools import TOOLS_BY_NAME
    for name in ("deadlines_from_minutes", "tax_pack", "document_link_tx"):
        assert name in TOOLS_BY_NAME and len(TOOLS_BY_NAME[name].description.splitlines()[0]) <= 110
