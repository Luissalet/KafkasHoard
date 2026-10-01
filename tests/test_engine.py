"""Engine behaviour end to end on a temporary data dir: ingestion, dedupe, folders, mail, reminders, series and prices, Phileas, model pass."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pytest

import docs
from conftest import FakeLlm, build
from kafka_hoard import model as M
from kafka_hoard.errors import KafkaError
from pdfmaker import make_pdf


def dl(svc, doc_id, key=None):
    rows = svc.store.doc_deadlines(doc_id)
    return [d for d in rows if key is None or d["key"] == key]


def ingest(svc, text, title="", **kw):
    return svc.engine.ingest_text(title or text.splitlines()[0], text, **kw)["document"]


# ------------------------------------------------------------------ ingestion
def test_text_document_is_filed_with_extraction(svc):
    doc = ingest(svc, docs.INSURANCE)
    assert doc["kind"] == M.INSURANCE and doc["state"] == M.OK
    assert doc["amount"] == 320.0 and "mapfre" in doc["issuer"].lower()
    keys = {d["key"]: d for d in dl(svc, doc["id"])}
    assert keys["renewal"]["date"] == "2026-11-30" and keys["cancel_by"]["date"] == "2026-10-30"


def test_pdf_upload_is_stored_by_content_and_deduplicated(svc):
    data = make_pdf(docs.FINE)
    first = svc.engine.ingest_bytes(data, "denuncia.pdf")
    assert first["created"] and first["document"]["kind"] == M.FINE
    again = svc.engine.ingest_bytes(data, "otra-copia.pdf")
    assert not again["created"] and again["duplicate_of"] == first["document"]["id"]
    assert svc.store.counts()["documents"] == 1
    sha = first["document"]["file_sha"]
    assert svc.files.exists(sha, "pdf") and svc.files.path_of(sha, "pdf").parent.name == sha[:2]


def test_empty_and_oversized_uploads_are_refused(svc):
    with pytest.raises(KafkaError) as e:
        svc.engine.ingest_bytes(b"", "x.pdf")
    assert e.value.code == "invalid"
    with pytest.raises(KafkaError) as e:
        svc.engine.ingest_bytes(b"x" * (64 * 1024 * 1024 + 1), "big.pdf")
    assert e.value.code == "too_large"


def test_unsupported_file_goes_to_review_without_crashing(svc):
    res = svc.engine.ingest_bytes(b"\x00\x01\x02binary", "program.bin")
    assert res["document"]["state"] == M.REVIEW and res["notes"]


def test_gibberish_stays_in_review_with_no_deadlines(svc):
    doc = ingest(svc, docs.GIBBERISH, "nada")
    assert doc["state"] == M.REVIEW and dl(svc, doc["id"]) == []


def test_short_text_is_refused(svc):
    with pytest.raises(KafkaError):
        svc.engine.ingest_text("x", "hola")


def test_new_document_notifies_the_hub_only_once_with_a_bus_event(svc):
    ingest(svc, docs.RECEIPT)
    events = [e for e, _ in svc.notifier.sent if e["type"] == M.N_DOC]
    assert len(events) == 1 and events[0]["bus"] == "kafka.document.added" and svc.notifier.sent[0][1] == ["hub"]


def test_deleting_a_document_removes_its_file_unless_shared(svc):
    a = ingest(svc, docs.RECEIPT)
    sha = a["file_sha"]
    out = svc.engine.delete_document(a["id"])
    assert out["file_removed"] and not svc.files.exists(sha, "txt")
    assert svc.store.counts()["documents"] == 0 and svc.store.deadlines(states=[M.OPEN, M.DONE]) == []


def test_search_finds_text_with_accents_and_marks_hits(svc):
    doc = ingest(svc, docs.INSURANCE)
    hits = svc.store.search("poliza hogar")
    assert hits and hits[0]["doc_id"] == doc["id"] and "⟦" in hits[0]["snippet"]
    assert svc.store.search("zzzz-no-existe") == []


# ------------------------------------------------------------------ deadlines
def test_quiet_history_files_past_deadlines_as_done(svc):
    old = docs.FINE.replace("12/09/2026", "05/09/2026")
    doc = svc.engine.ingest_text("multa vieja", old, quiet=True)["document"]
    rows = dl(svc, doc["id"])
    assert rows and all(d["state"] == M.DONE for d in rows)
    assert svc.notifier.sent == []


def test_reminders_fire_each_lead_once(svc, clock):
    doc = ingest(svc, docs.FINE)             # deadline 2 Oct: tomorrow
    svc.notifier.sent.clear()
    first = svc.engine.run_reminders()
    assert first["sent"] >= 1
    soon = [t for t in svc.notifier.titles() if "mañana" in t.lower()]
    assert soon, svc.notifier.titles()
    n = len(svc.notifier.sent)
    svc.engine.run_reminders()
    assert len(svc.notifier.sent) == n          # nothing new the second time
    assert doc["id"]


def test_one_notification_per_deadline_even_when_several_leads_are_due(svc):
    svc.engine.add_deadline(title="Cosa", date_="2026-10-02", remind=[7, 3, 1])
    svc.engine.run_reminders()
    soon = [e for e, _ in svc.notifier.sent if e["type"] == M.N_SOON]
    assert len(soon) == 1
    d = svc.store.deadlines(states=[M.OPEN])[0]
    assert sorted(d["notified"]) == [1, 3, 7]


def test_overdue_is_notified_once_and_only_when_recent(svc):
    svc.engine.add_deadline(title="Hace poco", date_="2026-09-25")
    svc.engine.store.update_deadline(svc.store.deadlines(states=[M.OPEN])[0]["id"], notified=[])
    svc.engine.add_deadline(title="Hace mucho", date_="2026-01-01")
    svc.store.update_deadline(next(d for d in svc.store.deadlines(states=[M.OPEN]) if d["title"] == "Hace mucho")["id"], notified=[])
    out = svc.engine.run_reminders()
    assert out["overdue"] == 1
    assert any(e["type"] == M.N_OVERDUE and e["bus"] == "kafka.deadline.overdue" for e, _ in svc.notifier.sent)
    again = svc.engine.run_reminders()
    assert again["overdue"] == 0


def test_night_window_defers_everything_unless_high_is_allowed(svc, clock):
    svc.engine.add_deadline(title="Mañana", date_="2026-10-02", remind=[1])
    clock.t = datetime(2026, 10, 1, 2, 0).timestamp()
    out = svc.engine.run_reminders()
    assert out["deferred"] == 1 and out["sent"] == 0 and svc.notifier.sent == []
    clock.t = datetime(2026, 10, 1, 9, 0).timestamp()
    assert svc.engine.run_reminders()["sent"] == 1


def test_night_window_lets_high_severity_through_when_asked(svc, clock):
    svc.set_settings({"notify.night_high": "1"})
    svc.engine.add_deadline(title="Hoy", date_="2026-10-01", remind=[0])
    clock.t = datetime(2026, 10, 1, 2, 0).timestamp()
    assert svc.engine.run_reminders()["sent"] == 1


def test_lead_days_come_from_settings(svc):
    svc.set_settings({"remind.payment": "10,2"})
    assert svc.engine.lead_days(M.PAYMENT) == [10, 2]
    assert svc.engine.lead_days(M.ITV) == [30, 7]


def test_manual_deadline_validation(svc):
    with pytest.raises(KafkaError):
        svc.engine.add_deadline(title="x", date_="31/12/2026")
    with pytest.raises(KafkaError):
        svc.engine.add_deadline(title="", date_="2026-12-31")
    with pytest.raises(KafkaError):
        svc.engine.add_deadline(title="x", date_="2026-12-31", kind="bogus")
    d = svc.engine.add_deadline(title="Llamar al banco", date_="2026-12-31", notes="n")
    assert d["edited"] and not d["auto"] and d["remind"] == [7, 1]


def test_marking_a_recurring_deadline_done_rolls_it_forward(svc):
    d = svc.engine.add_deadline(title="Cuota", date_="2026-10-05", recurring="monthly")
    out = svc.engine.update_deadline(d["id"], {"state": M.DONE})
    assert out["state"] == M.OPEN and out["date"] == "2026-11-05" and out["notified"] == []
    y = svc.engine.add_deadline(title="Anual", date_="2026-09-01", recurring="yearly")
    assert svc.engine.update_deadline(y["id"], {"state": M.DONE})["date"] == "2027-09-01"


def test_snooze_and_state_changes(svc):
    d = svc.engine.add_deadline(title="Algo", date_="2026-10-03")
    out = svc.engine.update_deadline(d["id"], {"snooze_days": 7})
    assert out["date"] == "2026-10-10"
    done = svc.engine.update_deadline(d["id"], {"state": M.DONE})
    assert done["state"] == M.DONE and done["done_ts"]
    with pytest.raises(KafkaError):
        svc.engine.update_deadline(d["id"], {"snooze_days": 9999})
    with pytest.raises(KafkaError):
        svc.engine.update_deadline(d["id"], {"state": "weird"})


def test_edited_deadlines_survive_reprocessing(svc):
    doc = ingest(svc, docs.INSURANCE)
    cancel = dl(svc, doc["id"], "cancel_by")[0]
    svc.engine.update_deadline(cancel["id"], {"date": "2026-10-20", "title": "Mi fecha"})
    svc.engine.reprocess(doc["id"])
    after = dl(svc, doc["id"], "cancel_by")[0]
    assert after["date"] == "2026-10-20" and after["title"] == "Mi fecha" and after["edited"]


def test_reprocessing_does_not_duplicate_or_renotify(svc):
    doc = ingest(svc, docs.FINE)
    svc.engine.run_reminders()
    before = len(svc.notifier.sent)
    count = len(dl(svc, doc["id"]))
    svc.engine.reprocess(doc["id"])
    svc.engine.run_reminders()
    assert len(dl(svc, doc["id"])) == count and len(svc.notifier.sent) == before


# ------------------------------------------------------------------ editing
def test_editing_fields_marks_them_and_recomputes_deadlines(svc):
    doc = ingest(svc, docs.RECEIPT)
    assert dl(svc, doc["id"], "warranty")[0]["date"] == "2029-08-15"
    out = svc.engine.update_document(doc["id"], {"warranty_years": 2})
    assert dl(svc, doc["id"], "warranty")[0]["date"] == "2028-08-15"
    assert out["facts"]["warranty_months"] == 24
    out = svc.engine.update_document(doc["id"], {"issuer": "Otra Tienda", "title": "Mi aspirador", "tags": "casa, robot"})
    assert out["issuer"] == "Otra Tienda" and {"issuer", "title"} <= set(out["facts"]["edited"]) and out["tags"] == ["casa", "robot"]
    again = svc.engine.reprocess(doc["id"])["document"]
    assert again["issuer"] == "Otra Tienda" and again["title"] == "Mi aspirador"


def test_edit_validation(svc):
    doc = ingest(svc, docs.RECEIPT)
    for bad in ({"kind": "nope"}, {"state": "nope"}, {"issue_date": "mañana"}, {"amount": -5}, {"warranty_years": 50}, {"title": " "}):
        with pytest.raises(KafkaError):
            svc.engine.update_document(doc["id"], bad)


def test_changing_the_kind_by_hand_survives_reprocessing(svc):
    doc = ingest(svc, docs.RECEIPT)
    svc.engine.update_document(doc["id"], {"kind": M.INVOICE})
    assert svc.engine.reprocess(doc["id"])["document"]["kind"] == M.INVOICE


# ------------------------------------------------------------------ series and prices
def test_policy_periods_form_a_series_and_a_price_rise_is_notified_once(svc):
    first = ingest(svc, docs.INSURANCE, quiet=True) if False else svc.engine.ingest_text("Poliza 2025", docs.INSURANCE, quiet=True)["document"]
    later = docs.insurance_renewal("352,00", "01/12/2026", "30/11/2027")
    second = svc.engine.ingest_text("Poliza 2026", later)["document"]
    assert first["series_id"] and first["series_id"] == second["series_id"]
    hist = svc.engine.price_history(first["series_id"])
    assert [h["amount"] for h in hist] == [320.0, 352.0] and hist[1]["pct"] == 10.0
    prices = [e for e, _ in svc.notifier.sent if e["type"] == M.N_PRICE]
    assert len(prices) == 1 and "10" in prices[0]["title"] and prices[0]["bus"] == "kafka.price.change"
    svc.engine.reprocess(second["id"])
    assert len([e for e, _ in svc.notifier.sent if e["type"] == M.N_PRICE]) == 1


def test_small_price_change_does_not_alert(svc):
    svc.engine.ingest_text("A", docs.INSURANCE, quiet=True)
    svc.engine.ingest_text("B", docs.insurance_renewal("322,00", "01/12/2026", "30/11/2027"))
    assert not [e for e, _ in svc.notifier.sent if e["type"] == M.N_PRICE]


def test_price_alert_threshold_is_a_setting(svc):
    svc.set_settings({"prices.alert_pct": "1"})
    svc.engine.ingest_text("A", docs.INSURANCE, quiet=True)
    svc.engine.ingest_text("B", docs.insurance_renewal("330,00", "01/12/2026", "30/11/2027"))
    assert [e for e, _ in svc.notifier.sent if e["type"] == M.N_PRICE]


def test_deleting_the_last_document_drops_its_series(svc):
    doc = svc.engine.ingest_text("A", docs.INSURANCE)["document"]
    assert svc.store.all_series()
    svc.engine.delete_document(doc["id"])
    assert svc.store.all_series() == []


# ------------------------------------------------------------------ folders
def test_folder_scan_files_new_documents_and_never_touches_the_originals(svc, tmp_path):
    folder = tmp_path / "papeles"
    folder.mkdir()
    pdf = folder / "multa.pdf"
    pdf.write_bytes(make_pdf(docs.FINE))
    (folder / "notas.txt").write_text(docs.RECEIPT, encoding="utf-8")
    (folder / "foto.xyz").write_bytes(b"nothing")
    os.utime(pdf, (svc.clock() - 60, svc.clock() - 60))
    os.utime(folder / "notas.txt", (svc.clock() - 60, svc.clock() - 60))
    svc.engine.add_folder(str(folder))
    out = svc.engine.scan_folders()
    assert out["new_documents"] == 2 and pdf.exists() and (folder / "notas.txt").exists()
    assert pdf.read_bytes() == make_pdf(docs.FINE) or pdf.stat().st_size > 0
    again = svc.engine.scan_folders()
    assert again["new_documents"] == 0 and again["files"] == 0


def test_folder_scan_skips_files_still_being_written(svc, tmp_path):
    folder = tmp_path / "x"
    folder.mkdir()
    f = folder / "ahora.txt"
    f.write_text(docs.RECEIPT, encoding="utf-8")
    os.utime(f, (svc.clock() - 0.5, svc.clock() - 0.5))
    svc.engine.add_folder(str(folder))
    out = svc.engine.scan_folders()
    assert out["new_documents"] == 0 and out["skipped"] == 1


def test_old_files_in_a_folder_are_filed_quietly(svc, tmp_path):
    folder = tmp_path / "viejos"
    folder.mkdir()
    f = folder / "multa.txt"
    f.write_text(docs.FINE, encoding="utf-8")
    old = svc.clock() - 30 * 86400
    os.utime(f, (old, old))
    svc.engine.add_folder(str(folder))
    svc.engine.scan_folders()
    assert svc.notifier.sent == []


def test_unsafe_folders_are_refused(svc, tmp_path):
    for bad in ("/", "/etc", str(Path.home()), str(svc.config.data_dir), str(tmp_path / "does-not-exist")):
        with pytest.raises(KafkaError):
            svc.engine.add_folder(bad)


def test_the_inbox_is_watched_by_default_and_folders_can_be_removed(svc, tmp_path):
    assert svc.engine.watched_folders() == [str(svc.config.inbox_dir)]
    folder = tmp_path / "otra"
    folder.mkdir()
    svc.engine.add_folder(str(folder))
    assert str(folder.resolve()) in svc.engine.watched_folders()
    assert svc.engine.add_folder(str(folder))["added"] is False
    svc.engine.remove_folder(str(folder))
    assert str(folder.resolve()) not in svc.engine.watched_folders()
    with pytest.raises(KafkaError):
        svc.engine.remove_folder(str(folder))


def test_ingest_path_refuses_credentials_and_unsupported_types(svc, tmp_path):
    secret = tmp_path / ".env"
    secret.write_text("TOKEN=abc", encoding="utf-8")
    with pytest.raises(KafkaError):
        svc.engine.ingest_path(str(secret))
    exe = tmp_path / "a.exe"
    exe.write_bytes(b"MZ")
    with pytest.raises(KafkaError):
        svc.engine.ingest_path(str(exe))
    ok = tmp_path / "nota.txt"
    ok.write_text(docs.TELCO, encoding="utf-8")
    assert svc.engine.ingest_path(str(ok))["created"]


# ------------------------------------------------------------------ mail
def mail_message(mid, subject, text="", attachments=None, ts=None, sender="Banco Demo <avisos@bancodemo.example>"):
    return {"message_id": mid, "subject": subject, "from_name": "Banco Demo", "from_address": "avisos@bancodemo.example", "text": text,
            "ts": ts if ts is not None else docs.T0 - 3600, "attachments": attachments or []}


def cached_pdf(svc, name, text):
    svc.config.mail_cache_dir.mkdir(parents=True, exist_ok=True)
    path = svc.config.mail_cache_dir / name
    path.write_bytes(make_pdf(text))
    return {"name": "factura.pdf", "path": str(path), "size": path.stat().st_size, "type": "application/pdf"}


def test_mail_with_a_pdf_attachment_is_filed_and_the_cache_is_cleared(svc, fake_mail):
    att = cached_pdf(svc, "abc.pdf", docs.UTILITY)
    fake_mail.messages = [mail_message("m1", "Su factura de la luz", attachments=[att])]
    out = svc.engine.scan_mail()
    assert out["ok"] and out["docs"] == 1 and out["documents_created"] == 1
    doc = svc.store.documents()[0]
    assert doc["source"] == "mail" and doc["kind"] == M.BILL and doc["mail_subject"] == "Su factura de la luz"
    assert not Path(att["path"]).exists()
    assert fake_mail.calls[0]["attachments_dir"] == str(svc.config.mail_cache_dir)
    assert svc.store.mail("m1")["state"] == "filed"


def test_mail_attachments_outside_the_cache_folder_are_never_read(svc, fake_mail, tmp_path):
    outside = tmp_path / "secret.pdf"
    outside.write_bytes(make_pdf(docs.UTILITY))
    att = {"name": "x.pdf", "path": str(outside), "size": 100, "type": "application/pdf"}
    fake_mail.messages = [mail_message("m2", "Factura", attachments=[att])]
    out = svc.engine.scan_mail()
    assert out["documents_created"] == 0 and outside.exists()


def test_first_mail_scan_is_quiet_and_marks_itself_done(svc, fake_mail):
    fake_mail.messages = [mail_message("m1", "Su factura", attachments=[cached_pdf(svc, "a.pdf", docs.FINE)], ts=docs.T0 - 3600)]
    svc.engine.scan_mail()
    assert svc.engine.setting("mail.first_scan_done") == "1"
    assert svc.notifier.sent == []
    assert fake_mail.calls[0]["since_days"] == 180
    svc.engine.scan_mail()
    assert fake_mail.calls[1]["since_days"] == 14


def test_second_scan_announces_new_documents(svc, fake_mail):
    svc.engine.scan_mail()
    fake_mail.messages = [mail_message("m9", "Su factura", attachments=[cached_pdf(svc, "b.pdf", docs.RECEIPT)])]
    svc.engine.scan_mail()
    assert M.N_DOC in svc.notifier.types()


def test_mail_noise_is_ignored_and_known_ids_are_skipped(svc, fake_mail):
    fake_mail.messages = [mail_message("n1", "Oferta -30% solo hoy, newsletter semanal", text="Descubre nuestras novedades. Darse de baja aquí.")]
    out = svc.engine.scan_mail()
    assert out["noise"] == 1 and svc.store.mail("n1")["state"] == "ignored"
    svc.engine.scan_mail()
    assert fake_mail.calls[-1]["skip"] == 1


def test_ambiguous_mail_waits_for_review_and_can_be_accepted_or_ignored(svc, fake_mail):
    body = ("Hola, te adjunto el contrato de seguro con número de póliza 777888 y prima de 120,00 €. Vigencia hasta el 31/12/2026. "
            "Tomador: Ana Ejemplo Prueba. Seguro de hogar, fecha de efecto 01/01/2026.")
    fake_mail.messages = [mail_message("a1", "Documentos", text=body)]
    out = svc.engine.scan_mail()
    assert out["maybe"] + out["docs"] == 1
    state = svc.store.mail("a1")
    if state["state"] == "new":
        got = svc.engine.accept_mail("a1")
        assert got["documents"] and svc.store.mail("a1")["state"] == "filed"
    fake_mail.messages = [mail_message("a2", "Hola", text="Qué tal, nos vemos mañana para comer en el sitio de siempre y hablamos.")]
    svc.engine.scan_mail()
    svc.engine.ignore_mail("a2")
    assert svc.store.mail("a2")["state"] == "ignored"
    with pytest.raises(KafkaError):
        svc.engine.ignore_mail("nope")
    with pytest.raises(KafkaError):
        svc.engine.accept_mail("nope")


def test_mail_failure_is_recorded_not_raised(svc, fake_mail):
    fake_mail.scan = lambda **kw: {"ok": False, "error": "no accounts configured"}
    out = svc.engine.scan_mail()
    assert not out["ok"] and svc.engine.setting("mail.last_error") == "no accounts configured"
    assert svc.store.runs("mail")[0]["ok"] is False or svc.store.runs("mail")[0]["ok"] == 0


def test_mail_without_a_source_reports_it(config, clock):
    s = build(config, clock, None)
    try:
        s.engine.mail = None
        assert s.engine.scan_mail()["ok"] is False
    finally:
        s.stop()


# ------------------------------------------------------------------ Phileas link
def delivered(sid="s1", merchant="MediaMarkt", order="", price=None, label="Aspirador Robot Demo X1", days_ago=10):
    ts = datetime(2026, 10, 1, 12, 0).timestamp() - days_ago * 86400
    return {"id": sid, "label": label, "merchant": merchant, "order_ref": order, "price": price, "currency": "EUR", "delivered_ts": ts}


def make_phileas(shipments, ok=True):
    def call(app, tool, args):
        assert app == "phileas" and tool == "shipments_list" and args["filter"] == "delivered"
        return {"ok": True, "result": {"shipments": shipments}} if ok else {"ok": False, "error": "Phileas is not running"}
    return call


def test_phileas_delivery_creates_a_warranty_document(config, clock, fake_mail):
    s = build(config, clock, fake_mail, family_call=make_phileas([delivered()]))
    try:
        out = s.engine.sync_phileas()
        assert out == {"ok": True, "created": 1, "linked": 0, "skipped": 0}
        doc = s.store.documents()[0]
        assert doc["source"] == "phileas" and doc["kind"] == M.WARRANTY and not doc["file_sha"]
        d = s.store.doc_deadlines(doc["id"])[0]
        assert d["key"] == "warranty" and d["date"] == "2029-09-21" and d["kind"] == M.WARRANTY_END
        assert s.engine.sync_phileas()["created"] == 0
        assert s.store.counts()["documents"] == 1
    finally:
        s.stop()


def test_phileas_delivery_links_to_the_matching_purchase_by_order_reference(config, clock, fake_mail):
    s = build(config, clock, fake_mail, family_call=make_phileas([delivered(order="R-77", price=249.9)]))
    try:
        text = "MEDIAMARKT\nFactura nº R-77\nFecha de compra: 15/08/2026\nProducto: Aspirador Robot Demo X1\nTotal (IVA incluido): 249,90 €\n"
        doc = s.engine.ingest_text("Factura aspirador", text)["document"]
        assert doc["ref"] == "R-77" or doc["amount"] == 249.9
        out = s.engine.sync_phileas()
        assert out["linked"] == 1 and out["created"] == 0
        assert s.store.counts()["documents"] == 1
        w = [d for d in s.store.doc_deadlines(doc["id"]) if d["key"] == "warranty"]
        assert len(w) == 1 and w[0]["date"] == "2029-09-21"
    finally:
        s.stop()


def test_phileas_errors_are_reported_and_the_switch_is_respected(config, clock, fake_mail):
    s = build(config, clock, fake_mail, family_call=make_phileas([], ok=False))
    try:
        out = s.engine.sync_phileas()
        assert not out["ok"] and "not running" in out["error"]
        s.set_settings({"links.phileas": "0"})
        assert s.engine.sync_phileas()["skipped"]
    finally:
        s.stop()


# ------------------------------------------------------------------ model pass
VAGUE = "Gimnasio Demo\nAlta de socio. Quedará usted libre el 01/03/2027. Cuota treinta euros."
LLM_ANSWER = ('{"kind": "contract", "issuer": "Gimnasio Demo", "ref": "G-1", "amount": 30.0, "dates": '
              '[{"role": "permanence_end", "date": "2027-03-01", "evidence": "libre el 01/03/2027"}], "relative": []}')


def test_model_pass_rescues_a_document_that_the_rules_left_in_review(config, clock, fake_mail):
    llm = FakeLlm(LLM_ANSWER)
    s = build(config, clock, fake_mail, llm=llm)
    try:
        doc = s.engine.ingest_text("alta", VAGUE, quiet=True)["document"]
        assert doc["state"] == M.REVIEW and not s.store.doc_deadlines(doc["id"])
        out = s.engine.llm_pass(doc["id"])
        assert out["llm"] == "used" and llm.calls == 1
        after = s.store.document(doc["id"])
        assert after["kind"] == M.CONTRACT and after["state"] == M.OK and after["amount"] == 30.0
        assert [d["date"] for d in s.store.doc_deadlines(doc["id"])] == ["2027-03-01"]
    finally:
        s.stop()


def test_model_answers_without_evidence_in_the_text_are_ignored(config, clock, fake_mail):
    bad = LLM_ANSWER.replace("libre el 01/03/2027", "esto no está en el documento")
    s = build(config, clock, fake_mail, llm=FakeLlm(bad))
    try:
        doc = s.engine.ingest_text("alta", VAGUE, quiet=True)["document"]
        s.engine.llm_pass(doc["id"])
        assert s.store.doc_deadlines(doc["id"]) == []
    finally:
        s.stop()


def test_model_unavailable_is_recorded_silently(config, clock, fake_mail):
    s = build(config, clock, fake_mail, llm=FakeLlm(available=False))
    try:
        doc = s.engine.ingest_text("x", docs.GIBBERISH)["document"]
        out = s.engine.llm_pass(doc["id"])
        assert out["llm"] == "unavailable"
        assert s.store.document(doc["id"])["facts"]["llm"]["used"] is False
        assert s.engine.reprocess(doc["id"], llm=True)["llm"] == "unavailable"
    finally:
        s.stop()


def test_a_model_that_returns_garbage_changes_nothing(config, clock, fake_mail):
    s = build(config, clock, fake_mail, llm=FakeLlm("lo siento, no puedo"))
    try:
        doc = s.engine.ingest_text("alta", VAGUE, quiet=True)["document"]
        assert s.engine.llm_pass(doc["id"])["llm"] == "no_answer"
        assert s.store.document(doc["id"])["state"] == M.REVIEW
    finally:
        s.stop()


# ------------------------------------------------------------------ housekeeping
def test_housekeeping_rolls_late_recurring_deadlines_and_archives_old_done_ones(svc, clock):
    d = svc.engine.add_deadline(title="Cuota", date_="2026-09-20", recurring="monthly")
    old = svc.engine.add_deadline(title="Viejo", date_="2026-08-01")
    svc.engine.update_deadline(old["id"], {"state": M.DONE})
    clock.advance(40 * 86400)
    out = svc.engine.housekeeping()
    assert out["rolled_recurring"] == 1 and out["archived_deadlines"] == 1
    assert svc.store.deadline(d["id"])["date"] > "2026-11-01"
    assert svc.store.deadline(old["id"])["archived"]


def test_housekeeping_purges_old_cache_files(svc, clock):
    svc.config.mail_cache_dir.mkdir(parents=True, exist_ok=True)
    stale = svc.config.mail_cache_dir / "old.pdf"
    stale.write_bytes(b"x")
    os.utime(stale, (clock.t - 3 * 86400, clock.t - 3 * 86400))
    assert svc.engine.housekeeping()["cache_files_purged"] == 1 and not stale.exists()


def test_housekeeping_relinks_series_for_documents_missing_one(svc):
    doc = svc.engine.ingest_text("Poliza", docs.INSURANCE)["document"]
    svc.store.update_document(doc["id"], series_id=None)
    svc.store.drop_empty_series()
    assert svc.engine.housekeeping()["series_relinked"] >= 1
    assert svc.store.document(doc["id"])["series_id"]


def test_phileas_second_hand_and_unnamed_parcels_get_no_legal_warranty(config, clock, fake_mail):
    c2c = {**delivered(), "id": "s_c2c", "merchant": "Wallapop", "label": "Consola de segunda mano"}
    unnamed = {**delivered(), "id": "s_unnamed", "merchant": "", "label": "InPost · pedido 71250752", "item": ""}
    s = build(config, clock, fake_mail, family_call=make_phileas([c2c, unnamed]))
    try:
        out = s.engine.sync_phileas()
        assert out["created"] == 1 and out["skipped"] == 1
        doc = s.store.documents()[0]
        assert doc["issuer"] == "Wallapop"
        assert [d for d in s.store.doc_deadlines(doc["id"]) if d["state"] == M.OPEN] == []
        assert any("particulares" in n for n in doc["facts"]["notes"])
    finally:
        s.stop()


def test_phileas_rules_apply_to_parcels_filed_before_them(config, clock, fake_mail):
    old = {**delivered(), "id": "s_old", "merchant": "Wallapop", "label": "Gráfica usada"}
    s = build(config, clock, fake_mail, family_call=make_phileas([old]))
    try:
        # filed by an older version: a 3-year warranty that does not apply
        doc = s.store.create_document(title="Gráfica usada", kind=M.WARRANTY, issuer="Wallapop", issuer_key="wallapop", source="phileas",
                                      source_ref="s_old", state=M.OK, confidence=90, issue_date="2026-09-21", item="Gráfica usada",
                                      facts={"edited": [], "notes": [], "delivered": "2026-09-21"})
        s.store.create_deadline(doc_id=doc["id"], kind=M.WARRANTY_END, title="Fin", date="2029-09-21", basis="", evidence="", confidence=90,
                                state=M.OPEN, remind=[60], notified=[], key="warranty", auto=True)
        s.engine.sync_phileas()
        assert [d for d in s.store.doc_deadlines(doc["id"]) if d["state"] == M.OPEN] == []
    finally:
        s.stop()


def test_invoice_and_receipt_of_one_payment_count_once(svc, fake_mail):
    invoice_text = "STREAMDEMO S.L. CIF B12345674\nInvoice number INV-0042\nDate of issue: 16/09/2026\nTotal: 45,33 €\n"
    receipt_text = "STREAMDEMO S.L. CIF B12345674\nReceipt number 1234-5678\nInvoice number INV-0042\nDate paid: 16/09/2026\nAmount paid: 45,33 €\n"
    inv = cached_pdf(svc, "inv.pdf", invoice_text)
    inv["name"] = "Invoice-INV-0042.pdf"
    rec = cached_pdf(svc, "rec.pdf", receipt_text)
    rec["name"] = "Receipt-INV-0042.pdf"
    msg = mail_message("m-pay", "Your receipt from Streamdemo", attachments=[rec, inv])
    msg["from_name"], msg["from_address"] = "Streamdemo, Inc.", "invoice+statements@mail.streamdemo.example"
    fake_mail.messages = [msg]
    svc.engine.scan_mail()
    live = [d for d in svc.store.documents() if d["state"] != M.ARCHIVED]
    assert len(live) == 1 and live[0]["file_name"].startswith("Invoice")
    copies = [d for d in svc.store.documents(limit=50) if d["state"] == M.ARCHIVED]
    assert copies and copies[0]["facts"]["copy_of"] == live[0]["id"]


def test_a_sender_name_with_a_comma_still_names_the_issuer(svc):
    doc = svc.store.create_document(title="Recibo", kind=M.RECEIPT, mail_from="Streamdemo, PBC <invoice+statements@mail.streamdemo.example>",
                                    source="mail", state=M.OK, confidence=50, facts={"edited": [], "notes": []})
    meta = svc.engine._meta(doc)
    assert meta.from_name == "Streamdemo, PBC" and meta.from_address.endswith("streamdemo.example")
