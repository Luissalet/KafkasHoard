"""Every tool of the catalogue through the agent bridge and the UI bridge."""

from __future__ import annotations

import json

import pytest

import docs
from conftest import tool
from kafka_hoard import model as M
from kafka_hoard.agent_tools import TOOLS, uncapped
from kafka_hoard.errors import KafkaError


def seed(svc):
    for text, title in ((docs.INSURANCE, "Poliza hogar"), (docs.FINE, "Multa velocidad"), (docs.RECEIPT, "Ticket aspirador"),
                        (docs.ITV, "ITV coche"), (docs.DNI, "DNI de Ana")):
        tool(svc, "doc_add_text", title=title, text=text)


def test_the_catalogue_has_unique_names_and_valid_schemas():
    names = [t.name for t in TOOLS]
    assert len(names) == len(set(names)) >= 51
    for t in TOOLS:
        schema = t.input_model.model_json_schema()
        assert schema["type"] == "object"
        assert "Sinónimos:" in t.description
        assert {"readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"} <= set(t.annotations)


def test_overview_orders_what_matters(svc):
    seed(svc)
    o = tool(svc, "kafka_overview")
    assert o["today"] == "2026-10-01"
    titles = [d["title"] for d in o["this_week"] + o["next_30_days"]]
    assert any("50 %" in t for t in titles)
    assert o["counts"]["documents"] == 5 and o["counts"]["deadlines_open"] >= 5
    assert all(d["cite"].startswith("[d_") for d in o["this_week"])


def test_status_reports_every_part(svc):
    s = tool(svc, "kafka_status")
    for key in ("folders", "ocr", "scheduler", "settings", "secrets", "counts"):
        assert key in s, key
    assert s["ocr"]["available"] is False


def test_deadlines_list_filters(svc):
    seed(svc)
    up = tool(svc, "deadlines_list", filter="upcoming", days=30)
    assert up["count"] >= 3 and all(0 <= d["days_left"] <= 30 for d in up["deadlines"])
    by_kind = tool(svc, "deadlines_list", filter="open", kind="itv")
    assert by_kind["count"] == 1 and by_kind["deadlines"][0]["kind"] == "itv"
    assert tool(svc, "deadlines_list", filter="overdue")["count"] == 0
    assert tool(svc, "deadlines_list", filter="open", text="garantía")["count"] == 1


def test_deadline_get_and_explain_cite_the_document_page(svc):
    seed(svc)
    t = tool(svc, "deadlines_list", filter="open", text="50 %")["deadlines"][0]
    full = tool(svc, "deadline_get", deadline=t["id"])["deadline"]
    assert full["basis"] and full["rule"]["name"].startswith("DGT")
    ex = tool(svc, "deadline_explain", deadline=t["id"])
    assert ex["cite"].startswith("[d_") and ex["evidence"] and "legal advice" in ex["note"]
    assert ex["document"]["title"] == "Multa velocidad"


def test_deadline_write_tools(svc):
    added = tool(svc, "deadline_add", title="Renovar el pasaporte", date="2026-12-01", kind="expiry", remind=[30, 7])["deadline"]
    assert added["remind"] == [30, 7] and added["state"] == "open"
    upd = tool(svc, "deadline_update", deadline=added["id"], snooze_days=10)["deadline"]
    assert upd["date"] == "2026-12-01" or upd["date"] >= "2026-10-11"
    done = tool(svc, "deadline_update", deadline=added["id"], state="done")["deadline"]
    assert done["state"] == "done"
    with pytest.raises(KafkaError) as e:
        tool(svc, "deadline_delete", deadline=added["id"])
    assert e.value.code == "confirm_required"
    assert tool(svc, "deadline_delete", deadline=added["id"], confirm=True)["deleted"] == added["id"]
    with pytest.raises(KafkaError) as e:
        tool(svc, "deadline_get", deadline=added["id"])
    assert e.value.code == "not_found"


def test_docs_list_and_doc_get(svc):
    seed(svc)
    rows = tool(svc, "docs_list", kind="insurance")
    assert rows["count"] == 1
    doc_id = rows["documents"][0]["id"]
    got = tool(svc, "doc_get", doc=doc_id)
    assert got["document"]["kind"] == "insurance" and got["pages"] and got["deadlines"] and got["facts"]
    assert all(f["cite"].startswith(f"[{doc_id}") for f in got["facts"])
    assert tool(svc, "doc_get", doc=doc_id, include_text=False)["pages"] == []
    assert tool(svc, "docs_list", text="mapfre")["count"] == 1 and tool(svc, "docs_list", year="2019")["count"] == 0


def test_doc_search_returns_citations(svc):
    seed(svc)
    r = tool(svc, "doc_search", query="velocidad")
    assert r["count"] >= 1 and r["results"][0]["cite"].startswith("[d_") and "**" in r["results"][0]["snippet"]
    assert tool(svc, "doc_search", query="inexistente zzz")["count"] == 0
    assert tool(svc, "doc_search", query="prima", kind="insurance")["count"] == 1


def test_doc_add_file_and_its_refusals(svc, tmp_path):
    ok = tmp_path / "ticket.txt"
    ok.write_text(docs.RECEIPT, encoding="utf-8")
    r = tool(svc, "doc_add_file", path=str(ok))
    assert r["created"] and r["documents"][0]["kind"] == "receipt"
    assert tool(svc, "doc_add_file", path=str(ok))["created"] is False
    for bad in (tmp_path / ".env", tmp_path / "x.exe", tmp_path / "missing.pdf"):
        if bad.name == ".env":
            bad.write_text("A=1", encoding="utf-8")
        with pytest.raises(KafkaError):
            tool(svc, "doc_add_file", path=str(bad))


def test_doc_add_text_reports_deadlines(svc):
    r = tool(svc, "doc_add_text", title="Contrato móvil", text=docs.TELCO)
    d = r["documents"][0]
    assert d["kind"] == "contract" and any(x["kind"] == "permanence_end" for x in d["deadlines"])


def test_doc_update_reprocess_delete(svc):
    r = tool(svc, "doc_add_text", title="Ticket", text=docs.RECEIPT)
    did = r["documents"][0]["id"]
    out = tool(svc, "doc_update", doc=did, warranty_years=2)
    assert out["deadlines"][0]["date"] == "2028-08-15"
    out = tool(svc, "doc_update", doc=did, issuer="Tienda Demo", tags=["casa"])
    assert out["document"]["issuer"] == "Tienda Demo"
    rep = tool(svc, "doc_reprocess", doc=did)
    assert "issuer" in rep["kept_user_edits"] and rep["llm"] == "off"
    with pytest.raises(KafkaError):
        tool(svc, "doc_delete", doc=did)
    assert tool(svc, "doc_delete", doc=did, confirm=True)["deleted"] == did
    assert tool(svc, "docs_list")["count"] == 0 and tool(svc, "deadlines_list", filter="all")["count"] == 0


def test_extract_preview_saves_nothing(svc):
    p = tool(svc, "extract_preview", text=docs.FINE, received="2026-09-12")
    assert p["kind"] == "fine" and p["deadlines"] and p["note"].startswith("Preview only")
    assert svc.store.counts()["documents"] == 0


def test_warranty_check(svc):
    seed(svc)
    w = tool(svc, "warranty_check", text="aspirador")
    assert w["count"] == 1 and w["warranties"][0]["ends"] == "2029-08-15" and w["warranties"][0]["active"]
    assert tool(svc, "warranty_check", text="nevera")["count"] == 0
    assert tool(svc, "warranty_check")["count"] == 1


def test_series_and_price_history(svc):
    tool(svc, "doc_add_text", title="Poliza 2025", text=docs.INSURANCE)
    tool(svc, "doc_add_text", title="Poliza 2026", text=docs.insurance_renewal("352,00", "01/12/2026", "30/11/2027"))
    s = tool(svc, "series_list", kind="insurance")
    assert s["count"] == 1 and s["series"][0]["documents"] == 2 and s["series"][0]["latest_pct"] == 10.0
    h = tool(svc, "price_history", issuer="mapfre")
    assert [x["amount"] for x in h["series"][0]["history"]] == [320.0, 352.0]
    assert tool(svc, "price_history", series=s["series"][0]["id"])["series"][0]["latest_amount"] == 352.0
    with pytest.raises(KafkaError):
        tool(svc, "price_history")
    with pytest.raises(KafkaError):
        tool(svc, "price_history", issuer="nadie")


def test_mail_tools(svc, fake_mail):
    fake_mail.messages = [{"message_id": "q1@example.org", "subject": "Hola", "from_name": "Ana", "from_address": "ana@example.org", "ts": docs.T0 - 3600,
                           "text": "Nos vemos mañana en el sitio de siempre para comer, que hace mucho que no hablamos.", "attachments": []}]
    r = tool(svc, "mail_scan", since_days=7)
    assert r["ok"] and r["messages"] == 1
    listed = tool(svc, "mail_list", kind="all")
    assert listed["count"] == 1
    mid = listed["mails"][0]["message_id"]
    assert tool(svc, "mail_ignore", message_id=mid)["ignored"] == mid
    assert tool(svc, "mail_status")["ok"]
    with pytest.raises(KafkaError):
        tool(svc, "mail_accept", message_id="zzz@none")


def test_folder_tools(svc, tmp_path):
    folder = tmp_path / "papeles"
    folder.mkdir()
    f = folder / "nota.txt"
    f.write_text(docs.RECEIPT, encoding="utf-8")
    import os
    os.utime(f, (svc.clock() - 60, svc.clock() - 60))
    added = tool(svc, "folder_add", path=str(folder))
    assert added["added"]
    assert any(x["path"] == str(folder.resolve()) for x in tool(svc, "folders_list")["folders"])
    scan = tool(svc, "folder_scan")
    assert scan["new_documents"] == 1
    assert tool(svc, "folder_remove", path=str(folder))["removed"] == str(folder.resolve())
    with pytest.raises(KafkaError):
        tool(svc, "folder_add", path="/etc")


def test_phileas_sync_without_the_hub(svc):
    r = tool(svc, "phileas_sync")
    assert r["ok"] is False


def test_notification_tools(svc):
    seed(svc)
    svc.engine.run_reminders()
    assert tool(svc, "notifications_list")["notifications"]
    assert "toast" in tool(svc, "notify_status")["channels"]
    assert tool(svc, "notify_test", channel="toast")["ok"]
    assert tool(svc, "telegram_find_chat_id")["ok"] is False


def test_settings_and_secrets(svc):
    out = tool(svc, "settings_set", values={"warranty.years": 2, "calendar.region": "ES-CT", "remind.itv": "45,10"})
    assert out["settings"]["warranty.years"] == "2" and out["settings"]["calendar.region"] == "ES-CT"
    assert svc.engine.lead_days(M.ITV) == [45, 10]
    for bad in ({"warranty.years": 99}, {"nonsense.key": 1}, {"prices.alert_pct": "mucho"}, {"calendar.region": "ZZ"}):
        with pytest.raises(KafkaError):
            tool(svc, "settings_set", values=bad)
    sec = tool(svc, "secret_set", name="NTFY_TOPIC", value="mi-tema-secreto-123")
    assert sec["NTFY_TOPIC"]["value"].endswith("-123") and "mi-tema" not in json.dumps(sec)
    assert svc.secrets_status()["NTFY_TOPIC"]["configured"]
    assert tool(svc, "secret_set", name="NTFY_TOPIC", value="")["NTFY_TOPIC"]["configured"] is False


def test_scheduler_runs_and_housekeeping(svc):
    assert "lanes" in tool(svc, "scheduler_status")
    assert "archived_deadlines" in tool(svc, "housekeeping_run")
    tool(svc, "folder_scan")
    assert tool(svc, "runs_list")["runs"]


def test_language_setting_changes_titles(svc):
    tool(svc, "settings_set", values={"ui.language": "en"})
    r = tool(svc, "doc_add_text", title="Fine", text=docs.FINE)
    assert r["documents"][0]["deadlines"][0]["title"].startswith("Last day to pay")


# ------------------------------------------------------------------ masking
ID_TEXT = "Certificado\nTitular: Ana Ejemplo Prueba\nDNI 12345678Z\nCuenta ES9121000418450200051332\nTeléfono 612345678\nFecha de caducidad: 14/12/2026\n"


def test_identifiers_are_masked_for_the_assistant_unless_revealed(client):
    client.post("/api/ui/call", json={"name": "doc_add_text", "arguments": {"title": "Certificado", "text": ID_TEXT}})
    did = client.post("/api/ui/call", json={"name": "docs_list", "arguments": {}}).json()["documents"][0]["id"]
    masked = client.post("/api/agent/call", json={"name": "doc_get", "arguments": {"doc": did}}, headers=client.bearer).json()
    flat = json.dumps(masked)
    assert "12345678Z" not in flat and "ES9121000418450200051332" not in flat and "612345678" not in flat
    revealed = client.post("/api/agent/call", json={"name": "doc_get", "arguments": {"doc": did, "reveal": True}}, headers=client.bearer).json()
    assert "12345678Z" in json.dumps(revealed)
    ui = client.post("/api/ui/call", json={"name": "doc_get", "arguments": {"doc": did}}).json()
    assert "12345678Z" in json.dumps(ui)


def test_search_snippets_are_masked_too(client):
    client.post("/api/ui/call", json={"name": "doc_add_text", "arguments": {"title": "Certificado", "text": ID_TEXT}})
    r = client.post("/api/agent/call", json={"name": "doc_search", "arguments": {"query": "titular"}}, headers=client.bearer).json()
    assert r["count"] == 1 and "12345678Z" not in json.dumps(r)


def test_assistant_results_are_capped_but_the_ui_is_not(svc):
    for i in range(260):
        svc.engine.add_deadline(title=f"Plazo largo número {i} " + "x" * 100, date_=f"2026-11-{(i % 27) + 1:02d}")
    capped = tool(svc, "deadlines_list", filter="open", limit=500)
    assert "truncated" in capped and capped["count"] == 260 and len(capped["deadlines"]) < 260
    with uncapped():
        full = tool(svc, "deadlines_list", filter="open", limit=500)
    assert "truncated" not in full and len(full["deadlines"]) == 260


# ------------------------------------------------------------------ workshop tools in the catalogue
def test_the_workshop_tools_are_in_the_catalogue_and_documented():
    wanted = {"pdf_merge", "pdf_split", "pdf_pages", "pdf_compress", "pdf_protect", "pdf_watermark", "pdf_info", "pdf_metadata_set", "pdf_from_images",
              "pdf_from_office", "pdf_to_images", "images_compress"}
    names = {t.name for t in TOOLS}
    assert wanted <= names and len(names) == 55
    from pathlib import Path
    api = (Path(__file__).resolve().parent.parent / "docs" / "API.md").read_text(encoding="utf-8")
    for name in wanted:
        assert f"## `{name}`" in api
    assert "/api/workshop/upload" in api and "/api/workshop/file" in api


def test_a_workshop_tool_runs_through_the_bridge_and_writes_next_to_the_source(svc, tmp_path):
    from pathlib import Path
    from pdfmaker import make_pdf
    src = tmp_path / "papeles" / "cuenta.pdf"
    src.parent.mkdir()
    src.write_bytes(make_pdf(["Página uno", "Página dos"]))
    info = tool(svc, "pdf_info", file=str(src))
    assert info["pages"] == 2 and info["has_text"] is True
    r = tool(svc, "pdf_pages", action="extract", file=str(src), pages="2")
    assert Path(r["output"]) == src.parent / "cuenta_paginas.pdf" and r["pages"] == 1 and r["size_before"] == src.stat().st_size
    assert tool(svc, "pdf_pages", action="extract", file=str(src), pages="2")["output"].endswith("cuenta_paginas (2).pdf")


def test_manual_kind_files_without_deadlines_and_is_searchable(svc, tmp_path):
    path = tmp_path / "lavadora.txt"
    path.write_text(docs.MANUAL, encoding="utf-8")
    r = tool(svc, "doc_add_file", path=str(path), kind="manual", item="Lavadora Demo WX-100")
    d = r["documents"][0]
    assert d["kind"] == "manual" and d["deadlines"] == [] and d["state"] == "ok" and d["item"] == "Lavadora Demo WX-100"
    hit = tool(svc, "doc_search", query="filtro bomba", doc_ids=[d["id"]])
    assert hit["count"] >= 1 and hit["results"][0]["doc_id"] == d["id"] and hit["results"][0]["cite"].startswith(f"[{d['id']} · p.")
    # the same manual pasted as text is recognised by its words, and a reprocess keeps it a manual
    t = tool(svc, "doc_add_text", title="Manual pegado", text=docs.MANUAL + "\nModelo WX-100")["documents"][0]
    assert t["kind"] == "manual" and t["deadlines"] == []
    assert tool(svc, "doc_reprocess", doc=d["id"])["document"]["kind"] == "manual"
    with pytest.raises(KafkaError) as e:
        tool(svc, "doc_add_text", title="x", text=docs.RECEIPT, kind="folleto")
    assert e.value.code == "invalid"


def test_explicit_kind_does_not_override_a_user_correction_on_a_duplicate(svc):
    first = tool(svc, "doc_add_text", title="Ticket", text=docs.RECEIPT)["documents"][0]
    tool(svc, "doc_update", doc=first["id"], kind="invoice")
    again = tool(svc, "doc_add_text", title="Ticket", text=docs.RECEIPT, kind="manual")
    assert again["created"] is False and again["documents"][0]["kind"] == "invoice"


def test_search_and_list_restricted_to_document_ids(svc):
    seed(svc)
    ids = {d["kind"]: d["id"] for d in tool(svc, "docs_list")["documents"]}
    only = tool(svc, "doc_search", query="2026", doc_ids=[ids["insurance"]])
    assert only["count"] >= 1 and {r["doc_id"] for r in only["results"]} == {ids["insurance"]}
    assert tool(svc, "doc_search", query="velocidad", doc_ids=[ids["insurance"]])["count"] == 0
    assert tool(svc, "doc_search", query="velocidad", doc_ids=[])["count"] == 0
    listed = tool(svc, "docs_list", doc_ids=[ids["fine"], ids["receipt"], "d_missing"])
    assert {d["id"] for d in listed["documents"]} == {ids["fine"], ids["receipt"]} and listed["missing"] == ["d_missing"]
    tool(svc, "doc_update", doc=ids["receipt"], state="archived")
    assert tool(svc, "docs_list", doc_ids=[ids["receipt"]])["count"] == 1


def test_deadlines_from_another_app_upsert_by_external_key(svc, clock):
    args = dict(title="Mantenimiento: revisión de la caldera (Caldera demo)", date="2026-11-15", source="homehoard", external_key="mt_1",
                basis="Mantenimiento por empresa habilitada al menos cada 2 años (RITE, RD 1027/2007, IT 3.3).", rule="RITE IT 3.3",
                remind=[30, 7, 0], url="http://127.0.0.1:5196/maintenance")
    first = tool(svc, "deadline_add", **args)
    assert first["action"] == "created" and first["deadline"]["source"] == "homehoard" and first["deadline"]["external_key"] == "mt_1"
    again = tool(svc, "deadline_add", **{**args, "title": "Mantenimiento: caldera (Caldera demo)"})
    assert again["action"] == "updated" and again["deadline"]["id"] == first["deadline"]["id"]
    assert again["deadline"]["title"] == "Mantenimiento: caldera (Caldera demo)"
    assert tool(svc, "deadlines_list", filter="all", source="homehoard")["count"] == 1
    ex = tool(svc, "deadline_explain", deadline=first["deadline"]["id"])
    assert ex["rule"]["name"] == "RITE IT 3.3" and "IT 3.3" in ex["basis"] and ex["source"] == "homehoard"
    # done in the owner app: it sends the next occurrence and the deadline reopens with fresh reminders
    moved = tool(svc, "deadline_add", **{**args, "date": "2028-11-15"})
    assert moved["action"] == "rescheduled" and moved["deadline"]["date"] == "2028-11-15" and moved["deadline"]["state"] == "open"
    closed = tool(svc, "deadline_update_by_key", source="homehoard", external_key="mt_1", state="done")
    assert closed["action"] == "closed" and closed["deadline"]["state"] == "done"
    reopened = tool(svc, "deadline_add", **{**args, "date": "2028-11-15"})
    assert reopened["action"] == "reopened" and reopened["deadline"]["state"] == "open"
    resched = tool(svc, "deadline_update_by_key", source="homehoard", external_key="mt_1", date="2029-01-10")
    assert resched["action"] == "rescheduled" and resched["deadline"]["date"] == "2029-01-10"
    with pytest.raises(KafkaError) as e:
        tool(svc, "deadline_update_by_key", source="homehoard", external_key="nope", state="done")
    assert e.value.code == "not_found"
    with pytest.raises(KafkaError):
        tool(svc, "deadline_add", title="x y", date="2026-12-01", source="homehoard")
    with pytest.raises(KafkaError):
        tool(svc, "deadline_add", title="x y", date="2026-12-01", source="Home Hoard!", external_key="k")


def test_user_edits_win_over_the_owner_app(svc):
    args = dict(title="Mantenimiento: purgar radiadores (Salón)", date="2026-10-20", source="homehoard", external_key="mt_2")
    t = tool(svc, "deadline_add", **args)["deadline"]
    tool(svc, "deadline_update", deadline=t["id"], title="Purgar radiadores yo mismo", date="2026-10-25")
    same = tool(svc, "deadline_add", **{**args, "title": "Mantenimiento: otra cosa"})
    assert same["action"] == "kept_user_edits"
    assert same["deadline"]["title"] == "Purgar radiadores yo mismo" and same["deadline"]["date"] == "2026-10-25"
    nxt = tool(svc, "deadline_add", **{**args, "date": "2027-10-20"})
    assert nxt["action"] == "rescheduled" and nxt["deadline"]["date"] == "2027-10-20" and nxt["deadline"]["title"] == "Purgar radiadores yo mismo"
    tool(svc, "deadline_update", deadline=t["id"], state="dismissed")
    kept = tool(svc, "deadline_add", **{**args, "date": "2028-10-20"})
    assert kept["action"] == "kept_user_dismissed" and kept["deadline"]["state"] == "dismissed"
    assert tool(svc, "deadline_update_by_key", source="homehoard", external_key="mt_2", state="open")["action"] == "kept_user_dismissed"


def test_reminders_of_an_external_deadline_link_back_to_the_owner_app(svc, clock):
    tool(svc, "deadline_add", title="Mantenimiento: filtro de la campana (Cocina)", date="2026-10-02", source="homehoard", external_key="mt_3",
         remind=[1, 0], url="http://127.0.0.1:5196/maintenance")
    svc.engine.run_reminders()
    event = next(e for e, _ in svc.notifier.sent if e["data"].get("external_key") == "mt_3")
    assert event["url"] == "http://127.0.0.1:5196/maintenance" and event["data"]["source"] == "homehoard"
