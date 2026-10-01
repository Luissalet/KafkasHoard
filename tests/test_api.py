"""HTTP surface: health, upload, the stored file and page previews, dashboard, UI and agent bridges, PWA."""

from __future__ import annotations


import docs
from kafka_hoard.model import FINE
from pdfmaker import make_pdf


def upload(client, *files, headers=None):
    return client.post("/api/documents/upload", files=[("files", f) for f in files], headers=headers or {})


def test_health_and_status(client):
    r = client.get("/api/health")
    assert r.status_code == 200 and r.json()["service"] == "kafka-hoard"
    s = client.get("/api/status").json()
    assert s["counts"]["documents"] == 0 and "ocr" in s and "scheduler" in s


def test_upload_pdf_creates_a_document_with_deadlines(client):
    data = make_pdf(docs.FINE)
    r = upload(client, ("denuncia.pdf", data, "application/pdf"))
    assert r.status_code == 200
    body = r.json()
    assert body["created"] == 1 and body["results"][0]["documents"][0]["kind"] == FINE
    did = body["results"][0]["documents"][0]["id"]
    detail = client.get(f"/api/documents/{did}").json()
    assert detail["deadlines"] and detail["pages"] and detail["document"]["file_name"] == "denuncia.pdf"
    again = upload(client, ("copia.pdf", data, "application/pdf")).json()
    assert again["created"] == 0 and again["duplicates"] == 1


def test_upload_several_files_reports_each(client):
    r = upload(client, ("a.txt", docs.RECEIPT.encode(), "text/plain"), ("vacio.pdf", b"", "application/pdf"), ("b.txt", docs.TELCO.encode(), "text/plain"))
    results = r.json()["results"]
    assert [x["ok"] for x in results] == [True, False, True]
    assert results[1]["code"] == "invalid"


def test_stored_file_is_served_inline_for_pdf_and_as_text_for_everything_else(client):
    pdf = upload(client, ("x.pdf", make_pdf(docs.FINE), "application/pdf")).json()["results"][0]["documents"][0]["id"]
    r = client.get(f"/api/documents/{pdf}/file")
    assert r.status_code == 200 and r.headers["content-type"] == "application/pdf" and r.headers["x-content-type-options"] == "nosniff"
    html = upload(client, ("page.html", b"<html><script>alert(1)</script><p>Factura 10,00 EUR</p></html>", "text/html")).json()["results"][0]["documents"][0]["id"]
    r = client.get(f"/api/documents/{html}/file")
    assert r.headers["content-type"].startswith("text/plain") and r.headers["content-security-policy"] == "sandbox"


def test_file_endpoint_refuses_cross_site_requests(client):
    did = upload(client, ("x.pdf", make_pdf(docs.FINE), "application/pdf")).json()["results"][0]["documents"][0]["id"]
    assert client.get(f"/api/documents/{did}/file", headers={"Sec-Fetch-Site": "cross-site"}).status_code == 403
    assert client.get(f"/api/documents/{did}/file", headers={"Sec-Fetch-Site": "same-origin"}).status_code == 200


def test_page_png_is_rendered_and_cached(client):
    did = upload(client, ("x.pdf", make_pdf([docs.FINE, "Segunda página con otra cosa"]), "application/pdf")).json()["results"][0]["documents"][0]["id"]
    for n in (1, 2):
        r = client.get(f"/api/documents/{did}/page/{n}.png")
        assert r.status_code == 200 and r.content[:8] == b"\x89PNG\r\n\x1a\n"
    assert (client.svc.config.cache_dir / "pages" / f"{did}-1.png").is_file()
    assert client.get(f"/api/documents/{did}/page/1.png").status_code == 200
    assert client.get(f"/api/documents/{did}/page/9.png").status_code == 404
    assert client.get(f"/api/documents/{did}/page/0.png").status_code == 404


def test_text_documents_have_no_page_preview_and_unknown_ids_are_404(client):
    did = upload(client, ("n.txt", docs.RECEIPT.encode(), "text/plain")).json()["results"][0]["documents"][0]["id"]
    assert client.get(f"/api/documents/{did}/page/1.png").status_code in (400, 404, 415, 422)
    assert client.get("/api/documents/d_nope").status_code == 404
    assert client.get("/api/documents/d_nope/file").status_code == 404


def test_dashboard_groups_deadlines_and_tracks_news(client):
    for name, text in (("multa.txt", docs.FINE), ("seguro.txt", docs.INSURANCE), ("itv.txt", docs.ITV)):
        upload(client, (name, text.encode(), "text/plain"))
    client.svc.engine.run_reminders()
    d = client.get("/api/dashboard").json()
    assert d["today"] == "2026-10-01"
    assert any("50 %" in c["title"] for c in d["week"])
    assert any(c["kind"] == "cancel_by" for c in d["month"])
    assert d["counts"]["documents"] == 3 and d["recent_documents"] and d["news"]
    assert client.post("/api/dashboard/visit").json()["marked_seen"] >= 1
    assert client.get("/api/dashboard").json()["news"] == []


def test_ui_call_runs_tools_uncapped_and_shapes_errors(client):
    r = client.post("/api/ui/call", json={"name": "doc_add_text", "arguments": {"title": "Ticket", "text": docs.RECEIPT}})
    did = r.json()["documents"][0]["id"]
    r = client.post("/api/ui/call", json={"name": "doc_delete", "arguments": {"doc": did}})
    assert r.status_code == 400 and r.json()["code"] == "confirm_required" and r.json()["hint"]
    r = client.post("/api/ui/call", json={"name": "deadline_get", "arguments": {"deadline": "t_none"}})
    assert r.status_code == 404 and r.json()["code"] == "not_found"
    assert client.post("/api/ui/call", json={"name": "no_such_tool"}).json()["error"].startswith("Unknown tool")
    r = client.post("/api/ui/call", json={"name": "deadline_add", "arguments": {"title": "x", "date": "mañana"}})
    assert r.status_code in (400, 422)
    assert "doc_get" in client.get("/api/ui/tools").json()["tools"]


def test_agent_needs_the_token(client):
    assert client.post("/api/agent/call", json={"name": "kafka_overview"}).status_code == 401
    assert client.post("/api/agent/call", json={"name": "kafka_overview"}, headers={"Authorization": "Bearer nope"}).status_code == 401
    r = client.post("/api/agent/call", json={"name": "kafka_overview"}, headers=client.bearer)
    assert r.status_code == 200 and "overdue" in r.json()
    tools = client.get("/api/agent/tools").json()["tools"]
    assert len(tools) >= 39 and all(len(t["description"].splitlines()[0]) <= 110 for t in tools)


def test_agent_unknown_tool_and_bad_arguments(client):
    r = client.post("/api/agent/call", json={"name": "no_such"}, headers=client.bearer)
    assert r.status_code in (400, 404)
    r = client.post("/api/agent/call", json={"name": "doc_get", "arguments": {}}, headers=client.bearer)
    assert r.status_code in (400, 422)


def test_guard_rejects_cross_site(client):
    r = client.post("/api/ui/call", json={"name": "kafka_status"}, headers={"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403
    r = client.post("/api/documents/upload", files=[("files", ("a.txt", b"hola mundo cruel", "text/plain"))],
                    headers={"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403


def test_settings_roundtrip_through_the_ui(client):
    r = client.post("/api/ui/call", json={"name": "settings_set", "arguments": {"values": {"ui.language": "en", "warranty.years": 2}}})
    assert r.json()["settings"]["ui.language"] == "en"
    r = client.post("/api/ui/call", json={"name": "settings_set", "arguments": {"values": {"mail.interval_min": 1}}})
    assert r.status_code == 400
    assert client.post("/api/ui/call", json={"name": "kafka_status"}).json()["settings"]["warranty.years"] == "2"


def test_pwa_manifest_and_service_worker(client):
    m = client.get("/manifest.webmanifest").json()
    assert m["name"] == "Kafka's Hoard" and m["theme_color"] == "#1c1c14"
    assert client.get("/sw.js").status_code == 200
