"""Document dossier for a question and the requirements diff: invented data built at test time."""
import pytest

from kafka_hoard import dossier as D
from kafka_hoard.hoard_link.agentkit import AppError
from conftest import tool
from pdfmaker import make_pdf

CONTRACT = ["CONTRATO DE SERVICIOS EJEMPLO\nPrecio total: 12.000,00 EUR\nPlazo de entrega: 15/03/2026\nGarantia de 24 meses.",
            "Clausula 4. El cliente debera aportar el certificado de residencia."]
MAIL = ("Asunto: confirmacion del pedido\nHola, os confirmo que la entrega sera el 20/03/2026.\n"
        "El precio total sera de 11.500,00 euros.\nNo sera necesario aportar el certificado de residencia.\nLa garantia es de 24 meses.")
TABLE = b"Concepto;Fecha;Importe (EUR)\nEntrega;2026-03-20;\nPrecio total;;12.000,00\nSeguro;2026-04-01;300,00\n"


def file_all(svc):
    pdf = svc.engine.ingest_bytes(make_pdf(CONTRACT), "contrato.pdf", source="manual")["documents"][0]["id"]
    mail = tool(svc, "doc_add_text", title="Correo del proveedor", text=MAIL)["documents"][0]["id"]
    csv = svc.engine.ingest_bytes(TABLE, "tabla.csv", source="manual")["documents"][0]["id"]
    return pdf, mail, csv


def positions(group):
    return {p["display"]: sorted(s["doc_id"] for s in p["sources"]) for p in group["positions"]}


def test_claims_from_text_dates_amounts_quantities_and_obligations():
    c = D.claims_from_text("Garantia de 24 meses. Penalizacion del 5 % por retraso, entrega el 15 de marzo de 2026 y precio de 1.200,50 EUR.")
    kinds = {(x["kind"], x["concept"]) for x in c}
    assert ("quantity", "warranty") in kinds and ("quantity", "penalty") in kinds and ("date", "delivery") in kinds and ("amount", "price") in kinds
    assert [x["value"] for x in c if x["kind"] == "amount"] == [1200.5]
    neg = D.claims_from_text("No sera necesario aportar el certificado.")[0]
    pos = D.claims_from_text("El cliente debera aportar el certificado.")[0]
    assert neg["value"] != pos["value"] and neg["kind"] == pos["kind"] == "requirement"


def test_dossier_reports_contradictions_with_both_citations_and_agreement(svc):
    pdf, mail, csv = file_all(svc)
    d = tool(svc, "kafka_dossier", question="entrega y precio del contrato", doc_ids=[pdf, mail, csv])
    assert d["derived_by"] == "rules" and d["model_derived_text"] is False and d["id"].startswith("dos_")
    syn = d["synthesis"]
    by = {g["concept"]: g for g in syn["contradictions"] if g["kind"] != "requirement"}
    delivery = by["delivery"]
    assert delivery["status"] == "conflict" and delivery["resolved"] is False
    assert set(positions(delivery)) == {"15/03/2026", "20/03/2026"}
    assert positions(delivery)["15/03/2026"] == [pdf] and set(positions(delivery)["20/03/2026"]) == {mail, csv}
    cites = [s["cite"] for p in delivery["positions"] for s in p["sources"]]
    assert f"[{pdf} · p. 1 · l. 3]" in cites and any(c.startswith(f"[{mail} · p. 1 · l.") for c in cites)
    price = by["price"]
    assert set(positions(price)) == {"12.000,00 EUR", "11.500,00 EUR"} and set(positions(price)["12.000,00 EUR"]) == {pdf, csv}
    # agreement: the warranty is 24 months in the contract and the mail
    agreed = {g["concept"]: g for g in syn["agreements"]}
    assert "warranty" in agreed and sorted(agreed["warranty"]["sources"]) == sorted([pdf, mail])
    # the requirement conflict (must / not necessary) is a contradiction too
    assert any(g["kind"] == "requirement" and g["nature"] == "disjoint" for g in syn["contradictions"])
    con = d["conclusion"]
    assert any(c["subject"] == "la entrega" and len(c["positions"]) == 2 and not c["resolved"] for c in con["conflicts"])
    assert any("garantía" in a["statement"] for a in con["agreed"])
    assert any("entrega" in q and "15/03/2026" in q and "20/03/2026" in q for q in con["open_questions"])
    # the table claim points to its row and column
    row = next(c for sec in d["sections"].values() for c in sec["claims"] if c["doc_id"] == csv and c["kind"] == "date")
    assert row["table"]["row"] == 1 and row["claim"].startswith("Entrega")


def test_a_requirement_conflict_is_named_by_its_clause_not_by_a_concept(svc):
    v1 = tool(svc, "doc_add_text", title="Pedido caja v1", text="La entrega sera el 15/12/2026.\nEl proveedor debe entregar los planos en PDF.")["documents"][0]["id"]
    v2 = tool(svc, "doc_add_text", title="Pedido caja v2", text="La entrega sera el 20/12/2026.\nNo es necesario entregar los planos.")["documents"][0]["id"]
    con = tool(svc, "kafka_dossier", question="entrega y planos", doc_ids=[v1, v2])["conclusion"]
    subjects = [c["subject"] for c in con["conflicts"]]
    assert subjects.count("la entrega") == 1
    assert any("planos" in s for s in subjects)
    assert any("planos" in q and "obligatorio" in q for q in con["open_questions"])


def test_passages_follow_the_question_and_english_output(svc):
    pdf, mail, _ = file_all(svc)
    d = tool(svc, "kafka_dossier", question="certificate of residence required?", doc_ids=[pdf, mail], lang="en")
    assert d["conclusion"]["summary"]["conflicts"] >= 1
    d = tool(svc, "kafka_dossier", question="certificado de residencia", doc_ids=[pdf, mail], lang="en")
    assert all(p["cite"].startswith("[d_") for s in d["sections"].values() for p in s["passages"])
    assert any("certificado" in p["text"] for p in d["sections"][f"source:{pdf}"]["passages"])
    assert d["conclusion"]["conflicts"] and "disagree" in d["conclusion"]["conflicts"][0]["statement"]


def test_sources_picked_from_the_question_when_none_given(svc):
    pdf, mail, csv = file_all(svc)
    d = tool(svc, "kafka_dossier", question="garantia")
    assert {s["doc_id"] for s in d["status"]["sources"]} >= {pdf, mail}
    with pytest.raises(AppError):
        tool(svc, "kafka_dossier", question="zzzz inexistente qqqq")
    with pytest.raises(AppError):
        tool(svc, "kafka_dossier", question="algo", doc_ids=["d_nope"])


def test_requirements_diff_finds_the_modified_clause(svc):
    v1 = tool(svc, "doc_add_text", title="Condiciones v1", text="Clausula 1. Objeto del contrato.\nClausula 2. El plazo de entrega sera de 30 dias.\n"
              "Clausula 3. El cliente debera aportar el certificado de residencia.\nClausula 4. Garantia de 12 meses.")["documents"][0]["id"]
    v2 = tool(svc, "doc_add_text", title="Condiciones v2", text="Clausula 1. Objeto del contrato.\nClausula 2. El plazo de entrega sera de 45 dias.\n"
              "Clausula 3. No sera necesario aportar el certificado de residencia.\nClausula 4. Garantia de 12 meses.\nClausula 5. Penalizacion del 5 % por retraso.")["documents"][0]["id"]
    r = tool(svc, "kafka_requirements_diff", before=v1, after=v2)
    assert r["summary"] == {"added": 1, "removed": 0, "modified": 2, "unchanged": 2}
    mod = {c["clause"]: c for c in r["changes"] if c["change"] == "modified"}
    assert set(mod) == {"2", "3"}
    two = mod["2"]
    assert two["before"]["cite"].startswith(f"[{v1} · p. 1") and two["after"]["cite"].startswith(f"[{v2} · p. 1")
    assert any(w["before"] == "30" and w["after"] == "45" for w in two["words"])
    q = two["claims"][0]
    assert q["change"] == "modified" and q["before"][0]["display"] == "30 días" and q["after"][0]["display"] == "45 días"
    # the subject keeps the internal concept; the label is what a person reads, in the requested language
    assert q["subject"] in D.CONCEPT_ORDER and q["label"] == D.T["es"]["subject"][q["subject"]].capitalize()
    en = next(c for c in tool(svc, "kafka_requirements_diff", before=v1, after=v2, lang="en")["changes"] if c["clause"] == "2")["claims"][0]
    assert en["label"] == D.T["en"]["subject"][q["subject"]].capitalize()
    assert mod["3"]["claims"][0]["kind"] == "requirement"
    added = next(c for c in r["changes"] if c["change"] == "added")
    assert added["clause"] == "5" and added["claims"][0]["after"][0]["display"] == "5 %"
    same = tool(svc, "kafka_requirements_diff", before=v1, after=v1)
    assert same["changes"] == [] and same["summary"]["unchanged"] == 4


def test_a_changed_source_marks_the_dossier_stale_and_refresh_recomputes_only_its_part(svc, monkeypatch):
    pdf, mail, csv = file_all(svc)
    d = tool(svc, "kafka_dossier", question="entrega y precio", doc_ids=[pdf, mail, csv])
    did = d["id"]
    assert tool(svc, "kafka_dossier_status", dossier=did)["status"]["state"] == "fresh"
    assert tool(svc, "kafka_dossier_status")["stale"] == 0
    stamp = {k: v["computed_ts"] for k, v in d["sections"].items()}
    # the mail is corrected: its text (so its hash) changes
    svc.clock.advance(3600) if hasattr(svc.clock, "advance") else None
    svc.store.set_pages(mail, "Correo del proveedor", "", [MAIL.replace("20/03/2026", "15/03/2026")])
    st = tool(svc, "kafka_dossier_status", dossier=did)["status"]
    assert st["state"] == "stale" and st["stale_sections"] == [f"source:{mail}", "synthesis", "conclusion"]
    assert {s["doc_id"]: s["state"] for s in st["sources"]} == {pdf: "fresh", mail: "changed", csv: "fresh"}
    calls = []
    real = D.extract_source
    monkeypatch.setattr(D, "extract_source", lambda doc, *a, **k: (calls.append(doc["id"]), real(doc, *a, **k))[1])
    r = tool(svc, "kafka_dossier_refresh", dossier=did)
    assert calls == [mail] and r["recomputed"] == [f"source:{mail}", "synthesis", "conclusion"] and not r["unchanged"]
    secs = r["dossier"]["sections"]
    assert secs[f"source:{pdf}"]["computed_ts"] == stamp[f"source:{pdf}"] and secs[f"source:{csv}"]["computed_ts"] == stamp[f"source:{csv}"]
    assert secs[f"source:{mail}"]["computed_ts"] >= stamp[f"source:{mail}"]
    delivery = next(g for g in r["dossier"]["synthesis"]["groups"] if g["concept"] == "delivery")
    assert set(positions(delivery)) == {"15/03/2026", "20/03/2026"}      # the mail now agrees with the contract; the table still says 20/03
    assert tool(svc, "kafka_dossier_status", dossier=did)["status"]["state"] == "fresh"
    again = tool(svc, "kafka_dossier_refresh", dossier=did)
    assert again["unchanged"] is True and again["recomputed"] == [] and calls == [mail]


def test_a_deleted_source_is_dropped_on_refresh_and_delete_needs_confirmation(svc):
    pdf, mail, _ = file_all(svc)
    did = tool(svc, "kafka_dossier", question="precio", doc_ids=[pdf, mail])["id"]
    svc.engine.delete_document(mail)
    st = tool(svc, "kafka_dossier_status", dossier=did)["status"]
    assert st["state"] == "stale" and {s["doc_id"]: s["state"] for s in st["sources"]}[mail] == "missing"
    r = tool(svc, "kafka_dossier_refresh", dossier=did)
    assert r["dropped"] == [mail] and list(r["dossier"]["sections"]) == [f"source:{pdf}"]
    with pytest.raises(AppError):
        tool(svc, "kafka_dossier_delete", dossier=did)
    assert tool(svc, "kafka_dossier_delete", dossier=did, confirm=True) == {"deleted": did}
    assert tool(svc, "kafka_dossier_status")["count"] == 0


def test_scanned_table_text_and_ocr_flag(svc):
    text = "Concepto | Fecha | Importe\nEntrega | 20/03/2026 | 1.500,00 EUR\nSeguro | 01/04/2026 | 300,00 EUR\nPlazo de pago: 30 dias"
    doc = tool(svc, "doc_add_text", title="Tabla escaneada", text=text)["documents"][0]["id"]
    svc.store.update_document(doc, ocr=True)
    d = tool(svc, "kafka_dossier", question="entrega", doc_ids=[doc])
    claims = d["sections"][f"source:{doc}"]["claims"]
    assert any(c["kind"] == "date" and c["table"]["row"] == 1 and c["from_ocr"] for c in claims)
    assert d["sections"][f"source:{doc}"]["table_rows"] == 2
    assert any("OCR" in q for q in d["conclusion"]["open_questions"])


def test_rest_routes_and_masking(client):
    svc = client.svc
    a = tool(svc, "doc_add_text", title="A", text="Plazo de entrega: 15/03/2026\nDNI 12345678Z")["documents"][0]["id"]
    b = tool(svc, "doc_add_text", title="B", text="Plazo de entrega: 20/03/2026")["documents"][0]["id"]
    made = client.post("/api/dossiers", json={"question": "plazo de entrega", "doc_ids": [a, b]}).json()
    assert made["synthesis"]["counts"]["contradictions"] == 1
    listed = client.get("/api/dossiers").json()
    assert listed["count"] == 1 and listed["dossiers"][0]["state"] == "fresh"
    assert client.get(f"/api/dossiers/{made['id']}").json()["dossier"]["id"] == made["id"]
    assert client.post(f"/api/dossiers/{made['id']}/refresh").json()["unchanged"] is True
    diff = client.get("/api/requirements-diff", params={"before": a, "after": b}).json()
    assert diff["summary"]["modified"] == 1
    agent = client.post("/api/agent/call", headers=client.bearer, json={"name": "kafka_dossier", "arguments": {"question": "plazo", "doc_ids": [a, b]}, "reason": "test"})
    assert agent.status_code == 200 and "12345678Z" not in agent.text
    assert client.delete(f"/api/dossiers/{made['id']}").json() == {"deleted": made["id"]}
