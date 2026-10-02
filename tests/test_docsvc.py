"""The family's extraction and OCR service: doc_extract, ocr_image, ocr_pdf, ocr_status (Hoard Link services contract, section 5)."""

from __future__ import annotations

import io
import threading
import zipfile
from types import SimpleNamespace

import pytest
from PIL import Image

import docs
from conftest import build, tool
from kafka_hoard import ocr as ocr_pkg
from kafka_hoard.docsvc import JobTable
from kafka_hoard.errors import KafkaError
from kafka_hoard.ocr import Ocr
from pdfmaker import make_pdf


class FakeEngine:
    """Stands in for rapidocr: two lines for any image; ``gate`` holds the first call until the test lets it go."""

    def __init__(self, texts=("Factura de ejemplo", "Total 10,00 EUR"), gate=None):
        self.texts, self.calls, self.gate = texts, 0, gate

    def __call__(self, array):
        self.calls += 1
        if self.gate is not None:
            self.gate.wait(10)
        boxes = [[[10, 10 + 40 * i], [200, 10 + 40 * i], [200, 30 + 40 * i], [10, 30 + 40 * i]] for i in range(len(self.texts))]
        return SimpleNamespace(boxes=boxes, txts=list(self.texts), scores=[0.97] * len(self.texts))


def png(size=(600, 400)):
    buf = io.BytesIO()
    Image.new("RGB", size, "white").save(buf, "PNG")
    return buf.getvalue()


@pytest.fixture
def engine():
    return FakeEngine()


@pytest.fixture
def ocr_svc(config, clock, fake_mail, engine):
    s = build(config, clock, fake_mail, ocr=Ocr(True, factory=lambda: engine))
    yield s
    s.stop()


def write(tmp_path, name, data):
    p = tmp_path / "in" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return str(p)


def minimal_docx():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", '<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                   "<w:body><w:p><w:r><w:t>Contrato de alquiler</w:t></w:r></w:p><w:p><w:r><w:t>Renta mensual 700,00 EUR</w:t></w:r></w:p></w:body></w:document>")
    return buf.getvalue()


# ------------------------------------------------------------------------------------------------ doc_extract
def test_doc_extract_reads_a_text_layer_without_ocr(ocr_svc, engine, tmp_path):
    path = write(tmp_path, "multa.pdf", make_pdf([docs.FINE, "Segunda página con texto suficiente para contar como útil aquí"]))
    r = tool(ocr_svc, "doc_extract", path=path)
    assert r["status"] == "done" and r["kind"] == "pdf" and r["mime"] == "application/pdf" and r["pages_ocr"] == 0
    assert [u["number"] for u in r["units"]] == [1, 2] and all(u["kind"] == "page" for u in r["units"])
    assert "Boletín de denuncia" in r["units"][0]["text"] and r["text"].startswith(r["units"][0]["text"])
    assert r["needs_ocr"] is False and not r.get("error") and engine.calls == 0


def test_doc_extract_reads_only_the_pages_without_a_text_layer(ocr_svc, engine, tmp_path):
    path = write(tmp_path, "mixto.pdf", make_pdf(["", docs.FINE], blank_pages=(0,)))
    r = tool(ocr_svc, "doc_extract", path=path)
    assert r["pages_ocr"] == 1 and engine.calls == 1
    assert "Factura de ejemplo" in r["units"][0]["text"] and "Boletín de denuncia" in r["units"][1]["text"]
    assert r["needs_ocr"] is False and not any(u.get("needs_ocr") for u in r["units"])


def test_doc_extract_ocr_off_leaves_scans_flagged(ocr_svc, engine, tmp_path):
    path = write(tmp_path, "escaneo.pdf", make_pdf(["", ""], blank_pages=(0, 1)))
    r = tool(ocr_svc, "doc_extract", path=path, ocr="off")
    assert r["needs_ocr"] is True and r["pages_ocr"] == 0 and engine.calls == 0
    assert all(u["needs_ocr"] for u in r["units"]) and any("OCR" in n for n in r["notes"])


def test_doc_extract_force_runs_ocr_on_every_page(ocr_svc, engine, tmp_path):
    path = write(tmp_path, "texto.pdf", make_pdf([docs.FINE, "Segunda página con texto suficiente para contar como útil aquí"]))
    r = tool(ocr_svc, "doc_extract", path=path, ocr="force")
    assert engine.calls == 2 and r["pages_ocr"] == 2 and "Factura de ejemplo" in r["units"][0]["text"]


def test_doc_extract_without_an_engine_says_the_scan_needs_ocr(svc, tmp_path):
    path = write(tmp_path, "escaneo.pdf", make_pdf(["", ""], blank_pages=(0, 1)))
    r = tool(svc, "doc_extract", path=path)
    assert r["needs_ocr"] is True and r["pages_ocr"] == 0 and any("OCR is unavailable" in n for n in r["notes"])


def test_doc_extract_respects_max_pages(ocr_svc, tmp_path):
    path = write(tmp_path, "largo.pdf", make_pdf([f"Página {i} con texto suficiente para contar como útil aquí sí" for i in range(1, 6)]))
    r = tool(ocr_svc, "doc_extract", path=path, max_pages=2)
    assert len(r["units"]) == 2 and any("first 2" in n for n in r["notes"])


def test_doc_extract_reads_office_html_and_text_through_the_light_readers(ocr_svc, tmp_path):
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "c.docx", minimal_docx()))
    assert r["kind"] == "docx" and "Renta mensual 700,00 EUR" in r["text"] and r["pages_ocr"] == 0 and r["status"] == "done"
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "a.html", b"<html><script>evil()</script><p>Total <b>10,00</b> EUR</p></html>"))
    assert r["kind"] == "html" and "10,00" in r["text"] and "evil" not in r["text"]
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "n.txt", "Prima: 320,00 €\nTomador: Ñandú".encode("utf-8")))
    assert r["kind"] == "text" and "Ñandú" in r["text"]


def test_doc_extract_reads_a_photo_with_ocr(ocr_svc, engine, tmp_path):
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "ticket.png", png()))
    assert r["kind"] == "image" and r["pages_ocr"] == 1 and "Total 10,00 EUR" in r["text"] and r["needs_ocr"] is False
    assert r["units"] == [{"kind": "page", "number": 1, "title": "", "text": r["text"]}]
    off = tool(ocr_svc, "doc_extract", path=write(tmp_path, "ticket.png", png()), ocr="off")
    assert off["needs_ocr"] is True and off["text"] == "" and any("OCR was not run" in n for n in off["notes"])


def test_a_damaged_file_is_an_answer_with_error_not_a_failed_call(ocr_svc, tmp_path):
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "roto.pdf", b"%PDF-1.4 not really"))
    assert r["status"] == "done" and r["error"] and r["text"] == "" and r["kind"] == "pdf"
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "roto.docx", b"PK\x03\x04junk"))
    assert r["error"] and r["text"] == ""
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "raro.bin", b"\x00\x01\x02\x03"))
    assert r["error"] and "unsupported" in r["error"]


def test_the_text_of_the_file_is_not_masked_because_another_app_indexes_it(ocr_svc, tmp_path):
    r = tool(ocr_svc, "doc_extract", path=write(tmp_path, "dni.txt", docs.DNI.encode("utf-8")))
    assert "12345678Z" in r["text"] or "DNI" in r["text"]
    assert "<DNI>" not in r["text"]


@pytest.mark.parametrize("code", ["invalid", "not_found"])
def test_doc_extract_refuses_bad_paths(ocr_svc, tmp_path, code):
    arguments = {"path": "relative/x.pdf" if code == "invalid" else str(tmp_path / "missing.pdf")}
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "doc_extract", **arguments)
    assert e.value.code == code


def test_doc_extract_refuses_credential_files_and_empty_or_huge_ones(ocr_svc, tmp_path, monkeypatch):
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "doc_extract", path=write(tmp_path, "id_rsa", b"-----BEGIN"))
    assert e.value.code == "forbidden" and e.value.status == 403
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "doc_extract", path=write(tmp_path, "vacio.txt", b""))
    assert e.value.code == "invalid"
    from kafka_hoard import docsvc
    monkeypatch.setattr(docsvc, "MAX_BYTES", 10)
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "doc_extract", path=write(tmp_path, "grande.txt", b"x" * 50))
    assert e.value.code == "too_large" and e.value.status == 413


def test_doc_extract_rejects_an_unknown_ocr_mode(ocr_svc, tmp_path):
    with pytest.raises(Exception) as e:
        tool(ocr_svc, "doc_extract", path=write(tmp_path, "a.txt", b"hola mundo"), ocr="sometimes")
    assert "ocr" in str(e.value)


# ------------------------------------------------------------------------------------------------ jobs
def test_a_long_extraction_answers_a_job_that_ocr_status_follows(config, clock, fake_mail, tmp_path):
    gate = threading.Event()
    engine = FakeEngine(gate=gate)
    s = build(config, clock, fake_mail, ocr=Ocr(True, factory=lambda: engine))
    try:
        path = write(tmp_path, "escaneo.pdf", make_pdf(["", ""], blank_pages=(0, 1)))
        first = tool(s, "doc_extract", path=path, wait_s=0)
        assert first["status"] in ("queued", "running") and first["job_id"].startswith("ocr_")
        assert set(first) >= {"job_id", "status", "progress"} and "text" not in first
        gate.set()
        done = tool(s, "ocr_status", job_id=first["job_id"], wait_s=20)
        assert done["status"] == "done" and done["pages_ocr"] == 2 and done["pages_done"] == 2 and done["pages_total"] == 2
        assert done["progress"] == 1.0 and "Factura de ejemplo" in done["text"]
        again = tool(s, "ocr_status", job_id=first["job_id"])
        assert again["status"] == "done" and again["text"] == done["text"]
    finally:
        gate.set()
        s.stop()


def test_ocr_status_waits_but_never_longer_than_asked(config, clock, fake_mail, tmp_path):
    gate = threading.Event()
    s = build(config, clock, fake_mail, ocr=Ocr(True, factory=lambda: FakeEngine(gate=gate)))
    try:
        path = write(tmp_path, "escaneo.pdf", make_pdf([""], blank_pages=(0,)))
        job = tool(s, "doc_extract", path=path, wait_s=0)
        still = tool(s, "ocr_status", job_id=job["job_id"], wait_s=0.3)
        assert still["status"] == "running" and still.get("still_running") is True and "text" not in still
    finally:
        gate.set()
        s.stop()


def test_an_unknown_job_is_not_found(svc):
    with pytest.raises(KafkaError) as e:
        tool(svc, "ocr_status", job_id="ocr_nope")
    assert e.value.code == "not_found" and e.value.status == 404 and "no such job" in e.value.message


def test_finished_jobs_are_kept_one_hour_and_the_newest_fifty():
    now = [1000.0]
    table = JobTable(workers=1, keep_s=3600, keep_n=3, clock=lambda: now[0])
    ids = []
    for i in range(6):
        jid = table.submit("t", lambda job, i=i: {"n": i})
        assert table.wait(jid, 5)["status"] == "done"
        ids.append(jid)
        now[0] += 1
    table.submit("t", lambda job: {})                          # pruning happens on submit
    assert [table.view(j) is not None for j in ids] == [False, False, False, False, True, True] or sum(table.view(j) is not None for j in ids) <= 4
    now[0] += 4000
    last = table.submit("t", lambda job: {"n": 99})
    assert table.wait(last, 5)["n"] == 99 and all(table.view(j) is None for j in ids)
    table.stop()


def test_a_failing_job_is_an_error_view_not_a_dead_worker():
    table = JobTable(workers=1)
    bad = table.submit("t", lambda job: (_ for _ in ()).throw(RuntimeError("boom")))
    v = table.wait(bad, 5)
    assert v["status"] == "error" and "boom" in v["error"] and v["ok"] is False
    good = table.submit("t", lambda job: {"fine": True})
    assert table.wait(good, 5)["fine"] is True
    table.stop()


# ------------------------------------------------------------------------------------------------ ocr_image
def test_ocr_image_returns_text_and_blocks_on_request(ocr_svc, tmp_path):
    path = write(tmp_path, "ticket.png", png())
    plain = tool(ocr_svc, "ocr_image", path=path)
    assert plain["text"] == "Factura de ejemplo\nTotal 10,00 EUR" and plain["backend"] == "rapidocr" and "blocks" not in plain
    full = tool(ocr_svc, "ocr_image", path=path, blocks=True)
    assert [b["text"] for b in full["blocks"]] == ["Factura de ejemplo", "Total 10,00 EUR"]
    assert full["blocks"][0]["box"] == [[10.0, 10.0], [200.0, 10.0], [200.0, 30.0], [10.0, 30.0]] and full["blocks"][0]["score"] == 0.97


def test_ocr_image_blocks_are_in_the_coordinates_of_the_original_when_it_was_shrunk(ocr_svc, tmp_path):
    big = io.BytesIO()
    Image.new("RGB", (6400, 3200), "white").save(big, "PNG")
    full = tool(ocr_svc, "ocr_image", path=write(tmp_path, "grande.png", big.getvalue()), blocks=True)
    assert full["blocks"][0]["box"][1][0] == pytest.approx(400.0, abs=1.0)          # 200 px of a 3200-wide copy are 400 px of 6400


def test_ocr_image_without_an_engine_is_503_with_an_install_hint(svc, tmp_path):
    with pytest.raises(KafkaError) as e:
        tool(svc, "ocr_image", path=write(tmp_path, "ticket.png", png()))
    assert e.value.code == "ocr_unavailable" and e.value.status == 503 and "pip install" in e.value.hint


def test_ocr_image_refuses_what_is_not_an_image(ocr_svc, tmp_path):
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "ocr_image", path=write(tmp_path, "nota.txt", b"hola"))
    assert e.value.code == "invalid" and "not a readable image" in e.value.message


# ------------------------------------------------------------------------------------------------ ocr_pdf
def test_ocr_pdf_recognises_the_pages_asked_for(ocr_svc, engine, tmp_path):
    path = write(tmp_path, "escaneo.pdf", make_pdf(["", "", ""], blank_pages=(0, 1, 2)))
    r = tool(ocr_svc, "ocr_pdf", path=path, pages="1,3")
    assert r["status"] == "done" and r["pages_done"] == 2 and r["pages_total"] == 2 and r["backend"] == "rapidocr"
    assert [p["page"] for p in r["pages"]] == [1, 3] and "Factura de ejemplo" in r["pages"][0]["text"] and engine.calls == 2
    assert r["text"].count("Total 10,00 EUR") == 2


def test_ocr_pdf_defaults_to_the_first_max_pages_and_says_so(ocr_svc, engine, tmp_path):
    path = write(tmp_path, "escaneo.pdf", make_pdf([""] * 4, blank_pages=(0, 1, 2, 3)))
    r = tool(ocr_svc, "ocr_pdf", path=path, max_pages=2)
    assert [p["page"] for p in r["pages"]] == [1, 2] and any("only 2 of 4" in n for n in r["notes"])


def test_ocr_pdf_errors_are_clear(ocr_svc, svc, tmp_path):
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "ocr_pdf", path=write(tmp_path, "a.txt", b"hola"))
    assert e.value.code == "invalid" and "not a PDF" in e.value.message
    with pytest.raises(KafkaError) as e:
        tool(ocr_svc, "ocr_pdf", path=write(tmp_path, "b.pdf", make_pdf(["", ""], blank_pages=(0, 1))), pages="9")
    assert e.value.code == "invalid" and "no existe" in e.value.message
    with pytest.raises(KafkaError) as e:
        tool(svc, "ocr_pdf", path=write(tmp_path, "c.pdf", make_pdf([""], blank_pages=(0,))))
    assert e.value.code == "ocr_unavailable" and e.value.status == 503


# ------------------------------------------------------------------------------------------------ ocr_status and backends
def test_ocr_status_reports_the_engine(ocr_svc, svc):
    r = tool(ocr_svc, "ocr_status")
    assert r["available"] is True and r["backend"] == "rapidocr" and "es" in r["languages"]
    off = tool(svc, "ocr_status")
    assert off["available"] is False and off["backend"] == "" and "KAFKA_OCR" in off["reason"]


class FakeBackend(ocr_pkg.OcrBackend):
    ok, why, started = True, "ok", []

    def __init__(self, language="es"):
        type(self).started.append(self.name)

    @classmethod
    def available(cls):
        return cls.ok, cls.why

    def recognise(self, image, lang="es"):
        return ocr_pkg.OcrResult(blocks=[ocr_pkg.Block("hola", [[0, 0], [10, 0], [10, 10], [0, 10]], 0.9)], text="hola", backend=self.name)


def fake_classes(monkeypatch, **state):
    made = {}
    for name in ("winocr", "rapidocr", "tesseract"):
        ok, why = state.get(name, (False, f"{name} is not installed"))
        made[name] = type(f"Fake_{name}", (FakeBackend,), {"name": name, "ok": ok, "why": why, "started": []})
    monkeypatch.setattr(ocr_pkg, "_classes", lambda: made)
    return made


def test_auto_prefers_winocr_then_rapidocr_then_tesseract(monkeypatch):
    classes = fake_classes(monkeypatch, winocr=(True, "ok"), rapidocr=(True, "ok"), tesseract=(True, "ok"))
    engine, notes = ocr_pkg.select_ocr("auto")
    assert engine.name == "winocr" and notes == []
    classes["winocr"].ok, classes["winocr"].why = False, "winocr only works on Windows"
    engine, notes = ocr_pkg.select_ocr("auto")
    assert engine.name == "rapidocr" and notes == ["winocr: winocr only works on Windows"]
    classes["rapidocr"].ok = False
    assert ocr_pkg.select_ocr("auto")[0].name == "tesseract"


def test_a_requested_backend_wins_and_falls_back_with_a_note(monkeypatch):
    fake_classes(monkeypatch, winocr=(True, "ok"), rapidocr=(True, "ok"), tesseract=(False, "no program"))
    engine, notes = ocr_pkg.select_ocr("rapidocr")
    assert engine.name == "rapidocr" and notes == []
    engine, notes = ocr_pkg.select_ocr("tesseract")
    assert engine.name == "winocr" and notes[0] == "tesseract: no program" and "requested tesseract unavailable, using winocr" in notes


def test_no_backend_at_all_is_none_with_every_reason(monkeypatch):
    fake_classes(monkeypatch)
    engine, notes = ocr_pkg.select_ocr("auto")
    assert engine is None and len(notes) == 4 and notes[-1] == "no OCR engine available"
    ok, why = Ocr(True).available()
    assert ok is False and "rapidocr" in why


def test_the_facade_uses_the_selected_backend_and_reports_availability(monkeypatch):
    fake_classes(monkeypatch, tesseract=(True, "ok"))
    o = Ocr(True, backend="auto")
    assert o.available() == (True, "tesseract") and o.read(Image.new("RGB", (20, 20))) == "hola"
    status = o.status()
    assert status["available"] and status["backend"] == "tesseract" and status["backends"]["winocr"]["available"] is False


def test_a_backend_that_cannot_run_hands_over_to_the_next_one(monkeypatch):
    classes = fake_classes(monkeypatch, winocr=(True, "ok"), rapidocr=(True, "ok"))

    def broken(self, image, lang="es"):
        raise ImportError("DLL load failed")
    classes["winocr"].recognise = broken
    o = Ocr(True)
    result = o.recognise(Image.new("RGB", (20, 20)))
    assert result.backend == "rapidocr" and result.text == "hola"
    assert o.available() == (True, "rapidocr")


def test_one_odd_image_does_not_disable_ocr(monkeypatch):
    classes = fake_classes(monkeypatch, rapidocr=(True, "ok"))
    calls = {"n": 0}
    original = classes["rapidocr"].recognise

    def flaky(self, image, lang="es"):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("odd image")
        return original(self, image, lang)
    classes["rapidocr"].recognise = flaky
    o = Ocr(True)
    first = o.recognise(Image.new("RGB", (20, 20)))
    assert first.error and first.text == ""
    assert o.recognise(Image.new("RGB", (20, 20))).text == "hola"


def test_language_codes_are_normalised():
    from kafka_hoard.ocr.base import lang_code
    assert [lang_code(x) for x in ("es", "ES", "es-ES", "spa", "eng", "fr_FR", "", None)] == ["es", "es", "es", "es", "en", "fr", "es", "es"]


# ------------------------------------------------------------------------------------------------ through the HTTP bridge
def test_the_new_tools_are_in_the_catalogue_and_callable_over_http(client, tmp_path):
    names = {t["name"] for t in client.get("/api/agent/tools").json()["tools"]}
    assert {"doc_extract", "ocr_image", "ocr_pdf", "ocr_status"} <= names
    path = write(tmp_path, "nota.txt", b"Total a pagar: 55,00 EUR")
    r = client.post("/api/agent/call", json={"name": "doc_extract", "arguments": {"path": path}}, headers=client.bearer)
    assert r.status_code == 200 and r.json()["status"] == "done" and "55,00" in r.json()["text"]
    assert client.post("/api/agent/call", json={"name": "doc_extract", "arguments": {"path": path}}).status_code == 401
    bad = client.post("/api/agent/call", json={"name": "ocr_status", "arguments": {"job_id": "ocr_nope"}}, headers=client.bearer)
    assert bad.status_code == 404 and bad.json()["code"] == "not_found"
    unavailable = client.post("/api/agent/call", json={"name": "ocr_image", "arguments": {"path": write(tmp_path, "t.png", png())}}, headers=client.bearer)
    assert unavailable.status_code == 503 and unavailable.json()["code"] == "ocr_unavailable"
