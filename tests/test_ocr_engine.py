"""The «Motor de OCR» choice: auto (Indra, the current engines as backup), indra, current; the family contract of ocr_image stays as it was."""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from PIL import Image

from conftest import build, tool
from kafka_hoard import ocr as ocr_pkg
from kafka_hoard.errors import KafkaError
from kafka_hoard.ocr import Ocr
from kafka_hoard.ocr.indra import IndraBackend


def png():
    buf = io.BytesIO()
    Image.new("RGB", (600, 400), "white").save(buf, "PNG")
    return buf.getvalue()


def write(tmp_path, name, data):
    p = tmp_path / "in" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return str(p)


class Backend(ocr_pkg.OcrBackend):
    ok, why, boom = True, "ok", None

    def __init__(self, language="es"):
        pass

    @classmethod
    def available(cls):
        return cls.ok, cls.why

    def recognise(self, image, lang="es"):
        if type(self).boom is not None:
            raise type(self).boom
        return ocr_pkg.OcrResult(blocks=[ocr_pkg.Block(f"texto de {self.name}", [[0, 0], [10, 0], [10, 10], [0, 10]], 0.9)],
                                 text=f"texto de {self.name}", backend=self.name,
                                 markdown="# titulo" if self.name == "indra" else "", layout=[{"label": "Title", "bbox": [0, 0, 10, 10], "text": "t"}] if self.name == "indra" else [])


@pytest.fixture
def chain(monkeypatch):
    made = {name: type(f"Fake_{name}", (Backend,), {"name": name, "ok": name in ("indra", "rapidocr"), "why": "ok" if name in ("indra", "rapidocr") else f"{name} is not installed",
                                                  "boom": None}) for name in ocr_pkg.ORDER}
    monkeypatch.setattr(ocr_pkg, "_classes", lambda: made)
    return made


def img():
    return Image.new("RGB", (40, 40), "white")


# ------------------------------------------------------------------------------------------------ selection
def test_auto_takes_indra_first_and_current_alone_never_takes_it(chain):
    assert ocr_pkg.select_ocr("auto")[0].name == "indra"
    assert ocr_pkg.select_ocr("current")[0].name == "rapidocr"
    assert ocr_pkg.select_ocr("indra")[0].name == "indra"


def test_auto_without_indra_uses_the_current_chain(chain):
    chain["indra"].ok, chain["indra"].why = False, "indra-ocr is not installed (pip install indra-ocr)"
    engine, notes = ocr_pkg.select_ocr("auto")
    assert engine.name == "rapidocr" and notes[0] == "indra: indra-ocr is not installed (pip install indra-ocr)"
    engine, notes = ocr_pkg.select_ocr("indra")
    assert engine.name == "rapidocr" and "requested indra unavailable, using rapidocr" in notes


def test_a_new_choice_applies_without_a_restart(chain):
    o = Ocr(True)
    assert o.recognise(img()).backend == "indra"
    o.set_backend("current")
    assert o.recognise(img()).backend == "rapidocr"
    o.set_backend("auto")
    assert o.recognise(img()).backend == "indra"
    status = o.status()
    assert status["engine"] == "auto" and status["engines"] == ["auto", "indra", "current"] and status["chain"][0] == "indra"
    assert status["backends"]["indra"]["available"] is True


# ------------------------------------------------------------------------------------------------ backup
def test_when_indra_fails_on_an_image_the_current_engine_reads_it_and_indra_stays_chosen(chain):
    o = Ocr(True)
    chain["indra"].boom = RuntimeError("indra could not read the image")
    r = o.recognise(img())
    assert r.backend == "rapidocr" and r.text == "texto de rapidocr" and r.fallback.startswith("indra: RuntimeError")
    chain["indra"].boom = None
    again = o.recognise(img())
    assert again.backend == "indra" and again.fallback == ""


def test_indra_that_cannot_load_is_dropped_for_the_current_chain(chain):
    o = Ocr(True)
    chain["indra"].boom = ImportError("DLL load failed")
    first = o.recognise(img())
    assert first.backend == "rapidocr"
    assert o.available() == (True, "rapidocr")


def test_explicit_current_never_runs_indra(chain):
    chain["indra"].boom = RuntimeError("would fail")
    o = Ocr(True, backend="current")
    assert o.recognise(img()).backend == "rapidocr"


# ------------------------------------------------------------------------------------------------ the backend itself
class FakePage:
    def __init__(self, lines=(), markdown="", blocks=(), source="ocr"):
        self.lines, self.markdown, self.blocks, self.source, self.note = list(lines), markdown, list(blocks), source, ""


def line(box, text, conf):
    return SimpleNamespace(box=box, text=text, conf=conf)


def test_the_indra_backend_maps_lines_markdown_and_layout():
    page = FakePage([line([10, 20, 110, 40], "github.com/owner/repo", 0.93), line([0, 0, 1, 1], "  ", 0.5)], "# repo",
                    [{"label": "Title", "bbox": [10, 20, 110, 40], "text": "repo"}])
    seen = {}
    conv = SimpleNamespace(ocr=lambda gray: seen.setdefault("shape", gray.shape) and page)
    r = IndraBackend(factory=lambda: conv).recognise(Image.new("RGB", (200, 100), "white"))
    assert seen["shape"] == (100, 200) and r.text == "github.com/owner/repo" and r.markdown == "# repo" and r.backend == "indra"
    assert r.blocks[0].box == [[10, 20], [110, 20], [110, 40], [10, 40]] and r.blocks[0].score == 0.93
    assert r.layout == [{"label": "Title", "bbox": [10.0, 20.0, 110.0, 40.0], "text": "repo"}]


def test_an_indra_page_marked_as_error_raises_so_the_backup_can_run():
    conv = SimpleNamespace(ocr=lambda gray: FakePage(source="error"))
    with pytest.raises(RuntimeError):
        IndraBackend(factory=lambda: conv).recognise(Image.new("RGB", (20, 20)))


def test_layout_boxes_follow_the_original_when_the_image_was_shrunk():
    page = FakePage([line([100, 100, 200, 140], "x", 0.9)], "x", [{"label": "Text", "bbox": [100, 100, 200, 140], "text": "x"}])
    o = Ocr(True, factory=None, backend="auto")
    result = o._run(IndraBackend(factory=lambda: SimpleNamespace(ocr=lambda g: page)), Image.new("RGB", (6400, 3200)), "es")
    assert result.layout[0]["bbox"] == [200.0, 200.0, 400.0, 280.0] and result.blocks[0].box[0] == [200.0, 200.0]


# ------------------------------------------------------------------------------------------------ tools, contract, setting
@pytest.fixture
def indra_svc(config, clock, fake_mail, chain):
    s = build(config, clock, fake_mail, ocr=Ocr(True))
    yield s
    s.stop()


def test_the_family_contract_of_ocr_image_is_intact(indra_svc, tmp_path):
    path = write(tmp_path, "c.png", png())
    r = tool(indra_svc, "ocr_image", path=path)
    assert {"text", "backend"} <= set(r) and r["text"] == "texto de indra" and r["backend"] == "indra" and "blocks" not in r and "markdown" not in r
    full = tool(indra_svc, "ocr_image", path=path, blocks=True, markdown=True)
    assert full["blocks"][0]["text"] == "texto de indra" and full["markdown"] == "# titulo" and full["layout"][0]["label"] == "Title"


def test_ocr_image_markdown_is_the_text_when_the_engine_has_none(indra_svc, tmp_path):
    path = write(tmp_path, "c.png", png())
    r = tool(indra_svc, "ocr_image", path=path, markdown=True, engine="current")
    assert r["backend"] == "rapidocr" and r["markdown"] == "texto de rapidocr" and r["layout"] == []


def test_ocr_engine_tool_reports_chooses_and_persists_the_setting(indra_svc, tmp_path):
    assert tool(indra_svc, "ocr_engine")["engine"] == "auto"
    r = tool(indra_svc, "ocr_engine", engine="current")
    assert r["engine"] == "current" and indra_svc.settings()["ocr.engine"] == "current"
    assert tool(indra_svc, "ocr_image", path=write(tmp_path, "c.png", png()))["backend"] == "rapidocr"
    with pytest.raises(KafkaError) as e:
        tool(indra_svc, "ocr_engine", engine="magic")
    assert e.value.code == "invalid"


def test_the_setting_is_validated_and_listed(indra_svc):
    assert indra_svc.settings()["ocr.engine"] == "auto"
    with pytest.raises(KafkaError):
        indra_svc.set_settings({"ocr.engine": "magic"})
    assert indra_svc.status()["ocr"]["engine"] == "auto"


def test_ocr_file_compares_engines_without_changing_the_setting(indra_svc, tmp_path):
    path = write(tmp_path, "c.png", png())
    a = tool(indra_svc, "ocr_file", path=path, engine="indra")
    b = tool(indra_svc, "ocr_file", path=path, engine="current")
    assert a["backend"] == "indra" and b["backend"] == "rapidocr" and a["engine"] == "indra" and b["engine"] == "current"
    assert a["seconds"] >= 0 and a["markdown"] == "# titulo" and a["blocks"]
    assert indra_svc.settings()["ocr.engine"] == "auto" and tool(indra_svc, "ocr_status")["engine"] == "auto"
    with pytest.raises(KafkaError) as e:
        tool(indra_svc, "ocr_file", path=path, engine="magic")
    assert e.value.code == "invalid"


def test_the_fallback_is_reported_to_the_consumer(indra_svc, chain, tmp_path):
    chain["indra"].boom = RuntimeError("bad page")
    r = tool(indra_svc, "ocr_image", path=write(tmp_path, "c.png", png()))
    assert r["backend"] == "rapidocr" and r["text"] == "texto de rapidocr" and r["fallback"].startswith("indra:")


def test_a_stored_choice_wins_at_start(config, clock, fake_mail, chain):
    import conftest as C

    def start():
        return C.Services(config, clock_fn=clock, http_transport=C._no_network(), notifier=C.FakeNotifier(), mail_source=fake_mail, ocr=None,
                          llm=C.FakeLlm(available=False), family_call=lambda *a, **k: {"ok": False})
    s = start()
    assert s.ocr.requested == config.ocr_backend
    s.set_settings({"ocr.engine": "current"})
    s.stop()
    s2 = start()
    try:
        assert s2.ocr.requested == "current"
    finally:
        s2.stop()


def test_rest_routes_report_and_choose(client, chain):
    r = client.get("/api/ocr")
    assert r.status_code == 200 and r.json()["engines"] == ["auto", "indra", "current"]
    ok = client.post("/api/ocr/engine", json={"engine": "indra"})
    assert ok.status_code == 200 and ok.json()["engine"] == "indra"
    assert client.post("/api/ocr/engine", json={"engine": "nope"}).status_code in (400, 422)
    names = {t["name"] for t in client.get("/api/agent/tools", headers=client.bearer).json()["tools"]}
    assert {"ocr_engine", "ocr_file"} <= names
