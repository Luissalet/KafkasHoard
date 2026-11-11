"""Structured OCR: Markdown, typed blocks, tables as rows (CSV, XLSX), language, the engine report, and the tools that expose them.

The first half uses a fake page; the second runs the real Indra engine on a synthetic scan (a known table, Spanish and English text)
and is skipped when indra-ocr or the Arial font is not installed."""

from __future__ import annotations

import importlib.util
import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image, ImageDraw, ImageFont

from conftest import build, tool
from kafka_hoard import ocr as ocr_pkg
from kafka_hoard.ocr import Ocr, structure
from kafka_hoard.ocr.base import Block, OcrResult
from kafka_hoard.ocr.indra import IndraBackend

TABLE = [["Concepto", "Cantidad", "Precio", "Total"], ["Cuota mensual", "1", "45,90", "45,90"], ["Consumo extra", "3", "2,50", "7,50"],
         ["IVA 21 %", "", "", "11,85"]]
ES = "Estimado cliente: le enviamos la factura del periodo. El importe se cargará en su cuenta el día 15 del mes siguiente."
EN = "Dear customer, this is the summary of your invoice. The amount will be charged to your bank account on the 15th."
MD = ("## Factura FAC-1\n\n" + ES + "\n\n" + EN + "\n\n<table>\n<tr><th>Concepto</th><th>Cantidad</th><th>Precio</th><th>Total</th></tr>\n"
      "<tr><td>Cuota mensual</td><td>1</td><td>45,90</td><td>45,90</td></tr>\n<tr><td>Consumo extra</td><td>3</td><td>2,50</td><td>7,50</td></tr>\n"
      "<tr><td colspan=\"3\">IVA 21 %</td><td>11,85</td></tr>\n</table>")


def fake_result() -> OcrResult:
    layout = [{"label": "Page-Header", "bbox": [0, 0, 100, 10], "text": "Mi compañía"},
              {"label": "Section-Header", "bbox": [0, 20, 300, 40], "text": "Factura FAC-1"},
              {"label": "Text", "bbox": [0, 50, 900, 90], "text": ES},
              {"label": "Text", "bbox": [0, 100, 900, 140], "text": EN},
              {"label": "Table", "bbox": [0, 150, 900, 300], "text": "Concepto\nCantidad\nPrecio\nTotal\nCuota mensual\n1\n45,90"},
              {"label": "Picture", "bbox": [0, 310, 100, 400], "text": ""}]
    lines = [Block("Factura FAC-1", [[0, 20], [300, 20], [300, 40], [0, 40]], 0.99), Block("Concepto", [[0, 150], [90, 150], [90, 170], [0, 170]], 0.9)]
    return OcrResult(blocks=lines, text="Factura FAC-1\n" + ES, backend="indra", markdown=MD, layout=layout, meta={"rotation": 0, "skew": 0.4, "source": "ocr"})


# ------------------------------------------------------------------------------------------------ structure of one page
def test_tables_come_out_as_rows_with_merged_cells_padded():
    tables = structure.parse_tables(MD)
    assert len(tables) == 1 and tables[0]["header"] and tables[0]["n_cols"] == 4
    assert tables[0]["rows"][:3] == TABLE[:3] and tables[0]["rows"][3] == ["IVA 21 %", "", "", "11,85"]
    piped = structure.markdown_with_pipes(MD)
    assert "| Concepto | Cantidad | Precio | Total |" in piped and "<table>" not in piped and "| IVA 21 % |  |  | 11,85 |" in piped


def test_a_page_has_typed_blocks_in_reading_order_lines_tables_and_languages():
    st = structure.build(fake_result(), page=2)
    assert [b["kind"] for b in st["blocks"]] == ["page_header", "heading", "paragraph", "paragraph", "table", "picture"]
    assert [b["order"] for b in st["blocks"]] == list(range(6))
    assert st["blocks"][1]["lines"][0]["text"] == "Factura FAC-1" and st["blocks"][4]["table"] == 0
    table = st["tables"][0]
    assert table["page"] == 2 and table["bbox"] == [0.0, 150.0, 900.0, 300.0] and table["csv"].splitlines()[1] == 'Cuota mensual,1,"45,90","45,90"'
    assert "Cuota mensual  1  45,90  45,90" in st["text"] and st["text"].startswith("Mi compañía")
    assert {lang["code"] for lang in st["languages"]} == {"es", "en"} and st["skew"] == 0.4
    assert "| Concepto |" in st["markdown"] and "<table>" in structure.build(fake_result(), tables_as="html")["markdown"]


def test_language_by_words_and_by_script():
    assert structure.detect_language(ES)["code"] == "es" and structure.detect_language(EN)["code"] == "en"
    assert structure.detect_language("东京大学的研究")["code"] == "zh" and structure.detect_language("これは日本語です")["code"] == "ja"
    assert structure.detect_language("ok")["code"] == ""


def test_xlsx_and_csv_files_hold_the_rows():
    data = structure.xlsx_bytes([("Tabla 1", TABLE), ("Tabla 1", [["a"]])])
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        sheet = z.read("xl/worksheets/sheet1.xml").decode("utf-8")
        book = z.read("xl/workbook.xml").decode("utf-8")
    assert "Cuota mensual" in sheet and "<v>45.90</v>" in sheet and book.count("<sheet ") == 2
    assert structure.number_value("1.234") is None and structure.number_value("007") is None and structure.number_value("1.234,56") == "1234.56"
    assert structure.to_csv([["a", "b,c"]]) == 'a,"b,c"\r\n'


# ------------------------------------------------------------------------------------------------ the engine report
class Backend(ocr_pkg.OcrBackend):
    ok, why = True, "ok"

    def __init__(self, language="es"):
        pass

    @classmethod
    def available(cls):
        return cls.ok, cls.why

    def recognise(self, image, lang="es"):
        return OcrResult(blocks=[], text=f"texto de {self.name}", backend=self.name)


@pytest.fixture
def chain(monkeypatch):
    made = {name: type(f"Fake_{name}", (Backend,), {"name": name, "ok": name in ("indra", "rapidocr"),
                                                  "why": "ok" if name in ("indra", "rapidocr") else f"{name} is not installed"}) for name in ocr_pkg.ORDER}
    monkeypatch.setattr(ocr_pkg, "_classes", lambda: made)
    return made


def test_auto_says_which_engine_read_and_why_it_was_not_indra(chain):
    o = Ocr(True)
    r = o.recognise(Image.new("RGB", (20, 20)))
    assert o.engine_report(r) == {"requested": "auto", "used": "indra", "fallback": False, "reason": ""}
    chain["indra"].ok, chain["indra"].why = False, "indra-ocr is not installed (pip install indra-ocr)"
    o2 = Ocr(True)
    r2 = o2.recognise(Image.new("RGB", (20, 20)))
    report = o2.engine_report(r2)
    assert report["used"] == "rapidocr" and report["fallback"] is True and report["reason"] == "indra: indra-ocr is not installed (pip install indra-ocr)"
    assert Ocr(True, backend="current").engine_report()["fallback"] is False


def test_ocr_image_reports_the_engine_and_returns_the_structure(config, clock, fake_mail, chain, tmp_path):
    chain["indra"].ok, chain["indra"].why = False, "indra-ocr is not installed (pip install indra-ocr)"
    s = build(config, clock, fake_mail, ocr=Ocr(True))
    try:
        p = tmp_path / "c.png"
        Image.new("RGB", (60, 40), "white").save(p)
        r = tool(s, "ocr_image", path=str(p), structure=True)
        assert r["backend"] == "rapidocr" and r["engine_report"]["fallback"] and r["engine_report"]["reason"].startswith("indra:")
        assert r["structure"]["blocks"][0]["kind"] == "paragraph" and r["structure"]["tables"] == []
    finally:
        s.stop()


def test_the_indra_backend_passes_rotation_skew_and_options():
    page = SimpleNamespace(lines=[SimpleNamespace(box=[0, 0, 10, 10], text="x", conf=0.9)], markdown="x", blocks=[], source="ocr", note="",
                           rotation=90, skew=1.25, vertical=False, timings={"total_ms": 12.0})
    backend = IndraBackend(factory=lambda: SimpleNamespace(ocr=lambda g: page))
    r = backend.recognise(Image.new("RGB", (30, 20)), options={"furniture": True})
    assert r.meta["rotation"] == 90 and r.meta["skew"] == 1.25 and r.meta["ms"] == 12.0 and r.meta["size"] == [30, 20] and r.meta["device"] == "cpu"


# ------------------------------------------------------------------------------------------------ the real engine
FONT = Path("C:/Windows/Fonts/arial.ttf")
real = pytest.mark.skipif(importlib.util.find_spec("indra_ocr") is None or not FONT.is_file(), reason="indra-ocr or Arial not installed")


def scan_page() -> Image.Image:
    img = Image.new("RGB", (1654, 1400), "white")
    d = ImageDraw.Draw(img)
    big, body = ImageFont.truetype(str(FONT), 52), ImageFont.truetype(str(FONT), 34)
    d.text((150, 120), "Factura FAC-2026-7", font=big, fill="black")
    for i, line in enumerate(["Estimado cliente: le enviamos la factura del periodo de facturación.",
                              "El importe se cargará en su cuenta el día 15 del mes siguiente.",
                              "Dear customer, this is the English summary of your invoice.",
                              "The amount will be charged to your bank account next month."]):
        d.text((150, 240 + i * 50), line, font=body, fill="black")
    cols, top, rh = [150, 650, 900, 1150, 1500], 520, 70
    for r, row in enumerate(TABLE):
        for c, cell in enumerate(row):
            d.text((cols[c] + 15, top + r * rh + 15), cell, font=body, fill="black")
    for r in range(len(TABLE) + 1):
        d.line([(cols[0], top + r * rh), (cols[-1], top + r * rh)], fill="black", width=3)
    for x in cols:
        d.line([(x, top), (x, top + rh * len(TABLE))], fill="black", width=3)
    return img


@pytest.fixture(scope="module")
def indra_svc_files(tmp_path_factory):
    folder = tmp_path_factory.mktemp("scan")
    img = scan_page()
    img.save(folder / "factura.png")
    img.save(folder / "factura.pdf", "PDF", resolution=200.0, save_all=True, append_images=[scan_page()])
    return folder


@real
def test_a_synthetic_scan_gives_the_known_table_and_both_languages(indra_svc_files):
    o = Ocr(True, workers=0)
    try:
        r = o.recognise(Image.open(indra_svc_files / "factura.png"))
        st = structure.build(r, page=1)
        assert r.backend == "indra" and st["tables"], st["markdown"]
        assert st["tables"][0]["rows"] == TABLE
        assert {lang["code"] for lang in st["languages"]} == {"es", "en"}
        assert st["blocks"][0]["kind"] == "heading" and "Cuota mensual  1  45,90  45,90" in st["text"]
        pages = o.pdf_pages((indra_svc_files / "factura.pdf").read_bytes(), [1, 2], mode="ocr")
        assert [n for n, _ in pages] == [1, 2] and all(structure.parse_tables(x.markdown)[0]["rows"] == TABLE for _, x in pages)
    finally:
        o.close()


@real
def test_the_tools_write_markdown_xlsx_and_csv_from_a_scan(config, clock, fake_mail, indra_svc_files, tmp_path):
    s = build(config, clock, fake_mail, ocr=Ocr(True, workers=0))
    try:
        pdf = str(indra_svc_files / "factura.pdf")
        tables = tool(s, "ocr_tables", path=pdf, format="csv", out_dir=str(tmp_path / "out"))
        assert tables["status"] == "done" and tables["count"] == 2 and tables["tables"][0]["rows"] == TABLE
        csv_text = Path(tables["outputs"][0]["path"]).read_text(encoding="utf-8-sig")
        assert csv_text.splitlines()[1] == 'Cuota mensual,1,"45,90","45,90"'
        md = tool(s, "pdf_to_markdown", file=pdf, out_dir=str(tmp_path / "md"))
        out = Path(md["outputs"][0]["path"])
        assert out.suffix == ".md" and "| Cuota mensual | 1 | 45,90 | 45,90 |" in out.read_text(encoding="utf-8") and md["engine_report"]["used"] == "indra"
        xl = tool(s, "pdf_tables", file=pdf, format="xlsx", out_dir=str(tmp_path / "xl"))
        assert xl["outputs"][0]["name"].endswith("_tablas.xlsx") and xl["count"] == 2
        ocr = tool(s, "ocr_pdf", path=pdf, structure=True, pages="1")
        assert ocr["tables"][0]["rows"] == TABLE and ocr["pages"][0]["structure"]["blocks"] and ocr["language"]["code"] in ("es", "en")
    finally:
        s.stop()


@real
def test_a_filed_scan_keeps_its_structure_and_search_finds_table_rows(config, clock, fake_mail, indra_svc_files):
    s = build(config, clock, fake_mail, ocr=Ocr(True, workers=0))
    try:
        out = s.engine.ingest_bytes((indra_svc_files / "factura.pdf").read_bytes(), "factura.pdf")
        doc = out["document"]
        stored = s.store.page_structures(doc["id"])
        assert sorted(stored) == [1, 2] and stored[1]["tables"][0]["rows"] == TABLE
        assert "Cuota mensual 1 45,90 45,90" in s.store.page_text(doc["id"], 1)
        assert s.store.search("Cuota mensual") and (doc.get("facts") or {}).get("ocr_engine", {}).get("used") == "indra"
        got = tool(s, "doc_structure", doc=doc["id"])
        assert got["pages_with_structure"] == [1, 2] and len(got["tables"]) == 2
        again = tool(s, "doc_structure", doc=doc["id"], rebuild=True, pages="1")
        assert again["status"] == "done" and again["rebuilt"] == [1]
    finally:
        s.stop()
