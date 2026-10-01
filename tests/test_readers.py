"""Reading files: PDF text layers, OCR (fake engine), images, mail files, HTML and text, damaged inputs."""

from __future__ import annotations

import io
from email.message import EmailMessage
from types import SimpleNamespace

import pytest
from PIL import Image

import docs
from kafka_hoard import readers
from kafka_hoard.ocr import Ocr, _rows
from pdfmaker import make_pdf


class FakeEngine:
    """Stands in for rapidocr: returns the same two boxes for any image."""

    def __init__(self, texts=("Factura de ejemplo", "Total 10,00 EUR"), fail=False):
        self.texts, self.fail, self.calls = texts, fail, 0

    def __call__(self, array):
        self.calls += 1
        if self.fail:
            raise RuntimeError("boom")
        boxes = [[[10, 10 + 40 * i], [200, 10 + 40 * i], [200, 30 + 40 * i], [10, 30 + 40 * i]] for i in range(len(self.texts))]
        return SimpleNamespace(boxes=boxes, txts=list(self.texts))


def test_pdf_text_layer_is_read_per_page():
    r = readers.read_bytes("a.pdf", make_pdf([docs.FINE, "Segunda página con texto suficiente para contar como útil aquí"]), Ocr(False))
    assert r.kind == "pdf" and len(r.pages) == 2 and "Boletín de denuncia" in r.pages[0] and r.ocr_pages == 0


def test_blank_pages_without_ocr_leave_a_note_not_an_error():
    r = readers.read_bytes("a.pdf", make_pdf([docs.FINE, ""], blank_pages=(1,)), Ocr(False))
    assert r.pages[1] == "" and any("OCR is unavailable" in n for n in r.notes)


def test_blank_pages_are_read_with_ocr_when_available():
    engine = FakeEngine()
    r = readers.read_bytes("scan.pdf", make_pdf(["", docs.FINE], blank_pages=(0,)), Ocr(True, factory=lambda: engine))
    assert r.ocr_pages == 1 and "Factura de ejemplo" in r.pages[0] and "Total 10,00 EUR" in r.pages[0] and engine.calls == 1


def test_a_page_with_text_is_not_sent_to_ocr():
    engine = FakeEngine()
    readers.read_bytes("a.pdf", make_pdf(docs.FINE), Ocr(True, factory=lambda: engine))
    assert engine.calls == 0


def test_ocr_failure_never_crashes_the_read():
    r = readers.read_bytes("scan.pdf", make_pdf([""], blank_pages=(0,)), Ocr(True, factory=lambda: FakeEngine(fail=True)))
    assert r.pages == [""] and r.ocr_pages == 0


def test_ocr_reports_unavailable_when_off_or_missing():
    ok, why = Ocr(False).available()
    assert not ok and "KAFKA_OCR" in why
    broken = Ocr(True, factory=lambda: (_ for _ in ()).throw(ImportError("no module")))
    assert broken.read(Image.new("RGB", (10, 10))) == ""
    assert broken.available()[0] is False


def test_ocr_rows_group_boxes_into_reading_order_lines():
    boxes = [[[200, 12], [300, 12], [300, 30], [200, 30]], [[10, 10], [100, 10], [100, 30], [10, 30]], [[10, 60], [100, 60], [100, 80], [10, 80]]]
    assert _rows(boxes, ["derecha", "izquierda", "segunda línea"]) == "izquierda  derecha\nsegunda línea"
    assert _rows(None, None) == "" and _rows([[[0, 0]]], ["x"]) == "x"


def png_bytes(size=(600, 400)):
    buf = io.BytesIO()
    Image.new("RGB", size, "white").save(buf, "PNG")
    return buf.getvalue()


def test_image_is_read_with_ocr_or_noted_without():
    r = readers.read_bytes("ticket.png", png_bytes(), Ocr(True, factory=lambda: FakeEngine()))
    assert r.kind == "image" and "Factura de ejemplo" in r.pages[0] and r.ocr_pages == 1
    r = readers.read_bytes("ticket.png", png_bytes(), Ocr(False))
    assert r.pages == [""] and any("OCR is unavailable" in n for n in r.notes)


def test_huge_images_are_shrunk_before_ocr():
    seen = {}

    def engine(array):
        seen["shape"] = array.shape
        return SimpleNamespace(boxes=[], txts=[])
    Ocr(True, factory=lambda: engine).read(Image.new("RGB", (6400, 3200)))
    assert max(seen["shape"][:2]) == 3200


def test_corrupt_inputs_raise_read_error():
    with pytest.raises(readers.ReadError):
        readers.read_bytes("x.pdf", b"%PDF-1.4 not really", Ocr(False))
    with pytest.raises(readers.ReadError):
        readers.read_bytes("x.png", b"\x89PNG garbage", Ocr(False))


def test_sniff_trusts_content_over_name():
    assert readers.sniff("factura", make_pdf("hola")) == "pdf"
    assert readers.sniff("a.txt", b"\x89PNG\r\n\x1a\n....") == "png"
    assert readers.sniff("a.TXT", b"hola") == "txt" and readers.sniff("", b"hola") == ""


def test_text_and_html_are_read_and_html_scripts_dropped():
    r = readers.read_bytes("a.txt", "Prima total: 320,00 €\nTomador: Ñandú".encode("utf-8"), Ocr(False))
    assert "Ñandú" in r.pages[0]
    r = readers.read_bytes("a.txt", "Prima: 5 euros ñ".encode("latin-1"), Ocr(False))
    assert "ñ" in r.pages[0]
    h = readers.read_bytes("a.html", b"<html><style>p{}</style><script>evil()</script><p>Total <b>10,00</b> EUR</p></html>", Ocr(False))
    assert "10,00" in h.pages[0] and "evil" not in h.pages[0] and "p{}" not in h.pages[0]


def test_long_text_is_split_into_pages():
    pages = readers.split_pages("línea de texto\n" * 6000)
    assert len(pages) > 1 and all(p.strip() for p in pages)


def test_unknown_types_are_reported_as_unsupported():
    r = readers.read_bytes("a.xyz", b"data", Ocr(False))
    assert r.kind == "unsupported" and r.notes


def make_eml(with_pdf=True, html=False):
    msg = EmailMessage()
    msg["Subject"] = "Su factura de agosto"
    msg["From"] = "Facturas Demo <facturas@demo.example>"
    msg["Date"] = "Wed, 30 Sep 2026 09:00:00 +0200"
    msg["Message-ID"] = "<abc@demo.example>"
    if html:
        msg.set_content("<p>Total <b>48,40</b> EUR</p>", subtype="html")
    else:
        msg.set_content("Adjuntamos su factura. Total 48,40 EUR.")
    if with_pdf:
        msg.add_attachment(make_pdf(docs.UTILITY), maintype="application", subtype="pdf", filename="factura.pdf")
    msg.add_attachment(b"MZ....", maintype="application", subtype="octet-stream", filename="virus.exe")
    return msg.as_bytes()


def test_eml_exposes_headers_body_and_only_readable_attachments():
    r = readers.read_bytes("mail.eml", make_eml(), Ocr(False))
    assert r.kind == "eml" and r.mail["subject"] == "Su factura de agosto" and r.mail["from_address"] == "facturas@demo.example"
    assert r.mail["ts"] and r.mail["message_id"] == "<abc@demo.example>"
    assert [c.name for c in r.children] == ["factura.pdf"] and "48,40" in r.pages[0]


def test_html_mail_body_is_converted_to_text():
    r = readers.read_bytes("mail.eml", make_eml(with_pdf=False, html=True), Ocr(False))
    assert "<b>" not in r.pages[0] and "48,40" in r.pages[0]


def test_docx_text_is_extracted(tmp_path):
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", '<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                   "<w:body><w:p><w:r><w:t>Contrato de alquiler</w:t></w:r></w:p><w:p><w:r><w:t>Renta mensual 700,00 EUR</w:t></w:r></w:p></w:body></w:document>")
    r = readers.read_bytes("c.docx", buf.getvalue(), Ocr(False))
    assert r.kind == "docx" and "Contrato de alquiler" in r.pages[0] and "700,00" in r.pages[0]
