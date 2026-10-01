"""Mail: the classifier and the helper that turns messages (with attachments) into records."""

from __future__ import annotations

import email
import email.policy
import os
from email.message import EmailMessage

import docs
from kafka_hoard.mail import faustus_mail as helper
from kafka_hoard.mail.classify import classify_mail, readable_attachments
from pdfmaker import make_pdf


def msg(subject="", text="", sender=("Demo", "avisos@demo.example"), attachments=None, **kw):
    return {"message_id": "m", "subject": subject, "text": text, "from_name": sender[0], "from_address": sender[1], "attachments": attachments or [], **kw}


PDF = {"name": "factura.pdf", "path": "/cache/a.pdf", "size": 90_000, "mime": "application/pdf"}


def test_a_pdf_attachment_makes_a_document_mail():
    r = classify_mail(msg("Su factura de septiembre", "Adjuntamos su factura.", attachments=[PDF]))
    assert r.kind == "doc" and r.attachments == [PDF] and "PDF attached" in r.reasons


def test_newsletters_with_offers_are_noise_even_with_a_pdf_catalogue_in_the_body():
    r = classify_mail(msg("Ofertas de otoño: 30% de descuento", "Newsletter semanal. Darte de baja aquí. Ofertas y rebajas.", sender=("Tienda", "news@tienda.example")))
    assert r.kind == "noise"


def test_social_networks_are_noise():
    assert classify_mail(msg("Tienes una notificación", "Alguien te ha mencionado", sender=("LinkedIn", "no-reply@linkedin.com"))).kind == "noise"


def test_body_that_looks_like_a_document_without_attachment_is_a_doc_or_maybe():
    r = classify_mail(msg("Aviso de cargo", docs.UTILITY, sender=("Iberdrola", "clientes@iberdrola.example")))
    assert r.kind in ("doc", "maybe")


def test_plain_chat_is_noise():
    assert classify_mail(msg("Hola", "Nos vemos mañana para comer", sender=("Ana", "ana@example.org"))).kind == "noise"


def test_own_mail_is_noise():
    assert classify_mail(msg("Factura", "x", attachments=[PDF], from_self=True)).kind == "noise"


def test_small_images_and_logos_are_not_readable_attachments():
    atts = [{"name": "logo.png", "path": "/c/1.png", "size": 90_000}, {"name": "firma.jpg", "path": "/c/2.jpg", "size": 80_000},
            {"name": "pequeno.png", "path": "/c/3.png", "size": 5_000}, {"name": "ticket.jpg", "path": "/c/4.jpg", "size": 120_000},
            {"name": "sin-ruta.pdf", "size": 90_000}]
    assert [a["name"] for a in readable_attachments(msg(attachments=atts))] == ["ticket.jpg"]


def test_an_image_attachment_alone_is_a_maybe_with_document_words():
    r = classify_mail(msg("Justificante de pago", "Te envío la foto", attachments=[{"name": "foto.jpg", "path": "/c/f.jpg", "size": 200_000}]))
    assert r.kind == "maybe"


# ------------------------------------------------------------------ helper: message_to_record
def build_message(with_pdf=True, with_logo=True):
    m = EmailMessage()
    m["Subject"] = "Su factura"
    m["From"] = "Facturas Demo <facturas@demo.example>"
    m["Date"] = "Wed, 30 Sep 2026 09:00:00 +0200"
    m["Message-ID"] = "<id1@demo.example>"
    m.set_content("Texto plano de la factura. Total 48,40 EUR.")
    m.add_alternative("<html><body><p>Total <b>48,40</b> EUR</p><a href='https://demo.example/f'>ver</a></body></html>", subtype="html")
    if with_pdf:
        m.add_attachment(make_pdf(docs.UTILITY), maintype="application", subtype="pdf", filename="factura.pdf")
    if with_logo:
        m.add_attachment(b"\x89PNG....", maintype="image", subtype="png", disposition="inline")         # nameless inline image: a logo
    m.add_attachment(b"MZ", maintype="application", subtype="octet-stream", filename="programa.exe")
    return email.message_from_bytes(m.as_bytes(), policy=email.policy.default)


def test_record_has_text_links_and_saved_attachments(tmp_path):
    rec = helper.message_to_record(build_message(), None, str(tmp_path / "att"))
    assert rec["subject"] == "Su factura" and rec["from_address"] == "facturas@demo.example" and rec["ts"] and "48,40" in rec["text"]
    assert any(l["url"] == "https://demo.example/f" for l in rec["links"])
    assert [a["name"] for a in rec["attachments"]] == ["factura.pdf"]
    att = rec["attachments"][0]
    assert os.path.basename(att["path"]) == f"{att['sha']}.pdf" and os.path.getsize(att["path"]) == att["size"]


def test_no_attachments_are_written_without_a_directory(tmp_path):
    rec = helper.message_to_record(build_message(), None, "")
    assert rec["attachments"] == []


def test_attachment_limits(tmp_path, monkeypatch):
    monkeypatch.setattr(helper, "MAX_ATTACHMENT_BYTES", 1000)
    rec = helper.message_to_record(build_message(), None, str(tmp_path / "att"))
    assert rec["attachments"] == []
    monkeypatch.setattr(helper, "MAX_ATTACHMENT_BYTES", 15 * 1024 * 1024)
    monkeypatch.setattr(helper, "MAX_ATTACHMENTS", 0)
    assert helper.message_to_record(build_message(), None, str(tmp_path / "att2"))["attachments"] == []


def test_the_helper_search_defaults_cover_paperwork():
    for word in ("factura", "póliza", "contrato", "garantía", "requerimiento", "multa", "invoice"):
        assert word in helper.SUBJECT_TERMS
    assert "has:attachment filename:pdf" in helper.GMAIL_QUERY
