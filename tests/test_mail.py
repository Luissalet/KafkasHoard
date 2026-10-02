"""Mail: the classifier that decides whether a message carries paperwork."""

from __future__ import annotations

import docs
from kafka_hoard.mail.classify import classify_mail, readable_attachments


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
