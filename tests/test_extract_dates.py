"""Date formats and the role each date gets from the words around it."""

from datetime import date

import pytest

from kafka_hoard.extract import dates
from kafka_hoard.util import fold


def found(text):
    return [h.date for h in dates.find_dates(fold(text))]


@pytest.mark.parametrize("text,expected", [
    ("Fecha 12/10/2026", date(2026, 10, 12)),
    ("Fecha 12-10-2026", date(2026, 10, 12)),
    ("Fecha 12.10.2026", date(2026, 10, 12)),
    ("Fecha 12/10/26", date(2026, 10, 12)),
    ("Fecha 2026-10-12", date(2026, 10, 12)),
    ("el 12 de octubre de 2026", date(2026, 10, 12)),
    ("el 1 de Enero de 2027", date(2027, 1, 1)),
    ("12 oct. 2026", date(2026, 10, 12)),
    ("12 Oct 2026", date(2026, 10, 12)),
    ("October 12, 2026", date(2026, 10, 12)),
    ("Oct 12 2026", date(2026, 10, 12)),
    ("12 September 2026", date(2026, 9, 12)),
    ("3 de septiembre del 2026", date(2026, 9, 3)),
    ("12 sept 2026", date(2026, 9, 12)),
])
def test_every_date_format(text, expected):
    assert found(text) == [expected]


@pytest.mark.parametrize("text", ["31/02/2026", "45/13/2026", "Página 1/2", "tel. 91/123/4567", "1234567890"])
def test_not_dates(text):
    assert found(text) == []


def roles(text):
    folded = fold(text)
    hits = dates.find_dates(folded)
    dates.assign_roles(folded, hits)
    return {h.date.isoformat(): h.role for h in hits}


@pytest.mark.parametrize("text,date_iso,role", [
    ("Fecha de factura: 02/10/2026", "2026-10-02", "issue"),
    ("Fecha de emisión: 02/10/2026", "2026-10-02", "issue"),
    ("Fecha de vencimiento: 15/10/2026", "2026-10-15", "due"),
    ("Fecha de cargo: 15/10/2026", "2026-10-15", "due"),
    ("Se cargará en su cuenta el 15/10/2026", "2026-10-15", "due"),
    ("Pagar antes del 15/10/2026", "2026-10-15", "due"),
    ("Due date: 15/10/2026", "2026-10-15", "due"),
    ("Fecha de efecto: 01/12/2025", "2025-12-01", "effect_from"),
    ("Vigencia hasta el 30/11/2026", "2026-11-30", "effect_to"),
    ("Próxima renovación: 30/11/2026", "2026-11-30", "renewal"),
    ("Fecha de compra: 15/08/2026", "2026-08-15", "purchase"),
    ("Fecha de entrega: 20/08/2026", "2026-08-20", "delivery"),
    ("Válido hasta: 14/12/2026", "2026-12-14", "expiry"),
    ("Fecha de caducidad: 14/12/2026", "2026-12-14", "expiry"),
    ("Fecha de notificación: 12/09/2026", "2026-09-12", "notified"),
    ("Notificado el 12/09/2026", "2026-09-12", "notified"),
    ("Puesta a disposición: 12/09/2026", "2026-09-12", "notified"),
    ("Próxima inspección: 20/10/2026", "2026-10-20", "itv_next"),
    ("Permanencia hasta el 01/09/2027", "2027-09-01", "permanence_end"),
    ("Próximo cobro: 15/10/2026", "2026-10-15", "due"),
    ("Fecha de nacimiento: 03/04/1990", "1990-04-03", "birth"),
])
def test_role_by_label(text, date_iso, role):
    assert roles(text)[date_iso] == role


def test_from_to_pairing_in_one_sentence():
    r = roles("Vigencia desde las 00:00 horas del 01/12/2025 hasta las 24:00 horas del 30/11/2026")
    assert r["2025-12-01"] == "effect_from" and r["2026-11-30"] == "effect_to"


def test_billing_period_from_to():
    r = roles("Periodo de facturación: 01/09/2026 al 30/09/2026")
    assert r["2026-09-01"] == "period_from" and r["2026-09-30"] == "period_to"


def test_label_on_previous_line():
    r = roles("Fecha de vencimiento\n15/10/2026")
    assert r["2026-10-15"] == "due"


def test_table_header_labels_columns():
    r = roles("Fecha de emisión   Fecha de vencimiento\n02/10/2026   15/10/2026")
    assert r["2026-10-02"] == "issue" and r["2026-10-15"] == "due"


def test_longest_nearest_label_wins():
    # «fecha de vencimiento de la póliza» is a renewal, plain «vencimiento» a due date
    assert roles("Fecha de vencimiento de la póliza: 30/11/2026")["2026-11-30"] == "renewal"
    assert roles("Vencimiento: 30/11/2026")["2026-11-30"] == "due"


def test_unlabelled_date_has_no_role_and_is_uncertain():
    folded = fold("Gracias por todo 15/10/2026")
    hits = dates.find_dates(folded)
    dates.assign_roles(folded, hits)
    assert hits[0].role == "" and not hits[0].certain


def test_evidence_and_page_are_attached():
    text = "Primera página\n\nFecha de vencimiento: 15/10/2026\nresto"
    folded = fold(text)
    hits = dates.find_dates(folded)
    dates.assign_roles(folded, hits)
    dates.attach_evidence(text, folded, hits, lambda off: 1 if off < 10 else 2)
    assert hits[0].evidence == "Fecha de vencimiento: 15/10/2026" and hits[0].page == 2


def test_dated_first_picks_first_matching_role():
    folded = fold("Fecha de compra: 15/08/2026\nFecha de entrega: 20/08/2026")
    hits = dates.find_dates(folded)
    dates.assign_roles(folded, hits)
    d = dates.Dated(hits)
    assert d.first("delivery", "purchase").date == date(2026, 8, 20)
    assert d.first("purchase", "delivery").date == date(2026, 8, 15)
    assert d.first("birth") is None
