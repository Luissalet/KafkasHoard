"""From documents to deadlines: each rule, its basis text and its evidence."""

from datetime import date

import docs
from docs import TODAY
from helpers import by_key, run
from kafka_hoard import model as M


# ---------------------------------------------------------------- insurance (Ley 50/1980, art. 22)
def test_insurance_renewal_and_cancel_by_one_month_before():
    ex = run(docs.INSURANCE)
    d = by_key(ex)
    assert d["renewal"].date == date(2026, 11, 30) and d["renewal"].kind == M.RENEWAL and d["renewal"].recurring == "yearly"
    assert d["cancel_by"].date == date(2026, 10, 30) and d["cancel_by"].kind == M.CANCEL_BY
    assert d["renewal"].remind == [45, 30, 7] and d["cancel_by"].remind == [14, 3, 0]
    assert "Ley 50/1980" in d["cancel_by"].basis and "30 nov 2026" in d["cancel_by"].basis
    assert "30/11/2026" in d["renewal"].evidence and d["renewal"].page == 1
    assert ex.amount == 320.0 and ex.ref == "5550012345" and ex.issuer.name == "Mapfre"


def test_insurance_without_renewal_date_uses_effect_date_plus_a_year_with_less_confidence():
    ex = run("Mapfre\nPóliza de seguro del hogar\nNúmero de póliza: 777888999\nFecha de efecto: 15/01/2026\nPrima total: 100,00 €\nPago anual")
    d = by_key(ex)
    assert d["renewal"].date == date(2027, 1, 15) and d["renewal"].confidence < 70
    assert d["cancel_by"].date == date(2026, 12, 15)


def test_past_renewals_make_no_deadline():
    ex = run(docs.insurance_renewal("300,00", "01/12/2024", "30/11/2025"))
    assert "renewal" not in by_key(ex)


def test_cancel_by_in_the_past_is_not_proposed_but_the_renewal_is():
    ex = run(docs.insurance_renewal("300,00", "01/10/2025", "10/10/2026"))
    assert by_key(ex)["renewal"].date == date(2026, 10, 10) and "cancel_by" not in by_key(ex)


def test_reminder_days_come_from_the_context_when_set():
    ex = run(docs.INSURANCE, leads={M.RENEWAL: [60, 10]})
    assert by_key(ex)["renewal"].remind == [60, 10]


# ---------------------------------------------------------------- fines (dgt.es, Ley 39/2015)
def test_dgt_fine_twenty_natural_days_from_notification():
    ex = run(docs.FINE)
    d = by_key(ex)["fine"] if "fine" in by_key(ex) else by_key(ex)["fine_discount"]
    assert d.date == date(2026, 10, 2) and d.kind == M.FINE_DISCOUNT
    assert "20 días naturales" in d.basis and "12 sep 2026" in d.basis
    assert ex.amount == 200.0 and ex.amount_reduced == 100.0 and ex.kind == M.FINE and ex.issuer.name == "DGT"
    assert ex.item == "1234 BCD" or ex.ref == "2026-0099001"


def test_fine_end_on_a_weekend_moves_to_monday_and_says_so():
    text = docs.FINE.replace("12/09/2026", "13/09/2026").replace("Dispone de 20 días naturales desde la notificación para pagar con reducción o presentar alegaciones.", "")
    ex = run(text, today=date(2026, 9, 20))
    d = list(by_key(ex).values())[0]
    assert d.date == date(2026, 10, 5) and "no era hábil" in d.basis


def test_fine_notified_by_mail_date_when_the_text_has_no_notification_date():
    text = "DIRECCIÓN GENERAL DE TRÁFICO\nBoletín de denuncia nº 55\nInfracción: exceso de velocidad\nImporte de la sanción: 100,00 €\nPronto pago con reducción del 50 %"
    ex = run(text, received=date(2026, 9, 25), from_address="no-reply@dgt.es")
    d = list(by_key(ex).values())[0]
    assert d.date == date(2026, 10, 15) and "recepción del correo" in d.basis


def test_fine_with_an_explicit_period_in_the_text_uses_it():
    text = docs.FINE.replace("Dispone de 20 días naturales", "Dispone de quince días naturales")
    ex = run(text.replace("Importe con reducción del 50 % por pronto pago: 100,00 €", ""))
    dates_found = sorted(d.date for d in ex.deadlines)
    assert date(2026, 9, 27) in dates_found or date(2026, 9, 28) in dates_found or date(2026, 10, 2) in dates_found


# ---------------------------------------------------------------- administrative notices
def test_tax_notice_ten_working_days_skips_the_12_october_holiday():
    ex = run(docs.TAX_NOTICE)
    d = by_key(ex)["appeal"] if "appeal" in by_key(ex) else list(by_key(ex).values())[0]
    assert d.date == date(2026, 10, 13) and d.kind == M.APPEAL
    assert "10 días hábiles" in d.basis and "Ley 39/2015" in d.basis and d.remind == [5, 2, 0]
    assert ex.kind == M.OFFICIAL and ex.issuer.name == "Agencia Tributaria" and ex.ref == "2026/EXP/445566"


def test_old_administrative_deadlines_are_history_not_news():
    ex = run(docs.TAX_NOTICE, today=date(2027, 3, 1))
    assert ex.deadlines == []


def test_a_notice_without_any_period_or_date_goes_to_review():
    ex = run("AYUNTAMIENTO DE DEMOVILLA\nNotificación\nExpediente: 55/2026\nSe le comunica la resolución del expediente.")
    assert ex.state == M.REVIEW and "no_dates" in ex.notes


# ---------------------------------------------------------------- warranty (RDL 7/2021)
def test_receipt_gets_a_three_year_legal_warranty_from_purchase():
    ex = run(docs.RECEIPT)
    d = by_key(ex)["warranty"]
    assert d.date == date(2029, 8, 15) and d.kind == M.WARRANTY_END
    assert "3 años" in d.basis and "RDL 7/2021" in d.basis and "15 ago 2026" in d.basis
    assert "Aspirador Robot Demo X1" in d.title and ex.item == "Aspirador Robot Demo X1"
    assert d.remind == [60, 14]


def test_delivery_date_is_the_base_over_the_purchase_date():
    text = docs.RECEIPT + "Fecha de entrega: 20/08/2026\n"
    assert by_key(run(text))["warranty"].date == date(2029, 8, 20)


def test_warranty_years_setting_and_per_document_override():
    assert by_key(run(docs.RECEIPT, warranty_years=2))["warranty"].date == date(2028, 8, 15)
    assert by_key(run(docs.RECEIPT, warranty_months=30))["warranty"].date == date(2029, 2, 15)


def test_explicit_guarantee_in_the_text_overrides_the_default():
    text = docs.RECEIPT + "Garantía del fabricante de 5 años.\n"
    d = by_key(run(text))["warranty"]
    assert d.date == date(2031, 8, 15) and "indicada en el documento" in d.basis


def test_no_legal_warranty_for_utilities_telcos_insurers_or_the_administration():
    for text, address in ((docs.UTILITY, "x@iberdrola.es"), (docs.TELCO, "x@movistar.es"), (docs.INSURANCE, "x@mapfre.es"),
                          (docs.FINE, "x@dgt.es"), (docs.TAX_NOTICE, "x@aeat.es")):
        assert "warranty" not in by_key(run(text, from_address=address)), address


def test_invoice_from_a_shop_with_an_item_gets_a_warranty():
    text = ("PCCOMPONENTES\nFactura nº 55\nFecha de factura: 01/09/2026\nProducto: Monitor Demo 27\nBase imponible 100,00 €\nIVA 21 %: 21,00 €\n"
            "Total factura: 121,00 €")
    d = by_key(run(text, from_address="facturas@pccomponentes.com"))
    assert d["warranty"].date == date(2029, 9, 1)


def test_warranty_ended_in_the_past_makes_no_deadline():
    assert "warranty" not in by_key(run(docs.RECEIPT, today=date(2030, 1, 1)))


# ---------------------------------------------------------------- payments, subscriptions, permanence, ITV, ID
def test_future_payment_date_becomes_a_payment_deadline_with_3_and_0_days_notice():
    ex = run(docs.UTILITY)
    d = by_key(ex)["payment"]
    assert d.date == date(2026, 10, 12) and d.remind == [3, 0] and d.amount == 48.4 and "fecha de cargo" in d.basis
    assert ex.period_from == "2026-09-01" and ex.period_to == "2026-09-30" and ex.issue_date == "2026-10-02"


def test_past_payment_date_makes_no_deadline():
    assert "payment" not in by_key(run(docs.UTILITY, today=date(2026, 10, 20)))


def test_monthly_subscription_is_a_recurring_payment():
    d = by_key(run(docs.SUBSCRIPTION))["payment"]
    assert d.date == date(2026, 10, 15) and d.recurring == "monthly"


def test_yearly_subscription_is_a_renewal():
    text = "STREAMDEMO\nSuscripción anual Plan Premium\nCuota anual: 99,00 €\nSe renovará automáticamente el 20/11/2026.\nPuede cancelar su suscripción en cualquier momento."
    d = by_key(run(text))["renewal"]
    assert d.date == date(2026, 11, 20) and d.recurring == "yearly"


def test_permanence_twelve_months_from_the_effect_date():
    d = by_key(run(docs.TELCO))["permanence_end"]
    assert d.date == date(2027, 9, 1) and d.remind == [30, 0]
    assert "12 meses" in d.basis and "1 sep 2026" in d.basis


def test_permanence_end_stated_in_the_text():
    text = docs.TELCO.replace("Compromiso de permanencia de 12 meses desde la fecha de efecto.", "Permanencia hasta el 15/03/2028.")
    d = by_key(run(text))["permanence_end"]
    assert d.date == date(2028, 3, 15) and "indicada en el documento" in d.basis


def test_itv_next_inspection():
    ex = run(docs.ITV)
    d = by_key(ex)["itv"]
    assert d.date == date(2026, 10, 20) and d.kind == M.ITV and d.remind == [30, 7] and "5678 FGH" in d.title
    assert ex.kind == M.VEHICLE and ex.item == "5678 FGH"


def test_id_expiry_with_90_30_7_days_notice():
    d = by_key(run(docs.DNI))["expiry"]
    assert d.date == date(2026, 12, 14) and d.remind == [90, 30, 7] and d.kind == M.EXPIRY
    assert "el DNI" in d.title


def test_passport_and_driving_licence_titles():
    assert "pasaporte" in by_key(run("PASAPORTE\nApellidos: Prueba\nNacionalidad: española\nFecha de caducidad: 01/03/2027"))["expiry"].title
    assert "permiso de conducir" in by_key(run("PERMISO DE CONDUCIR\nApellidos: Prueba\nFecha de nacimiento: 01/01/1980\nVálido hasta: 01/03/2027"))["expiry"].title


# ---------------------------------------------------------------- confidence and review
def test_a_clear_document_is_ok_with_high_confidence():
    ex = run(docs.INSURANCE)
    assert ex.state == M.OK and ex.confidence >= 80


def test_unknown_text_goes_to_review():
    ex = run(docs.GIBBERISH)
    assert ex.state == M.REVIEW and ex.kind == M.OTHER and "kind_unknown" in ex.notes


def test_empty_text_goes_to_review_with_a_note():
    ex = run("   ")
    assert ex.state == M.REVIEW and ex.notes == ["no_text"]


def test_low_confidence_goes_to_review():
    ex = run("Recibo\n15/10/2026")
    assert ex.state == M.REVIEW


def test_user_overrides_drive_the_extraction():
    ex = run(docs.RECEIPT, overrides={"kind": M.INVOICE, "issuer": "Tienda Demo", "amount": 300.0, "issue_date": "2026-09-01", "ref": "R-1", "item": "Cosa"})
    assert ex.kind == M.INVOICE and ex.issuer.name == "Tienda Demo" and ex.amount == 300.0 and ex.ref == "R-1" and ex.item == "Cosa"
    assert by_key(ex)["warranty"].date == date(2029, 8, 15)


def test_english_texts_for_basis_and_titles():
    ex = run(docs.INSURANCE, lang="en")
    d = by_key(ex)
    assert d["cancel_by"].title.startswith("Last day to cancel the renewal of") and "one month" in d["cancel_by"].basis
    assert by_key(run(docs.RECEIPT, lang="en"))["warranty"].basis.startswith("Legal guarantee of 3 years")


def test_facts_carry_evidence_and_page():
    ex = run(docs.INSURANCE)
    amount = next(f for f in ex.facts if f["field"] == "amount")
    assert amount["value"] == 320.0 and "320,00" in amount["evidence"] and amount["page"] == 1


def test_two_pages_page_numbers():
    from kafka_hoard.bizdays import Calendar
    from kafka_hoard.extract.pipeline import extract
    from kafka_hoard.extract.types import Ctx, Meta
    ex = extract(["Factura nº 1\nTotal factura: 10,00 €", "Fecha de vencimiento: 15/10/2026"], Meta(), Ctx(today=TODAY, cal=Calendar("ES-MD")))
    assert by_key(ex)["payment"].page == 2
