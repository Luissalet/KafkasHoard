"""Titles and «basis» sentences of the deadlines Kafka derives (Spanish and English)."""

from __future__ import annotations

from typing import Any

TEXTS: dict[str, tuple[str, str]] = {
    # key: (es, en)
    "payment_title": ("Pago de {who}", "Payment to {who}"),
    "payment_basis": ("Fecha de pago indicada en el documento («{label}»: {date}).", "Payment date stated in the document («{label}»: {date})."),
    "tax_title": ("Pago de impuestos: {who}", "Tax payment: {who}"),
    "tax_basis": ("Plazo de ingreso indicado en el documento: {date}.", "Payment deadline stated in the document: {date}."),
    "renewal_title": ("Renovación de {who}", "Renewal of {who}"),
    "renewal_basis_insurance": ("Fin del periodo del seguro indicado en el documento ({date}); el contrato se prorroga si nadie se opone (Ley 50/1980, art. 22).",
                                "End of the insurance period stated in the document ({date}); the contract renews unless someone opposes it (Ley 50/1980, art. 22)."),
    "renewal_basis": ("Fecha de renovación o fin de periodo indicada en el documento ({date}).", "Renewal or end-of-period date stated in the document ({date})."),
    "renewal_basis_year": ("Un año desde la fecha de efecto ({date}), plazo anual habitual de la póliza.", "One year from the effect date ({date}), the usual yearly policy term."),
    "cancel_title": ("Último día para cancelar la renovación de {who}", "Last day to cancel the renewal of {who}"),
    "cancel_basis": ("Ley 50/1980, art. 22 (reformada por la Ley 20/2015): el tomador se opone a la prórroga con un preaviso escrito de al menos "
                     "un mes antes de que acabe el periodo ({end}); el asegurador, con dos meses.",
                     "Ley 50/1980, art. 22 (amended by Ley 20/2015): the policyholder opposes the renewal by written notice at least one month before "
                     "the period ends ({end}); the insurer needs two months."),
    "warranty_title": ("Fin de la garantía de {what}", "End of the warranty of {what}"),
    "warranty_basis": ("Garantía legal de {years} desde la {base_es} (RDL 7/2021); {base_es} el {date}.",
                       "Legal guarantee of {years} from the {base_en} (RDL 7/2021); {base_en} on {date}."),
    "warranty_basis_text": ("Garantía de {years} indicada en el documento, desde la {base_es} ({date}).",
                            "Guarantee of {years} stated in the document, from the {base_en} ({date})."),
    "permanence_title": ("Fin de la permanencia de {who}", "End of the commitment period of {who}"),
    "permanence_basis_text": ("Permanencia hasta el {date} indicada en el documento.", "Commitment period until {date} stated in the document."),
    "permanence_basis_months": ("Permanencia de {n} {unit} desde {start_label} ({start}).", "Commitment of {n} {unit} from {start_label} ({start})."),
    "fine_title": ("Último día para pagar con el 50 % o alegar: {who}", "Last day to pay with the 50 % reduction or to appeal: {who}"),
    "fine_basis": ("{n} días naturales desde la notificación del {date} (dgt.es: pronto pago con reducción del 50 % o alegaciones).",
                   "{n} natural days from the notification of {date} (dgt.es: early payment with the 50 % reduction, or allegations)."),
    "fine_basis_period": ("{n} {unit} {kind_es} desde la notificación del {date} (Ley 39/2015, art. 30).",
                          "{n} {unit} {kind_en} from the notification of {date} (Ley 39/2015, art. 30)."),
    "appeal_title": ("Plazo para alegar o recurrir: {who}", "Deadline to respond or appeal: {who}"),
    "pay_title": ("Plazo de pago: {who}", "Payment deadline: {who}"),
    "period_basis": ("{n} {unit} {kind_es} desde {base_es} ({date}), contados desde el día siguiente (Ley 39/2015, art. 30).",
                     "{n} {unit} {kind_en} from {base_en} ({date}), counted from the next day (Ley 39/2015, art. 30)."),
    "period_basis_working": ("sin contar sábados, domingos ni festivos", "excluding Saturdays, Sundays and holidays"),
    "period_basis_months": ("de fecha a fecha", "from date to date"),
    "moved_basis": ("El último día ({original}) no era hábil y pasa al siguiente día hábil.", "The last day ({original}) was not a working day and moves to the next one."),
    "itv_title": ("ITV de {what}", "ITV of {what}"),
    "itv_basis": ("Próxima inspección indicada en el documento ({date}).", "Next inspection stated in the document ({date})."),
    "expiry_title": ("Caduca {what}", "{what} expires"),
    "expiry_basis": ("Fecha de caducidad indicada en el documento ({date}).", "Expiry date stated in the document ({date})."),
    "basis_notified": ("la notificación", "the notification"),
    "basis_received": ("la recepción del correo", "the mail's reception"),
    "basis_issue": ("la fecha del documento", "the document's date"),
    "basis_effect": ("la fecha de efecto", "the effect date"),
    "basis_delivery": ("la entrega", "the delivery"),
    "basis_purchase": ("la compra", "the purchase"),
    "base_delivery": ("entrega", "delivery"),
    "base_purchase": ("compra", "purchase"),
    "base_issue": ("emisión del documento", "issue of the document"),
    "base_received": ("recepción del documento", "reception of the document"),
    "kind_working": ("hábiles", "working"),
    "kind_natural": ("naturales", "natural"),
    "unit_day": ("días", "days"),
    "unit_month": ("meses", "months"),
    "unit_year": ("años", "years"),
    "phileas_basis": ("Garantía legal de {years} desde la entrega (RDL 7/2021); entregado el {date}.",
                      "Legal guarantee of {years} from delivery (RDL 7/2021); delivered on {date}."),
    "years_n": ("{n} años", "{n} years"),
    "copy_note": ("Otra copia del mismo pago que llegó en el mismo correo; el documento es {name}.",
                  "Another copy of the same payment from the same mail; the document is {name}."),
    "c2c_note": ("Compra entre particulares: la garantía legal de consumo (RDL 7/2021) obliga al vendedor profesional, no a un particular; "
                 "si el vendedor era una empresa, pon los años de garantía en el documento.",
                 "Bought from a private person: the consumer guarantee (RDL 7/2021) binds business sellers, not individuals; "
                 "if the seller was a business, set the warranty years on the document."),
    "years_1": ("1 año", "1 year"),
    "months_n": ("{n} meses", "{n} months"),
    "doc_dni": ("el DNI", "the ID card"), "doc_passport": ("el pasaporte", "the passport"), "doc_driving": ("el permiso de conducir", "the driving licence"),
    "doc_health": ("la tarjeta sanitaria", "the health card"), "doc_nie": ("el NIE", "the NIE"), "doc_residence": ("la tarjeta de residencia", "the residence card"),
    "doc_generic": ("un documento", "a document"),
    "vehicle_generic": ("tu vehículo", "your vehicle"),
    "custom_basis": ("Plazo añadido a mano.", "Deadline added by hand."),
    "external_basis": ("Plazo enviado por {source}.", "Deadline sent by {source}."),
    "price_change": ("{issuer}: {noun} sube un {pct} % ({old} → {new})", "{issuer}: {noun} goes up {pct} % ({old} → {new})"),
    "price_change_down": ("{issuer}: {noun} baja un {pct} % ({old} → {new})", "{issuer}: {noun} goes down {pct} % ({old} → {new})"),
    "noun_insurance": ("la prima", "the premium"), "noun_subscription": ("la cuota", "the fee"), "noun_other": ("el importe", "the amount"),
}


def tr(lang: str, key: str, **kw: Any) -> str:
    pair = TEXTS[key]
    text = pair[1] if lang == "en" else pair[0]
    return text.format(**kw) if kw else text


def years_text(lang: str, months: int) -> str:
    if months % 12 == 0:
        n = months // 12
        return tr(lang, "years_1") if n == 1 else tr(lang, "years_n", n=n)
    return tr(lang, "months_n", n=months)
