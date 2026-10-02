"""Vocabulary shared by the extraction, the store, the reminders and the UI: document kinds, deadline kinds and states,
default reminder lead days, and the labels used in notifications."""

from __future__ import annotations

# ---- document kinds
INVOICE = "invoice"
RECEIPT = "receipt"
BILL = "bill"
CONTRACT = "contract"
INSURANCE = "insurance"
WARRANTY = "warranty"
TAX = "tax"
OFFICIAL = "official_notice"
FINE = "fine"
VEHICLE = "vehicle"
IDENTITY = "identity"
SUBSCRIPTION = "subscription"
PAYSLIP = "payslip"
BANK = "bank"
MANUAL = "manual"          # instruction manuals: searchable, never produce deadlines
OTHER = "other"
KINDS = (INVOICE, RECEIPT, BILL, CONTRACT, INSURANCE, WARRANTY, TAX, OFFICIAL, FINE, VEHICLE, IDENTITY, SUBSCRIPTION, PAYSLIP, BANK, MANUAL, OTHER)

KIND_LABELS = {
    "es": {INVOICE: "Factura", RECEIPT: "Ticket / recibo de compra", BILL: "Recibo", CONTRACT: "Contrato", INSURANCE: "Seguro",
           WARRANTY: "Garantía", TAX: "Impuestos", OFFICIAL: "Notificación oficial", FINE: "Multa", VEHICLE: "Vehículo (ITV)",
           IDENTITY: "Identidad", SUBSCRIPTION: "Suscripción", PAYSLIP: "Nómina", BANK: "Banco", MANUAL: "Manual", OTHER: "Otro"},
    "en": {INVOICE: "Invoice", RECEIPT: "Receipt", BILL: "Bill", CONTRACT: "Contract", INSURANCE: "Insurance", WARRANTY: "Warranty",
           TAX: "Tax", OFFICIAL: "Official notice", FINE: "Fine", VEHICLE: "Vehicle (ITV)", IDENTITY: "Identity",
           SUBSCRIPTION: "Subscription", PAYSLIP: "Payslip", BANK: "Bank", MANUAL: "Manual", OTHER: "Other"},
}

# ---- document states
REVIEW = "review"
OK = "ok"
ARCHIVED = "archived"
DOC_STATES = (REVIEW, OK, ARCHIVED)

# ---- sources
SOURCES = ("upload", "folder", "mail", "paste", "phileas", "manual")

# ---- deadline kinds
PAYMENT = "payment"
RENEWAL = "renewal"
CANCEL_BY = "cancel_by"
WARRANTY_END = "warranty_end"
PERMANENCE_END = "permanence_end"
APPEAL = "appeal"
FINE_DISCOUNT = "fine_discount"
EXPIRY = "expiry"
ITV = "itv"
TAX_DUE = "tax"
CUSTOM = "custom"
DEADLINE_KINDS = (PAYMENT, RENEWAL, CANCEL_BY, WARRANTY_END, PERMANENCE_END, APPEAL, FINE_DISCOUNT, EXPIRY, ITV, TAX_DUE, CUSTOM)

DEADLINE_LABELS = {
    "es": {PAYMENT: "Pago", RENEWAL: "Renovación", CANCEL_BY: "Último día para cancelar", WARRANTY_END: "Fin de garantía",
           PERMANENCE_END: "Fin de permanencia", APPEAL: "Plazo para recurrir o alegar", FINE_DISCOUNT: "Pronto pago de multa",
           EXPIRY: "Caducidad", ITV: "ITV", TAX_DUE: "Impuestos", CUSTOM: "Otro plazo"},
    "en": {PAYMENT: "Payment", RENEWAL: "Renewal", CANCEL_BY: "Last day to cancel", WARRANTY_END: "End of warranty",
           PERMANENCE_END: "End of commitment", APPEAL: "Deadline to appeal or respond", FINE_DISCOUNT: "Fine discount",
           EXPIRY: "Expiry", ITV: "ITV", TAX_DUE: "Tax", CUSTOM: "Other deadline"},
}

# Default reminder lead days (days before the date; 0 = on the day). Settings ``remind.<kind>`` override them.
DEFAULT_LEADS = {
    PAYMENT: [3, 0], RENEWAL: [45, 30, 7], CANCEL_BY: [14, 3, 0], WARRANTY_END: [60, 14], PERMANENCE_END: [30, 0],
    APPEAL: [5, 2, 0], FINE_DISCOUNT: [5, 2, 0], EXPIRY: [90, 30, 7], ITV: [30, 7], TAX_DUE: [15, 5, 1], CUSTOM: [7, 1],
}

# ---- deadline states
OPEN = "open"
DONE = "done"
DISMISSED = "dismissed"
DEADLINE_STATES = (OPEN, DONE, DISMISSED)
RECURRING = ("none", "monthly", "yearly")

# ---- mails
MAIL_KINDS = ("doc", "maybe", "noise")
MAIL_STATES = ("new", "filed", "ignored")

# ---- notifications
N_SOON = "deadline_soon"
N_OVERDUE = "deadline_overdue"
N_PRICE = "price_change"
N_DOC = "document_added"
NOTIFY_TYPES = (N_SOON, N_OVERDUE, N_PRICE, N_DOC)

SEVERITIES = ("low", "medium", "high")


def kind_label(kind: str, lang: str = "es") -> str:
    return KIND_LABELS.get(lang, KIND_LABELS["es"]).get(kind, kind)


def deadline_label(kind: str, lang: str = "es") -> str:
    return DEADLINE_LABELS.get(lang, DEADLINE_LABELS["es"]).get(kind, kind)


def severity_for(kind: str, days_left: int) -> str:
    """Overdue, today, and appeal / fine / cancel-by within three days are high; within a week medium; the rest low."""
    if days_left <= 0:
        return "high"
    if kind in (APPEAL, FINE_DISCOUNT, CANCEL_BY) and days_left <= 3:
        return "high"
    if days_left <= 7:
        return "medium"
    return "low"
