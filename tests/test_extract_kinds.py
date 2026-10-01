"""Kind classifier, issuers and references on invented documents."""

import pytest

import docs
from helpers import run
from kafka_hoard import model as M
from kafka_hoard.extract import issuers, kinds, refs
from kafka_hoard.util import fold, issuer_key


@pytest.mark.parametrize("text,kind", [
    (docs.INSURANCE, M.INSURANCE), (docs.FINE, M.FINE), (docs.UTILITY, M.BILL), (docs.RECEIPT, M.RECEIPT), (docs.TELCO, M.CONTRACT),
    (docs.ITV, M.VEHICLE), (docs.DNI, M.IDENTITY), (docs.TAX_NOTICE, M.OFFICIAL), (docs.SUBSCRIPTION, M.SUBSCRIPTION),
    ("Factura nº 2026-15\nBase imponible 100,00 €\nIVA 21 % 21,00 €\nTotal factura 121,00 €\nCIF B12345674", M.INVOICE),
    ("Certificado de garantía\nPeriodo de garantía: 24 meses\nNúmero de serie 99887766", M.WARRANTY),
    ("AGENCIA TRIBUTARIA\nLiquidación provisional\nModelo 100 IRPF\nImporte a ingresar 300,00 €", M.TAX),
    ("NÓMINA\nSalario base 2.000,00 €\nDevengos\nDeducciones\nLíquido a percibir 1.600,00 €", M.PAYSLIP),
    ("Extracto de cuenta\nSaldo anterior 1.000,00 €\nSaldo final 1.200,00 €\nMovimientos", M.BANK),
    ("Statement of account\nbank statement for September", M.BANK),
    ("Invoice number 884\nVAT 21 %\nInvoice date 2026-09-30\nTotal due 100.00 EUR", M.INVOICE),
])
def test_classification(text, kind):
    f = fold(text)
    r = kinds.classify(f)
    assert r.kind == kind, (r.kind, r.scores)
    assert r.score >= kinds.MIN_SCORE and r.reasons


def test_unknown_text_is_other_with_no_reasons():
    r = kinds.classify(fold(docs.GIBBERISH))
    assert r.kind == M.OTHER and r.reasons == []


def test_simplified_invoice_is_a_receipt():
    assert kinds.classify(fold("Factura simplificada nº 55\nTicket de caja\nTotal 12,00 €")).kind == M.RECEIPT


def test_a_fine_is_not_an_official_notice():
    assert kinds.classify(fold(docs.FINE)).kind == M.FINE


def test_issuer_nudges_the_kind():
    ex = run("Resumen del periodo\nPrima 100,00 €\nFecha de efecto 01/01/2026", from_address="clientes@mapfre.es")
    assert ex.kind == M.INSURANCE


# ---------------------------------------------------------------- issuers
@pytest.mark.parametrize("address,name,category", [
    ("no-reply@iberdrola.es", "Iberdrola", "utility"), ("avisos@mapfre.com", "Mapfre", "insurer"), ("x@aeat.es", "Agencia Tributaria", "admin"),
    ("noreply@movistar.es", "Movistar", "telco"), ("info@dgt.es", "DGT", "admin"), ("a@lineadirecta.com", "Línea Directa", "insurer"),
    ("pedidos@amazon.es", "Amazon", "shop"), ("x@mail.bbva.es", "BBVA", "bank"),
])
def test_known_issuer_by_sender_domain(address, name, category):
    i = issuers.detect_issuer("texto", "texto", "", address)
    assert (i.name, i.category, i.source) == (name, category, "domain")


@pytest.mark.parametrize("text,name", [
    ("Factura de MAPFRE SEGUROS", "Mapfre"), ("Cliente de Endesa Energía", "Endesa"), ("Canal de Isabel II\nFactura de agua", "Canal de Isabel II"),
    ("Orange España\nRecibo", "Orange"), ("Mutua Madrileña Automovilista", "Mutua Madrileña"), ("Santander Consumer\nExtracto", "Santander"),
])
def test_known_issuer_by_text(text, name):
    assert issuers.detect_issuer(text, fold(text)).name == name


def test_ayuntamiento_name():
    text = "AYUNTAMIENTO DE ALCOBENDAS\nNotificación del padrón"
    i = issuers.detect_issuer(text, fold(text))
    assert i.name == "Ayuntamiento de Alcobendas" and i.category == "admin"


def test_company_line_near_a_cif():
    text = "Suministros Demo, S.L.\nCIF B12345674\nFactura 1"
    i = issuers.detect_issuer(text, fold(text))
    assert i.source == "company" and i.name.startswith("Suministros Demo") and i.key == "suministros demo"


def test_sender_name_is_the_fallback_and_generic_senders_are_not_used():
    assert issuers.detect_issuer("sin pistas", "sin pistas", "Taller Ejemplo", "a@b.es").name == "Taller Ejemplo"
    assert issuers.detect_issuer("sin pistas", "sin pistas", "no-reply", "x@ferreteria-demo.es").name == "Ferreteria Demo"


def test_letterhead_line_is_the_last_resort():
    i = issuers.detect_issuer("STREAMDEMO\nSuscripción mensual", fold("STREAMDEMO\nSuscripción mensual"))
    assert i.name == "Streamdemo" and i.source == "header"


def test_all_caps_names_keep_acronyms_and_lowercase_connectors():
    assert issuers.smart_title("ESTACIÓN DE ITV DEMO") == "Estación de ITV Demo"
    assert issuers.smart_title("DGT  SERVICIOS Y TRAMITES") == "DGT Servicios y Tramites"
    assert issuers.smart_title("DE LA TORRE SL") == "De la Torre SL"
    text = "AYUNTAMIENTO DE SAN SEBASTIAN DE LOS REYES\nNotificación"
    assert issuers.detect_issuer(text, fold(text)).name == "Ayuntamiento de San Sebastian de los Reyes"


def test_no_issuer():
    assert issuers.detect_issuer("sin pistas aquí", "sin pistas aqui").name == ""


@pytest.mark.parametrize("name,key", [("Mutua Madrileña S.A.", "mutua madrilena"), ("Suministros Demo, S.L.U.", "suministros demo"),
                                      ("  AXA  ", "axa"), ("El Corte Inglés", "el corte ingles"), ("Línea Directa Aseguradora", "linea directa aseguradora")])
def test_issuer_key_is_ascii_lowercase_without_legal_suffix(name, key):
    assert issuer_key(name) == key


# ---------------------------------------------------------------- references
@pytest.mark.parametrize("text,kind,value", [
    ("Número de póliza: 5550012345", M.INSURANCE, "5550012345"),
    ("Nº de póliza 5550012345", M.INSURANCE, "5550012345"),
    ("Número de contrato: 98765432", M.CONTRACT, "98765432"),
    ("Factura nº FE-2026-778899", M.INVOICE, "FE-2026-778899"),
    ("Nº factura: A/2026/55", M.INVOICE, "A/2026/55"),
    ("Expediente: 2026/EXP/445566", M.OFFICIAL, "2026/EXP/445566"),
    ("Boletín de denuncia nº 2026-0099001", M.FINE, "2026-0099001"),
    ("Policy number: POL-778-22", M.INSURANCE, "POL-778-22"),
    ("Invoice number: INV-1001", M.INVOICE, "INV-1001"),
    ("Factura de electricidad nº FE-2026-778899", M.INVOICE, "FE-2026-778899"),
])
def test_reference_labels(text, kind, value):
    main, _stable = refs.choose_ref(refs.find_refs(text, fold(text)), kind)
    assert main is not None and main.value == value


def test_stable_reference_for_series_is_the_policy_or_contract_not_the_invoice():
    text = "Número de póliza: 5550012345\nFactura nº 77"
    _main, stable = refs.choose_ref(refs.find_refs(text, fold(text)), M.INSURANCE)
    assert stable is not None and stable.value == "5550012345"
    text = "Factura nº 77"
    _main, stable = refs.choose_ref(refs.find_refs(text, fold(text)), M.INVOICE)
    assert stable is None


def test_bill_reference_is_the_invoice_number_and_the_series_one_is_the_supply_point():
    text = "CUPS: ES0021000000000001AB\nFactura de electricidad nº FE-2026-778899"
    main, stable = refs.choose_ref(refs.find_refs(text, fold(text)), M.BILL)
    assert main.value == "FE-2026-778899"
    assert stable is not None and stable.value == "ES0021000000000001AB"


def test_no_reference():
    assert refs.choose_ref(refs.find_refs("nada", "nada"), M.OTHER) == (None, None)
