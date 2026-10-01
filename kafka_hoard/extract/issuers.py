"""Who issued a document: known companies and administrations by sender domain, name or text, then «Ayuntamiento de X»,
then a company line with S.A. / S.L. near a CIF, then the sender."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from ..util import fold, issuer_key

# category: admin | utility | telco | insurer | bank | shop | other
# (name, category, domains, aliases (folded; matched as whole words), case-sensitive aliases (matched in the original text))
KNOWN: list[tuple[str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]] = [
    ("Agencia Tributaria", "admin", ("agenciatributaria.es", "agenciatributaria.gob.es", "aeat.es"), ("agencia tributaria",), ("AEAT",)),
    ("DGT", "admin", ("dgt.es", "dgt.gob.es"), ("direccion general de trafico",), ("DGT",)),
    ("Seguridad Social", "admin", ("seg-social.es", "seg-social.gob.es"), ("seguridad social", "tesoreria general de la seguridad social"), ()),
    ("Comunidad de Madrid", "admin", ("comunidad.madrid", "madrid.org"), ("comunidad de madrid",), ()),
    ("Iberdrola", "utility", ("iberdrola.es", "iberdrola.com"), ("iberdrola",), ()),
    ("Endesa", "utility", ("endesa.com", "endesa.es", "endesaclientes.com"), ("endesa",), ()),
    ("Naturgy", "utility", ("naturgy.es", "naturgy.com"), ("naturgy",), ()),
    ("Repsol", "utility", ("repsol.com", "repsol.es"), ("repsol",), ()),
    ("TotalEnergies", "utility", ("totalenergies.es", "totalenergies.com"), ("totalenergies", "total energies"), ()),
    ("Holaluz", "utility", ("holaluz.com",), ("holaluz",), ()),
    ("Canal de Isabel II", "utility", ("canaldeisabelsegunda.es", "canaldeisabelsegunda.com", "gestiona.canaldeisabelsegunda.es"),
     ("canal de isabel ii", "canal isabel ii"), ()),
    ("Movistar", "telco", ("movistar.es", "telefonica.com", "telefonica.es"), ("movistar", "telefonica de espana", "telefonica moviles"), ()),
    ("Vodafone", "telco", ("vodafone.es", "vodafone.com"), ("vodafone",), ()),
    ("Orange", "telco", ("orange.es", "orange.com"), ("orange",), ()),
    ("MásMóvil", "telco", ("masmovil.es", "masmovil.com"), ("masmovil", "mas movil"), ()),
    ("Yoigo", "telco", ("yoigo.com", "yoigo.es"), ("yoigo",), ()),
    ("Digi", "telco", ("digimobil.es", "digimobil.com"), ("digi mobil", "digimobil"), ("Digi",)),
    ("Pepephone", "telco", ("pepephone.com",), ("pepephone",), ()),
    ("Jazztel", "telco", ("jazztel.com", "jazztel.es"), ("jazztel",), ()),
    ("Lowi", "telco", ("lowi.es",), ("lowi",), ()),
    ("O2", "telco", ("o2online.es",), (), ("O2",)),
    ("Mapfre", "insurer", ("mapfre.com", "mapfre.es"), ("mapfre",), ()),
    ("Mutua Madrileña", "insurer", ("mutua.es", "mutua.com"), ("mutua madrilena",), ()),
    ("Línea Directa", "insurer", ("lineadirecta.com",), ("linea directa",), ()),
    ("AXA", "insurer", ("axa.es", "axa.com"), (), ("AXA",)),
    ("Allianz", "insurer", ("allianz.es", "allianz.com"), ("allianz",), ()),
    ("Generali", "insurer", ("generali.es", "generali.com"), ("generali",), ()),
    ("Zurich", "insurer", ("zurich.es", "zurich.com"), ("zurich",), ()),
    ("Sanitas", "insurer", ("sanitas.es",), ("sanitas",), ()),
    ("Adeslas", "insurer", ("segurcaixaadeslas.es", "adeslas.es"), ("adeslas", "segurcaixa adeslas"), ()),
    ("DKV", "insurer", ("dkv.es",), (), ("DKV",)),
    ("Asisa", "insurer", ("asisa.es",), ("asisa",), ()),
    ("Caser", "insurer", ("caser.es",), ("caser seguros", "caser"), ()),
    ("Reale", "insurer", ("reale.es",), ("reale seguros",), ()),
    ("Verti", "insurer", ("verti.es",), ("verti seguros", "verti"), ()),
    ("Pelayo", "insurer", ("pelayo.com",), ("pelayo",), ()),
    ("Santander", "bank", ("santander.com", "bancosantander.es", "gruposantander.es"), ("banco santander", "santander"), ()),
    ("BBVA", "bank", ("bbva.es", "bbva.com"), ("bbva",), ()),
    ("CaixaBank", "bank", ("caixabank.es", "caixabank.com", "lacaixa.es"), ("caixabank", "la caixa"), ()),
    ("ING", "bank", ("ing.es", "ing.com"), (), ("ING",)),
    ("Sabadell", "bank", ("bancsabadell.com", "bancosabadell.com", "sabadell.com"), ("banco sabadell", "sabadell"), ()),
    ("Bankinter", "bank", ("bankinter.com", "bankinter.es"), ("bankinter",), ()),
    ("Openbank", "bank", ("openbank.es", "openbank.com"), ("openbank",), ()),
    ("Unicaja", "bank", ("unicajabanco.es", "unicaja.es"), ("unicaja",), ()),
    ("Abanca", "bank", ("abanca.com", "abanca.es"), ("abanca",), ()),
    ("Kutxabank", "bank", ("kutxabank.es", "kutxabank.com"), ("kutxabank",), ()),
    ("N26", "bank", ("n26.com",), (), ("N26",)),
    ("Revolut", "bank", ("revolut.com",), ("revolut",), ()),
    ("Amazon", "shop", ("amazon.es", "amazon.com", "amazon.co.uk", "amazon.de"), ("amazon",), ()),
    ("PcComponentes", "shop", ("pccomponentes.com",), ("pccomponentes", "pc componentes"), ()),
    ("MediaMarkt", "shop", ("mediamarkt.es", "mediamarkt.com"), ("mediamarkt", "media markt"), ()),
    ("El Corte Inglés", "shop", ("elcorteingles.es",), ("el corte ingles",), ()),
    ("Fnac", "shop", ("fnac.es", "fnac.com"), ("fnac",), ()),
    ("Apple", "shop", ("apple.com", "email.apple.com", "insideapple.apple.com"), ("apple",), ()),
    ("IKEA", "shop", ("ikea.com", "ikea.es"), ("ikea",), ()),
    ("Decathlon", "shop", ("decathlon.es", "decathlon.com"), ("decathlon",), ()),
    ("Leroy Merlin", "shop", ("leroymerlin.es",), ("leroy merlin",), ()),
    ("Carrefour", "shop", ("carrefour.es", "carrefour.com"), ("carrefour",), ()),
    ("Worten", "shop", ("worten.es",), ("worten",), ()),
]

AYUNTAMIENTO = re.compile(r"\b(?:Ayuntamiento|AYUNTAMIENTO)[ \t]+(?:de[ \t]+|DE[ \t]+)([A-ZÁÉÍÓÚÑ][\wáéíóúñÁÉÍÓÚÑ]+(?:[ \t]+(?:(?:de|del|la|las|los|DE|DEL|LA|LAS|LOS)[ \t]+){0,2}[A-ZÁÉÍÓÚÑ][\wáéíóúñÁÉÍÓÚÑ]+){0,3})")
CIF = re.compile(r"\b[A-HJ-NP-SUVW]\d{7}[0-9A-J]\b")
COMPANY = re.compile(r"^(?P<name>[A-ZÁÉÍÓÚÑ0-9][\w&.,'’ \-ÁÉÍÓÚÑáéíóúñ]{2,70}?)[,\s]+(?P<suf>S\.\s?A\.\s?U\.?|S\.\s?L\.\s?U\.?|S\.\s?A\.|S\.\s?L\.|S\.\s?C\.|SAU|SLU|SA|SL)\s*(?:$|[\s,.;:(])")
GENERIC_SENDERS = re.compile(r"no[-_. ]?reply|noreply|notificaciones?|avisos?|info|facturas?|facturacion|atencion|clientes|servicio|mailer|"
                             r"newsletter|comunicaciones|contacto|support|billing|invoice|admin|sistema", re.I)


@dataclass
class Issuer:
    name: str
    key: str
    category: str       # admin | utility | telco | insurer | bank | shop | other
    source: str         # domain | sender | text | ayuntamiento | company | sender_fallback | none
    confidence: int     # 0-100

    def to_dict(self) -> dict:
        return {"name": self.name, "key": self.key, "category": self.category, "source": self.source, "confidence": self.confidence}


NONE = Issuer("", "", "other", "none", 0)


def domain_of(address: str) -> str:
    return (address or "").rsplit("@", 1)[-1].strip().lower().strip(">")


def _by_domain(domain: str) -> Optional[Issuer]:
    if not domain:
        return None
    for name, category, domains, _aliases, _cs in KNOWN:
        for d in domains:
            if domain == d or domain.endswith("." + d):
                return Issuer(name, issuer_key(name), category, "domain", 95)
    return None


def _by_text(original: str, folded: str, limit: int = 6000) -> Optional[Issuer]:
    head_f, head_o = folded[:limit], original[:limit]
    best: Optional[tuple[int, int, str, str]] = None   # (count, -first position, name, category)
    for name, category, _domains, aliases, cs in KNOWN:
        count, first = 0, 10**9
        for alias in aliases:
            for m in re.finditer(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", head_f):
                count += 1
                first = min(first, m.start())
        for alias in cs:
            for m in re.finditer(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", head_o):
                count += 1
                first = min(first, m.start())
        if count:
            cand = (count, -first, name, category)
            if best is None or cand[:2] > best[:2]:
                best = cand
    if best is None:
        return None
    return Issuer(best[2], issuer_key(best[2]), best[3], "text", 80)


ACRONYMS = {"itv", "dgt", "sl", "sa", "slu", "sau", "iva", "irpf", "aeat", "bbva", "tv", "ibi", "ite", "oca", "rac", "csic", "uned", "upm", "ucm"}
CONNECTORS = {"de", "del", "la", "las", "el", "los", "y", "e", "en", "a", "al", "para", "por", "con"}


def smart_title(text: str) -> str:
    """Title-case an ALL CAPS name: acronyms stay upper case, Spanish connectors lower case (except as first word)."""
    out: list[str] = []
    for i, word in enumerate(re.sub(r"\s+", " ", text.strip()).split(" ")):
        low = word.lower()
        if low.strip(".,") in ACRONYMS:
            out.append(word.upper())
        elif i > 0 and low in CONNECTORS:
            out.append(low)
        else:
            out.append(word[:1].upper() + word[1:].lower())
    return " ".join(out)


def _ayuntamiento(original: str) -> Optional[Issuer]:
    m = AYUNTAMIENTO.search(original[:6000])
    if not m:
        return None
    name = "Ayuntamiento de " + smart_title(m.group(1))
    return Issuer(name, issuer_key(name), "admin", "ayuntamiento", 85)


def _company(original: str) -> Optional[Issuer]:
    lines = original[:8000].split("\n")
    first: Optional[tuple[str, str]] = None
    for i, line in enumerate(lines):
        m = COMPANY.match(line.strip())
        if not m:
            continue
        name = re.sub(r"\s+", " ", m.group("name")).strip(" ,.-") + " " + re.sub(r"\s+", "", m.group("suf"))
        if first is None:
            first = (name, "")
        near = "\n".join(lines[max(0, i - 3): i + 4])
        if CIF.search(near) or re.search(r"\b(?:cif|nif)\b", near, re.I):
            return Issuer(name, issuer_key(name), "other", "company", 65)
    if first:
        return Issuer(first[0], issuer_key(first[0]), "other", "company", 50)
    return None


def _sender(from_name: str, from_address: str) -> Optional[Issuer]:
    name = re.sub(r"[\"'<>]", "", from_name or "").strip()
    name = re.sub(r",?\s+(?:PBC|Inc\.?|LLC|L\.L\.C\.|Corp\.?|GmbH)$", "", name).strip()
    if name and not GENERIC_SENDERS.fullmatch(name.lower()) and "@" not in name and len(name) <= 60:
        # «Iberdrola Clientes» -> the display name as it is; a known issuer would have matched by domain already
        return Issuer(name, issuer_key(name), "other", "sender", 45)
    domain = domain_of(from_address)
    if domain:
        label = domain.split(".")
        core = label[-3] if len(label) >= 3 and label[-2] in ("com", "co", "org", "net", "gob") else label[-2] if len(label) >= 2 else label[0]
        if core and core not in ("gmail", "hotmail", "outlook", "yahoo", "icloud", "live", "msn", "protonmail"):
            pretty = smart_title(core.replace("-", " "))
            return Issuer(pretty, issuer_key(pretty), "other", "sender_fallback", 30)
    return None


HEADER_LINE = re.compile(r"^[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ0-9 &.'\-]{2,38}$")
HEADER_STOP = re.compile(r"(?i)factura|ticket|contrato|recibo|poliza|póliza|documento|certificado|notificaci|boletin|boletín|extracto|requerimiento|"
                         r"nomina|nómina|suscripci|resumen|pedido|albaran|albarán|presupuesto|informe|solicitud|comunicaci|carta|aviso|invoice|receipt|statement")


def _header(original: str) -> Optional[Issuer]:
    """A short all-caps first line («STREAMDEMO») is usually the letterhead of whoever sent the document."""
    for line in [x.strip() for x in original[:600].split("\n") if x.strip()][:3]:
        if HEADER_LINE.match(line) and len(line.split()) <= 4 and not HEADER_STOP.search(line):
            name = smart_title(line) if len(line) > 4 else line
            return Issuer(name, issuer_key(name), "other", "header", 35)
    return None


def detect_issuer(original: str, folded: str, from_name: str = "", from_address: str = "") -> Issuer:
    by_domain = _by_domain(domain_of(from_address))
    if by_domain:
        return by_domain
    town = _ayuntamiento(original)
    if town and re.search(r"ayuntamiento", folded[:800]):
        return town
    known = _by_text(original, folded)
    if known:
        return known
    if town:
        return town
    found = _company(original)
    if found:
        return found
    sender = _sender(from_name, from_address)
    if sender and sender.source == "sender":
        return sender
    return _header(original) or sender or NONE


def category_of_name(name: str) -> str:
    """Category of an issuer by its display name (used for Phileas merchants and manual edits)."""
    key = issuer_key(name)
    for known, category, _d, aliases, _cs in KNOWN:
        if issuer_key(known) == key or any(fold(a) == key for a in aliases):
            return category
    return "other"
