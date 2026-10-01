"""Is a mail a document (it carries paperwork), a maybe (worth a look) or noise (newsletters, offers, social networks)?

Pure functions over the record the Faustus helper returns: subject, sender, text and the attachments it saved.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .. import model as M
from ..extract import issuers, kinds
from ..util import fold

DOC_SUBJECT = re.compile(
    r"factura|recibo|poliza|contrato|garantia|notificacion|requerimiento|liquidacion|multa|sancion|renovacion|vencimiento|"
    r"aviso\s+de\s+cargo|tu\s+factura|extracto|presupuesto|justificante|resolucion|citacion|invoice|receipt|policy|renewal|statement|"
    r"your\s+order|confirmacion\s+de\s+(?:compra|pedido)|ticket")
NOISE = [
    (r"newsletter|bolet[ií]n\s+informativo|boletin\s+de\s+noticias", 3), (r"\boferta|ofertas|rebajas|black\s+friday|cyber\s+monday", 3),
    (r"\d+\s*%\s*(?:de\s+)?(?:descuento|dto)|descuento\s+del?\s+\d+", 3), (r"darte\s+de\s+baja|dar\s+de\s+baja|unsubscribe|cancelar\s+suscripcion\s+a\s+(?:nuestro|esta)", 2),
    (r"has\s+(?:recibido|ganado)|sorteo|promocion|cup[oó]n|c[oó]digo\s+promocional", 2), (r"te\s+recomendamos|novedades|lo\s+mas\s+vendido", 2),
    (r"\bmarketing\b|publicidad", 1),
]
NOISE_COMPILED = [(re.compile(p), w) for p, w in NOISE]
SOCIAL_DOMAINS = ("facebook", "facebookmail", "linkedin", "twitter", "x.com", "instagram", "tiktok", "youtube", "pinterest", "reddit", "snapchat",
                  "telegram.org", "discord", "twitch", "quora", "medium.com", "substack")
DOC_EXT = (".pdf",)
IMG_EXT = (".png", ".jpg", ".jpeg", ".webp", ".heic", ".tif", ".tiff")
MIN_IMAGE_BYTES = 40_000        # smaller images are logos and signatures


@dataclass
class MailClass:
    kind: str
    score: int
    reasons: list[str] = field(default_factory=list)
    attachments: list[dict[str, Any]] = field(default_factory=list)      # the ones worth reading

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "score": self.score, "reasons": self.reasons, "attachments": len(self.attachments)}


def readable_attachments(message: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for a in message.get("attachments") or []:
        name = str(a.get("name") or "").lower()
        size = int(a.get("size") or 0)
        if not a.get("path") and not a.get("sha"):
            continue
        if name.endswith(DOC_EXT) or str(a.get("mime") or "") == "application/pdf":
            out.append(a)
        elif name.endswith(IMG_EXT) and size >= MIN_IMAGE_BYTES and not re.search(r"logo|firma|signature|banner|facebook|twitter|linkedin", name):
            out.append(a)
    return out


def classify_mail(message: dict[str, Any]) -> MailClass:
    if message.get("from_self"):
        return MailClass("noise", 0, ["own mail"])
    subject = str(message.get("subject") or "")
    text = str(message.get("text") or "")
    folded = fold(subject + "\n" + text)
    sender = fold(str(message.get("from_address") or "") + " " + str(message.get("from_name") or ""))
    reasons: list[str] = []
    noise = 0
    for pattern, weight in NOISE_COMPILED:
        if pattern.search(folded):
            noise += weight
    if any(d in sender for d in SOCIAL_DOMAINS):
        noise += 4
        reasons.append("social network")
    if noise:
        reasons.append(f"marketing signs ({noise})")
    atts = readable_attachments(message)
    pdfs = [a for a in atts if str(a.get("name") or "").lower().endswith(".pdf") or a.get("mime") == "application/pdf"]
    issuer = issuers.detect_issuer(subject + "\n" + text, folded, str(message.get("from_name") or ""), str(message.get("from_address") or ""))
    body = kinds.classify(folded, issuer.name, issuer.category)
    doc_subject = bool(DOC_SUBJECT.search(fold(subject)))
    score = 0
    if pdfs:
        score += 8
        reasons.append("PDF attached")
    elif atts:
        score += 4
        reasons.append("image attached")
    if doc_subject:
        score += 3
        reasons.append("document words in the subject")
    if body.kind != M.OTHER:
        score += min(10, body.score)
        reasons.append(f"body looks like {body.kind}")
    if issuer.name and issuer.category in ("admin", "utility", "telco", "insurer", "bank"):
        score += 2
        reasons.append(f"known issuer {issuer.name}")
    score -= noise * 2
    if pdfs and noise < 4:
        kind = "doc"
    elif body.kind != M.OTHER and body.score >= 10 and noise < 3 and not (issuer.category == "shop" and not doc_subject):
        kind = "doc"
    elif noise >= 3 and not pdfs:
        kind = "noise"
    elif atts or doc_subject or body.kind != M.OTHER:
        kind = "maybe" if score >= 3 else "noise"
    else:
        kind = "noise"
    return MailClass(kind, score, reasons, atts)
