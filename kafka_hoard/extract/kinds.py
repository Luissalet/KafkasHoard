"""Classify a document by weighted keywords (and a nudge from who issued it): kind + score + the reasons."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .. import model as M

# kind -> [(pattern on folded text, weight, max occurrences counted)]
RULES: dict[str, list[tuple[str, int, int]]] = {
    M.INVOICE: [(r"factura", 3, 2), (r"(?:n\.?\s*[ºo°]?\s*(?:de\s+)?factura|numero\s+de\s+factura|factura\s+n)", 4, 1), (r"base\s+imponible", 3, 1),
                (r"\biva\b", 1, 2), (r"\binvoice\b", 3, 2), (r"invoice\s+(?:number|no)", 3, 1), (r"datos\s+de\s+facturacion|datos\s+del\s+cliente", 1, 1),
                (r"\b(?:cif|nif)\b", 1, 1), (r"cuota\s+iva|tipo\s+de\s+iva|tipo\s+iva", 2, 1), (r"vat\b", 1, 1)],
    M.RECEIPT: [(r"\bticket\b", 4, 1), (r"factura\s+simplificada", 6, 1), (r"recibo\s+de\s+compra", 5, 1), (r"order\s+confirmation|confirmacion\s+de(?:l)?\s+pedido", 4, 1),
                (r"gracias\s+por\s+(?:su|tu)\s+compra", 3, 1), (r"comprobante\s+de\s+compra", 4, 1), (r"\breceipt\b", 4, 1), (r"iva\s+incluido", 2, 1),
                (r"forma\s+de\s+pago|pago\s+con\s+tarjeta|pagado\s+con", 1, 1), (r"\b(?:unidades|cant\.?|uds\.?)\b", 1, 1), (r"articulo|producto", 1, 1)],
    M.BILL: [(r"\brecibo\b", 3, 2), (r"cargo\s+en\s+(?:su\s+)?cuenta", 4, 1), (r"domiciliacion|domiciliad[oa]", 3, 1), (r"importe\s+a\s+cargar", 5, 1),
             (r"suministro", 3, 1), (r"periodo\s+de\s+facturacion", 4, 1), (r"\bcups\b", 4, 1), (r"potencia\s+contratada", 4, 1), (r"\bkwh\b", 3, 1),
             (r"se\s+cargara", 3, 1), (r"adeudo", 2, 1), (r"fecha\s+de\s+cargo", 3, 1), (r"\btarifa\b", 1, 1), (r"consumo", 2, 1),
             (r"linea\s+movil|numero\s+de\s+linea|\bmovil\b", 1, 1), (r"lectura\s+(?:actual|anterior)", 2, 1)],
    M.CONTRACT: [(r"contrato", 3, 3), (r"condiciones\s+particulares", 5, 1), (r"condiciones\s+generales", 3, 1), (r"permanencia", 4, 1),
                 (r"las\s+partes", 3, 1), (r"clausula", 2, 2), (r"arrendamiento|alquiler", 3, 1), (r"arrendador|arrendatario", 4, 1),
                 (r"objeto\s+del\s+contrato", 5, 1), (r"duracion\s+del\s+contrato", 4, 1), (r"\bcontract\b", 3, 1), (r"\bagreement\b", 2, 1),
                 (r"firma\s+del\s+contrato|firmado\s+en", 2, 1), (r"rescision|resolucion\s+del\s+contrato", 2, 1)],
    M.INSURANCE: [(r"poliza", 5, 2), (r"tomador", 5, 1), (r"asegurado", 4, 1), (r"\bprima\b", 3, 2), (r"aseguradora", 4, 1), (r"fecha\s+de\s+efecto", 4, 1),
                  (r"seguro", 2, 2), (r"coberturas?", 2, 1), (r"siniestro", 3, 1), (r"franquicia", 2, 1), (r"garantias\s+contratadas", 3, 1),
                  (r"\binsurance\b", 3, 1), (r"\bpolicy\b", 3, 1), (r"policyholder", 4, 1), (r"\bpremium\b", 2, 1), (r"consorcio\s+de\s+compensacion", 4, 1)],
    M.WARRANTY: [(r"certificado\s+de\s+garantia", 8, 1), (r"garantia", 3, 3), (r"periodo\s+de\s+garantia", 4, 1), (r"\bwarranty\b", 4, 2), (r"garantia\s+legal", 3, 1),
                 (r"numero\s+de\s+serie", 2, 1), (r"servicio\s+tecnico", 1, 1)],
    M.TAX: [(r"agencia\s+tributaria", 6, 1), (r"\baeat\b", 5, 1), (r"modelo\s+\d{3}\b", 4, 1), (r"liquidacion\s+provisional", 6, 1),
            (r"declaracion\s+de\s+la\s+renta|borrador\s+de\s+la\s+renta|\birpf\b", 3, 1), (r"hacienda", 3, 1), (r"impuesto", 2, 2), (r"periodo\s+ejecutivo", 3, 1),
            (r"impuesto\s+sobre\s+bienes\s+inmuebles|\bibi\b", 5, 1), (r"plusvalia", 4, 1), (r"tributo", 3, 1), (r"deuda\s+tributaria", 4, 1), (r"carta\s+de\s+pago", 4, 1),
            (r"vehiculos\s+de\s+traccion\s+mecanica|impuesto\s+de\s+circulacion", 4, 1)],
    M.OFFICIAL: [(r"notificacion", 3, 2), (r"requerimiento", 4, 1), (r"tramite\s+de\s+audiencia", 6, 1), (r"alegaciones", 4, 1), (r"resolucion", 3, 1),
                 (r"providencia\s+de\s+apremio", 7, 1), (r"ayuntamiento", 3, 1), (r"seguridad\s+social", 3, 1), (r"tesoreria\s+general", 4, 1),
                 (r"sede\s+electronica", 3, 1), (r"\bdehu\b|direccion\s+electronica\s+habilitada", 6, 1), (r"expediente", 3, 2),
                 (r"recurso\s+de\s+(?:alzada|reposicion)", 5, 1), (r"ley\s+(?:39|40)/2015", 3, 1), (r"acuerdo\s+de\s+inicio|propuesta\s+de\s+resolucion", 5, 1),
                 (r"empadronamiento|padron", 4, 1), (r"juzgado|tribunal", 3, 1), (r"administracion", 1, 1)],
    M.FINE: [(r"boletin\s+de\s+denuncia", 8, 1), (r"\bmulta\b", 4, 2), (r"sancion", 4, 2), (r"infraccion", 4, 1), (r"\bdgt\b|direccion\s+general\s+de\s+trafico", 3, 1),
             (r"pronto\s+pago", 5, 1), (r"reduccion\s+del\s+50", 6, 1), (r"detraccion\s+de\s+puntos", 5, 1), (r"cinemometro|radar|exceso\s+de\s+velocidad", 3, 1),
             (r"denunciad[oa]|denuncia", 3, 1), (r"procedimiento\s+sancionador", 4, 1), (r"estacionamiento|\bora\b", 2, 1), (r"matricula", 1, 1)],
    M.VEHICLE: [(r"\bitv\b", 5, 2), (r"inspeccion\s+tecnica", 6, 1), (r"permiso\s+de\s+circulacion", 6, 1), (r"ficha\s+tecnica", 5, 1), (r"tarjeta\s+itv", 5, 1),
                (r"matricula", 2, 1), (r"bastidor|\bvin\b", 3, 1), (r"resultado\s+de\s+la\s+inspeccion|\bfavorable\b|\bdesfavorable\b", 3, 1),
                (r"proxima\s+inspeccion", 4, 1), (r"estacion\s+de\s+itv", 4, 1), (r"cambio\s+de\s+titularidad", 3, 1)],
    M.IDENTITY: [(r"documento\s+nacional\s+de\s+identidad", 8, 1), (r"\bdni\b", 5, 1), (r"pasaporte|\bpassport\b", 6, 1), (r"\bnie\b", 5, 1),
                 (r"permiso\s+de\s+conducir|carnet\s+de\s+conducir|licencia\s+de\s+conducir|driving\s+licen[cs]e", 7, 1), (r"tarjeta\s+sanitaria", 7, 1),
                 (r"tarjeta\s+de\s+residencia", 7, 1), (r"apellidos", 2, 1), (r"nacionalidad", 2, 1), (r"fecha\s+de\s+nacimiento", 2, 1), (r"identity\s+card|id\s+card", 5, 1),
                 (r"validez", 2, 1), (r"\bsexo\b", 1, 1)],
    M.SUBSCRIPTION: [(r"suscripcion", 5, 2), (r"membresia", 5, 1), (r"renovacion\s+automatica", 5, 1), (r"se\s+renovara\s+automaticamente|renovara\s+automaticamente", 4, 1),
                     (r"subscription", 5, 2), (r"membership", 5, 1), (r"plan\s+(?:mensual|anual|premium|familiar|basico|estandar|standard)", 4, 1),
                     (r"cuota\s+(?:mensual|anual)", 3, 1), (r"cancelar\s+(?:tu\s+|su\s+)?suscripcion|cancel\s+(?:your\s+)?subscription", 4, 1),
                     (r"periodo\s+de\s+prueba|prueba\s+gratuita|free\s+trial", 3, 1), (r"cobro\s+recurrente|pago\s+recurrente|recurring\s+payment", 3, 1),
                     (r"proximo\s+cobro|next\s+billing|billing\s+date|renews?\s+on", 4, 1)],
    M.PAYSLIP: [(r"\bnomina\b", 8, 1), (r"recibo\s+de\s+salarios", 8, 1), (r"salario\s+base", 5, 1), (r"devengos", 6, 1), (r"deducciones", 4, 1),
                (r"liquido\s+a\s+percibir", 6, 1), (r"contingencias\s+comunes", 5, 1), (r"\bpayslip\b|\bpayroll\b", 6, 1), (r"cotizacion", 2, 1)],
    M.BANK: [(r"extracto", 5, 1), (r"estado\s+de\s+cuenta", 5, 1), (r"\bsaldo\b", 3, 2), (r"movimientos", 3, 1), (r"\biban\b", 2, 1), (r"cuenta\s+corriente", 3, 1),
             (r"bank\s+statement|statement\s+of\s+account", 6, 1), (r"saldo\s+(?:anterior|final|disponible)", 4, 1), (r"prestamo|hipoteca", 3, 1), (r"transferencia", 2, 1)],
}
RULES[M.MANUAL] = [(r"manual\s+de\s+(?:instrucciones|usuario|uso)", 8, 1), (r"instrucciones\s+de\s+(?:uso|instalacion|montaje)", 5, 1),
                   (r"(?:user|instruction|owner'?s)\s+manual|operating\s+instructions", 8, 1), (r"lea\s+(?:atentamente\s+)?(?:estas|las)\s+instrucciones", 4, 1),
                   (r"instrucciones\s+de\s+seguridad|safety\s+instructions", 2, 1), (r"solucion\s+de\s+problemas|troubleshooting", 3, 1),
                   (r"mantenimiento\s+y\s+limpieza|cleaning\s+and\s+maintenance", 3, 1)]
COMPILED = {kind: [(re.compile(p), w, cap) for p, w, cap in rules] for kind, rules in RULES.items()}

# issuer category -> (kind, bonus)
CATEGORY_NUDGE = {"insurer": [(M.INSURANCE, 3)], "utility": [(M.BILL, 3)], "telco": [(M.BILL, 3), (M.CONTRACT, 1)], "bank": [(M.BANK, 3)],
                  "shop": [(M.RECEIPT, 2), (M.INVOICE, 1)]}
ISSUER_NUDGE = {"Agencia Tributaria": [(M.TAX, 5)], "DGT": [(M.FINE, 3)], "Seguridad Social": [(M.OFFICIAL, 3)]}

# when scores are close, the more specific kind wins
PRIORITY = [M.FINE, M.TAX, M.INSURANCE, M.VEHICLE, M.IDENTITY, M.PAYSLIP, M.OFFICIAL, M.MANUAL, M.WARRANTY, M.CONTRACT, M.SUBSCRIPTION, M.BILL,
            M.INVOICE, M.RECEIPT, M.BANK]
MIN_SCORE = 3
CLOSE_MARGIN = 2


@dataclass
class KindResult:
    kind: str
    score: int
    margin: int
    scores: dict[str, int] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"kind": self.kind, "score": self.score, "margin": self.margin, "reasons": self.reasons[:8]}


def classify(folded: str, issuer_name: str = "", issuer_category: str = "other") -> KindResult:
    scores: dict[str, int] = {}
    reasons: dict[str, list[str]] = {}
    for kind, rules in COMPILED.items():
        total = 0
        for pattern, weight, cap in rules:
            n = len(pattern.findall(folded))
            if n:
                total += weight * min(n, cap)
                reasons.setdefault(kind, []).append(f"{pattern.pattern.split('|')[0][:28]}×{min(n, cap)}")
        scores[kind] = total
    # a simplified invoice is a receipt, not an invoice
    if re.search(r"factura\s+simplificada", folded):
        scores[M.INVOICE] = max(0, scores[M.INVOICE] - 6)
    # a payslip lists tax and social-security lines
    if scores[M.PAYSLIP] >= 8:
        scores[M.TAX] = max(0, scores[M.TAX] - 6)
        scores[M.OFFICIAL] = max(0, scores[M.OFFICIAL] - 4)
    # a fine from the traffic authority also says «notificación» and «expediente»
    if scores[M.FINE] >= 8:
        scores[M.OFFICIAL] = max(0, scores[M.OFFICIAL] - 4)
        scores[M.VEHICLE] = max(0, scores[M.VEHICLE] - 3)
    for kind, bonus in CATEGORY_NUDGE.get(issuer_category, []):
        if scores.get(kind, 0) > 0 or kind in (M.INSURANCE, M.TAX):
            scores[kind] = scores.get(kind, 0) + bonus
            reasons.setdefault(kind, []).append(f"emisor:{issuer_category}")
    for kind, bonus in ISSUER_NUDGE.get(issuer_name, []):
        scores[kind] = scores.get(kind, 0) + bonus
        reasons.setdefault(kind, []).append(f"emisor:{issuer_name}")
    ranked = sorted(scores.items(), key=lambda kv: (-kv[1], PRIORITY.index(kv[0]) if kv[0] in PRIORITY else 99))
    best_kind, best = ranked[0]
    second = ranked[1][1] if len(ranked) > 1 else 0
    if best < MIN_SCORE:
        return KindResult(M.OTHER, best, best - second, scores, [])
    # within the close margin, the more specific kind wins
    close = [k for k, s in ranked if best - s <= CLOSE_MARGIN and s >= MIN_SCORE]
    if len(close) > 1:
        close.sort(key=lambda k: PRIORITY.index(k) if k in PRIORITY else 99)
        best_kind = close[0]
        best = scores[best_kind]
        second = max((s for k, s in scores.items() if k != best_kind), default=0)
    return KindResult(best_kind, best, best - second, scores, reasons.get(best_kind, []))
