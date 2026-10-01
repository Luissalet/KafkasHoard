"""Invented documents used by the tests: no real names, numbers or addresses. Every date is relative to TODAY (2026-10-01)."""

from __future__ import annotations

from datetime import date, datetime

TODAY = date(2026, 10, 1)
T0 = datetime(2026, 10, 1, 10, 0, 0).timestamp()      # "now" of the fake clock: 1 Oct 2026, 10:00 local time

INSURANCE = """MAPFRE SEGUROS DE HOGAR
Póliza de seguro del hogar
Número de póliza: 5550012345
Tomador: Ana Ejemplo Prueba
Fecha de efecto: 01/12/2025
Vigencia desde las 00:00 horas del 01/12/2025 hasta las 24:00 horas del 30/11/2026
Prima total: 320,00 €
Forma de pago: anual
"""

FINE = """DIRECCIÓN GENERAL DE TRÁFICO
Boletín de denuncia nº 2026-0099001
Infracción: exceso de velocidad
Matrícula: 1234 BCD
Fecha de notificación: 12/09/2026
Importe de la sanción: 200,00 €
Importe con reducción del 50 % por pronto pago: 100,00 €
Dispone de 20 días naturales desde la notificación para pagar con reducción o presentar alegaciones.
"""

UTILITY = """IBERDROLA CLIENTES
Factura de electricidad nº FE-2026-778899
Periodo de facturación: 01/09/2026 al 30/09/2026
Fecha de emisión: 02/10/2026
Fecha de cargo: 12/10/2026
CUPS: ES0021000000000000AB
Consumo: 210 kWh
Base imponible: 40,00 €
IVA 21 %: 8,40 €
Total a pagar: 48,40 €
"""

RECEIPT = """MEDIAMARKT
Ticket de compra
Fecha de compra: 15/08/2026
Producto: Aspirador Robot Demo X1
Cantidad: 1
Total (IVA incluido): 249,90 €
Pago con tarjeta
"""

TELCO = """MOVISTAR
Contrato de servicios de telecomunicaciones
Número de contrato: 98765432
Fecha de efecto: 01/09/2026
Compromiso de permanencia de 12 meses desde la fecha de efecto.
Cuota mensual: 45,00 €
"""

ITV = """ESTACIÓN DE ITV DEMO
Inspección técnica de vehículos
Matrícula: 5678 FGH
Resultado: favorable
Fecha de inspección: 20/10/2025
Próxima inspección: 20/10/2026
"""

DNI = """DOCUMENTO NACIONAL DE IDENTIDAD
Apellidos: Ejemplo Prueba
Nombre: Ana
DNI 12345678Z
Fecha de nacimiento: 03/04/1990
Válido hasta: 14/12/2026
"""

TAX_NOTICE = """AGENCIA TRIBUTARIA
Requerimiento. Trámite de audiencia
Expediente: 2026/EXP/445566
Fecha de notificación: 28/09/2026
Dispone de un plazo de diez (10) días hábiles contados desde el día siguiente al de la notificación para presentar alegaciones.
"""

SUBSCRIPTION = """STREAMDEMO
Suscripción mensual Plan Premium
Cuota mensual: 9,99 €
Próximo cobro: 15/10/2026
Se renovará automáticamente cada mes. Puede cancelar su suscripción en cualquier momento.
"""

GIBBERISH = "lorem ipsum dolor sit amet consectetur adipiscing elit"


def insurance_renewal(amount: str, start: str, end: str, ref: str = "5550012345") -> str:
    """A later period of the same invented policy, with another premium."""
    return (f"MAPFRE SEGUROS DE HOGAR\nPóliza de seguro del hogar\nNúmero de póliza: {ref}\nTomador: Ana Ejemplo Prueba\n"
            f"Fecha de efecto: {start}\nVigencia desde las 00:00 horas del {start} hasta las 24:00 horas del {end}\nPrima total: {amount} €\n")
