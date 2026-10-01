# Kafka's Hoard

Un archivador local de papeleo. Lee facturas, contratos, pólizas de seguro, tickets, garantías, cartas de Hacienda, de la DGT o del ayuntamiento, multas, ITV y documentos de identidad; averigua quién los emite, los importes y todas las fechas que importan; convierte esas fechas en plazos (contando días hábiles españoles donde la ley lo exige); te avisa antes de cada uno y responde a tus preguntas citando el documento y la página.

Forma parte de la familia de apps locales Hoard: corre en tu PC, guarda sus datos en `data/`, funciona sola en el navegador y un asistente puede manejarla por MCP.

[English version](README.md)

## Qué hace

- **Lee lo que le das.** Arrastra archivos a la página, déjalos en una carpeta vigilada (por defecto `data/inbox`) o deja que lea la cuenta de correo configurada en Faustus: los adjuntos PDF e imagen se archivan y la contraseña nunca sale de Faustus. Admite PDF (capa de texto y, con el paquete OCR opcional, páginas escaneadas), imágenes (PNG, JPEG, WebP, HEIC, TIFF, BMP, GIF), `.eml`, `.docx`, HTML y texto plano. Cada archivo se guarda una sola vez por su SHA-256 y el original no se modifica nunca.
- **Entiende el documento.** Reglas (sin modelo) lo clasifican (factura, ticket, recibo, contrato, seguro, garantía, impuestos, notificación oficial, multa, vehículo/ITV, identidad, suscripción, nómina, banco), identifican al emisor, eligen la referencia (póliza, contrato, factura, expediente, boletín de multa), el importe y el periodo de facturación, y encuentran cada fecha con su papel: emisión, vencimiento, renovación, efecto desde/hasta, compra, entrega, caducidad, notificación, próxima ITV, fin de permanencia. Cada dato conserva su evidencia: la cita y la página.
- **Crea plazos y explica por qué.** Vencimientos de pago, renovaciones, el último día para cancelar una renovación (un mes antes para el tomador, Ley 50/1980 art. 22), fin de la garantía legal de 3 años (Real Decreto Legislativo 7/2021), fin de permanencia, los 20 días naturales para pagar una multa de tráfico con el 50 % de descuento o presentar alegaciones (dgt.es), plazos para responder a la administración en días hábiles (Ley 39/2015 art. 30), caducidad de documentos y próxima ITV. «¿Por qué esta fecha?» muestra la regla, la fecha base, los días contados y la cita. Ver [docs/RULES.md](docs/RULES.md).
- **Te avisa.** Cada tipo de plazo tiene días de antelación que puedes cambiar (una renovación: 45, 30 y 7 días antes; el descuento de una multa: 5, 2 y el último día). Un aviso por plazo y por pasada, los vencidos una sola vez, nada por la noche (de 23:00 a 07:00 por defecto; los urgentes pueden saltarse la pausa). Canales: aviso de Windows, bus de la familia Hoard, ntfy, Telegram y correo (enviado con la cuenta de Faustus). Cada aviso se envía una sola vez.
- **Sigue lo que se repite.** Recibos, seguros y suscripciones del mismo emisor y referencia forman una serie. Un cambio de precio del 5 % o más (configurable) genera un aviso; el historial se ve por serie. Los plazos recurrentes (mensual, anual) pasan a su siguiente fecha al marcarlos como hechos.
- **Responde con citas.** La búsqueda de texto completo (SQLite FTS5, sin distinguir tildes) recorre todas las páginas y devuelve fragmentos con documento y página; `warranty_check` dice si una compra sigue en garantía y hasta cuándo.
- **Historia en silencio.** El primer escaneo del correo y los archivos antiguos se archivan sin avisos y sus plazos pasados se crean como hechos, así que solo te enteras de lo que aún está por delante.
- **Privado de cara al asistente.** DNI/NIE, IBAN, tarjetas y teléfonos se enmascaran en lo que devuelven MCP y la ruta del agente salvo que pidas expresamente ese número. La interfaz local lo muestra todo.

## Pantallas

En castellano por defecto, inglés con un clic, oscura.

- **Plazos** — vencidos, esta semana, próximos 30 días y más adelante; marcar hecho, aplazar, editar, descartar, añadir a mano; novedades desde la última visita; documentos esperando tu decisión.
- **Documentos** — subida arrastrando archivos a cualquier parte, lista con filtros por tipo y estado, búsqueda de texto completo con fragmentos.
- **Detalle** — vista previa de la página con el original, datos extraídos con su evidencia, plazos con «¿por qué esta fecha?», series e historial de precios, edición de cualquier campo (tu edición siempre gana al releer).
- **Revisión** — documentos de los que las reglas no estaban seguras y correos que pueden ser papeleo: aceptar, cambiar, ignorar.
- **Ajustes** — carpetas vigiladas, correo, región del calendario y festivos extra, días de antelación, canales de aviso con botón de prueba, estado del OCR y del modelo, idioma, actividad reciente.

## Ponerla en marcha

Requisitos: Python 3.11+ (probada en 3.11 y 3.13), Node 22 solo para reconstruir la interfaz, Faustus con una cuenta de correo para leer el correo automáticamente.

```bash
python -m venv venv
venv/Scripts/python -m pip install -r requirements.txt      # Windows; venv/bin/python en otros sistemas
venv/Scripts/python -m kafka_hoard                            # http://127.0.0.1:5200
```

La interfaz viene compilada en `kafka_hoard/static`. Para reconstruirla: `npm install && npx vite build`. `python scripts/launch.py` arranca la app en un puerto libre y abre el navegador.

El OCR es opcional: `pip install ".[ocr]"` (rapidocr + onnxruntime). Sin él, las páginas escaneadas y las fotos se guardan y se marcan para revisión, y todo lo demás sigue funcionando.

Entorno: `KAFKA_PORT` (5200), `KAFKA_DATA_DIR`, `PORT_STRICT=1`, `KAFKA_SCHEDULER=0` (sin trabajo en segundo plano), `KAFKA_OFFLINE=1` (sin red), `KAFKA_OCR=0`, `KAFKA_ALLOWED_HOSTS`, `KAFKA_HTTP_TIMEOUT_S`. Los secretos (`KAFKA_TELEGRAM_TOKEN`, `KAFKA_TELEGRAM_CHAT_ID`, `KAFKA_NTFY_TOPIC`, `KAFKA_SMTP_*`, `KAFKA_FAUSTUS_DIR`) pueden ir en `.env` (ver `.env.example`) o en Ajustes. Lo demás son ajustes guardados en `data/kafka.db`.

## Asistentes (MCP)

`mcp_server.py` es un puente MCP por stdio llamado `kafka-hoard`. No abre nunca la base de datos: reenvía cada llamada a la app en marcha con el token de `data/mcp-token` y arranca la app si no responde. `faustus-plugin.json` describe la app, su comprobación de salud y el puente para Faustus y el Hoard Hub.

Herramientas (39): `kafka_overview`, `kafka_status`, `deadlines_list`, `deadline_get`, `deadline_add`, `deadline_update`, `deadline_explain`, `deadline_delete`, `docs_list`, `doc_get`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `extract_preview`, `warranty_check`, `series_list`, `price_history`, `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan`, `phileas_sync`, `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run`. Argumentos en [docs/API.md](docs/API.md).

Eventos en el bus de la familia: `kafka.document.added`, `kafka.price.change`, `kafka.deadline.soon` y `kafka.deadline.overdue`. `phileas_sync` pide a Phileas's Hoard (por el enlace de la familia) las compras entregadas y crea sus plazos de garantía.

## Cómo está hecha

FastAPI + SQLite (WAL, FTS5) + un planificador de dos carriles; interfaz React 19 + Vite + Tailwind; la extracción en `kafka_hoard/extract/` (funciones puras); los días hábiles en `kafka_hoard/bizdays.py`; el trabajo en `kafka_hoard/engine.py`. Ver [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Pruebas: `python -m pytest -q` (sin red; los documentos y PDF los generan las propias pruebas con datos inventados).

## Límites

- Las fechas se leen con reglas para texto en español e inglés. Un formato inusual puede acabar en Revisión en vez de convertirse en plazo.
- Sin el paquete OCR opcional, los documentos escaneados se guardan pero no se lee su texto.
- Los calendarios de días hábiles cubren los festivos nacionales y las regiones de Madrid, Cataluña, Andalucía, Comunidad Valenciana, Galicia y País Vasco; añade otros festivos locales en Ajustes.
- El paso opcional con modelo (por el enlace Hoard) solo propone datos; se descarta todo lo que no tenga la fecha escrita en el documento. Los plazos calculados por reglas no son asesoramiento jurídico: comprueba la notificación original en lo importante.

## Licencia

MIT
