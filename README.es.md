# Kafka's Hoard

Un archivador local de papeleo. Lee facturas, contratos, pólizas de seguro, tickets, garantías, cartas de Hacienda, de la DGT o del ayuntamiento, multas, ITV y documentos de identidad; averigua quién los emite, los importes y todas las fechas que importan; convierte esas fechas en plazos (contando días hábiles españoles donde la ley lo exige); te avisa antes de cada uno y responde a tus preguntas citando el documento y la página.

Forma parte de la familia de apps locales Hoard: corre en tu PC, guarda sus datos en `data/`, funciona sola en el navegador y un asistente puede manejarla por MCP.

[English version](README.md)

## Qué hace

- **Lee lo que le das.** Arrastra archivos a la página, déjalos en una carpeta vigilada (por defecto `data/inbox`) o deja que lea la cuenta de correo configurada en Faustus: los adjuntos PDF e imagen se archivan y la contraseña nunca sale de Faustus. Admite PDF (capa de texto y, con el paquete OCR opcional, páginas escaneadas), imágenes (PNG, JPEG, WebP, HEIC, TIFF, BMP, GIF), `.eml`, `.docx`, `.pptx` (una página por diapositiva, en el orden en que las muestra la presentación, con las notas del orador), HTML y texto plano. Cada archivo se guarda una sola vez por su SHA-256 y el original no se modifica nunca.
- **Entiende el documento.** Reglas (sin modelo) lo clasifican (factura, ticket, recibo, contrato, seguro, garantía, impuestos, notificación oficial, multa, vehículo/ITV, identidad, suscripción, nómina, banco, manual de instrucciones), identifican al emisor, eligen la referencia (póliza, contrato, factura, expediente, boletín de multa), el importe y el periodo de facturación, y encuentran cada fecha con su papel: emisión, vencimiento, renovación, efecto desde/hasta, compra, entrega, caducidad, notificación, próxima ITV, fin de permanencia. Cada dato conserva su evidencia: la cita y la página.
- **Crea plazos y explica por qué.** Vencimientos de pago, renovaciones, el último día para cancelar una renovación (un mes antes para el tomador, Ley 50/1980 art. 22), fin de la garantía legal de 3 años (Real Decreto Legislativo 7/2021), fin de permanencia, los 20 días naturales para pagar una multa de tráfico con el 50 % de descuento o presentar alegaciones (dgt.es), plazos para responder a la administración en días hábiles (Ley 39/2015 art. 30), caducidad de documentos y próxima ITV. «¿Por qué esta fecha?» muestra la regla, la fecha base, los días contados y la cita. Ver [docs/RULES.md](docs/RULES.md).
- **Te avisa.** Cada tipo de plazo tiene días de antelación que puedes cambiar (una renovación: 45, 30 y 7 días antes; el descuento de una multa: 5, 2 y el último día). Un aviso por plazo y por pasada, los vencidos una sola vez, nada por la noche (de 23:00 a 07:00 por defecto; los urgentes pueden saltarse la pausa). Canales: aviso de Windows, bus de la familia Hoard, ntfy, Telegram y correo (enviado con la cuenta de Faustus). Cada aviso se envía una sola vez.
- **Sigue lo que se repite.** Recibos, seguros y suscripciones del mismo emisor y referencia forman una serie. Un cambio de precio del 5 % o más (configurable) genera un aviso; el historial se ve por serie. Los plazos recurrentes (mensual, anual) pasan a su siguiente fecha al marcarlos como hechos.
- **Responde con citas.** La búsqueda de texto completo (SQLite FTS5, sin distinguir tildes) recorre todas las páginas y devuelve fragmentos con documento y página, también limitada a una lista de documentos (`doc_ids`, por ejemplo los manuales de un aparato); `warranty_check` dice si una compra sigue en garantía y hasta cuándo.
- **Guarda manuales.** Un documento archivado con el tipo `manual` (manuales de instrucciones) se guarda y se busca como los demás y nunca crea plazos. Los manuales también se reconocen por su texto («manual de instrucciones», «instrucciones de uso»).
- **Guarda plazos de otras apps.** Otra app de la familia (HomeHoard, para el mantenimiento) envía sus plazos con `deadline_add` y un `source` + `external_key`: si lo envía otra vez se actualiza el mismo plazo en lugar de duplicarse, una fecha nueva lo reabre, y la base y la norma que envía aparecen en «¿por qué esta fecha?». Lo que cambias aquí gana: un título, avisos o fecha editados se mantienen hasta que la app envía una nueva ocurrencia, y un plazo que descartas sigue descartado. `deadline_update_by_key` permite a la app cerrarlo o reprogramarlo. Los avisos salen por los mismos canales y enlazan con la app.
- **Habla con el hub de la familia.** Los avisos salen por el hub (`POST /api/notify`, con prioridad, agrupación y clave de deduplicación); si el hub no está o falla, se usan los canales propios de siempre (ajuste `notify.via`: automático, solo hub o solo propios). El correo de papeleo se lee por la pasarela de correo del hub (ajuste `mail.source`: automático, hub o ayudante de Faustus), con marca de lectura propia, y cada correo archivado se reclama como documento (`hoard://kafka/document/<id>`). Los plazos aparecen en la agenda familiar (`GET /api/family/agenda`: renovaciones, mantenimiento, seguimientos de otras apps y plazos, con prioridad según cercanía y gravedad).
- **Eventos.** `kafka.document.archived` (con `order_ref` cuando el documento trae un número de pedido), `kafka.warranty.created`, `kafka.deadline.created`, `kafka.deadline.soon` y `kafka.deadline.overdue` (una vez por plazo y estado).
- **Enlaza facturas con Ledger.** Al archivar una factura o un recibo con importe y fecha, busca en Ledger (`tx_find`) y, si solo un movimiento puntúa 0,8 o más, lo enlaza (`tx_attach_doc` y la referencia `hoard://ledger/tx/<id>`); en Detalle aparece como chip y, si no hay un candidato claro, se elige entre los candidatos (`document_link_tx`). Se reintenta una vez al día con los documentos de los últimos 45 días (ajuste `links.ledger`).
- **Actas a plazos y carpeta de la renta.** `deadlines_from_minutes` convierte en plazos las tareas con fecha de un acta de Funes que son tuyas o no tienen responsable (tu nombre en `minutes.me`); `tax_pack` prepara una carpeta nueva del año natural con los documentos que suelen hacer falta, un índice, un CSV, el resumen anual de Ledger y la lista de certificados que faltan (en el Taller, abajo).
- **Historia en silencio.** El primer escaneo del correo y los archivos antiguos se archivan sin avisos y sus plazos pasados se crean como hechos, así que solo te enteras de lo que aún está por delante.
- **Trabaja con los propios ficheros (Taller).** Unir PDF (con las páginas que elijas de cada uno), dividirlos (un fichero por página, por rangos como `1-3,4-6` o cada N páginas), extraer, borrar, girar o reordenar páginas, comprimir (con Ghostscript si está instalado; si no, un compresor integrado que recomprime las imágenes incrustadas; opcionalmente hasta un tamaño objetivo en MB), poner o quitar contraseña (AES-256), estampar una marca de agua de texto en diagonal, leer o cambiar título, autor, asunto y palabras clave, convertir fotos en PDF (A4, Carta o ajustado, respetando la orientación EXIF), convertir Word, ODT y RTF a PDF (Microsoft Word en Windows; si no, LibreOffice), pasar las páginas de un PDF a PNG o JPG y comprimir imágenes PNG, JPEG y WEBP por debajo de un límite de peso, una sola o una carpeta entera. Cada resultado es un fichero nuevo: nada se sobrescribe (si el nombre existe queda «nombre (2)») y los originales no se tocan. Las contraseñas se usan solo en memoria y nunca se registran, se devuelven ni se guardan.
- **Privado de cara al asistente.** DNI/NIE, IBAN, tarjetas y teléfonos se enmascaran en lo que devuelven MCP y la ruta del agente salvo que pidas expresamente ese número. La interfaz local lo muestra todo.

## Pantallas

En castellano por defecto, inglés con un clic, oscura.

- **Plazos** — vencidos, esta semana, próximos 30 días y más adelante; marcar hecho, aplazar, editar, descartar, añadir a mano; novedades desde la última visita; documentos esperando tu decisión.
- **Documentos** — subida arrastrando archivos a cualquier parte, lista con filtros por tipo y estado, búsqueda de texto completo con fragmentos.
- **Detalle** — vista previa de la página con el original, datos extraídos con su evidencia, plazos con «¿por qué esta fecha?», series e historial de precios, el movimiento de Ledger enlazado en facturas y recibos, edición de cualquier campo (tu edición siempre gana al releer).
- **Revisión** — documentos de los que las reglas no estaban seguras y correos que pueden ser papeleo: aceptar, cambiar, ignorar.
- **Taller** — una rejilla con las operaciones anteriores; cada una tiene su zona para soltar ficheros o un campo de ruta, sus opciones y una tarjeta de resultado con el tamaño antes y después, botón de descarga y «Archivar en Kafka» para los PDF. Detalle tiene un botón «Abrir en el taller» para los documentos PDF. Abajo, la tarjeta de la carpeta de la renta (año, ZIP opcional).
- **Ajustes** — carpeta del taller, carpetas vigiladas, correo (hub o Faustus), región del calendario y festivos extra, días de antelación, ruta de avisos (hub o propia) y canales con botón de prueba, estado del OCR y del modelo, idioma, actividad reciente.

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

Herramientas (60): `kafka_overview`, `kafka_status`, `deadlines_list`, `deadline_get`, `deadline_add`, `deadlines_from_minutes`, `deadline_update_by_key`, `deadline_update`, `deadline_explain`, `deadline_delete`, `docs_list`, `doc_get`, `doc_compare`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `document_link_tx`, `extract_preview`, `warranty_check`, `series_list`, `price_history`, `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan`, `phileas_sync`, `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run`, `pdf_merge`, `pdf_split`, `pdf_pages`, `pdf_compress`, `pdf_protect`, `pdf_watermark`, `pdf_info`, `pdf_metadata_set`, `pdf_from_images`, `pdf_from_office`, `pdf_to_images`, `images_compress`, `tax_pack`, `doc_extract`, `ocr_image`, `ocr_pdf`, `ocr_status`. Argumentos en [docs/API.md](docs/API.md).

Eventos en el bus de la familia: `kafka.document.added`, `kafka.price.change`, `kafka.deadline.soon` y `kafka.deadline.overdue`. `phileas_sync` pide a Phileas's Hoard (por el enlace de la familia) las compras entregadas y crea sus plazos de garantía.

## Lectura de documentos para la familia

Kafka es el dueño de la lectura de documentos y del OCR de la familia (contrato: `HoardLink/docs/commons/services.md`, sección 5). Las demás apps lo llaman a través del hub y pasan rutas, nunca bytes:

- `doc_extract(path, mode?, max_pages?, lang?, wait_s?)`: texto por página de PDF, imágenes, `.docx`, `.eml`, HTML, texto, hojas de cálculo y archivos comprimidos, con OCR en las páginas sin capa de texto. Devuelve `{kind, title, text, units, needs_ocr, notes, mime, pages_ocr}` o la vista de un trabajo si tarda más de `wait_s`.
- `ocr_image(path, lang?, wait_s?)` y `ocr_pdf(path, pages?, dpi?, lang?, mode?, wait_s?)`: texto con bloques (caja y confianza) por página; `ocr_pdf` aplica OCR a las páginas elegidas aunque tengan capa de texto.
- `ocr_status(job_id?, wait_s?)`: el informe del motor (cuál de `winocr`, `rapidocr`, `light-ocr` y `tesseract` está instalado y elegido) o la vista de un trabajo; `wait_s` se limita a 150 s.

El idioma por defecto es el español. `KAFKA_OCR_BACKEND` (`auto`, `winocr`, `rapidocr`, `light-ocr`, `tesseract`) fuerza un motor y `KAFKA_OCR_LANG` cambia el idioma por defecto. Se rechazan los ficheros de más de 200 MB.

El motor nativo opcional `light-ocr` se instala con `npm install --include=optional` en esta carpeta y necesita Node.js 22 o posterior. Usa la CPU y lee las imágenes localmente, con cajas y confianza por línea; no descarga modelos al leer. Para seleccionarlo: `KAFKA_OCR_BACKEND=light-ocr`. En automático sigue después de winocr y rapidocr, antes de tesseract. Si no está instalado o no puede cargar, se prueba el siguiente motor disponible. Los plazos siguen usando las mismas reglas y evidencias.

La extracción compartida conserva los encabezados y filas de tablas repetidos
en cada página del PDF, también cuando cambian los importes. Las comparaciones
reproducibles con escaneos inventados están en [el banco de OCR](docs/OCR_BENCHMARK.md).

## El taller

Las doce herramientas del taller (`pdf_*` e `images_compress`) aceptan una ruta absoluta o el id de un documento archivado (`d_…`) y devuelven las rutas, el número de páginas y los tamaños antes y después de lo que han escrito. Dónde va el resultado:

- Un fichero subido en la página Taller: `data/workshop/out/<trabajo>/`. Lo subido vive en `data/workshop/in/<trabajo>/`; las carpetas de trabajo de más de 7 días las borra la limpieza horaria.
- Un documento archivado indicado por su id: la carpeta del ajuste `workshop.dir` (por defecto `Documentos\Kafka's Hoard\Taller`).
- Una ruta: junto al original, con un sufijo (`_unido`, `_paginas`, `_rotado`, `_comprimido`, `_protegido`, `_sin_clave`, `_marca`, `_dividido`, `_imagenes`, `_comprimida`). `output` u `out_dir` eligen otro sitio. Se rechazan las carpetas del sistema, las ocultas y la de datos de Kafka.

Con `file_result: true` cada PDF resultante se archiva también en Kafka y se devuelven sus `doc_ids`. REST de la página: `POST /api/workshop/upload` (multipart, campo `files`, `job` opcional), `GET /api/workshop/file?path=` (solo ficheros que ha producido el taller o que están en `data/workshop/`, mismo origen, cabeceras de descarga) y `GET /api/workshop/status` (cuáles de Ghostscript, Word y LibreOffice se han encontrado).

PrintCraft 0.2.1 es un editor PDF local opcional, con 123 herramientas `printcraft_*` en el mismo catálogo de API y MCP. Configúralo con `printcraft.cli` en Ajustes → Taller de PDF, con `KAFKA_PRINTCRAFT_CLI` o colocando el bundle portable en `native-craft-bundles/`. Esta instalación guarda el ejecutable verificado en el ajuste existente `printcraft.cli`, así que lo descubre al arrancar sin variables de entorno. `printcraft_status` compara las herramientas activas con los esquemas JSON incluidos. Usa `printcraft_workflow` con el id (`d_…`) de un PDF archivado para inspeccionar párrafos, editar uno, insertar y girar páginas, y guardar un PDF nuevo. Cada resultado incluye un recibo `pc_…` con los SHA-256 de entrada y salida; pasa ese id como `doc` para continuar la edición y usa `printcraft_artifacts` para consultar el historial. Kafka copia el original antes de editarlo y nunca lo sobrescribe. La edición de texto existente depende de las fuentes y estructura del PDF; revisa el resultado exportado, sobre todo en escrituras que el motor indique que no admite.

## Cómo está hecha

FastAPI + SQLite (WAL, FTS5) + un planificador de dos carriles (el `LaneScheduler` de la familia, con las últimas ejecuciones en `data/scheduler.json`); interfaz React 19 + Vite + Tailwind; la extracción en `kafka_hoard/extract/` (funciones puras); los días hábiles en `kafka_hoard/bizdays.py`; el trabajo en `kafka_hoard/engine.py`. Ver [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Pruebas: `python -m pytest -q` (sin red; los documentos y PDF los generan las propias pruebas con datos inventados).

## Límites

- El taller nunca edita un fichero en su sitio y no lee páginas escaneadas (no hay OCR en el taller). Sin Ghostscript la compresión solo reduce las imágenes, así que un PDF de texto y dibujo vectorial apenas encoge; los PDF con contraseña necesitan indicarla; las fotos HEIC necesitan el paquete opcional `pillow-heif`; la conversión de Word necesita Word o LibreOffice instalado; la marca de agua es texto latino plano en Helvetica.
- Las fechas se leen con reglas para texto en español e inglés. Un formato inusual puede acabar en Revisión en vez de convertirse en plazo.
- Sin el paquete OCR opcional, los documentos escaneados se guardan pero no se lee su texto.
- Los calendarios de días hábiles cubren los festivos nacionales y las regiones de Madrid, Cataluña, Andalucía, Comunidad Valenciana, Galicia y País Vasco; añade otros festivos locales en Ajustes.
- El paso opcional con modelo (por el enlace Hoard) solo propone datos; se descarta todo lo que no tenga la fecha escrita en el documento. Los plazos calculados por reglas no son asesoramiento jurídico: comprueba la notificación original en lo importante.

## Licencia

MIT

## Comparar versiones archivadas

`doc_compare(before, after)` muestra los cambios de texto con citas a ambos documentos y sus páginas físicas. Alinea líneas a través de saltos de página, conserva filas repetidas y números e ignora únicamente líneas vacías y espacios dentro de una línea. Admite selección de páginas y paginación con `offset`/`next_offset`. Muestra las advertencias de extracción y páginas sin texto; la igualdad se refiere al texto archivado seleccionado, no a los archivos originales. No cambia originales, ediciones del usuario ni plazos. [Referencia y límites](docs/DOCUMENT_COMPARE.md).
