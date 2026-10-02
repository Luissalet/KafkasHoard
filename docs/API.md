# Kafka's Hoard — agent tools

Every tool is served by the app at `GET /api/agent/tools` and `POST /api/agent/call` (Bearer token from `data/mcp-token`), by the stdio bridge `mcp_server.py`, and to the bundled UI through `POST /api/ui/call`. The argument tables are generated from the code (`python scripts/gen_api_doc.py`).

## `kafka_overview`

Overdue and upcoming deadlines, documents to review, news. Mis papeles y plazos de un vistazo.

Overdue, this week, next 30 days, later; documents and mails that need review; recent documents; notifications since the last visit.
Sinónimos: plazos, vencimientos, qué tengo pendiente, papeles, qué vence, resumen, avisos

Annotations: readOnlyHint, idempotentHint.

## `kafka_status`

Health: folders, mail, OCR, model pass, channels, scheduler, settings. Estado de Kafka.

Sinónimos: configuración, claves, canales de aviso, último escaneo, carpetas vigiladas

Annotations: readOnlyHint, idempotentHint.

## `deadlines_list`

List deadlines: overdue, upcoming, open, done. Lista de plazos y vencimientos.

Filter by kind (payment, renewal, cancel_by, warranty_end, permanence_end, appeal, fine_discount, expiry, itv, tax, custom) and text.
Sinónimos: qué vence este mes, plazos de pago, renovaciones, multas pendientes, garantías

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `filter` (overdue \| upcoming \| open \| done \| all) | no |  |
| `days` (integer) | no | For 'upcoming': how many days ahead. |
| `kind` (string) | no | One of payment, renewal, cancel_by, warranty_end, permanence_end, appeal, fine_discount, expiry, itv, tax, custom. |
| `text` (string) | no |  |
| `limit` (integer) | no |  |

## `deadline_get`

One deadline with its basis, rule, evidence and document. Detalle de un plazo.

Sinónimos: cuándo vence, de dónde sale esta fecha, plazo concreto

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `deadline` (string) | yes | Deadline id (t_…). |
| `reveal` (boolean) | no | Show personal identifiers unmasked (only when the user asks for that exact number). |

## `deadline_add`

Add a deadline by hand (title, date, reminders, recurring). Añadir un plazo a mano.

Sinónimos: recuérdame, apunta una fecha, nuevo vencimiento, aviso

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `title` (string) | yes |  |
| `date` (string) | yes | YYYY-MM-DD |
| `kind` (string) | no | One of payment, renewal, cancel_by, warranty_end, permanence_end, appeal, fine_discount, expiry, itv, tax, custom. |
| `remind` (array/null) | no | Lead days before the date, e.g. [30, 7, 0]. Default depends on the kind. |
| `doc` (string) | no | Document id it belongs to (optional). |
| `recurring` (none \| monthly \| yearly) | no |  |
| `notes` (string) | no |  |
| `amount` (number/null) | no |  |

## `deadline_update`

Change a deadline: date, title, reminders, done, dismissed, snooze. Editar o cerrar un plazo.

Marking a recurring deadline done moves it to its next occurrence.
Sinónimos: hecho, pagado, posponer, descartar, cambiar fecha, aplazar

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `deadline` (string) | yes |  |
| `date` (string/null) | no | YYYY-MM-DD |
| `title` (string/null) | no |  |
| `remind` (array/null) | no |  |
| `state` (string/null) | no |  |
| `snooze_days` (integer/null) | no | Move the date this many days from today (or from its date when later). |
| `notes` (string/null) | no |  |
| `recurring` (string/null) | no |  |

## `deadline_explain`

Why a deadline has that date: basis, legal rule, evidence quote, page. Explicar un plazo.

Quote the basis and the citation to the user; Kafka does not give legal advice.
Sinónimos: cómo se calcula, días hábiles, por qué esa fecha, fuente, artículo

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `deadline` (string) | yes | Deadline id (t_…). |
| `reveal` (boolean) | no | Show personal identifiers unmasked (only when the user asks for that exact number). |

## `deadline_delete`

Delete a deadline (confirm=true). Borrar un plazo.

Sinónimos: eliminar plazo, quitar vencimiento

Annotations: destructiveHint.

| Argument | Required | Description |
|---|---|---|
| `deadline` (string) | yes |  |
| `confirm` (boolean) | no |  |

## `docs_list`

List documents with filters: kind, issuer, year, state, text. Lista de documentos.

Sinónimos: facturas, contratos, pólizas, tickets, multas, notificaciones, mis papeles, buscar documento

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `kind` (string) | no | One of invoice, receipt, bill, contract, insurance, warranty, tax, official_notice, fine, vehicle, identity, subscription, payslip, bank, other. |
| `issuer` (string) | no |  |
| `year` (string) | no |  |
| `state` (string) | no | review, ok or archived. Empty: everything but archived. |
| `text` (string) | no | Matches title, issuer, reference, item, file name or mail subject. |
| `limit` (integer) | no |  |

## `doc_get`

One document: fields, extracted facts with evidence, deadlines, series, page text. Detalle de un documento.

Facts carry a citation [d_id · p. N]. Identifiers are masked unless reveal=true.
Sinónimos: abre este documento, qué dice, importe, emisor, fechas, texto de la página

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Document id (d_…). |
| `include_text` (boolean) | no | Include the page text (capped). |
| `pages` (array/null) | no | Only these page numbers. |
| `reveal` (boolean) | no | Show personal identifiers unmasked (only when the user asks for that exact number). |

## `doc_search`

Full-text search over every page, with snippets and citations. Buscar en los documentos.

Quote only what the snippets say and cite them as [d_id · p. N].
Sinónimos: busca, encuentra, dónde pone, cuánto pagué, número de póliza, buscar en mis papeles

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `query` (string) | yes |  |
| `kind` (string) | no |  |
| `issuer` (string) | no |  |
| `year` (string) | no |  |
| `state` (string) | no |  |
| `limit` (integer) | no |  |
| `reveal` (boolean) | no | Show personal identifiers unmasked (only when the user asks for that exact number). |

## `doc_add_file`

File a document from an absolute path on this computer (PDF, image, .eml, text). Archivar un fichero.

Accepts PDF, image, .eml, .txt, .html and .docx. System folders and credential files are refused. Identical content is filed once.
Sinónimos: añade este PDF, sube esta factura, guarda este documento, importar

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of a PDF, image, .eml, .txt, .html or .docx on this computer. |

## `doc_add_text`

File a document pasted as text. Archivar un texto pegado.

Sinónimos: pegar texto, guardar este correo, nota

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `title` (string) | no |  |
| `text` (string) | yes | The document as plain text. |

## `doc_update`

Correct a document: title, kind, issuer, ref, amount, dates, item, tags, notes, warranty. Editar documento.

What you fix is kept when the document is re-read, and its deadlines are recomputed.
Sinónimos: corrige el importe, cambia el emisor, es una factura, garantía de 2 años, archivar

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes |  |
| `title` (string/null) | no |  |
| `kind` (string/null) | no |  |
| `issuer` (string/null) | no |  |
| `ref` (string/null) | no |  |
| `amount` (number/null) | no |  |
| `issue_date` (string/null) | no | YYYY-MM-DD |
| `period_from` (string/null) | no | YYYY-MM-DD |
| `period_to` (string/null) | no | YYYY-MM-DD |
| `item` (string/null) | no |  |
| `tags` (array/null) | no |  |
| `notes` (string/null) | no |  |
| `state` (string/null) | no |  |
| `warranty_years` (number/null) | no | Legal guarantee for this purchase, in years (default 3). |

## `doc_reprocess`

Re-read a document with the current rules (optionally the local model). Reprocesar documento.

Sinónimos: vuelve a leer, recalcula plazos, revisar extracción

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes |  |
| `llm` (boolean) | no | Also ask the local model when the rules leave the document in review. |

## `doc_delete`

Delete a document and its deadlines (confirm=true). Borrar documento.

The stored file goes too unless another document uses the same content.
Sinónimos: eliminar, quitar, borrar papel

Annotations: destructiveHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes |  |
| `confirm` (boolean) | no |  |

## `extract_preview`

What Kafka would extract from a text (kind, issuer, amounts, dates, deadlines); saves nothing. Vista previa.

Sinónimos: qué plazos tiene este texto, probar extracción, analizar sin guardar

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `text` (string) | yes | The document text. |
| `subject` (string) | no |  |
| `from_address` (string) | no |  |
| `received` (string) | no | YYYY-MM-DD the document arrived (base date when it states none). |
| `reveal` (boolean) | no |  |

## `warranty_check`

Active warranties (legal guarantee) for a product or shop, with days left. Comprobar garantía.

Legal guarantee for goods: 3 years from delivery (RDL 7/2021).
Sinónimos: ¿sigue en garantía?, garantía de, cuánto le queda, compré, devolución, reclamar

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `text` (string) | no | Product or shop. Empty: every active warranty. |
| `include_expired` (boolean) | no |  |

## `series_list`

Documents of the same contract, policy or subscription across periods. Series de documentos.

Sinónimos: mi seguro cada año, histórico de recibos, suscripciones, evolución

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `kind` (string) | no |  |
| `issuer` (string) | no |  |

## `price_history`

Amounts of a series over time with the % change between periods. Histórico de precios.

Sinónimos: cuánto sube, subida de la prima, precio de la luz, evolución del precio, cuota

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `series` (string) | no | Series id (r_…). |
| `issuer` (string) | no | Or an issuer name: every series of that issuer. |

## `mail_scan`

Read paperwork mail now (or search back N days / a query) and file its attachments. Leer el correo ya.

Sinónimos: revisa el correo, importar facturas del correo, busca en el correo, adjuntos

Annotations: idempotentHint, openWorldHint.

| Argument | Required | Description |
|---|---|---|
| `since_days` (integer/null) | no | How far back to read (default: the configured window). |
| `query` (string) | no | Optional search, e.g. an issuer or a word. |

## `mail_list`

Mails Kafka read: to review (maybe), with documents (doc), noise. Correos leídos y dudosos.

Sinónimos: correos dudosos, qué correos ha leído, adjuntos, revisión

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `kind` (maybe \| doc \| noise \| all) | no |  |
| `state` (new \| filed \| ignored \| all) | no |  |
| `limit` (integer) | no |  |
| `reveal` (boolean) | no |  |

## `mail_accept`

File a doubtful mail (attachments or body) as documents. Aceptar correo dudoso.

Sinónimos: sí es un documento, archivar este correo

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `message_id` (string) | yes |  |

## `mail_ignore`

Ignore a doubtful mail. Ignorar correo dudoso.

Sinónimos: no es un papel, descartar correo

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `message_id` (string) | yes |  |

## `mail_status`

Which mail account Kafka reads (Faustus) and whether it answers. Estado del correo.

Sinónimos: cuenta de correo, conexión, Faustus

Annotations: readOnlyHint, idempotentHint, openWorldHint.

## `folders_list`

Watched folders (the inbox and any added) and the last scan. Carpetas vigiladas.

Sinónimos: carpeta de entrada, inbox, dónde dejo los papeles

Annotations: readOnlyHint, idempotentHint.

## `folder_add`

Watch a folder: new files are filed, nothing is moved or deleted. Vigilar una carpeta.

Drive roots, the profile folder and system folders are refused.
Sinónimos: añade carpeta, escanear carpeta, Descargas

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of an existing folder. |

## `folder_remove`

Stop watching a folder (its files are untouched). Dejar de vigilar una carpeta.

Sinónimos: quitar carpeta

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of an existing folder. |

## `folder_scan`

Scan the watched folders now. Escanear carpetas ya.

Sinónimos: busca documentos nuevos, revisa la carpeta

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `folder` (string) | no | One watched folder; empty scans all. |

## `phileas_sync`

Create warranties from delivered parcels in Phileas's Hoard. Garantías de compras entregadas.

Sinónimos: paquetes entregados, compras, sincronizar envíos, garantía legal 3 años

Annotations: idempotentHint, openWorldHint.

## `notifications_list`

Notifications sent (newest first). Avisos enviados.

Sinónimos: historial de avisos, qué me has avisado

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `limit` (integer) | no |  |

## `notify_status`

Notification channels: toast, hub, ntfy, Telegram, email. Canales de aviso.

Sinónimos: avisos, notificaciones, Telegram, correo, ntfy

Annotations: readOnlyHint, idempotentHint.

## `notify_test`

Send a test notification through one channel. Probar un canal de aviso.

Sinónimos: prueba de aviso, comprobar notificación

Annotations: openWorldHint.

| Argument | Required | Description |
|---|---|---|
| `channel` (toast \| hub \| ntfy \| telegram \| email) | yes |  |

## `telegram_find_chat_id`

Find and save the Telegram chat id after writing to the bot. Buscar chat de Telegram.

Sinónimos: configurar Telegram

Annotations: idempotentHint, openWorldHint.

## `settings_set`

Change settings: reminders per kind, warranty years, region, price alert, intervals. Cambiar ajustes.

Sinónimos: avisarme antes, días de antelación, comunidad autónoma, festivos, idioma, intervalo

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `values` (object) | yes | Setting key -> value. See kafka_status → settings for the keys. |

## `secret_set`

Store a key (Telegram, ntfy, SMTP, Faustus folder). Guardar una clave.

Sinónimos: token de Telegram, tema de ntfy, contraseña SMTP

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `name` (FAUSTUS_DIR \| TELEGRAM_TOKEN \| TELEGRAM_CHAT_ID \| NTFY_TOPIC \| NTFY_TOKEN \| SMTP_HOST \| SMTP_PORT \| SMTP_USER \| SMTP_PASSWORD \| SMTP_FROM \| SMTP_TO) | yes |  |
| `value` (string) | no | Empty clears it. |

## `scheduler_status`

Background jobs: lanes, queue, last scans and reminders. Estado del planificador.

Sinónimos: tareas en segundo plano

Annotations: readOnlyHint, idempotentHint.

## `runs_list`

Recent folder scans, mail scans and syncs with their result. Últimas ejecuciones.

Sinónimos: historial de escaneos, errores

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `limit` (integer) | no |  |

## `housekeeping_run`

Archive old closed deadlines, roll recurring ones, tidy series and caches. Mantenimiento.

Sinónimos: limpiar, ordenar, archivar plazos hechos

Annotations: idempotentHint.

## `pdf_merge`

Merge PDFs into one, optionally only some pages of each. Unir PDF en uno solo.

Inputs are absolute paths or document ids (d_…); order matters. Writes «<first>_unido.pdf» next to the first file (never overwrites). file_result=true also files the result in Kafka.
Sinónimos: une estos pdf, junta los pdf, juntar documentos, combinar pdf, unir facturas en un solo pdf

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `files` (array) | yes | PDFs to join, in order: absolute paths or document ids (d_…). |
| `ranges` (array/null) | no | Optional, same length as files: pages to take from each, e.g. ['', '1-3', '2,5-']. Empty string: all pages. |
| `password` (string) | no | Password for protected inputs (used in memory only). |

## `pdf_split`

Split a PDF: one file per page, per range or every N pages. Dividir un PDF en varios.

Writes the parts into a new folder «<name>_dividido» next to the source.
Sinónimos: divide el pdf, sepáralo por páginas, trocea el pdf, un pdf por página, parte el documento en dos

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `mode` (pages \| ranges \| every) | no | pages: one file per page. ranges: one file per comma-separated range. every: one file per N pages. |
| `ranges` (string) | no | For mode=ranges, e.g. '1-3,4-6,7-' (also last, -1, odd, even). |
| `every` (integer) | no | For mode=every: pages per file. |
| `password` (string) | no | Password of the PDF if it is protected. |
| `out_dir` (string) | no | Absolute folder for the files. Default: a new folder «<name>_dividido» next to the source. |
| `file_result` (boolean) | no | Also file each part in Kafka as a new document (doc_ids). |

## `pdf_pages`

Extract, delete, rotate or reorder the pages of a PDF. Extraer, quitar, girar o reordenar páginas.

action=extract keeps the given pages; delete removes them; rotate turns them (default all) 90, 180 or 270 degrees clockwise; reorder takes the full new order. Pages: '1-3,5,8-', 'last', '-1', 'odd', 'even'.
Sinónimos: quita la página 3, borra páginas del pdf, saca las páginas 2 a 5, gira el pdf, rota la página, pon las páginas en otro orden, invierte el pdf

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `action` (extract \| delete \| rotate \| reorder) | yes | extract: keep only these pages. delete: remove these pages. rotate: turn these pages (default all). reorder: new order of all pages. |
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `pages` (string) | no | Pages as '1-3,5,8-', 'last', '-1' (the last), 'odd', 'even'. Needed for extract and delete. |
| `degrees` (integer) | no | For rotate: 90, 180 or 270 clockwise (-90 turns left). |
| `order` (string) | no | For reorder: every page once in the new order, e.g. '3,1,2,4-6', or 'reverse'. |
| `password` (string) | no | Password of the PDF if it is protected. |

## `pdf_compress`

Shrink a PDF (Ghostscript if installed, else built-in), optionally under a size. Comprimir PDF.

target_mb tries stronger settings until the file fits and says when it cannot. Presets: screen, ebook, printer, prepress. Writes «<name>_comprimido.pdf».
Sinónimos: comprime el pdf, que pese menos de 2 MB, reduce el tamaño del pdf, el pdf es muy grande, aligera el documento, para enviarlo por correo

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `preset` (screen \| ebook \| printer \| prepress) | no | Strength: screen (smallest) < ebook < printer < prepress (best quality). |
| `target_mb` (number/null) | no | Try stronger settings until the file is at most this many MB; reports when that is impossible. |
| `engine` (auto \| ghostscript \| pypdf) | no | auto: Ghostscript when installed, else the built-in compressor. |
| `password` (string) | no | Password of the PDF if it is protected. |

## `pdf_protect`

Put a password on a PDF (AES-256) or remove it. Proteger con contraseña o quitársela a un PDF.

The password is never returned or stored. unprotect needs the current password. Writes «_protegido» or «_sin_clave».
Sinónimos: ponle contraseña, protege el pdf, cifra el documento, quítale la contraseña, desbloquea el pdf, quita la clave

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `action` (protect \| unprotect) | yes | protect: encrypt with AES-256. unprotect: remove the password (needs the current one). |
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `password` (string) | yes | protect: the password to set. unprotect: the current password. Never repeated back. |
| `owner_password` (string) | no | protect: optional separate owner password (default: the same). |
| `current_password` (string) | no | protect: password the input already has, if any. |
| `allow_print` (boolean) | no | protect: allow printing. |
| `allow_copy` (boolean) | no | protect: allow copying text. |
| `allow_modify` (boolean) | no | protect: allow editing. |

## `pdf_watermark`

Stamp a text watermark on the pages of a PDF. Marca de agua de texto en un PDF.

Diagonal by default; opacity, angle, size, colour and pages are adjustable. Writes «<name>_marca.pdf».
Sinónimos: marca de agua, ponle CONFIDENCIAL, sello de borrador, texto diagonal en cada página

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `text` (string) | yes | Watermark text, e.g. CONFIDENCIAL. |
| `opacity` (number) | no | 0.02 to 1 (default 0.3). |
| `angle` (number) | no | Degrees counter-clockwise (default 45, diagonal; 0 is horizontal). |
| `font_size` (number) | no | Size in points; long texts are shrunk to fit the page. |
| `color` (string) | no | gris, rojo, azul, negro, verde, naranja or a code like #808080. |
| `pages` (string) | no | Pages to mark (default all), e.g. '1-3,last'. |
| `password` (string) | no | Password of the PDF if it is protected. |

## `pdf_info`

Pages, sizes, password, metadata and whether a PDF has text. Información y metadatos de un PDF.

Read-only. Tells if it is scanned (no text layer) and if it needs a password.
Sinónimos: cuántas páginas tiene, qué tamaño tiene, quién es el autor, está protegido, es un escaneo, propiedades del pdf

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `password` (string) | no | Password of the PDF if it is protected. |

## `pdf_metadata_set`

Set the title, author, subject or keywords of a PDF. Cambiar metadatos de un PDF.

Omit a field to keep it; an empty string clears it. Writes «<name>_metadatos.pdf».
Sinónimos: cámbiale el título al pdf, pon el autor, edita las propiedades, palabras clave

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `title` (string/null) | no | New title (empty string clears it; omit to keep). |
| `author` (string/null) | no | New author (empty string clears it; omit to keep). |
| `subject` (string/null) | no | New subject (empty string clears it; omit to keep). |
| `keywords` (string/null) | no | New keywords, comma separated (empty string clears them; omit to keep). |
| `password` (string) | no | Password of the PDF if it is protected. |

## `pdf_from_images`

Make a PDF from images (JPG, PNG, WEBP, HEIC), one per page. Pasar fotos a PDF.

Page size A4, Letter or fit; margin; photo orientation is honoured. A folder means all its images in name order.
Sinónimos: pasa estas fotos a pdf, junta las imágenes en un pdf, escaneos a pdf, hazme un pdf con estas capturas

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `images` (array) | yes | Images in page order: absolute paths, document ids or a folder (its images, in natural name order). |
| `page_size` (A4 \| Letter \| fit) | no | Page size, or fit: each page takes its image's size. |
| `margin_mm` (number) | no | Margin around each image in millimetres. |
| `orientation` (auto \| portrait \| landscape) | no | auto: landscape for wide images. |

## `pdf_from_office`

Convert Word, ODT or RTF documents to PDF (Word or LibreOffice). Convertir un Word a PDF.

Uses Microsoft Word on Windows when installed, otherwise LibreOffice; says clearly when neither exists. Writes «<name>.pdf» next to the source.
Sinónimos: convierte el word a pdf, pasa el docx a pdf, guarda el documento como pdf, de doc a pdf

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `output` (string) | no | Absolute path of the result file. Default: next to the source with a suffix, or in the workshop folder for stored documents. |
| `out_dir` (string) | no | Absolute folder for the result instead of the default one. |
| `file_result` (boolean) | no | Also file each resulting PDF in Kafka as a new document and return its id (doc_ids). |
| `file` (string) | yes | A .docx, .doc, .odt or .rtf file: absolute path or document id (d_…). |
| `engine` (auto \| word \| libreoffice) | no | auto: Word on Windows when installed, else LibreOffice. |

## `pdf_to_images`

Render PDF pages as PNG or JPG images. Pasar páginas de un PDF a imágenes.

Writes into a new folder «<name>_imagenes». dpi 36-600 (default 150).
Sinónimos: pdf a png, pdf a jpg, exporta las páginas como imágenes, saca una foto de cada página

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `file` (string) | yes | PDF: absolute path or document id (d_…). |
| `pages` (string) | no | Pages to render (default all), e.g. '1-3'. |
| `format` (png \| jpg) | no | Image format of each page. |
| `dpi` (integer) | no | Resolution (default 150). |
| `quality` (integer) | no | JPEG quality. |
| `password` (string) | no | Password of the PDF if it is protected. |
| `out_dir` (string) | no | Absolute folder. Default: a new folder «<name>_imagenes» next to the source. |

## `images_compress`

Compress PNG, JPEG and WEBP images under a size limit, single or folder. Comprimir imágenes.

Copies, never touches the originals: lossless first, then fewer colours or lower quality, then smaller size. lossless_only reports what cannot fit. A folder goes to «<folder>_comprimidas»; a file to «<name>_comprimida».
Sinónimos: comprime las imágenes, que cada png pese menos de 5 MB, reduce el tamaño de las fotos, las fotos pesan mucho, optimiza los png

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `sources` (array) | yes | Image files (PNG, JPEG, WEBP), document ids or folders: absolute paths. |
| `limit_mb` (number/null) | no | Maximum size of each image in MB (1 MB = 1024 KB). |
| `limit_kb` (number/null) | no | Maximum size of each image in KB (use this or limit_mb). |
| `recursive` (boolean) | no | Folders: include subfolders. |
| `lossless_only` (boolean) | no | Never lower quality or size: images that stay above the limit are reported as failed. |
| `skip_small` (boolean) | no | Leave images already under the limit alone (not copied). |
| `out_dir` (string) | no | Absolute output folder. Default: «<folder>_comprimidas» next to a folder, or «<name>_comprimida» next to a file. An existing file there is skipped. |
| `time_limit_s` (number) | no | Stop after this many seconds and say what is left (0: no limit); repeat with the same out_dir to continue. |

## REST routes for the UI

- `GET /api/health`, `GET /api/status`
- `GET /api/dashboard` — open deadlines grouped as overdue, this week, next 30 days and later; recently closed; recent documents; notifications since the last visit; mails to review; mail and scheduler state.
- `POST /api/dashboard/visit` — marks notifications seen and records the visit.
- `POST /api/documents/upload` — multipart `files` (several allowed); each file is stored once by its SHA-256 and read.
- `GET /api/documents/{id}` — one document with facts and evidence, deadlines, series and price history, page text.
- `GET /api/documents/{id}/file` — the stored original, inline with its real type (HTML and mail are served as plain text); same-origin only.
- `GET /api/documents/{id}/page/{n}.png` — page preview of a PDF or the picture of an image document (cached in `data/cache/pages`).
- `POST /api/workshop/upload` — multipart `files` (PDF, images, Word/ODT/RTF; up to 60 files, 400 MB each) and an optional `job`; stores them in `data/workshop/in/<job>/` and returns the job id and each file's path (pages and whether it is protected, for PDFs). The UI then calls the `pdf_*` tools and `images_compress` with those paths; results of uploaded files go to `data/workshop/out/<job>/`.
- `GET /api/workshop/file?path=` — downloads a file under `data/workshop/` or one the workshop wrote in this run (attachment; `inline=1` shows a PDF or image); any other path is refused with 403; same-origin only.
- `GET /api/workshop/status` — workshop folder, pypdf version and whether Ghostscript, LibreOffice, Word and HEIC support were found.
- `POST /api/ui/call` `{name, arguments}` — any tool above, uncapped and unmasked (the UI is local).
