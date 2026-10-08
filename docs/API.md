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
| `source` (string) | no | Only deadlines another app keeps here (e.g. homehoard). |
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

Add a deadline by hand or for another app (source + external_key upserts). Añadir un plazo.

With source and external_key (another app of the family, e.g. homehoard) adding again updates the same deadline: a new date reopens it; a title, reminders or date the user changed here and a deadline the user dismissed are kept. basis and rule are shown by deadline_explain.
Sinónimos: recuérdame, apunta una fecha, nuevo vencimiento, aviso, mantenimiento

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `title` (string) | yes |  |
| `date` (string) | no | YYYY-MM-DD |
| `due` (string) | no | YYYY-MM-DD: the same as date, under the name other family apps use. One of date or due is required. |
| `kind` (string) | no | One of payment, renewal, cancel_by, warranty_end, permanence_end, appeal, fine_discount, expiry, itv, tax, custom (the agenda kinds followup, maintenance, exam and the like are filed as custom). |
| `remind` (array/null) | no | Lead days before the date, e.g. [30, 7, 0]. Default depends on the kind. |
| `doc` (string) | no | Document id it belongs to (optional). |
| `recurring` (none \| monthly \| yearly) | no |  |
| `notes` (string) | no |  |
| `note` (string) | no | The same as notes, under the name other family apps use. |
| `amount` (number/null) | no |  |
| `source_ref` (string) | no | hoard://<app>/<kind>/<id> of the record in another app this deadline comes from. With the title it makes adding idempotent: the same pair updates the deadline. |
| `source` (string) | no | App id that owns this deadline (e.g. homehoard). With external_key: adding again updates it. |
| `external_key` (string) | no | The owner app's stable key for this deadline; (source, external_key) never duplicates. |
| `basis` (string) | no | Why this date: the rule or legal basis the owner app applied (shown by deadline_explain). |
| `rule` (string) | no | Short name of the rule or norm, e.g. «RITE IT 3.3» or «Recomendación». |
| `url` (string) | no | Link back to the thing in the owner app; used by notifications. |

## `deadline_update_by_key`

Close, reopen or reschedule a deadline another app keeps here, by its key. Plazo por clave externa.

For the owner app: state done/dismissed closes it, a new date reschedules it. A deadline the user dismissed stays.
Sinónimos: cerrar plazo de otra app, mantenimiento hecho, reprogramar, mover fecha

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `source` (string) | yes | App id that owns the deadline (e.g. homehoard). |
| `external_key` (string) | yes | The owner app's key for the deadline. |
| `date` (string/null) | no | YYYY-MM-DD: reschedule (a new occurrence: reopened, reminders start again). |
| `state` (string/null) | no | Close (done, dismissed) or reopen. |
| `title` (string/null) | no |  |
| `notes` (string/null) | no |  |

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

## `deadlines_from_minutes`

Add a deadline per dated action item of meeting minutes. Plazos desde el acta de una reunión.

Reads the minutes from Funes through the family hub; items for someone else or without a date are skipped. Adding the same minutes again changes nothing. Own names go in the minutes.me setting.
Sinónimos: acta, reunión, tareas de la reunión, compromisos con fecha, acciones acordadas

Annotations: idempotentHint, openWorldHint.

| Argument | Required | Description |
|---|---|---|
| `minutes_id` (string) | yes | Id of the minutes in Funes (funes.minutes.ready event, minutes_get). |

## `tax_pack`

Build the income-tax folder of a year: documents, index, CSV, Ledger summary. Paquete de la renta.

Copies (never moves) the year's tax office letters, payslips and withholding certificates, bank certificates, donations, rent, mortgage and loan papers and invoices tagged deducible into «Renta <year>» with index.md, documentos.csv and a zip, and lists the usual certificates that are missing. The fiscal year is the calendar year.
Sinónimos: declaración de la renta, IRPF, Hacienda, certificados, desgravaciones, gastos deducibles, preparar la renta

Annotations: openWorldHint.

| Argument | Required | Description |
|---|---|---|
| `year` (integer/null) | no | Fiscal year (the calendar year in Spain). Default: the previous year. |
| `out_dir` (string) | no | Absolute folder to create «Renta <year>» in. Default: the workshop folder. |
| `zip` (boolean) | no | Also write «Renta <year>.zip» next to the folder. |

## `document_link_tx`

Link an invoice or receipt to its payment in Ledger; candidates when unsure. Enlazar factura con su pago.

Looks the amount and date up in Ledger through the family hub; a single strong match is linked both ways, otherwise the candidates come back and tx_id picks one.
Sinónimos: a qué pago corresponde esta factura, conciliar factura con movimiento, buscar el cargo, justificante del gasto

Annotations: idempotentHint, openWorldHint.

| Argument | Required | Description |
|---|---|---|
| `doc_id` (string) | yes | Document id (d_…) of an invoice or receipt with an amount and a date. |
| `tx_id` (string) | no | Ledger transaction to link (one of the candidates). Empty: look for it and link only a single strong match. |

## `docs_list`

List documents with filters: kind, issuer, year, state, text. Lista de documentos.

Sinónimos: facturas, contratos, pólizas, tickets, multas, notificaciones, mis papeles, buscar documento

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `kind` (string) | no | One of invoice, receipt, bill, contract, insurance, warranty, tax, official_notice, fine, vehicle, identity, subscription, payslip, bank, manual, other. |
| `issuer` (string) | no |  |
| `year` (string) | no |  |
| `state` (string) | no | review, ok or archived. Empty: everything but archived. |
| `text` (string) | no | Matches title, issuer, reference, item, file name or mail subject. |
| `doc_ids` (array/null) | no | Only these document ids (d_…), archived included; unknown ids are listed in missing. |
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

## `doc_compare`

Compare two archived documents with changed text and page citations. Comparar versiones de papeles.

Pass before and after document IDs. Optional before_pages/after_pages select physical pages. Aligns lines across page breaks, ignoring only whitespace and empty lines; preserves amounts and repeated rows. Returns ordered changes, same_text (null when equality cannot be established from empty page text), coverage and next_offset. Compare extracted text only: not visual differences or legal interpretation. Originals and deadlines unchanged.
Sinónimos: compare documents, diff, changed clauses, versions, comparar contratos, qué cambió, comparar recibos

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `before` (string) | yes | Earlier archived document ID, d_… |
| `after` (string) | yes | Later archived document ID, d_… |
| `before_pages` (array/null) | no | Physical pages to compare from before; absent means all archived pages. |
| `after_pages` (array/null) | no | Physical pages to compare from after; absent means all archived pages. |
| `offset` (integer) | no | Zero-based change index; use next_offset for more. |
| `limit` (integer) | no | Maximum changes; output byte budget can return fewer, with next_offset. |
| `reveal` (boolean) | no | Show personal identifiers only when the user explicitly requests them. |

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
| `doc_ids` (array/null) | no | Search only inside these documents (d_…), e.g. the manuals of one appliance. |
| `limit` (integer) | no |  |
| `reveal` (boolean) | no | Show personal identifiers unmasked (only when the user asks for that exact number). |

## `doc_add_file`

File a document from an absolute path on this computer (PDF, image, .eml, text). Archivar un fichero.

Accepts PDF, image, .eml, .txt, .html, .docx and .pptx. System folders and credential files are refused. Identical content is filed once.
Sinónimos: añade este PDF, sube esta factura, guarda este documento, importar

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of a PDF, image, .eml, .txt, .html, .docx or .pptx on this computer. |
| `kind` (string) | no | Kind to file it as, when the caller knows it (e.g. manual for an instruction manual); kept like a user edit. One of invoice, receipt, bill, contract, insurance, warranty, tax, official_notice, fine, vehicle, identity, subscription, payslip, bank, manual, other. |
| `item` (string) | no | The product or thing the document is about (optional); kept like a user edit. |

## `doc_add_text`

File a document pasted as text. Archivar un texto pegado.

Sinónimos: pegar texto, guardar este correo, nota

Annotations: idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `title` (string) | no |  |
| `text` (string) | yes | The document as plain text. |
| `kind` (string) | no | Kind to file it as, when the caller knows it (e.g. manual for an instruction manual); kept like a user edit. One of invoice, receipt, bill, contract, insurance, warranty, tax, official_notice, fine, vehicle, identity, subscription, payslip, bank, manual, other. |
| `item` (string) | no | The product or thing the document is about (optional); kept like a user edit. |

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

## `doc_extract`

Text of a file, scans too: PDF, image, Office, html, eml. Leer un documento, con OCR si es escaneo.

Stateless: nothing is filed in Kafka. Returns kind, title, text, units (page, section, slide, sheet), needs_ocr, notes and pages_ocr; pages without a text layer are read with OCR (ocr=auto|off|force). A long scan answers a job_id: follow it with ocr_status.
Sinónimos: extrae el texto, lee este pdf escaneado, OCR, leer una foto de un documento, texto de un word, contenido del fichero

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of the file to read: PDF, image, docx, odt, pptx, xlsx, epub, rtf, html, eml or text. |
| `ocr` (auto \| off \| force) | no | auto: OCR only the pages without a text layer. off: never. force: every page. |
| `max_pages` (integer) | no | Read at most this many pages. |
| `lang` (string) | no | Language of the text for OCR (es, en, fr, pt, it, de, ca). |
| `wait_s` (number) | no | Wait up to this many seconds (clamped to 150) for the result; past it the answer is the running job (job_id, status): follow it with ocr_status. |

## `ocr_image`

OCR of one image: its text, and the lines with boxes on request. Leer el texto de una imagen.

Spanish by default. 503 ocr_unavailable (with an install hint) when no engine is installed.
Sinónimos: reconocer texto, captura de pantalla, foto de un ticket, texto de una imagen, OCR

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of the image (PNG, JPEG, WEBP, HEIC, BMP, TIFF, GIF). |
| `lang` (string) | no | Language of the text (es, en, fr, pt, it, de, ca). |
| `blocks` (boolean) | no | Also return every recognised line with its box and confidence. |

## `ocr_pdf`

OCR of the pages of a PDF, without its text layer. Reconocer el texto de un PDF escaneado.

Runs as a job: wait_s up to 150 s, then the running job comes back; follow it with ocr_status. pages is e.g. '1-5,9'.
Sinónimos: pdf escaneado, ocr de las páginas, texto de un escaneo, reconocimiento de texto

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes | Absolute path of the PDF. |
| `pages` (string) | no | Pages to recognise, e.g. '1-5,9' (default: the first max_pages). |
| `dpi` (integer) | no | Resolution the pages are rendered at before OCR. |
| `lang` (string) | no | Language of the text (es, en, fr, pt, it, de, ca). |
| `max_pages` (integer) | no | Recognise at most this many pages. |
| `wait_s` (number) | no | Wait up to this many seconds (clamped to 150); past it the answer is the running job. |

## `ocr_status`

The OCR engine, or a doc_extract / ocr_pdf job. Estado del OCR y de sus trabajos.

Without job_id: {available, backend, languages, reason?}. With a job_id: that job, waiting up to wait_s (max 150 s) for it to finish.
Sinónimos: motor de OCR, ¿hay OCR instalado?, seguir un trabajo, cómo va el escaneo

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `job_id` (string) | no | A doc_extract or ocr_pdf job. Empty: report the OCR engine instead. |
| `wait_s` (number) | no | With a job_id: wait up to this many seconds (clamped to 150) for it to finish. |

## `printcraft_doc_open`

PrintCraft doc_open on a PDF. Operación PDF doc_open.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `password` (string) | no | User or owner password for encrypted files. |
| `path` (string) | no | Absolute path to a PDF, or use doc for a filed Kafka document. |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_list`

PrintCraft doc_list on a PDF. Operación PDF doc_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_info`

PrintCraft doc_info on a PDF. Operación PDF doc_info.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_close`

PrintCraft doc_close on a PDF. Operación PDF doc_close.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `discard_changes` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_save`

PrintCraft doc_save on a PDF. Operación PDF doc_save.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `full` (boolean) | no | Force a full rewrite (or, with false, an incremental update). |
| `path` (string) | no | Save as this file. Omit to save in place. |

## `printcraft_doc_set_info`

PrintCraft doc_set_info on a PDF. Operación PDF doc_set_info.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `key` (string) | yes |  |
| `value` (string) | yes |  |

## `printcraft_page_render`

PrintCraft page_render on a PDF. Operación PDF page_render.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `dpi` (number) | no |  |
| `page` (integer) | yes |  |

## `printcraft_text_extract`

PrintCraft text_extract on a PDF. Operación PDF text_extract.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers to extract (default: all). |

## `printcraft_text_find`

PrintCraft text_find on a PDF. Operación PDF text_find.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `limit` (integer) | no | Maximum matches (default 500). |
| `query` (string) | yes |  |

## `printcraft_page_rotate`

PrintCraft page_rotate on a PDF. Operación PDF page_rotate.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `degrees` (integer) | yes |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `orientation` (all \| landscape \| portrait) | no |  |
| `pages` (array) | no | 1-based page numbers to rotate (default: all). |
| `subset` (all \| even \| odd) | no |  |

## `printcraft_page_delete`

PrintCraft page_delete on a PDF. Operación PDF page_delete.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | yes | 1-based page numbers to delete. |

## `printcraft_page_move`

PrintCraft page_move on a PDF. Operación PDF page_move.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | yes | 1-based page numbers to move. |
| `to` (integer) | yes |  |

## `printcraft_page_insert_blank`

PrintCraft page_insert_blank on a PDF. Operación PDF page_insert_blank.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `at` (integer) | yes |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `height` (number) | no | Points. |
| `width` (number) | no | Points. |

## `printcraft_page_insert_file`

PrintCraft page_insert_file on a PDF. Operación PDF page_insert_file.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `at` (integer) | yes |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers of the source file (default: all). |
| `path` (string) | yes |  |

## `printcraft_page_extract`

PrintCraft page_extract on a PDF. Operación PDF page_extract.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `delete` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `open` (boolean) | no | Also open the result as a new document (default: only when out is omitted). |
| `out` (string) | no | File to write. Omit to open the result as a new unsaved document instead. |
| `out_dir` (string) | no |  |
| `pages` (array) | yes | 1-based page numbers to extract. |
| `separate` (boolean) | no |  |

## `printcraft_doc_combine`

PrintCraft doc_combine on a PDF. Operación PDF doc_combine.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `open` (boolean) | no | Also open the result as a new document (default: only when out is omitted). |
| `out` (string) | no | File to write. Omit to open the result as a new unsaved document instead. |
| `pages` (array) | no |  |
| `paths` (array) | yes |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_split`

PrintCraft doc_split on a PDF. Operación PDF doc_split.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `before` (array) | no | 1-based page numbers that start a new part. |
| `bookmarks` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `every` (integer) | no |  |
| `max_mb` (number) | no |  |
| `out_dir` (string) | yes |  |

## `printcraft_bookmark_list`

PrintCraft bookmark_list on a PDF. Operación PDF bookmark_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_bookmark_add`

PrintCraft bookmark_add on a PDF. Operación PDF bookmark_add.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |
| `parent` (array) | no | Parent bookmark (omit for the top level): 1-based positions from the top level, e.g. [2, 1] = the first child of the second bookmark. |
| `position` (integer) | no | 1-based position among the parent's children (default: last). |
| `title` (string) | yes |  |

## `printcraft_bookmark_rename`

PrintCraft bookmark_rename on a PDF. Operación PDF bookmark_rename.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `path` (array) | yes | The bookmark: 1-based positions from the top level, e.g. [2, 1] = the first child of the second bookmark. |
| `title` (string) | yes |  |

## `printcraft_bookmark_delete`

PrintCraft bookmark_delete on a PDF. Operación PDF bookmark_delete.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `path` (array) | yes | The bookmark: 1-based positions from the top level, e.g. [2, 1] = the first child of the second bookmark. |

## `printcraft_bookmark_move`

PrintCraft bookmark_move on a PDF. Operación PDF bookmark_move.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `parent` (array) | no | New parent (omit for the top level): 1-based positions from the top level, e.g. [2, 1] = the first child of the second bookmark. |
| `path` (array) | yes | The bookmark to move: 1-based positions from the top level, e.g. [2, 1] = the first child of the second bookmark. |
| `position` (integer) | no | 1-based position among the new parent's children, counted after the bookmark is removed (default: last). |

## `printcraft_bookmark_set_page`

PrintCraft bookmark_set_page on a PDF. Operación PDF bookmark_set_page.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |
| `path` (array) | yes | The bookmark: 1-based positions from the top level, e.g. [2, 1] = the first child of the second bookmark. |

## `printcraft_page_number`

PrintCraft page_number on a PDF. Operación PDF page_number.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `from` (integer) | yes |  |
| `prefix` (string) | no |  |
| `start` (integer) | no | Number of the first page in the range (default 1). |
| `style` (decimal \| upper-roman \| lower-roman \| upper-alpha \| lower-alpha \| none) | no | Numbering style (default decimal; none = prefix only). |
| `to` (integer) | yes |  |

## `printcraft_form_fields`

PrintCraft form_fields on a PDF. Operación PDF form_fields.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_form_fill`

PrintCraft form_fill on a PDF. Operación PDF form_fill.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `values` (object) | yes | Field name → value. |

## `printcraft_form_reset`

PrintCraft form_reset on a PDF. Operación PDF form_reset.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `fields` (array) | no |  |

## `printcraft_form_add_field`

PrintCraft form_add_field on a PDF. Operación PDF form_add_field.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `caption` (string) | no | Button label. |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `editable` (boolean) | no | Combo box accepts typed text. |
| `export` (string) | no | Radio button export value (default Choice1). |
| `group` (string) | no | Radio group to join. |
| `multi_select` (boolean) | no |  |
| `multiline` (boolean) | no |  |
| `name` (string) | no |  |
| `options` (array) | no | Combo box / list box items. |
| `page` (integer) | yes |  |
| `rect` (array) | yes |  |
| `type` (text \| date \| checkbox \| radio \| combo \| list \| button \| image \| signature) | yes |  |

## `printcraft_stamp_custom`

PrintCraft stamp_custom on a PDF. Operación PDF stamp_custom.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `at` (array) | yes |  |
| `author` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `file_page` (integer) | no |  |
| `name` (string) | no |  |
| `page` (integer) | yes |  |
| `path` (string) | yes |  |

## `printcraft_form_set_image`

PrintCraft form_set_image on a PDF. Operación PDF form_set_image.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | yes |  |
| `path` (string) | yes |  |

## `printcraft_form_set_props`

PrintCraft form_set_props on a PDF. Operación PDF form_set_props.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `align` (left \| center \| right) | no |  |
| `appearance` (object) | no | Appearance tab: border, fill, text_color (#RRGGBB, a name or "none"), width (1 thin, 2 medium, 3 thick), style (solid, dashed, beveled, inset, underline), font (helvetica, times, courier). |
| `calculate` () | no | Calculate tab: {"op": "sum"\|"product"\|"average"\|"min"\|"max", "fields": ["a", "b"]}, {"notation": "Price * Qty"} (simplified field notation), or "none". |
| `default` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | yes | The field's current name. |
| `flags` (object) | no |  |
| `font_size` (number) | no |  |
| `format` () | no | Format tab: {"type": "number", "decimals": 2, "currency": "$", "separator": 0-4, "negative": 0-3}, {"type": "percent"}, {"type": "date"\|"time", "pattern": "mm/dd/yyyy"}, {"type": "zip"\|"zip4"\|"phone"\|"ssn"}, {"type": "mask", "mask": "AA-9999"}, or "none". |
| `locked` (boolean) | no | Lock the field's properties (only unlocking is accepted while locked). |
| `max_length` (integer) | no |  |
| `multiline` (boolean) | no |  |
| `name` (string) | no |  |
| `options` (array) | no |  |
| `read_only` (boolean) | no |  |
| `rect` (array) | no | Move/resize: points from the top-left of the displayed page. |
| `required` (boolean) | no |  |
| `tooltip` (string) | no |  |
| `validate` () | no | Validate tab: {"min": 0, "max": 100} (either may be left out) or "none". |

## `printcraft_form_tab_order`

PrintCraft form_tab_order on a PDF. Operación PDF form_tab_order.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | no |  |
| `move` (earlier \| later) | no |  |
| `order` (row \| column \| structure \| annotations) | no |  |
| `pages` (array) | no | 1-based page numbers to set (default: all). |

## `printcraft_doc_export_data`

PrintCraft doc_export_data on a PDF. Operación PDF doc_export_data.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `path` (string) | yes |  |
| `what` (all \| comments \| fields) | no |  |

## `printcraft_doc_import_data`

PrintCraft doc_import_data on a PDF. Operación PDF doc_import_data.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `path` (string) | yes |  |

## `printcraft_form_delete_field`

PrintCraft form_delete_field on a PDF. Operación PDF form_delete_field.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | yes |  |

## `printcraft_redact_mark`

PrintCraft redact_mark on a PDF. Operación PDF redact_mark.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `author` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `fill` (string) | no | #RRGGBB or a colour name. |
| `find` (string) | no |  |
| `overlay` (string) | no |  |
| `page` (integer) | no |  |
| `pages` (array) | no | 1-based page numbers to search or mark (default: all). |
| `pattern` (phone \| email \| credit-card \| ssn \| date) | no |  |
| `rect` (array) | no |  |
| `whole_pages` (boolean) | no |  |

## `printcraft_redact_apply`

PrintCraft redact_apply on a PDF. Operación PDF redact_apply.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: destructiveHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers whose marks to apply (default: all). |

## `printcraft_redact_clear`

PrintCraft redact_clear on a PDF. Operación PDF redact_clear.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_hidden_info`

PrintCraft doc_hidden_info on a PDF. Operación PDF doc_hidden_info.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_remove_hidden`

PrintCraft doc_remove_hidden on a PDF. Operación PDF doc_remove_hidden.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `categories` (array) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_printers`

PrintCraft printers on a PDF. Operación PDF printers.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_print`

PrintCraft doc_print on a PDF. Operación PDF doc_print.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `auto_rotate` (boolean) | no |  |
| `binding` (left \| right) | no |  |
| `booklet_subset` (both \| front \| back) | no |  |
| `border` (boolean) | no |  |
| `collate` (boolean) | no |  |
| `comments_forms` (document \| document-and-markups \| document-and-stamps \| form-fields-only) | no |  |
| `copies` (integer) | no |  |
| `cut_marks` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `duplex` (off \| long-edge \| short-edge) | no |  |
| `grayscale` (boolean) | no |  |
| `layout` (fit \| actual \| shrink \| custom \| multiple \| booklet \| poster) | no |  |
| `order` (horizontal \| horizontal-reversed \| vertical \| vertical-reversed) | no |  |
| `orientation` (auto \| portrait \| landscape) | no |  |
| `overlap` (number) | no |  |
| `pages` (string) | no |  |
| `paper` (string) | no |  |
| `path` (string) | no |  |
| `per_sheet` (integer) | no |  |
| `printer` (string) | no |  |
| `reverse` (boolean) | no |  |
| `scale` (number) | no |  |
| `subset` (all \| odd \| even) | no |  |

## `printcraft_content_list`

PrintCraft content_list on a PDF. Operación PDF content_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_page_add_text`

PrintCraft page_add_text on a PDF. Operación PDF page_add_text.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `align` (left \| center \| right \| justify) | no |  |
| `at` (array) | no |  |
| `bold` (boolean) | no |  |
| `color` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `font` (helvetica \| times \| courier) | no |  |
| `italic` (boolean) | no |  |
| `page` (integer) | yes |  |
| `rect` (array) | no |  |
| `size` (number) | no |  |
| `text` (string) | yes |  |
| `width` (number) | no |  |

## `printcraft_page_add_image`

PrintCraft page_add_image on a PDF. Operación PDF page_add_image.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |
| `path` (string) | yes |  |
| `rect` (array) | no |  |

## `printcraft_content_update`

PrintCraft content_update on a PDF. Operación PDF content_update.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `align` (left \| center \| right \| justify) | no |  |
| `bold` (boolean) | no |  |
| `color` (string) | no |  |
| `crop` (array) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `flip_h` (boolean) | no |  |
| `flip_v` (boolean) | no |  |
| `font` (helvetica \| times \| courier) | no |  |
| `image` (string) | no | Replace the picture with this file. |
| `index` (integer) | yes |  |
| `italic` (boolean) | no |  |
| `page` (integer) | yes |  |
| `rect` (array) | no |  |
| `rotate` (integer) | no |  |
| `size` (number) | no |  |
| `text` (string) | no |  |

## `printcraft_content_delete`

PrintCraft content_delete on a PDF. Operación PDF content_delete.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `index` (integer) | yes |  |
| `page` (integer) | yes |  |

## `printcraft_link_list`

PrintCraft link_list on a PDF. Operación PDF link_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_link_add`

PrintCraft link_add on a PDF. Operación PDF link_add.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `color` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `highlight` (none \| invert \| outline \| inset) | no |  |
| `page` (integer) | yes |  |
| `rect` (array) | yes |  |
| `to_page` (integer) | no |  |
| `url` (string) | no |  |
| `visible` (boolean) | no |  |
| `width` (number) | no |  |

## `printcraft_link_edit`

PrintCraft link_edit on a PDF. Operación PDF link_edit.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `color` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `highlight` (none \| invert \| outline \| inset) | no |  |
| `index` (integer) | yes |  |
| `page` (integer) | yes |  |
| `rect` (array) | no |  |
| `to_page` (integer) | no |  |
| `url` (string) | no |  |
| `visible` (boolean) | no |  |
| `width` (number) | no |  |

## `printcraft_link_delete`

PrintCraft link_delete on a PDF. Operación PDF link_delete.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `index` (integer) | yes |  |
| `page` (integer) | yes |  |

## `printcraft_links_from_urls`

PrintCraft links_from_urls on a PDF. Operación PDF links_from_urls.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_links_remove`

PrintCraft links_remove on a PDF. Operación PDF links_remove.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_comment_list`

PrintCraft comment_list on a PDF. Operación PDF comment_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | no | Only this page. |

## `printcraft_comment_add`

PrintCraft comment_add on a PDF. Operación PDF comment_add.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `all` (boolean) | no | Mark every match of `find` on the page, not just the first. |
| `at` (array) | no | [x, y] in points from the top-left of the displayed page. |
| `author` (string) | no |  |
| `color` (string) | no | #RRGGBB or a name: yellow, red, orange, green, blue, purple, pink, black, gray, white. |
| `contents` (string) | no | The comment text (what a text box shows). |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `dynamic` (boolean) | no |  |
| `fill` (string) | no | #RRGGBB or a name: yellow, red, orange, green, blue, purple, pink, black, gray, white. |
| `find` (string) | no | Text on the page to mark up (case-insensitive). |
| `font_size` (number) | no |  |
| `from` (array) | no | [x, y] in points from the top-left of the displayed page. |
| `icon` (string) | no | note: Comment, Note, Help, Insert, Key, NewParagraph, Paragraph; attachment: PushPin, Paperclip, Graph, Tag. |
| `knee` (array) | no | [x, y] in points from the top-left of the displayed page. |
| `opacity` (number) | no |  |
| `page` (integer) | yes |  |
| `path` (string) | no | attachment: the file to attach. |
| `points` (array) | no |  |
| `quads` (array) | no |  |
| `rect` (array) | no |  |
| `stamp` (string) | no |  |
| `strokes` (array) | no |  |
| `to` (array) | no | [x, y] in points from the top-left of the displayed page. |
| `type` (note \| highlight \| underline \| strikeout \| squiggly \| replace \| rectangle \| oval \| line \| arrow \| ink \| textbox \| stamp \| polygon \| cloud \| polyline \| callout \| caret \| attachment) | yes |  |
| `width` (number) | no | Line width in points. |

## `printcraft_comment_reply`

PrintCraft comment_reply on a PDF. Operación PDF comment_reply.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `author` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `id` (string) | no | The comment's id (from comment_list). |
| `index` (integer) | no | 1-based position on the page, as comment_list reports. |
| `page` (integer) | no | With `index`, instead of `id`. |
| `text` (string) | yes |  |

## `printcraft_comment_set_status`

PrintCraft comment_set_status on a PDF. Operación PDF comment_set_status.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `author` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `id` (string) | no | The comment's id (from comment_list). |
| `index` (integer) | no | 1-based position on the page, as comment_list reports. |
| `page` (integer) | no | With `index`, instead of `id`. |
| `status` (none \| accepted \| rejected \| cancelled \| completed) | yes |  |

## `printcraft_sign_list`

PrintCraft sign_list on a PDF. Operación PDF sign_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_sign_id_create`

PrintCraft sign_id_create on a PDF. Operación PDF sign_id_create.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `country` (string) | no | Two-letter country code. |
| `email` (string) | no |  |
| `key` (rsa2048 \| rsa3072 \| rsa4096 \| p256) | no |  |
| `name` (string) | yes |  |
| `organization` (string) | no |  |
| `password` (string) | yes |  |
| `path` (string) | yes | The .p12 file to write. |
| `unit` (string) | no |  |
| `years` (integer) | no |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_sign_keychain_ids`

PrintCraft sign_keychain_ids on a PDF. Operación PDF sign_keychain_ids.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_sign_document`

PrintCraft sign_document on a PDF. Operación PDF sign_document.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `certify` (no_changes \| form_fill \| comments) | no |  |
| `contact` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | no |  |
| `id` (string) | yes | Path of the digital ID (.p12 / .pfx). |
| `location` (string) | no |  |
| `out` (string) | yes | Where to save the signed document. |
| `page` (integer) | no |  |
| `password` (string) | no |  |
| `reason` (string) | no |  |
| `rect` (array) | no |  |

## `printcraft_sign_trust`

PrintCraft sign_trust on a PDF. Operación PDF sign_trust.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `clear` (boolean) | no |  |
| `password` (string) | no |  |
| `paths` (array) | no |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_comment_mark`

PrintCraft comment_mark on a PDF. Operación PDF comment_mark.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `author` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `id` (string) | no | The comment's id (from comment_list). |
| `index` (integer) | no | 1-based position on the page, as comment_list reports. |
| `marked` (boolean) | no | Default true. |
| `page` (integer) | no | With `index`, instead of `id`. |

## `printcraft_comment_lock`

PrintCraft comment_lock on a PDF. Operación PDF comment_lock.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `id` (string) | no | The comment's id (from comment_list). |
| `index` (integer) | no | 1-based position on the page, as comment_list reports. |
| `locked` (boolean) | no | Default true. |
| `page` (integer) | no | With `index`, instead of `id`. |

## `printcraft_comments_hide`

PrintCraft comments_hide on a PDF. Operación PDF comments_hide.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `hidden` (boolean) | no | Default true. |

## `printcraft_comments_summarize`

PrintCraft comments_summarize on a PDF. Operación PDF comments_summarize.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `open` (boolean) | no | Also open the result as a new document (default: only when out is omitted). |
| `out` (string) | no | File to write. Omit to open the result as a new unsaved document instead. |
| `sort` (page \| author \| date \| type) | no |  |

## `printcraft_comment_edit`

PrintCraft comment_edit on a PDF. Operación PDF comment_edit.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `color` (string) | no | #RRGGBB or a name: yellow, red, orange, green, blue, purple, pink, black, gray, white. |
| `contents` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `id` (string) | no | The comment's id (from comment_list). |
| `index` (integer) | no | 1-based position on the page, as comment_list reports. |
| `move` (array) | no | [x, y] in points from the top-left of the displayed page. |
| `opacity` (number) | no |  |
| `page` (integer) | no | With `index`, instead of `id`. |
| `rect` (array) | no |  |
| `width` (number) | no |  |

## `printcraft_comment_delete`

PrintCraft comment_delete on a PDF. Operación PDF comment_delete.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `id` (string) | no | The comment's id (from comment_list). |
| `index` (integer) | no | 1-based position on the page, as comment_list reports. |
| `page` (integer) | no | With `index`, instead of `id`. |

## `printcraft_doc_protect`

PrintCraft doc_protect on a PDF. Operación PDF doc_protect.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `accessibility` (boolean) | no | Allow screen readers to read the text (default true). |
| `changes` (none \| pages \| fill-sign \| comment-fill-sign \| any-except-extract) | no | Default none. |
| `compatibility` (aes-256 \| aes-128 \| rc4-128 \| rc4-40) | no | Default aes-256 (Acrobat X and later). |
| `copy` (boolean) | no | Allow copying text and images (default false). |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `encrypt_metadata` (boolean) | no | Default true. |
| `open_password` (string) | no |  |
| `permissions_password` (string) | no |  |
| `printing` (none \| low \| high) | no | Default high. |

## `printcraft_doc_unprotect`

PrintCraft doc_unprotect on a PDF. Operación PDF doc_unprotect.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_page_replace`

PrintCraft page_replace on a PDF. Operación PDF page_replace.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `from_pages` (array) | no | 1-based page numbers of the other file, in order (default: its first pages). |
| `pages` (array) | yes | 1-based page numbers to replace. |
| `path` (string) | yes | A file path (relative to --root when set). |

## `printcraft_page_duplicate`

PrintCraft page_duplicate on a PDF. Operación PDF page_duplicate.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | yes | 1-based page numbers to duplicate. |

## `printcraft_page_set_box`

PrintCraft page_set_box on a PDF. Operación PDF page_set_box.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `box` (crop \| trim \| bleed \| art \| media) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `margins` (array) | no |  |
| `pages` (array) | no | 1-based page numbers to change (default: all). |
| `rect` (array) | no |  |

## `printcraft_doc_header_footer`

PrintCraft doc_header_footer on a PDF. Operación PDF doc_header_footer.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `color` (string) | no | #RRGGBB or a colour name. |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `font_size` (number) | no |  |
| `footer_center` (string) | no |  |
| `footer_left` (string) | no |  |
| `footer_right` (string) | no |  |
| `header_center` (string) | no |  |
| `header_left` (string) | no |  |
| `header_right` (string) | no |  |
| `margins` (array) | no | Top, bottom, left, right in points (default 36, 36, 72, 72). |
| `pages` (array) | no | 1-based page numbers to mark (default: all). |
| `replace` (boolean) | no |  |
| `start_number` (integer) | no |  |

## `printcraft_doc_watermark`

PrintCraft doc_watermark on a PDF. Operación PDF doc_watermark.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `behind` (boolean) | no |  |
| `color` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `file` (string) | no |  |
| `file_page` (integer) | no |  |
| `font_size` (number) | no |  |
| `opacity` (number) | no |  |
| `pages` (array) | no | 1-based page numbers to mark (default: all). |
| `replace` (boolean) | no |  |
| `rotation` (number) | no |  |
| `scale` (number) | no |  |
| `text` (string) | no |  |

## `printcraft_doc_background`

PrintCraft doc_background on a PDF. Operación PDF doc_background.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `color` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `file` (string) | no |  |
| `file_page` (integer) | no |  |
| `opacity` (number) | no |  |
| `pages` (array) | no | 1-based page numbers to fill (default: all). |
| `replace` (boolean) | no |  |
| `scale` (number) | no |  |

## `printcraft_doc_remove_marks`

PrintCraft doc_remove_marks on a PDF. Operación PDF doc_remove_marks.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `kind` (header_footer \| watermark \| background) | yes |  |

## `printcraft_doc_export_images`

PrintCraft doc_export_images on a PDF. Operación PDF doc_export_images.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `dpi` (number) | no |  |
| `folder` (string) | yes |  |
| `format` (png \| jpeg \| tiff) | no |  |
| `pages` (array) | no | 1-based page numbers to export (default: all). |
| `quality` (integer) | no | JPEG quality (default 85). |

## `printcraft_accessibility_check`

PrintCraft accessibility_check on a PDF. Operación PDF accessibility_check.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `all` (boolean) | no | Include rules that are off by default (colour contrast). |
| `categories` (array) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers for the page rules (default: all). |
| `rules` (array) | no |  |

## `printcraft_accessibility_report`

PrintCraft accessibility_report on a PDF. Operación PDF accessibility_report.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `all` (boolean) | no |  |
| `categories` (array) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers for the page rules (default: all). |
| `path` (string) | yes |  |
| `rules` (array) | no |  |

## `printcraft_accessibility_fix`

PrintCraft accessibility_fix on a PDF. Operación PDF accessibility_fix.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `rule` (primary-language \| title \| tab-order) | yes |  |
| `value` (string) | no |  |

## `printcraft_accessibility_figures`

PrintCraft accessibility_figures on a PDF. Operación PDF accessibility_figures.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_accessibility_set_alt`

PrintCraft accessibility_set_alt on a PDF. Operación PDF accessibility_set_alt.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `alt` (string) | no |  |
| `decorative` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `figure` (integer) | yes |  |

## `printcraft_form_merge_data`

PrintCraft form_merge_data on a PDF. Operación PDF form_merge_data.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `path` (string) | yes |  |
| `paths` (array) | yes |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_js_run`

PrintCraft js_run on a PDF. Operación PDF js_run.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | no | Run as this button's Mouse Up event. |
| `script` (string) | yes |  |

## `printcraft_js_document_scripts`

PrintCraft js_document_scripts on a PDF. Operación PDF js_document_scripts.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_js_set_document_script`

PrintCraft js_set_document_script on a PDF. Operación PDF js_set_document_script.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `name` (string) | yes |  |
| `script` (string) | no |  |

## `printcraft_form_set_script`

PrintCraft form_set_script on a PDF. Operación PDF form_set_script.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `event` (keystroke \| format \| validate \| calculate \| mouse_up) | yes |  |
| `field` (string) | yes |  |
| `script` (string) | no |  |

## `printcraft_doc_export_office`

PrintCraft doc_export_office on a PDF. Operación PDF doc_export_office.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `path` (string) | yes |  |

## `printcraft_pdfa_verify`

PrintCraft pdfa_verify on a PDF. Operación PDF pdfa_verify.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `level` (2b \| 3b) | no | Default 2b. |

## `printcraft_pdfa_convert`

PrintCraft pdfa_convert on a PDF. Operación PDF pdfa_convert.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `level` (2b \| 3b) | no |  |

## `printcraft_action_list`

PrintCraft action_list on a PDF. Operación PDF action_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_action_run`

PrintCraft action_run on a PDF. Operación PDF action_run.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `action` (string) | no |  |
| `folder` (string) | yes |  |
| `paths` (array) | yes |  |
| `steps` (array) | no |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_compare`

PrintCraft doc_compare on a PDF. Operación PDF doc_compare.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `limit` (integer) | no | List at most this many changes (default 500). |
| `other` (integer) | yes | The older document (from doc_open). |
| `visual` (boolean) | no | Also compare how pages look (page n with page n): regions that differ visually. |

## `printcraft_doc_compare_report`

PrintCraft doc_compare_report on a PDF. Operación PDF doc_compare_report.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `other` (integer) | yes |  |
| `path` (string) | yes |  |

## `printcraft_doc_compare_mark`

PrintCraft doc_compare_mark on a PDF. Operación PDF doc_compare_mark.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `other` (integer) | yes |  |

## `printcraft_form_detect_fields`

PrintCraft form_detect_fields on a PDF. Operación PDF form_detect_fields.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `add` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers to look at (default: all). |

## `printcraft_form_actions`

PrintCraft form_actions on a PDF. Operación PDF form_actions.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | yes |  |

## `printcraft_form_set_actions`

PrintCraft form_set_actions on a PDF. Operación PDF form_set_actions.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `actions` (array) | yes |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `field` (string) | yes |  |

## `printcraft_js_enabled`

PrintCraft js_enabled on a PDF. Operación PDF js_enabled.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `enabled` (boolean) | no |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_ocr_recognize`

PrintCraft ocr_recognize on a PDF. Operación PDF ocr_recognize.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `dpi` (number) | no | Resolution pages are read at (default 300; a scanned page is read at most at its own resolution). |
| `language` (en) | no | Document language (default en). |
| `pages` (array) | no | 1-based page numbers to recognise (default: all). |
| `skip_text_pages` (boolean) | no | Leave pages that already contain text alone (default true). |

## `printcraft_ocr_recognize_files`

PrintCraft ocr_recognize_files on a PDF. Operación PDF ocr_recognize_files.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `dpi` (number) | no |  |
| `folder` (string) | yes |  |
| `language` (en) | no |  |
| `paths` (array) | yes |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_ocr_status`

PrintCraft ocr_status on a PDF. Operación PDF ocr_status.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_export_all_images`

PrintCraft doc_export_all_images on a PDF. Operación PDF doc_export_all_images.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `folder` (string) | yes |  |
| `min_size` (integer) | no | Skip images smaller than this many pixels on their shorter side (default 0). |
| `pages` (array) | no | 1-based page numbers whose images to export (default: all). |

## `printcraft_doc_export_text`

PrintCraft doc_export_text on a PDF. Operación PDF doc_export_text.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `pages` (array) | no | 1-based page numbers to export (default: all). |
| `path` (string) | yes |  |

## `printcraft_fill_sign_add`

PrintCraft fill_sign_add on a PDF. Operación PDF fill_sign_add.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `at` (array) | yes | [x, y] in points from the top-left of the displayed page. |
| `author` (string) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |
| `text` (string) | no |  |
| `type` (text \| check \| cross \| dot \| line \| date \| signature \| initials) | yes |  |

## `printcraft_doc_create`

PrintCraft doc_create on a PDF. Operación PDF doc_create.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `from` (blank \| images \| text) | yes |  |
| `height` (number) | no |  |
| `name` (string) | no | Name for the new document (default derived from the source). |
| `pages` (integer) | no |  |
| `path` (string) | no |  |
| `paths` (array) | no |  |
| `text` (string) | no |  |
| `width` (number) | no |  |
| `doc` (string) | no | Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_reduce`

PrintCraft doc_reduce on a PDF. Operación PDF doc_reduce.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `path` (string) | yes |  |

## `printcraft_doc_initial_view`

PrintCraft doc_initial_view on a PDF. Operación PDF doc_initial_view.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `binding` (left \| right) | no |  |
| `center_window` (boolean) | no |  |
| `display_title` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `fit_window` (boolean) | no |  |
| `full_screen` (boolean) | no |  |
| `hide_menubar` (boolean) | no |  |
| `hide_toolbar` (boolean) | no |  |
| `hide_window_ui` (boolean) | no |  |
| `language` (string) | no |  |
| `layout` (default \| single \| continuous \| two_up \| two_up_continuous \| two_up_cover \| two_up_continuous_cover) | no |  |
| `magnification` () | no |  |
| `navigation` (page \| bookmarks \| pages \| attachments \| layers) | no |  |
| `page` (integer) | no |  |

## `printcraft_doc_audit_space`

PrintCraft doc_audit_space on a PDF. Operación PDF doc_audit_space.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_text_lines`

PrintCraft text_lines on a PDF. Operación PDF text_lines.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |

## `printcraft_page_images`

PrintCraft page_images on a PDF. Operación PDF page_images.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |

## `printcraft_image_edit`

PrintCraft image_edit on a PDF. Operación PDF image_edit.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `action` (move \| rotate \| flip_horizontal \| flip_vertical \| replace \| delete) | yes |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `image` (integer) | yes |  |
| `page` (integer) | yes |  |
| `path` (string) | no |  |
| `quarters` (integer) | no |  |
| `rect` (array) | no |  |

## `printcraft_image_save`

PrintCraft image_save on a PDF. Operación PDF image_save.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `image` (integer) | yes |  |
| `page` (integer) | yes |  |
| `path` (string) | yes |  |

## `printcraft_text_paragraphs`

PrintCraft text_paragraphs on a PDF. Operación PDF text_paragraphs.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `page` (integer) | yes |  |

## `printcraft_text_edit`

PrintCraft text_edit on a PDF. Operación PDF text_edit.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `align` (left \| center \| right \| justify) | no |  |
| `bold` (boolean) | no |  |
| `char_spacing` (number) | no | Points. |
| `color` (string) | no | #rrggbb |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `dx` (number) | no | Paragraph only: move right by this many points (negative: left). |
| `dy` (number) | no | Paragraph only: move up by this many points (negative: down). |
| `font` (helvetica \| times \| courier) | no |  |
| `italic` (boolean) | no |  |
| `line` (integer) | no |  |
| `line_spacing` (number) | no | Multiple of the font size (1.2 is ordinary). |
| `page` (integer) | yes |  |
| `paragraph` (integer) | no |  |
| `scale` (number) | no | Horizontal scale in percent. |
| `size` (number) | no |  |
| `text` (string) | no |  |
| `underline` (boolean) | no |  |
| `width` (number) | no | Paragraph only: rewrap to this width in points (dragging the box's edge). |

## `printcraft_doc_revisions`

PrintCraft doc_revisions on a PDF. Operación PDF doc_revisions.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_doc_open_revision`

PrintCraft doc_open_revision on a PDF. Operación PDF doc_open_revision.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `revision` (integer) | yes |  |

## `printcraft_doc_optimize`

PrintCraft doc_optimize on a PDF. Operación PDF doc_optimize.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `color` (object) | no |  |
| `discard` (array) | no |  |
| `discard_alternate_images` (boolean) | no |  |
| `discard_print_settings` (boolean) | no |  |
| `discard_tags` (boolean) | no |  |
| `discard_thumbnails` (boolean) | no |  |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `flate_unencoded` (boolean) | no |  |
| `gray` (object) | no |  |
| `path` (string) | yes |  |
| `remove_invalid_links` (boolean) | no |  |
| `remove_unreferenced_dests` (boolean) | no |  |

## `printcraft_doc_flatten`

PrintCraft doc_flatten on a PDF. Operación PDF doc_flatten.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: destructiveHint.

| Argument | Required | Description |
|---|---|---|
| `comments` (boolean) | no | Default true. |
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |
| `fields` (boolean) | no | Default true. |

## `printcraft_edit_undo`

PrintCraft edit_undo on a PDF. Operación PDF edit_undo.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_edit_redo`

PrintCraft edit_redo on a PDF. Operación PDF edit_redo.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_command_list`

PrintCraft command_list on a PDF. Operación PDF command_list.

PrintCraft 0.2.1 CLI operation. Pass doc as a Kafka document id (d_…) or a prior revision (pc_…). File inputs are sandboxed; saved PDFs are new workshop outputs with receipts. Existing text editing may not preserve every font or script; verify the exported PDF.
Sinónimos: PrintCraft, PDF editable, taller de PDF

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no | Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…). |

## `printcraft_workflow`

Edit a filed PDF in one saved workflow. Editar párrafos y páginas de un PDF.

Use an existing Kafka PDF id (d_…) or PrintCraft revision (pc_…). actions is an ordered list of {tool, arguments}; each tool is one of the printcraft_* tools. The source is copied, edits are saved as a new PDF, and an artifact receipt links it to the document. Original files are preserved.
Sinónimos: editar texto de un PDF, cambiar párrafo, insertar páginas, girar páginas, componer documento

Annotations: none.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | yes |  |
| `actions` (array) | yes |  |
| `confirm` (boolean) | no | Required for destructive actions. |

## `printcraft_status`

PrintCraft CLI availability and optional live 123-tool discovery. Estado y descubrimiento PrintCraft.

Set printcraft.cli in Settings or KAFKA_PRINTCRAFT_CLI; otherwise Kafka searches PATH and its portable bundle folder.
Sinónimos: motor PDF editable local

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `discover` (boolean) | no |  |

## `printcraft_artifacts`

List PDF revisions and receipt hashes linked to a Kafka document. Ver revisiones PrintCraft.

Pass doc=d_… or pc_…; omit doc to list the 100 newest receipts.
Sinónimos: revisiones de PDF, recibos de edición

Annotations: readOnlyHint, idempotentHint.

| Argument | Required | Description |
|---|---|---|
| `doc` (string) | no |  |

## `printcraft_catalog`

Inspect all 123 PrintCraft tool schemas. Consultar catálogo PrintCraft.

Returns the pinned JSON schemas used by API, UI and MCP dispatch.
Sinónimos: descubrir herramientas PDF, schemas PrintCraft

Annotations: readOnlyHint, idempotentHint.

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
- `GET /api/workshop/status` — workshop folder and installed helpers; includes PrintCraft CLI availability and schema release.
- `POST /api/ui/call` `{name, arguments}` — any tool above, uncapped and unmasked (the UI is local).
