# Kafka's Hoard

A local paperwork keeper. It reads invoices, contracts, insurance policies, receipts, warranties, letters from the tax office, the traffic authority or the town hall, fines, ITV reports and ID documents; finds who issued each one, the amounts and every date that matters; turns those dates into deadlines (counting Spanish working days where the law does); reminds you before each one; and answers questions with a citation to the document and page.

Part of the Hoard family of local apps: it runs on your PC, keeps its data in `data/`, works on its own in the browser and can be driven by an assistant over MCP.

[Versión en español](README.es.md)

## What it does

- **Reads what you give it.** Drop files on the page, put them in a watched folder (by default `data/inbox`), or let it read the mail account configured in Faustus: PDF and image attachments are filed, the password never leaves Faustus. Supported: PDF (text layer, with OCR for scanned pages when the optional OCR package is installed), images (PNG, JPEG, WebP, HEIC, TIFF, BMP, GIF), `.eml`, `.docx`, `.pptx` (one page per slide, in the order the deck shows them, with speaker notes), HTML and plain text. Each file is stored once by its SHA-256; the original is never modified.
- **Understands the document.** Rules (no model needed) classify it (invoice, receipt, bill, contract, insurance, warranty, tax, official notice, fine, vehicle/ITV, identity, subscription, payslip, bank, instruction manual), name the issuer, pick the reference (policy, contract, invoice, expedient, fine bulletin), the amount and the billing period, and find every date with its role: issued, due, renewal, effect from/to, purchase, delivery, expiry, notified, next ITV, end of commitment. Every fact keeps its evidence: the quote and the page.
- **Makes deadlines, with the reason.** Payment due dates, renewals, the last day to cancel a renewal (one month before for the policyholder, Ley 50/1980 art. 22), end of the 3-year legal warranty (Real Decreto Legislativo 7/2021), end of a commitment period, the 20 natural days to pay a traffic fine with 50 % off or to make allegations (dgt.es), deadlines to respond to the administration counted in working days (Ley 39/2015 art. 30), ID expiry, next ITV. «Why this date?» shows the rule, the base date, the days counted and the quote. See [docs/RULES.md](docs/RULES.md).
- **Reminds you.** Per deadline kind there are lead days you can change (a renewal: 45, 30 and 7 days before; a fine discount: 5, 2 and the last day). One notification per deadline per run, overdue ones once, nothing at night (23:00–07:00 by default; urgent ones can still go through). Channels: Windows toast (no console window; attributed to Kafka's Hoard), the Hoard family bus, ntfy, Telegram and email (sent with the Faustus account). Each notification is sent once.
- **Follows recurring papers.** Bills, insurance and subscriptions from the same issuer and reference form a series. A price change of 5 % or more (configurable) raises a notification; the history is shown per series. Recurring deadlines (monthly, yearly) roll to the next occurrence when you mark them done.
- **Answers with citations.** Full-text search (SQLite FTS5, accent-insensitive) across every page returns snippets with document and page, optionally restricted to a list of documents (`doc_ids`, e.g. the manuals of one appliance); `warranty_check` tells whether a purchase is still under warranty and until when.
- **Keeps manuals.** A document filed with kind `manual` (instruction manuals) is stored and searchable like the rest and never creates a deadline. Manuals are also recognised by their wording («manual de instrucciones», «instrucciones de uso»).
- **Keeps deadlines for other apps.** Another app of the family (HomeHoard, for maintenance) sends its deadlines with `deadline_add` and a `source` + `external_key`: sending again updates the same deadline instead of duplicating it, a new date reopens it, and the basis and rule it sends are shown by «why this date?». What you change here wins: an edited title, reminders or date stay until the app sends a new occurrence, and a deadline you dismiss stays dismissed. `deadline_update_by_key` lets the app close or reschedule it. Reminders go out through the same channels and link back to the app.
- **Talks to the family hub.** Notifications go out through the hub (`POST /api/notify`, with priority, grouping and a dedupe key); when the hub is not there or fails, the usual own channels are used (setting `notify.via`: automatic, hub only or own only). Paperwork mail is read through the hub's mail gateway (setting `mail.source`: automatic, hub or the Faustus helper) with its own watermark, and every archived mail is claimed as a document (`hoard://kafka/document/<id>`). Deadlines show in the family agenda (`GET /api/family/agenda`: renewals, maintenance, follow-ups from other apps and deadlines, prioritised by how close and how serious).
- **Events.** `kafka.document.archived` (with `order_ref` when the document carries an order number), `kafka.warranty.created`, `kafka.deadline.created`, `kafka.deadline.soon` and `kafka.deadline.overdue` (once per deadline and state).
- **Links invoices to Ledger.** When an invoice or receipt with an amount and a date is filed, it asks Ledger (`tx_find`) and, if exactly one movement scores 0.8 or more, links it (`tx_attach_doc` plus the reference `hoard://ledger/tx/<id>`); Detalle shows it as a chip and, when no candidate is clear, lets you pick one (`document_link_tx`). It retries once a day for documents of the last 45 days (setting `links.ledger`).
- **Minutes to deadlines and the tax return folder.** `deadlines_from_minutes` turns the dated action items of a Funes meeting minutes that are yours or have no owner into deadlines (your name in `minutes.me`); `tax_pack` prepares a new folder for the calendar year with the paperwork usually needed, an index, a CSV, Ledger's year summary and the list of missing certificates (Taller, at the bottom).
- **Quiet history.** The first mail scan and old files are filed without notifications and their past deadlines are created as done, so you only hear about what is still ahead.
- **Works on the files themselves (Taller).** Merge PDFs (with the pages you pick from each), split them (one file per page, by ranges such as `1-3,4-6`, or every N pages), extract, delete, rotate or reorder pages, compress (Ghostscript when it is installed, otherwise a built-in compressor that recompresses the embedded images; optionally down to a target size in MB), set or remove a password (AES-256), stamp a diagonal text watermark, read or edit title, author, subject and keywords, turn photos into a PDF (A4, Letter or fit, EXIF orientation honoured), turn Word, ODT and RTF into PDF (Microsoft Word on Windows, otherwise LibreOffice), render PDF pages as PNG or JPG, and shrink PNG, JPEG and WEBP images under a size limit, one file or a whole folder. Every result is a new file: nothing is overwritten (a taken name becomes «name (2)») and the originals are not touched. Passwords are used in memory only and never logged, returned or stored.
- **Private to the assistant.** Spanish ID numbers, IBAN, card and phone numbers are masked in what MCP and the agent route return unless you explicitly ask for that number. The local UI shows everything.

## Screens

Spanish by default, English with one click, dark.

- **Plazos** — overdue, this week, next 30 days and later; mark done, snooze, edit, dismiss, add by hand; news since the last visit; documents waiting for a decision.
- **Documentos** — upload by dragging files anywhere, list with filters by kind and state, full-text search with snippets.
- **Detalle** — page preview with the original file, extracted facts with their evidence, deadlines with «why this date?», series and price history, the linked Ledger movement for invoices and receipts, edit any field (your edit always wins on re-reading).
- **Revisión** — documents the rules were not sure about and mails that may be paperwork: accept, change, ignore.
- **Taller** — a grid of the operations above; each one has a drop zone or a path field, its options, a result card with sizes before and after, a download button and «File in Kafka» for PDF results. Detalle has an «Open in the workshop» button for PDF documents. At the bottom, the tax return folder card (year, optional ZIP).
- **Ajustes** — workshop folder, watched folders, mail (hub or Faustus), calendar region and extra holidays, reminder lead days, notification route (hub or own) and channels with a test button, OCR and model status, language, recent activity.

## Run it

Requirements: Python 3.11+ (tested on 3.11 and 3.13), Node 22 only to rebuild the UI, Faustus with a mail account for automatic mail reading.

```bash
python -m venv venv
venv/Scripts/python -m pip install -r requirements.txt      # Windows; venv/bin/python elsewhere
venv/Scripts/python -m kafka_hoard                            # http://127.0.0.1:5200
```

The UI is prebuilt in `kafka_hoard/static`. To rebuild it: `npm install && npx vite build`. `python scripts/launch.py` starts the app on a free port and opens the browser.

OCR is optional: `pip install ".[ocr]"` (rapidocr + onnxruntime). Without it, scanned pages and photos are stored and flagged as needing review, and everything else keeps working.

Environment: `KAFKA_PORT` (5200), `KAFKA_DATA_DIR`, `PORT_STRICT=1`, `KAFKA_SCHEDULER=0` (no background work), `KAFKA_OFFLINE=1` (no network), `KAFKA_OCR=0`, `KAFKA_ALLOWED_HOSTS`, `KAFKA_HTTP_TIMEOUT_S`. Secrets (`KAFKA_TELEGRAM_TOKEN`, `KAFKA_TELEGRAM_CHAT_ID`, `KAFKA_NTFY_TOPIC`, `KAFKA_SMTP_*`, `KAFKA_FAUSTUS_DIR`) can go in `.env` (see `.env.example`) or in Settings. Everything else is a setting stored in `data/kafka.db`.

## Faustus assistant

Once the shared Hoard Link launcher is installed, the app shows the family's Faustus panel (a floating button). The conversation, the model and the tools live in Faustus; this app only tells the panel what is open.

- **Context per view.** A document open in Documents (`#/documentos/<id>`) or in the workshop (`#/taller?doc=<id>`) is sent as kind `document` (title, document kind, issuer, reference, amount, currency, issue date, state) plus every associated deadline as kind `deadline` (date, state, recurrence, days left). A deadline being edited is added with its real id. The view reports the page (`deadlines`, `documents`, `review`, `workshop`, `settings`). Creating (`new`) gives no object. Document text is never sent, only these summary fields.
- **Unsaved fields.** The panel asks for them with the `kafka:context-request` window event. The forms `deadline-form`, `deadline-new` (no id yet) and `document-form` answer with their typed fields and a `dirty` flag.
- **Refresh.** When the assistant finishes a turn the open view reloads in place (`hoard:copilot-turn-end` / `window.hoardCopilot.onTurnEnd`, debounced into one reload). Open forms keep what you typed; untouched fields follow the new data.
- **Saving.** Forms send only the changed fields to `doc_update` and `deadline_update` (including `remind`), which merge just those fields; an explicit empty value clears one. No new tool arguments.
- **Tests.** `node --test client/copilot-context.test.mjs` and `python -m pytest tests/test_partial_updates.py`.

## Assistants (MCP)

`mcp_server.py` is a stdio MCP bridge named `kafka-hoard`. It never opens the database: it proxies every call to the running app with the token in `data/mcp-token`, and starts the app when it is not answering. `faustus-plugin.json` describes the app, its health check and the bridge for Faustus and the Hoard Hub.

Tools (60): `kafka_overview`, `kafka_status`, `deadlines_list`, `deadline_get`, `deadline_add`, `deadlines_from_minutes`, `deadline_update_by_key`, `deadline_update`, `deadline_explain`, `deadline_delete`, `docs_list`, `doc_get`, `doc_compare`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `document_link_tx`, `extract_preview`, `warranty_check`, `series_list`, `price_history`, `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan`, `phileas_sync`, `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run`, `pdf_merge`, `pdf_split`, `pdf_pages`, `pdf_compress`, `pdf_protect`, `pdf_watermark`, `pdf_info`, `pdf_metadata_set`, `pdf_from_images`, `pdf_from_office`, `pdf_to_images`, `images_compress`, `tax_pack`, `doc_extract`, `ocr_image`, `ocr_pdf`, `ocr_status`. Arguments in [docs/API.md](docs/API.md).

Events on the family bus: `kafka.document.added`, `kafka.price.change`, `kafka.deadline.soon` and `kafka.deadline.overdue`. `phileas_sync` asks Phileas's Hoard (through the family link) for delivered purchases and creates their warranty deadlines.

### Agent sessions, reasons and undo

Every write an assistant makes through the MCP bridge (or `POST /api/agent/call`) is accountable. The web interface is not an agent and is exempt from all of this.

- **Who.** The bridge sends the agent name and session it reads from `HOARD_AGENT_ID` / `HOARD_AGENT_SESSION` (headers `X-Agent-Id` / `X-Agent-Session`; a body `caller` / `_session` also works). A scoped token fixes the agent name whatever the headers say.
- **Why.** Every tool that is not read-only needs a `reason` argument of 3 to 300 characters (`400 reason_required` with a hint otherwise). Reading tools need none.
- **What.** Each write is a line of `data/agent_journal.jsonl` (tool, agent, session, reason, a digest and a masked summary of the arguments, the ids it returned, time, ok or error, and what was needed to take it back); the file rotates at about 5 MB. `GET /api/agent/journal?session=&agent=&limit=` (Bearer token) reads it, and the event `agent.write` goes to the family. Secrets are never copied into it: `secret_set` journals `***` instead of the value.
- **Undo a whole session.** `POST /api/agent/undo {"session": "...", "dry_run": true}` says what would be taken back; with `{"confirm": true, "reason": "..."}` it does it, newest write first.
  - Undoable (13 tools): `doc_add_file` and `doc_add_text` (the filed document, its deadlines and the stored original go; a kind or item applied to a document that was already filed is put back), `doc_update` and `doc_reprocess` (the document's fields and every deadline the correction recomputed come back exactly as they were), `deadline_add` (a new deadline is deleted; one that updated a deadline another app keeps goes back to its previous version), `deadline_update`, `deadline_update_by_key`, `deadline_delete` (the deadline returns with the same id), `deadlines_from_minutes` (the deadlines it added), `mail_ignore`, `folder_add`, `folder_remove` (with the files it had already seen) and `settings_set`.
  - Not undoable, and reported as such: `doc_delete` (the stored original goes with the document and Kafka keeps no trash), `mail_accept`, `mail_scan`, `folder_scan` and `phileas_sync` (they ingest from outside), `document_link_tx` (it writes to Ledger), `tax_pack`, every `pdf_*` tool, `images_compress` and the PrintCraft tools that write (they only create new files, never touch an original; deleting them is for the person), `notify_test`, `telegram_find_chat_id`, `secret_set` and `housekeeping_run`. Notifications already sent and links already made in other apps are not taken back.
  - It never touches what someone else did: if another session, or the person in the interface, changed the same document or deadline afterwards (a document counts together with its deadlines), that write is reported as a conflict and left as it is. A reminder that fires by itself in between is not a change.
- **Profiles.** `python -m hoard_link.tokens mint --app-data-dir data --agent drafter --profile drafts` prints a token (shown once; the file `data/agent_tokens.json` keeps only hashes). Profiles: `read_only` (reading tools), `drafts` (also the tools that create or edit and never delete, send or read from outside: `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `deadline_add`, `deadline_update`, `deadline_update_by_key`, `deadlines_from_minutes`, `mail_ignore` and the workshop tools that write new files: `pdf_merge`, `pdf_split`, `pdf_pages`, `pdf_compress`, `pdf_watermark`, `pdf_metadata_set`, `pdf_from_images`, `pdf_from_office`, `pdf_to_images`, `images_compress`) and `all`. A blocked call answers `403 profile_forbidden`; only the `all` profile can undo. The Hub tab "Sesiones de agente" shows the sessions of every app and undoes them, and mints and revokes tokens.

## Document reading for the family

Kafka owns the family's document reading and OCR (contract: `HoardLink/docs/commons/services.md`, section 5). Other apps call these through the hub and pass paths, never bytes:

- `doc_extract(path, mode?, max_pages?, lang?, wait_s?)`: text per page for PDF, images, `.docx`, `.eml`, HTML, text, spreadsheets and archives, with OCR on the pages that have no text layer. Returns `{kind, title, text, units, needs_ocr, notes, mime, pages_ocr}` or a job view when it takes longer than `wait_s`.
- `ocr_image(path, lang?, wait_s?)` and `ocr_pdf(path, pages?, dpi?, lang?, mode?, wait_s?)`: text with blocks (box and confidence) per page; `ocr_pdf` OCRs the chosen pages even when they have a text layer.
- `ocr_status(job_id?, wait_s?)`: the engine report (which of `winocr`, `rapidocr`, `light-ocr`, `tesseract` is installed and chosen) or the view of a job; `wait_s` is clamped to 150 s.

Spanish is the default language. `KAFKA_OCR_BACKEND` (`auto`, `winocr`, `rapidocr`, `light-ocr`, `tesseract`) forces an engine and `KAFKA_OCR_LANG` changes the default language. Files over 200 MB are refused.

The optional native `light-ocr` engine is installed with `npm install --include=optional` in this folder and needs Node.js 22 or newer. It uses the CPU and reads images locally, with boxes and confidence per line; it does not download models while reading. Select it with `KAFKA_OCR_BACKEND=light-ocr`. Automatic selection keeps winocr and rapidocr ahead of it, followed by tesseract. If it is absent or cannot load, the next available engine is tried. Deadline rules and evidence stay the same.

The family extraction service preserves repeated headings and table rows on
each PDF page, including rows whose amounts differ. For reproducible OCR
comparisons on invented scans, see [the OCR benchmark](docs/OCR_BENCHMARK.md).

## The workshop

The twelve workshop tools (`pdf_*` and `images_compress`) accept an absolute path or the id of a filed document (`d_…`) and return the paths, page counts and sizes before and after of what they wrote. Where the result goes:

- A file you uploaded in the Taller page: `data/workshop/out/<job>/`. Uploads live in `data/workshop/in/<job>/`; job folders older than 7 days are deleted by the hourly housekeeping.
- A filed document given by id: the folder in the `workshop.dir` setting (default `Documents\Kafka's Hoard\Taller`).
- A path: next to the source, with a suffix (`_unido`, `_paginas`, `_rotado`, `_comprimido`, `_protegido`, `_sin_clave`, `_marca`, `_dividido`, `_imagenes`, `_comprimida`). `output` or `out_dir` choose another place. System, hidden and Kafka's own data folders are refused.

`file_result: true` also files each resulting PDF in Kafka and returns its `doc_ids`. REST for the page: `POST /api/workshop/upload` (multipart, field `files`, optional `job`), `GET /api/workshop/file?path=` (only files the workshop produced or that live under `data/workshop/`, same-origin, download headers) and `GET /api/workshop/status` (which of Ghostscript, Word and LibreOffice were found).

Kafka includes its own PDF engine, built from incorporated source in `native/pdf`, with 123 `pdf_*` operations and the historical `printcraft_*` aliases in the same API/MCP catalogue (314 tools in total). Build it with `python scripts/build_pdf_engine.py`; the normal product uses only `native/bin/kafka-pdf-worker`, never a configured external editor. Historical `printcraft.cli` values and existing `pc_…` receipts remain data and do not select an executable. `pdf_engine_status` compares the actual worker catalogue with its schemas. `pdf_workflow` edits a filed PDF or previous revision, preserving the original and creating a new receipt; `pdf_artifacts` lists its history. Paragraphs, page edits, forms, annotations, signatures, export and the other catalogue operations share the owned engine. Rendering creates a real PNG artifact. Existing PDF text editing still depends on fonts and source structure; inspect exports for unsupported scripts. [Source provenance and build](native/pdf/README.md) describe the incorporated revision, local dependencies and retained notices.

## How it is built

FastAPI + SQLite (WAL, FTS5) + a two-lane scheduler (the family's `LaneScheduler`, last runs kept in `data/scheduler.json`); React 19 + Vite + Tailwind UI; extraction in `kafka_hoard/extract/` (pure functions); working days in `kafka_hoard/bizdays.py`; the work in `kafka_hoard/engine.py`. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Tests: `python -m pytest -q` (no network, documents and PDFs are generated by the tests with invented data).

## Limits

- The workshop never edits a file in place and does not read scanned pages (reading scans is `doc_extract` and `ocr_pdf`, not the workshop). Compression without Ghostscript only reduces images, so a PDF made of text and vector drawings barely shrinks; encrypted inputs need their password; HEIC photos need the optional `pillow-heif`; Word and LibreOffice conversion needs one of them installed; a watermark is plain Latin text in Helvetica.
- Dates are read with rules for Spanish and English text. An unusual layout may land in the review list instead of becoming a deadline.
- Without the optional OCR package, scanned documents are stored but their text is not read.
- Working-day calendars cover national holidays and the regions Madrid, Catalonia, Andalusia, Valencia, Galicia and the Basque Country; add other local holidays in Settings.
- The optional model pass (through the Hoard link) only proposes facts; anything whose date is not written in the document is discarded. Deadlines from rules are not legal advice: check the original notice for anything important.

## License

MIT

## Compare archived versions

`doc_compare(before, after)` reports changed text with citations to both documents and physical pages. It aligns lines across page breaks, keeps repeated rows and numbers, and ignores only empty lines and spacing inside a line. Use page selectors and `offset`/`next_offset` for focused, compact results. Source extraction warnings and empty-text pages are shown; equality refers to the selected archived text, not the original files. Originals, user edits and deadlines remain unchanged. [Reference and limits](docs/DOCUMENT_COMPARE.md).
