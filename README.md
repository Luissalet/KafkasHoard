# Kafka's Hoard

A local paperwork keeper. It reads invoices, contracts, insurance policies, receipts, warranties, letters from the tax office, the traffic authority or the town hall, fines, ITV reports and ID documents; finds who issued each one, the amounts and every date that matters; turns those dates into deadlines (counting Spanish working days where the law does); reminds you before each one; and answers questions with a citation to the document and page.

Part of the Hoard family of local apps: it runs on your PC, keeps its data in `data/`, works on its own in the browser and can be driven by an assistant over MCP.

[Versión en español](README.es.md)

## What it does

- **Reads what you give it.** Drop files on the page, put them in a watched folder (by default `data/inbox`), or let it read the mail account configured in Faustus: PDF and image attachments are filed, the password never leaves Faustus. Supported: PDF (text layer, with OCR for scanned pages when the optional OCR package is installed), images (PNG, JPEG, WebP, HEIC, TIFF, BMP, GIF), `.eml`, `.docx`, HTML and plain text. Each file is stored once by its SHA-256; the original is never modified.
- **Understands the document.** Rules (no model needed) classify it (invoice, receipt, bill, contract, insurance, warranty, tax, official notice, fine, vehicle/ITV, identity, subscription, payslip, bank), name the issuer, pick the reference (policy, contract, invoice, expedient, fine bulletin), the amount and the billing period, and find every date with its role: issued, due, renewal, effect from/to, purchase, delivery, expiry, notified, next ITV, end of commitment. Every fact keeps its evidence: the quote and the page.
- **Makes deadlines, with the reason.** Payment due dates, renewals, the last day to cancel a renewal (one month before for the policyholder, Ley 50/1980 art. 22), end of the 3-year legal warranty (Real Decreto Legislativo 7/2021), end of a commitment period, the 20 natural days to pay a traffic fine with 50 % off or to make allegations (dgt.es), deadlines to respond to the administration counted in working days (Ley 39/2015 art. 30), ID expiry, next ITV. «Why this date?» shows the rule, the base date, the days counted and the quote. See [docs/RULES.md](docs/RULES.md).
- **Reminds you.** Per deadline kind there are lead days you can change (a renewal: 45, 30 and 7 days before; a fine discount: 5, 2 and the last day). One notification per deadline per run, overdue ones once, nothing at night (23:00–07:00 by default; urgent ones can still go through). Channels: Windows toast, the Hoard family bus, ntfy, Telegram and email (sent with the Faustus account). Each notification is sent once.
- **Follows recurring papers.** Bills, insurance and subscriptions from the same issuer and reference form a series. A price change of 5 % or more (configurable) raises a notification; the history is shown per series. Recurring deadlines (monthly, yearly) roll to the next occurrence when you mark them done.
- **Answers with citations.** Full-text search (SQLite FTS5, accent-insensitive) across every page returns snippets with document and page; `warranty_check` tells whether a purchase is still under warranty and until when.
- **Quiet history.** The first mail scan and old files are filed without notifications and their past deadlines are created as done, so you only hear about what is still ahead.
- **Works on the files themselves (Taller).** Merge PDFs (with the pages you pick from each), split them (one file per page, by ranges such as `1-3,4-6`, or every N pages), extract, delete, rotate or reorder pages, compress (Ghostscript when it is installed, otherwise a built-in compressor that recompresses the embedded images; optionally down to a target size in MB), set or remove a password (AES-256), stamp a diagonal text watermark, read or edit title, author, subject and keywords, turn photos into a PDF (A4, Letter or fit, EXIF orientation honoured), turn Word, ODT and RTF into PDF (Microsoft Word on Windows, otherwise LibreOffice), render PDF pages as PNG or JPG, and shrink PNG, JPEG and WEBP images under a size limit, one file or a whole folder. Every result is a new file: nothing is overwritten (a taken name becomes «name (2)») and the originals are not touched. Passwords are used in memory only and never logged, returned or stored.
- **Private to the assistant.** Spanish ID numbers, IBAN, card and phone numbers are masked in what MCP and the agent route return unless you explicitly ask for that number. The local UI shows everything.

## Screens

Spanish by default, English with one click, dark.

- **Plazos** — overdue, this week, next 30 days and later; mark done, snooze, edit, dismiss, add by hand; news since the last visit; documents waiting for a decision.
- **Documentos** — upload by dragging files anywhere, list with filters by kind and state, full-text search with snippets.
- **Detalle** — page preview with the original file, extracted facts with their evidence, deadlines with «why this date?», series and price history, edit any field (your edit always wins on re-reading).
- **Revisión** — documents the rules were not sure about and mails that may be paperwork: accept, change, ignore.
- **Taller** — a grid of the operations above; each one has a drop zone or a path field, its options, a result card with sizes before and after, a download button and «File in Kafka» for PDF results. Detalle has an «Open in the workshop» button for PDF documents.
- **Ajustes** — workshop folder, watched folders, mail, calendar region and extra holidays, reminder lead days, notification channels with a test button, OCR and model status, language, recent activity.

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

## Assistants (MCP)

`mcp_server.py` is a stdio MCP bridge named `kafka-hoard`. It never opens the database: it proxies every call to the running app with the token in `data/mcp-token`, and starts the app when it is not answering. `faustus-plugin.json` describes the app, its health check and the bridge for Faustus and the Hoard Hub.

Tools (51): `kafka_overview`, `kafka_status`, `deadlines_list`, `deadline_get`, `deadline_add`, `deadline_update`, `deadline_explain`, `deadline_delete`, `docs_list`, `doc_get`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `extract_preview`, `warranty_check`, `series_list`, `price_history`, `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan`, `phileas_sync`, `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run`, `pdf_merge`, `pdf_split`, `pdf_pages`, `pdf_compress`, `pdf_protect`, `pdf_watermark`, `pdf_info`, `pdf_metadata_set`, `pdf_from_images`, `pdf_from_office`, `pdf_to_images`, `images_compress`. Arguments in [docs/API.md](docs/API.md).

Events on the family bus: `kafka.document.added`, `kafka.price.change`, `kafka.deadline.soon` and `kafka.deadline.overdue`. `phileas_sync` asks Phileas's Hoard (through the family link) for delivered purchases and creates their warranty deadlines.

## The workshop

The twelve workshop tools (`pdf_*` and `images_compress`) accept an absolute path or the id of a filed document (`d_…`) and return the paths, page counts and sizes before and after of what they wrote. Where the result goes:

- A file you uploaded in the Taller page: `data/workshop/out/<job>/`. Uploads live in `data/workshop/in/<job>/`; job folders older than 7 days are deleted by the hourly housekeeping.
- A filed document given by id: the folder in the `workshop.dir` setting (default `Documents\Kafka's Hoard\Taller`).
- A path: next to the source, with a suffix (`_unido`, `_paginas`, `_rotado`, `_comprimido`, `_protegido`, `_sin_clave`, `_marca`, `_dividido`, `_imagenes`, `_comprimida`). `output` or `out_dir` choose another place. System, hidden and Kafka's own data folders are refused.

`file_result: true` also files each resulting PDF in Kafka and returns its `doc_ids`. REST for the page: `POST /api/workshop/upload` (multipart, field `files`, optional `job`), `GET /api/workshop/file?path=` (only files the workshop produced or that live under `data/workshop/`, same-origin, download headers) and `GET /api/workshop/status` (which of Ghostscript, Word and LibreOffice were found).

## How it is built

FastAPI + SQLite (WAL, FTS5) + a two-lane scheduler; React 19 + Vite + Tailwind UI; extraction in `kafka_hoard/extract/` (pure functions); working days in `kafka_hoard/bizdays.py`; the work in `kafka_hoard/engine.py`. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Tests: `python -m pytest -q` (no network, documents and PDFs are generated by the tests with invented data).

## Limits

- The workshop never edits a file in place and does not read scanned pages (no OCR in the workshop). Compression without Ghostscript only reduces images, so a PDF made of text and vector drawings barely shrinks; encrypted inputs need their password; HEIC photos need the optional `pillow-heif`; Word and LibreOffice conversion needs one of them installed; a watermark is plain Latin text in Helvetica.
- Dates are read with rules for Spanish and English text. An unusual layout may land in the review list instead of becoming a deadline.
- Without the optional OCR package, scanned documents are stored but their text is not read.
- Working-day calendars cover national holidays and the regions Madrid, Catalonia, Andalusia, Valencia, Galicia and the Basque Country; add other local holidays in Settings.
- The optional model pass (through the Hoard link) only proposes facts; anything whose date is not written in the document is discarded. Deadlines from rules are not legal advice: check the original notice for anything important.

## License

MIT
