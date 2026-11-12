# Kafka's Hoard

A local paperwork keeper: it reads your invoices, contracts, insurance policies, receipts and official letters, extracts the amounts and dates that matter, turns them into deadlines and reminds you before each one.

[Versión en español](README.es.md)

## What it is

Kafka's Hoard reads invoices, contracts, insurance policies, receipts, warranties, letters from the tax office, the traffic authority or the town hall, fines, ITV reports and ID documents. It finds who issued each one, the amounts and every date with its role, turns those dates into deadlines (counting Spanish working days where the law does), reminds you before each one and answers questions with a citation to the document and page.

It is one app of the Hoard family. It runs on your PC, keeps its data in `data/`, works on its own in the browser and can be driven by an assistant over MCP.

## What it does

**Reading documents**
- Drop files on the page, put them in a watched folder (default `data/inbox`), or let it read the mail account configured in Faustus or the hub's mail gateway: PDF and image attachments are filed and the mail password never leaves Faustus.
- Formats: PDF (text layer, with OCR for scanned pages when the optional OCR package is installed), images (PNG, JPEG, WebP, HEIC, TIFF, BMP, GIF), `.eml`, `.docx`, `.pptx` (one page per slide, with speaker notes), HTML and plain text. Each file is stored once by SHA-256 and the original is never modified.
- Rules, with no model needed, classify the document (invoice, receipt, bill, contract, insurance, warranty, tax, official notice, fine, vehicle/ITV, identity, subscription, payslip, bank, instruction manual), name the issuer, pick the reference, amount and billing period, and find every date with its role (issued, due, renewal, effect, purchase, delivery, expiry, notified, next ITV, end of commitment). Every fact keeps its evidence: the quote and the page.
- Low-confidence documents and mails that may be paperwork go to a review list: accept, change or ignore. Your edit to any field always wins on re-reading.
- The first mail scan and old files are filed quietly, with past deadlines created as done, so you only hear about what is still ahead.

**Deadlines and reminders**
- Payment due dates, renewals, the last day to cancel a renewal, the end of the 3-year legal warranty, the end of a commitment period, the 20 natural days to pay a traffic fine with a discount or to make allegations, administrative response periods counted in working days, ID expiry and next ITV. "Why this date?" shows the rule, the base date, the days counted and the quote; see [docs/RULES.md](docs/RULES.md).
- Lead days per deadline kind that you can change. One notification per deadline per run, overdue ones once, nothing at night (23:00-07:00 by default; urgent ones can go through).
- Channels: Windows toast, the Hoard family hub, ntfy, Telegram and e-mail. Recurring deadlines roll to the next occurrence when marked done.
- Working-day calendars cover national holidays and the regions Madrid, Catalonia, Andalusia, Valencia, Galicia and the Basque Country, plus extra holidays you add.
- Other family apps (for example home maintenance) send their own deadlines with a source and external key; sending again updates instead of duplicating, and what you change or dismiss here wins.

**Series, warranties and search**
- Bills, insurance and subscriptions from the same issuer and reference form a series with price history; a price change of 5 % or more (configurable) raises a notification.
- Full-text search (SQLite FTS5, accent-insensitive) across every page returns snippets with document and page, optionally restricted to given documents. `warranty_check` tells whether a purchase is still under warranty and until when.
- Instruction manuals are stored and searchable and never create a deadline.
- Compare two archived versions of a document with citations to both ([docs/DOCUMENT_COMPARE.md](docs/DOCUMENT_COMPARE.md)).
- Dossiers: for a question and a set of filed documents, passages with `[d_id · p. N · l. M]` citations plus dates, amounts, durations and obligations read by fixed rules; contradictions are listed side by side and never resolved for you. A dossier is marked stale when a source changes, and a requirements diff aligns clauses of two versions ([docs/DOSSIER.md](docs/DOSSIER.md)).

**Links to other apps**
- Invoices and receipts with an amount and a date are matched against the money ledger; when exactly one movement scores 0.8 or more it is linked, and you can pick one by hand when none is clear.
- Dated action items of meeting minutes that are yours become deadlines; delivered purchases from the shipment tracker create their warranty deadlines.
- A tax-return folder for the calendar year: the usual paperwork, an index, a CSV, the ledger's year summary and a list of missing certificates.

**Workshop (Taller)**
- Merge PDFs, split by page, ranges or every N pages, extract, delete, rotate or reorder pages, compress (Ghostscript if installed, otherwise a built-in compressor; optionally to a target size), set or remove a password (AES-256), stamp a text watermark, edit title, author, subject and keywords, turn photos or Word, ODT and RTF into PDF, turn PDF pages into images, export structured Markdown and tables to XLSX or CSV.
- Kafka includes its own native PDF engine (`native/pdf`, built with `python scripts/build_pdf_engine.py`, needs a Rust toolchain). Without it only the PDF operations are unavailable.
- Results come with sizes before and after and a download button; PDF results can be filed in Kafka. The workshop never edits a file in place.

**Document reading for the family**
- Other apps call Kafka through the hub for text extraction and OCR and pass paths, never bytes: `doc_extract` (PDF, images, `.docx`, `.eml`, HTML, text, spreadsheets, archives), `ocr_image`, `ocr_file`, `ocr_pdf`, `ocr_engine`, `ocr_status` and `doc_structure`.
- OCR engines: `indra`, `winocr`, `rapidocr`, `light-ocr` and `tesseract`, chosen with the "Motor de OCR" setting (`auto`, `indra` or `current`); if the chosen engine fails on an image another reads it. Without an OCR package, scanned pages are stored and flagged for review.

**Privacy**
- Spanish ID numbers, IBAN, card and phone numbers are masked in what MCP returns unless you explicitly ask for that number. The local interface shows everything.

## Screens

Spanish by default, English with one click, dark theme.

- **Plazos**: overdue, this week, next 30 days and later; mark done, snooze, edit, dismiss, add by hand; what is new since the last visit.
- **Documentos**: upload by dragging files anywhere, list with filters by kind and state, full-text search with snippets.
- **Detalle**: page preview, extracted facts with evidence, deadlines with "why this date?", series and price history, the linked ledger movement.
- **Revisión**: documents and mails the rules were unsure about.
- **Taller**: the workshop operations, each with a drop zone, options and a result card; plus the tax-return folder card.
- **Dossiers**: build, refresh and compare dossiers.
- **Ajustes**: workshop and watched folders, mail source, calendar region, reminder lead days, notification route and channels with a test button, OCR and model status, language, recent activity.

## Run it

Requires Python 3.11+, Node 22 only to rebuild the UI, and Faustus with a mail account for automatic mail reading.

```bash
python -m venv venv
venv/Scripts/python -m pip install -r requirements.txt      # Windows; venv/bin/python elsewhere
venv/Scripts/python -m kafka_hoard                            # http://127.0.0.1:5200
```

The UI is prebuilt in `kafka_hoard/static`; to rebuild it run `npm install && npx vite build`. `python scripts/launch.py` starts the app on a free port and opens the browser. OCR is optional: `pip install ".[ocr]"`.

| Variable | Purpose |
| --- | --- |
| `KAFKA_PORT` | Port (default `5200`). |
| `PORT_STRICT=1` | Do not fall back to another port. |
| `KAFKA_DATA_DIR` | Data folder (default `./data`). |
| `KAFKA_ALLOWED_HOSTS` | Extra host names accepted behind a tunnel. |
| `KAFKA_SCHEDULER=0` | No background work. |
| `KAFKA_OFFLINE=1` | No network. |
| `KAFKA_OCR=0`, `KAFKA_OCR_BACKEND` | Disable OCR or choose the engine. |
| `KAFKA_TELEGRAM_TOKEN`, `KAFKA_TELEGRAM_CHAT_ID`, `KAFKA_NTFY_TOPIC`, `KAFKA_SMTP_*`, `KAFKA_FAUSTUS_DIR` | Notification and mail secrets; they can go in `.env` (see `.env.example`) or in Settings. |

Everything else is a setting stored in `data/kafka.db`. Hoards run on demand: the family hub can list, start and stop this one.

## Connect to Faustus and the Hoard family

- **MCP**: `mcp_server.py` is a stdio bridge named `kafka-hoard`. It never opens the database: it proxies every call to the running app with the token in `data/mcp-token`, and starts the app when it is not answering. `faustus-plugin.json` describes the app, its health check and the bridge. The full catalogue, with arguments, is in [docs/API.md](docs/API.md) and at `GET /api/agent/tools`.
- **Tool groups**:

| Area | Tools |
| --- | --- |
| Overview | `kafka_overview`, `kafka_status` |
| Deadlines | `deadlines_list`, `deadline_get`, `deadline_add`, `deadlines_from_minutes`, `deadline_update`, `deadline_update_by_key`, `deadline_explain`, `deadline_delete` |
| Documents | `docs_list`, `doc_get`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `doc_compare`, `extract_preview`, `document_link_tx` |
| Dossiers | `kafka_dossier`, `kafka_dossier_status`, `kafka_dossier_refresh`, `kafka_dossier_delete`, `kafka_requirements_diff` |
| Series and warranties | `series_list`, `price_history`, `warranty_check`, `phileas_sync` |
| Mail and folders | `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan` |
| Notifications and settings | `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run` |
| Workshop | `pdf_*` operations (more than a hundred), `images_compress`, `tax_pack` |
| Reading and OCR | `doc_extract`, `ocr_image`, `ocr_file`, `ocr_engine`, `ocr_pdf`, `ocr_status`, `doc_structure` |

- **Agent accountability**: every write an assistant makes through MCP needs a `reason` of 3 to 300 characters and is recorded in `data/agent_journal.jsonl` with agent, session and what is needed to take it back. `POST /api/agent/undo` can revert a whole session (dry run first); some tools cannot be undone and are reported as such, and changes made by someone else afterwards are reported as conflicts, not overwritten. Scoped tokens (`read_only`, `drafts`, `all`) are minted with `python -m hoard_link.tokens mint`.
- **Hub**: notifications (with fallback to the app's own channels), the mail gateway, the family agenda (`GET /api/family/agenda`: renewals, maintenance and deadlines from other apps), and events `kafka.document.archived`, `kafka.warranty.created`, `kafka.deadline.created`, `kafka.deadline.soon` and `kafka.deadline.overdue`. Other apps reach Kafka through the hub (`POST /api/apps/kafka/call`) and use `/api/ui/call`, `/api/agent/call`, `/api/dashboard` and `/api/documents/*`.
- **Copilot**: a floating Faustus button opens the assistant in a side panel. It is told which document or deadline is open (summary fields only, never document text), which page you are on and any unsaved form fields; the view reloads in place after each turn and forms send only the fields you changed.
- **App Dock**: beside a Faustus chat the page registers `kafka_open`, `kafka_filter` and `kafka_view` so the assistant can open documents, apply filters and switch sections.
- **Skills**: `paperwork-intake-and-deadlines` and `hoard-daily-digest` are offered to connecting agents.

## Project layout

| Path | Contents |
| --- | --- |
| `kafka_hoard/` | FastAPI app, SQLite store, extraction rules, working-day calendar, scheduler, notifications, mail, OCR, workshop, agent tools |
| `kafka_hoard/static/` | Prebuilt React 19 + Vite + Tailwind interface (source in `client/`) |
| `native/pdf/` | Source of the native PDF engine |
| `mcp_server.py` | Stdio MCP bridge |
| `docs/` | API reference, architecture, deadline rules, dossiers, document comparison, OCR benchmark |
| `tests/` | pytest suite |

```bash
python -m pytest -q    # no network; documents and PDFs are generated with invented data
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Limits

- The workshop does not read scanned pages (that is `doc_extract` and `ocr_pdf`). Compression without Ghostscript only reduces images; encrypted inputs need their password; HEIC needs the optional `pillow-heif`; Word and LibreOffice conversion needs one of them installed; a watermark is plain Latin text.
- Dates are read with rules for Spanish and English text; an unusual layout may land in the review list instead of becoming a deadline.
- The optional model pass only proposes facts, and anything whose date is not written in the document is discarded. Deadlines from rules are not legal advice: check the original notice for anything important.

## License

MIT
