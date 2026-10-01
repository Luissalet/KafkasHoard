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
- **Private to the assistant.** Spanish ID numbers, IBAN, card and phone numbers are masked in what MCP and the agent route return unless you explicitly ask for that number. The local UI shows everything.

## Screens

Spanish by default, English with one click, dark.

- **Plazos** — overdue, this week, next 30 days and later; mark done, snooze, edit, dismiss, add by hand; news since the last visit; documents waiting for a decision.
- **Documentos** — upload by dragging files anywhere, list with filters by kind and state, full-text search with snippets.
- **Detalle** — page preview with the original file, extracted facts with their evidence, deadlines with «why this date?», series and price history, edit any field (your edit always wins on re-reading).
- **Revisión** — documents the rules were not sure about and mails that may be paperwork: accept, change, ignore.
- **Ajustes** — watched folders, mail, calendar region and extra holidays, reminder lead days, notification channels with a test button, OCR and model status, language, recent activity.

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

Tools (39): `kafka_overview`, `kafka_status`, `deadlines_list`, `deadline_get`, `deadline_add`, `deadline_update`, `deadline_explain`, `deadline_delete`, `docs_list`, `doc_get`, `doc_search`, `doc_add_file`, `doc_add_text`, `doc_update`, `doc_reprocess`, `doc_delete`, `extract_preview`, `warranty_check`, `series_list`, `price_history`, `mail_scan`, `mail_list`, `mail_accept`, `mail_ignore`, `mail_status`, `folders_list`, `folder_add`, `folder_remove`, `folder_scan`, `phileas_sync`, `notifications_list`, `notify_status`, `notify_test`, `telegram_find_chat_id`, `settings_set`, `secret_set`, `scheduler_status`, `runs_list`, `housekeeping_run`. Arguments in [docs/API.md](docs/API.md).

Events on the family bus: `kafka.document.added`, `kafka.price.change`, `kafka.deadline.soon` and `kafka.deadline.overdue`. `phileas_sync` asks Phileas's Hoard (through the family link) for delivered purchases and creates their warranty deadlines.

## How it is built

FastAPI + SQLite (WAL, FTS5) + a two-lane scheduler; React 19 + Vite + Tailwind UI; extraction in `kafka_hoard/extract/` (pure functions); working days in `kafka_hoard/bizdays.py`; the work in `kafka_hoard/engine.py`. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Tests: `python -m pytest -q` (no network, documents and PDFs are generated by the tests with invented data).

## Limits

- Dates are read with rules for Spanish and English text. An unusual layout may land in the review list instead of becoming a deadline.
- Without the optional OCR package, scanned documents are stored but their text is not read.
- Working-day calendars cover national holidays and the regions Madrid, Catalonia, Andalusia, Valencia, Galicia and the Basque Country; add other local holidays in Settings.
- The optional model pass (through the Hoard link) only proposes facts; anything whose date is not written in the document is discarded. Deadlines from rules are not legal advice: check the original notice for anything important.

## License

MIT
