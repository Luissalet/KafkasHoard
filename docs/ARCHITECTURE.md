# Architecture

```
 upload (UI, REST) ─┐
 watched folders ───┼──► engine.ingest ──► files.py (content-addressed store, SHA-256, original never modified)
 mail attachments ──┤        │
 pasted text ───────┘        ▼
 mail/faustus_mail.py   readers.py  PDF text layer (pypdfium2) · OCR (ocr.py, optional rapidocr) · images · .eml · .docx · HTML · text
 (Faustus's Python,          │ pages
  stdlib only)               ▼
                     extract/pipeline.py ── texts · kinds · issuers · refs · amounts · dates · periods · deadlines · rules
                             │ Extraction (kind, issuer, ref, amount, facts with evidence, deadline drafts)
                             ▼  (optional: extract/llm.py asks a local model when the rules are unsure; answers are validated)
 store.py / db.py (SQLite WAL + FTS5: documents, pages, deadlines, series, mails, notifications, runs, settings)
        ▲                    │
        │                    ├──► engine.run_reminders ──► notify/ (toast, family bus, ntfy, Telegram, email via Faustus)
 services.py (wiring,        └──► series + price alerts · recurring roll · Phileas sync · housekeeping
 dashboard, settings)
        ▲
 agent_tools.py: one catalogue (39 tools) ──► api/agent.py (Bearer token, masked) · api/ui.py (local, unmasked) · mcp_server.py (stdio)
 scheduler.py: lane "ingest" (folder and mail scans) · lane "reminders" (reminder run every 10 min, housekeeping hourly)
```

## Layout

| Path | Role |
|---|---|
| `kafka_hoard/main.py` | App factory: guard middleware, error handlers, routers, single-page client |
| `kafka_hoard/config.py` | Process settings from the environment (`KAFKA_*`) and `.env`; paths under `data/` |
| `kafka_hoard/db.py`, `store.py` | Schema and migrations; every query. WAL, FTS5 `pages_fts` (unicode61, accent-insensitive) |
| `kafka_hoard/files.py` | Content-addressed originals: `files/<sha[:2]>/<sha256>.<ext>`; the same bytes are stored once |
| `kafka_hoard/readers.py` | Bytes to text per page; page splitting; notes when a page could not be read |
| `kafka_hoard/ocr.py` | Optional OCR with rapidocr; reports unavailable instead of failing |
| `kafka_hoard/extract/` | Pure functions from text to an `Extraction`; see below |
| `kafka_hoard/bizdays.py` | Spanish working days, holidays, regions, extra holidays |
| `kafka_hoard/engine.py` | Ingestion, reprocessing, deadlines, reminders, series, price alerts, mail, folders, Phileas, housekeeping |
| `kafka_hoard/services.py` | Wires everything from a `Config`; settings and secrets; dashboard and detail views |
| `kafka_hoard/agent_tools.py` | The tool catalogue: name, description, pydantic arguments, annotations, handler |
| `kafka_hoard/api/` | REST: `agent.py` (tools for assistants), `ui.py` (dashboard, UI calls), `documents.py` (upload, file, page image), `health.py`, `pwa.py` |
| `kafka_hoard/privacy.py` | Masking of DNI/NIE, IBAN, cards and phones for assistants |
| `kafka_hoard/guard.py` | Same-origin request guard shared by every Hoard app |
| `kafka_hoard/scheduler.py` | Two lanes, one worker each; jobs deduplicated; pause and run-now |
| `kafka_hoard/notify/` | Channels, minimum severity, night window, dedupe keys, test sends |
| `kafka_hoard/mail/` | `faustus_mail.py` (runs under Faustus's Python), `source.py` (talks to it), `classify.py` (paperwork, maybe, noise) |
| `kafka_hoard/hoard_link/` | Vendored family library (event bus, calls to other apps, local model). Not edited here |
| `mcp_server.py` | stdio bridge: proxies to the running app, starts it when needed |
| `client/` | React 19 + Vite 6 + Tailwind 4 UI; built into `kafka_hoard/static` |

## Ingestion

`engine.ingest` takes bytes and a name. The bytes are stored by SHA-256, so the same file arriving by upload, folder and mail is one document. `readers` produce one text per page; a PDF page without a text layer goes to OCR when available, otherwise a note records it. The extraction runs on the pages and fills the document: kind, issuer, reference, amount, dates with roles, facts with evidence (quote and page), and deadline drafts. A document whose confidence is below 60, whose kind is unknown or that produced no date lands in state `review`; the rest are `ok`.

A document edited by the user keeps its edits (`overrides`): the extraction reads them as the truth, so editing the issue date, the kind or a title changes every dependent deadline when it is read again.

## Extraction (`kafka_hoard/extract/`)

- `texts.py` holds the Spanish and English titles and basis sentences of the deadlines; `util.fold` lower-cases text and removes accents for matching.
- `kinds.py` scores weighted keyword rules per document kind; the issuer category nudges the result.
- `issuers.py` finds the issuer by sender domain, known names, «Ayuntamiento de …», a company line next to a CIF, the sender name, or an all-caps letterhead.
- `refs.py` finds labelled references (policy, contract, invoice, expedient, bulletin) and picks the one that identifies the document for its kind; the stable one (policy, contract, supply point) keys the series.
- `amounts.py` reads euro amounts in Spanish and English formats and picks the total, the reduced amount of a fine and the annual or monthly premium.
- `dates.py` finds numeric and written dates and gives each a role from the words around it.
- `periods.py` reads relative periods («20 días naturales», «10 días hábiles», «tres meses») with their purpose and base.
- `deadlines.py` turns roles and periods into drafts, using `rules.py` for the legal calculations; see [RULES.md](RULES.md).
- `llm.py` is the optional model pass: only for documents left in review; hints are accepted only when the date is written in the evidence it cites.
- `pipeline.py` runs the steps in order and computes the confidence.

Nothing in `extract/` touches the database, the clock or the network; `Ctx` carries today, the calendar, the language and the user's overrides, which is why the tests can run it on invented text.

## Deadlines

A deadline has a key (`payment`, `renewal`, `cancel_by`, `warranty`, `appeal:10d`, …), a date, a basis (human text with the rule), the evidence and page, lead days, an amount, recurring (`none`, `monthly`, `yearly`) and flags `auto` (created by the extraction) and `edited` (touched by the user). Reading a document again replaces its `auto` deadlines except the ones you edited or closed. Marking a recurring deadline done moves it to its next occurrence; housekeeping also rolls recurring deadlines left open three days past their date.

## Reminders

`run_reminders` runs every ten minutes. For each open deadline it finds the lead days that have arrived and were not yet notified, sends one notification for the most urgent, and records all of them. Overdue deadlines get one notification up to 14 days late. Night hours defer everything except (optionally) high severity. Notifications go to the enabled channels whose minimum severity is met; each has a dedupe key, so a restart never repeats one. History is quiet: old mails and files create past deadlines as done and no notifications.

## Series and prices

Insurance, subscriptions and optionally utility bills with the same issuer and stable reference join a series. A new document with an amount that differs by the configured percentage from the previous one raises a `price_change` notification; `series_list` and `price_history` show the evolution.

## Mail

`mail/faustus_mail.py` is stdlib-only and imports nothing from Kafka: Kafka starts it with Faustus's Python inside the Faustus folder, writes one JSON request to stdin and reads one JSON line. It reuses Faustus's mail server module to resolve accounts, so passwords never leave Faustus. A scan returns candidate messages (paperwork words in the subject or body, or a PDF/image attachment) and writes each attachment of 15 MB or less to the cache folder as `<sha256>.<ext>`. `classify.py` labels each message `doc`, `maybe` or `noise`; documents are filed, doubtful mails wait in Revisión, noise is remembered so it is not shown again. The first scan looks back `mail.first_days` (180) and is quiet. The same helper sends notification mails.

## Tools, API and MCP

`agent_tools.TOOLS` is the single catalogue. `GET /api/agent/tools` and `POST /api/agent/call` serve it to assistants with the bearer token from `data/mcp-token`: results are capped and personal identifiers are masked unless `reveal` is set. `POST /api/ui/call` serves the same handlers to the bundled UI uncapped and unmasked (the app only listens on 127.0.0.1 and the guard rejects cross-site requests). `mcp_server.py` exposes the catalogue over stdio as the `kafka-hoard` server and proxies to the running app. `docs/API.md` is generated from the catalogue by `scripts/gen_api_doc.py`; a test fails when it is out of date.

## Client

Hash routing (`#/plazos`, `#/documentos`, `#/documentos/<id>?p=2`, `#/revision`, `#/ajustes`). Strings are in `client/src/i18n.js` as `[español, English]` pairs. Colours come from the shared `hoard-theme.css` (vendored unchanged from the family) selected by `data-hoard-app="kafka"`. A shared `version` counter, bumped by `changed()`, refreshes every page after any change. Files can be dropped anywhere on the window.

## Data

Everything lives in `data/` (or `KAFKA_DATA_DIR`): `kafka.db`, `files/` (originals), `cache/` (page images, mail attachments awaiting filing), `inbox/` (default watched folder), `mcp-token`, `url`, `logs/`. Back up the folder to back up the app.
