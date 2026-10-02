# Working on Kafka's Hoard

A local paperwork keeper (package `kafka_hoard`, service `kafka-hoard`, app id `kafka`, port 5200). It reads documents, extracts dates, makes deadlines and reminds. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) first and [docs/RULES.md](docs/RULES.md) before touching anything that computes a date.

## Commands

```bash
python -m pytest -q                       # the whole suite; no network, no real mail, no OCR engine needed
python scripts/gen_api_doc.py             # regenerate docs/API.md after changing any tool (a test fails when it is stale)
npx vite build                            # rebuild the UI into kafka_hoard/static (run from the repository root)
python -m kafka_hoard                     # run on http://127.0.0.1:5200 (KAFKA_DATA_DIR=<folder> for a scratch copy)
python scripts/make_icon.py               # regenerate the icon and the copies in client/public and kafka_hoard/static
```

Run Python from the repository root or with absolute paths: running a script from inside `kafka_hoard/extract/` makes its `types.py` shadow the standard library.

## Rules of the code

- `kafka_hoard/extract/` is pure: text in, `Extraction` out. No database, clock, network or files; the current day, the calendar, the language and the user's overrides arrive in `Ctx`.
- Every state change goes through `Engine`; the store only stores. The UI, the REST agent route and MCP all call the same handlers in `agent_tools.py`: add a capability there once and the three surfaces get it. The bundled UI calls tools through `POST /api/ui/call`.
- A new tool needs: a pydantic argument model with descriptions, a first description line of at most 110 characters (English, then a Spanish phrase), synonyms, annotations, a test in `tests/test_tools.py`, and a regenerated `docs/API.md`. The README tool list must stay in step.
- Anything a user edits (a document field, a deadline's date, title or reminders) wins over the extraction and survives reprocessing. Never replace a deadline flagged `edited` or one the user closed.
- Notifications are deduplicated by key and sent once. History is quiet: old documents and the first mail scan create no notifications and their past deadlines are done.
- Identifiers (DNI/NIE, IBAN, cards, phones) are masked for assistants in `privacy.py`; the local UI shows everything. New tool results that can carry text must go through the mask.
- The mail helper `mail/faustus_mail.py` is stdlib-only and imports nothing from Kafka, because it runs under another interpreter. `hoard_link/` is vendored and byte-identical to upstream: never edit it here.
- User-facing text goes in `client/src/i18n.js` (Spanish first, English second) and in `extract/texts.py` for deadline explanations. Spanish is castellano de España. Keep the UI and the docs plain: no marketing lines.
- The workshop never overwrites or edits a file in place, never logs, returns or stores a password, and reaches Ghostscript, Word and LibreOffice only through `workshop/proc.py`'s `Env`, so tests fake them.
- Tests build their documents from invented data (`tests/docs.py`, `tests/pdfmaker.py`); never add real names, ID numbers, accounts or addresses. Time is injected (`clock` fixture); do not depend on the real date.

## Where things are

| Need | Look at |
|---|---|
| A date is wrong or missing | `extract/dates.py` (roles), `extract/periods.py` (relative periods), `extract/deadlines.py`, `extract/rules.py` |
| A document gets the wrong kind or issuer | `extract/kinds.py`, `extract/issuers.py`, `extract/refs.py` |
| A reminder did not fire | `engine.run_reminders`, the `notify.*` and `remind.*` settings, `notify/__init__.py` |
| A mail was not filed | `mail/classify.py`, `engine.scan_mail`, `mail/faustus_mail.py` |
| A tool or its masking | `agent_tools.py`, `privacy.py` |
| A PDF or image operation, where its result went, a workshop upload | `workshop/service.py`, `workshop/pdfops.py`, `workshop/imagetools.py`, `api/workshop.py` |
| The UI | `client/src/pages/*.jsx`, `client/src/components/*`, `client/src/i18n.js` |

## Before finishing a change

1. `python -m pytest -q` is green.
2. `python scripts/gen_api_doc.py` leaves `docs/API.md` unchanged.
3. If the UI changed: `npx vite build`, then open the running app and look at Plazos, Documentos, Detalle, Revisión, Taller and Ajustes in Spanish and English, at desktop and phone width.
4. README.md, README.es.md, docs/RULES.md and docs/ARCHITECTURE.md say what the app can and cannot do now.
