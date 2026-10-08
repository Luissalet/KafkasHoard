# OCR benchmark and multi-page pilot

## Reproduce

Use Kafka's Python environment and a directory outside the repository:

```powershell
./venv/Scripts/python.exe scripts/eval_document_ocr.py --directory D:/LocalAI/qa-ocr-pilot/light --backend light-ocr
./venv/Scripts/python.exe scripts/eval_document_ocr.py --directory D:/LocalAI/qa-ocr-pilot/rapid --backend rapidocr
```

The default counts are 1, 5 and 20 pages. The script generates invented Spanish
inventory tables as images and raster PDFs, then runs Kafka's actual `DocService`
with forced OCR. It saves image hashes, expected transcripts, raw extraction and
scores. No document is filed, no deadline is calculated and no real data is read.
The Windows default font is Arial; pass `--font` on another system.

Scores cover character edit distance after case/whitespace normalization, page
presence and order, empty pages, and the sequence of signed numeric quantities.
The quantity check preserves signs and fractions and accepts decimal comma/dot
variation. A low character error rate alone cannot hide a changed amount.
The pilot threshold is CER <= 5%, complete pages and no quantity mismatch; it is
not a guarantee of correctness on other documents.

External parsers can use the same manifest and export their verified page mapping:

```powershell
./venv/Scripts/python.exe scripts/eval_document_ocr.py --directory D:/LocalAI/qa-ocr-pilot/external --manifest D:/LocalAI/qa-ocr-pilot/light/pages-20/manifest.json --predictions D:/LocalAI/qa-ocr-pilot/predictions.json
```

Predictions must contain `{"units": [{"number": 1, "text": "..."}]}` in source page
order. Missing, extra, duplicated or reordered pages fail coverage. Do not invent
a page mapping from a parser's unsegmented output.

## Observed regression and correction (2026-10-05)

Before the correction, Light-OCR read a one-page raster PDF correctly. On five
pages it recognized all five, but the shared extraction subsequently removed
repeated lines with digits ignored. Four page texts became empty, including their
changing table amounts; aggregate CER was 95.8032%.

`DocService` now preserves the reader's page text. Repeated headings, column names
and rows remain on their original page. No backend or date rule was changed.

| Local CPU backend | Pages | Seconds | CER | Empty pages | Pages with numeric mismatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| Light-OCR | 1 | 1.428 | 0% | 0 | 0 |
| Light-OCR | 5 | 6.700 | 0% | 0 | 0 |
| Light-OCR | 20 | 27.225 | 0.3830% | 0 | 0 |
| RapidOCR | 1 | 2.650 | 0% | 0 | 0 |
| RapidOCR | 5 | 7.635 | 0% | 0 | 0 |
| RapidOCR | 20 | 28.816 | 0.8008% | 0 | 0 |

These are six independent service runs on clean synthetic print, with startup
cost included. Larger-run errors observed in these fixtures include omitted spaces
before currency labels. This is a baseline for detecting pipeline losses, not a
comparison on handwriting, noise, natural scans or general Spanish accuracy.
Reports reside at `D:/LocalAI/qa-ocr-pilot-20261005` on the development machine.
The full Kafka test suite passed (586 tests); the API catalogue check confirmed
the existing 59 tools without a schema change. Regression tests cover repeated
rows in text-layer PDFs and identical OCR pages, alongside benchmark failures for
missing, duplicated and reordered pages and altered signs.

## Faustus reasoning check

The saved five-page Light-OCR text was passed to the real Faustus agent loop with
`qwen3.8-27b-q8-llamacpp` at port 8081, a separate data directory and a synthetic
local account/workspace. Four conversational turns requested the Avena subtotal,
both rows with signed amounts, exclusion of page 3, and repetition with an
irrelevant instruction quoted as document data. Expected totals were 182.50,
118.75, 95.00 and 95.00 EUR. All four answers matched after the correction below.

The first turn executed Python successfully; the later answers used the retained
conversation context. Before the Faustus correction, the final two turns already
answered correctly but were forced into an unnecessary workspace-action round,
duplicating the answer. The corrected loop had zero such nudges. The workspace
remained empty, as these requests required calculations rather than file edits.

This is a component integration check using actual extracted text and the actual
agent executor, not a browser/UI or full MCP transport test. A separate direct
LLM call without tools failed three of the four totals; this is a reasoning
limitation in that configuration, not lost OCR amounts. These few synthetic
turns do not establish general calculation accuracy.

Two harder follow-ups replaced the five-page source with the actual 20-page
extraction: sum both rows except pages divisible by three (1172.50 EUR), then
include those pages again and subtract 17.35 EUR (1657.65 EUR). Both executed
Python, matched the expected totals and had no workspace-action nudge. The
combined conversation passed six turns; the workspace still contained no files.

## Unlimited-OCR candidate

[Baidu's official implementation](https://github.com/baidu/Unlimited-OCR) offers a
single `infer_multi` call over several page images, with a 32K generation limit in
the documented example. Its demonstrated runtime uses Python 3.12 and NVIDIA/CUDA;
Kafka's current environment is Python 3.13 with CPU OCR. The candidate has not been
installed or measured here and is not a default backend.

Use an isolated worker for the candidate, retain source-page provenance and score
against the same manifests. Then expand to licensed natural scans with tables,
skew and small print. [OmniDocBench](https://github.com/opendatalab/OmniDocBench)
and [olmOCR-bench](https://github.com/allenai/olmocr/blob/main/olmocr/bench/README.md)
provide primary benchmark references; verify the source-document terms before
redistributing images. Funes already uses Kafka's shared extraction service, so
a successful backend improvement belongs here rather than in a new Hoard.
