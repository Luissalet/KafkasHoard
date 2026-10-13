# Kafka PDF engine

Kafka's owned PDF engine exposes the complete 123-operation table through the normal API, MCP and document workshop. Its headless worker uses `kafka-pdf-catalog/v1`, `kafka-pdf-batch/v1` and `kafka-pdf-result/v1`; it does not launch or locate another application's CLI. The 26 engine crates, patched third-party PDF libraries and required font assets are maintained here with local workspace paths.

## Source and notices

Initial incorporated source: `storytold/printcraft` release `v0.2.1`, revision `dcb00238896e418b357e6dd777ee35bad36f0b97`. `PROVENANCE.json` records the pinned source and initial file hashes. The historical repository is provenance, not a runtime or build dependency. Future changes belong to this engine and need no compatibility with that repository. The complete original `LICENSE-MIT` and `LICENSE-APACHE` notices remain unchanged; patched libraries and bundled fonts retain their own notices. Branding assets and the external editor application are not incorporated.

## Build

From the Kafka repository root, run `python scripts/build_pdf_engine.py`. The script uses Rust 1.95.0, the committed Cargo lock and standard registry dependencies, and installs `native/bin/kafka-pdf-worker` (`.exe` on Windows). No source checkout or external Craft binary is required. The generated binary and build cache are ignored; source, manifests, notices, assets and the lock are versioned.

The Python service resolves only this installed worker. Stored `printcraft.cli`, `KAFKA_PRINTCRAFT_CLI`, PATH and old portable folders never select an engine. Existing API operation aliases and receipt IDs remain usable. Each edit copies its input, creates new output and preserves the original. The ordinary PDF and image workshop remains available.

## Verification

`cargo +1.95.0 test --locked --release --package kafka-pdf-automation --tests` exercises the incorporated automation core. Python tests cover actual paragraph edits, page insertion/rotation, receipt lineage, all operation aliases, catalogue equality, missing obsolete executable paths and PNG artifacts. Normal API/MCP and browser verification uses a private instance with an explicit private workshop output directory.

The source preserves optional OCR, printing, Office and existing PDF font/script behavior; installing this worker does not install OCR weights, printers or Office packages. It does not claim faithful editing of every PDF font or script.
