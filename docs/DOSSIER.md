# Dossier for a question

Tools: `kafka_dossier`, `kafka_dossier_status`, `kafka_dossier_refresh`, `kafka_dossier_delete`, `kafka_requirements_diff`.
REST: `GET/POST /api/dossiers`, `GET/DELETE /api/dossiers/{id}`, `POST /api/dossiers/{id}/refresh`, `GET /api/requirements-diff`.

## What a dossier holds

- **Sources**: filed documents (`doc_ids`, 1 to 12; without them, the best full-text matches of the question). Each one keeps the SHA-256 of its page text and of its file when the dossier was built.
- **Passages**: lines that contain a word of the question (prefix match, so «entrega» finds «entregar»), up to `max_passages` per source, each with `[d_id · p. N · l. M]`.
- **Claims**: read by fixed rules from every line and table cell, not only from passages: dates (with year), amounts with a currency, durations (days, working days, weeks become days, months, years, hours), percentages, and obligations (must, required, not required, forbidden). A claim has `claim`, `value`, `unit`, `date` (the only date on the same line), `concept` (deadline, delivery, payment, price, warranty, penalty, deposit, notice, ...), the citation and `from_ocr`. Table claims also carry `table.row`, `table.column` and the row label.
- **Synthesis**: claims grouped by subject. A group is `agree` (every source holds the same values), `conflict` (`disjoint`: two sources with no value in common; `partial`: one source has extra values; `internal`: one source contradicts itself) or `single` (one source only). Requirements about the same thing are matched by shared content words and conflict when their polarity differs.
- **Conclusion**: what all sources agree on, the conflicts with every position and its sources (`resolved` is always false), and open questions: which value is in force, what only one source says, sources without relevant passages or with pages without text, OCR data to check.

Nothing is written by a model: every item has `derived_by: rules`. If a model is ever used, its text must say `derived_by: model`.

## Staying current

`kafka_dossier_status` compares each source's recorded text hash with its present one: `fresh`, `changed` or `missing`. `kafka_dossier_refresh` re-reads only the changed sources, drops the deleted ones and rebuilds the synthesis and conclusion from the stored sections; fresh sections keep their `computed_ts`. With nothing stale it does nothing.

## Requirements diff

`kafka_requirements_diff(before, after)` splits each document into clauses (a heading «Cláusula N», «Artículo N», «N.», «N)» plus the lines under it; without headings, one line is one clause), pairs them by number and then by text similarity (0.6 or more), and returns `added`, `removed` and `modified` clauses with both citations, the changed words and `claims`: the dates, amounts, durations, percentages and obligations that differ. It works for two sources that are not versions of each other too. Use `offset`/`next_offset` for long lists.

## Limits

- Reads archived text only: no images, layout or signatures. OCR text can contain reading errors; claims from OCR are flagged.
- The subject of a value is guessed from the words next to it. A value with no known subject word is grouped only with values that share its words, so some real contradictions are missed rather than invented. An amount in a table row is grouped by the row label.
- Dates need a year. Currencies are not converted; different currencies are different values. Months and years are not converted to days.
- Tables are found when consecutive lines split into the same number of cells by `;`, `|`, a tab, two or more spaces, or a comma (CSV files). Text cleaning collapses runs of spaces in pasted text, so use a CSV or a PDF with a text layer for tables; OCR gives lines of text, not cells.
- Spanish and English wording. No legal interpretation and no deadline is created or changed.

## En español

`kafka_dossier` junta, para una pregunta, los pasajes y datos de varias fuentes ya archivadas, con cita de página y línea, y lista las contradicciones una al lado de la otra sin elegir ninguna. El expediente recuerda el hash del texto de cada fuente: `kafka_dossier_status` avisa de cuáles han cambiado y `kafka_dossier_refresh` recalcula solo esa sección, la síntesis y la conclusión. `kafka_requirements_diff` compara dos versiones cláusula a cláusula. Las reglas son fijas (sin modelo); el texto de OCR puede traer errores y las tablas solo se detectan si las celdas se separan por `;` `|` tabulador, dos espacios o coma (CSV).
