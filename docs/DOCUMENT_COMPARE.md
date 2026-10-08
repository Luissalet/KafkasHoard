# Comparing archived document text

`doc_compare` compares two archived document IDs. Typical requests:
“What changed between these two renewals?” or “Compare the corrected receipt
with the original and show the source pages.”

```json
{"before":"d_old","after":"d_new","limit":10}
```

Each change has an ordered `index`, `kind` (`insert`, `delete`, `replace`),
and `before`/`after` excerpts with physical page numbers and citations.
Lines are aligned across page boundaries, so a new cover does not make every
following page a replacement. Repeated table rows, signs, numbers, case and
accents are retained. Only empty lines and whitespace inside a line are ignored.

## Focus and pagination

- `before_pages` and `after_pages` select physical page numbers, in original
  document order. Missing pages are an ordinary `invalid` tool error.
- `offset` is a zero-based change index. Follow `next_offset` until it is null;
  a byte budget may return fewer changes than the requested `limit` (1–30).
- The default is ten changes. Excerpts keep up to 800 UTF-8 bytes per side.
  `excerpt_truncated` marks a shortened excerpt; read the cited page with
  `doc_get` to see more. At most ten source pages/citations are listed per side,
  with `pages_truncated` and the full `page_count` when there are more.
- Each source is limited to 200 archived pages, 200,000 characters and 4,000
  non-empty lines. Oversized inputs fail explicitly; choose fewer pages.
- Assistant masking is applied before shortening excerpts and measuring pagination. It does not
  hide the existence of a raw-text change. `reveal=true` follows the existing
  policy for an explicitly requested personal identifier; the UI is unmasked.

## What equality means

`same_text` describes the selected **archived, extracted text**, not binary
or visual equality of the original files. `scope` says whether page selection
was used. `equality_basis` is `selected_archived_text`; the operation does not
open or compare the original files.

When selected pages have empty text, `complete_text_available=false`. If no
change was observed but text is unavailable, `same_text=null`, rather than a
claim that the documents are identical. Extraction warnings are retained
under each source's `extraction_notes`, with a truncation marker if shortened.
These may report pages omitted during ingestion or unavailable OCR.

OCR errors, line wrapping and extraction differences can create or conceal
text differences. Images, signatures, styling and layout are not compared.
Moved text may appear as insertion/deletion; difflib does not guarantee the
smallest edit sequence. There is no semantic or legal interpretation and no
deadline calculation. Documents, user overrides and deadlines remain unchanged.

## References

- [Python difflib](https://docs.python.org/3.11/library/difflib.html): line
  alignment with `SequenceMatcher`, `get_opcodes`, quadratic worst-case and
  the automatic junk heuristic (disabled here for repeated document rows).
- [disparPDF](https://github.com/barak/disparPDF): precedent for document
  comparison; its visual renderer and dependencies are not incorporated.

No upstream code or new dependency was added. GitHub Trending's daily page
was inspected; no relevant comparison project was identified there, so this
feature is based on the local capability gap and primary references.

## En español

Compara con `before` y `after` los IDs archivados. La herramienta devuelve
los cambios de texto con fragmentos y citas de ambas páginas. Se conservan
números y filas repetidas; una portada nueva no obliga a emparejar páginas
por el mismo número. Usa los selectores de páginas y `next_offset` para
acotar o seguir leyendo.

La igualdad es únicamente del texto extraído y seleccionado. Páginas sin
texto producen incertidumbre; se muestran las advertencias de extracción.
No se comparan imágenes, firmas ni diseño, ni se interpretan cláusulas o
calculan nuevos plazos. Los originales y datos archivados permanecen intactos.
