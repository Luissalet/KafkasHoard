"""Build small PDFs at test time with pypdfium2 (no fixtures on disk). One string per page; lines are separated by newlines."""

from __future__ import annotations

import ctypes
import io

import pypdfium2 as pdfium
import pypdfium2.raw as raw


def make_pdf(pages: list[str] | str, *, blank_pages: tuple[int, ...] = ()) -> bytes:
    """``blank_pages``: indexes (0-based) of pages left without any text, to exercise the OCR path."""
    if isinstance(pages, str):
        pages = [pages]
    pdf = pdfium.PdfDocument.new()
    font = raw.FPDFText_LoadStandardFont(pdf, b"Helvetica")
    for index, text in enumerate(pages):
        page = pdf.new_page(595, 842)
        if index not in blank_pages:
            y = 790.0
            for line in text.split("\n"):
                if line.strip():
                    obj = raw.FPDFPageObj_CreateTextObj(pdf, font, 11.0)
                    buf = (line + "\0").encode("utf-16-le")
                    arr = (ctypes.c_ushort * (len(buf) // 2)).from_buffer_copy(buf)
                    raw.FPDFText_SetText(obj, ctypes.cast(arr, ctypes.POINTER(ctypes.c_ushort)))
                    raw.FPDFPageObj_Transform(obj, 1, 0, 0, 1, 40, y)
                    raw.FPDFPage_InsertObject(page, obj)
                y -= 16
            raw.FPDFPage_GenerateContent(page)
    out = io.BytesIO()
    pdf.save(out)
    return out.getvalue()
