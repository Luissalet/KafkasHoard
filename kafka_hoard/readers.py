"""Turn the bytes of a file into text per page: PDF (text layer, OCR for scanned pages), images (OCR), e-mails (.eml and their
attachments), plain text, HTML and .docx. Everything else is reported as unsupported. Originals are never modified.

The type sniffing, text cleaning, text decoding, page splitting, HTML-to-text and the .docx reader are the shared ones of Hoard Link
(``hoard_link.docs``); what stays here is Kafka's own: the PDF read with an OCR pass per page, the image read, and the e-mail reader
that also hands back the attachments as children to file."""

from __future__ import annotations

import email
import email.policy
import email.utils
import io
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Any, Callable, Optional

from .hoard_link.docs import readers_lite, textclean
from .hoard_link.docs.sniff import sniff as _sniff
from .ocr import Ocr

MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_TEXT_CHARS = 2_000_000
MIN_USEFUL_CHARS = 40          # a PDF page with fewer useful characters is OCR'd (when OCR is available)
OCR_DPI = 200
MAX_OCR_PAGES = 40
MAX_PDF_PAGES = 400
TEXT_PAGE_CHARS = 6000

PDF_EXT = {"pdf"}
IMAGE_EXT = {"png", "jpg", "jpeg", "webp", "heic", "heif", "bmp", "tif", "tiff", "gif"}
TEXT_EXT = {"txt", "md", "text", "csv"}
HTML_EXT = {"html", "htm", "xhtml"}
MIME = {"pdf": "application/pdf", "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp", "gif": "image/gif",
        "bmp": "image/bmp", "tif": "image/tiff", "tiff": "image/tiff", "heic": "image/heic", "heif": "image/heif", "eml": "message/rfc822",
        "txt": "text/plain", "md": "text/markdown", "text": "text/plain", "csv": "text/csv", "html": "text/html", "htm": "text/html",
        "xhtml": "application/xhtml+xml", "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
SUPPORTED_EXT = PDF_EXT | IMAGE_EXT | TEXT_EXT | HTML_EXT | {"eml", "docx"}


class ReadError(Exception):
    """The file cannot be read (corrupt, encrypted…); the document is still stored and goes to review."""


@dataclass
class Child:
    """An attachment found inside an e-mail file, ingested as its own document."""
    name: str
    data: bytes
    mime: str


@dataclass
class ReadResult:
    kind: str                                   # pdf | image | eml | text | html | docx | unsupported
    ext: str
    mime: str
    pages: list[str] = field(default_factory=list)
    ocr_pages: int = 0
    notes: list[str] = field(default_factory=list)
    children: list[Child] = field(default_factory=list)
    mail: dict[str, Any] = field(default_factory=dict)      # subject, from_name, from_address, ts, message_id (eml)
    body_is_text: bool = False                              # eml: the body itself has text worth classifying


def file_ext(name: str) -> str:
    return PurePath(name or "").suffix.lower().lstrip(".")[:8]


def sniff(name: str, data: bytes) -> str:
    """Extension of the file by its content when the name lies or is missing («» when neither says)."""
    ext = file_ext(name)
    found = _sniff(name, data)
    if found.kind in ("zip", "unknown", "text") and ext in SUPPORTED_EXT:
        return ext                      # a damaged .docx is still a .docx: the reader says so and the document goes to review
    if found.kind in ("text", "unknown", "zip", "archive", "ole"):
        return ext                      # a file Kafka does not read stays what its name says (and is reported as unsupported)
    return found.ext


def clean_text(text: str, *, layout: bool = False) -> str:
    """Tidy extracted text. ``layout`` also re-joins words hyphenated across lines and one-word-per-line PDF output."""
    return textclean.clean_text(text, dehyphenate=layout, unstack=layout)


useful_chars = textclean.useful_chars
decode_text = textclean.decode_text
html_to_text = readers_lite.html_to_text


def split_pages(text: str, size: int = TEXT_PAGE_CHARS) -> list[str]:
    return textclean.split_pages(text, size)


# ------------------------------------------------------------------------------------------------------------ docx
def read_docx(data: bytes) -> list[str]:
    try:
        units = readers_lite.read_docx(data, max_unzipped=300_000_000)
    except readers_lite.ZipBombError as exc:
        raise ReadError(f"docx refused: {exc}") from exc
    except ValueError as exc:
        raise ReadError("not a valid .docx" if "zip" in str(exc).lower() else f"the .docx cannot be read ({exc})") from exc
    return split_pages("\n\n".join("\n".join(x for x in (u.get("title"), u.get("text")) if x) for u in units))


# ------------------------------------------------------------------------------------------------------------ pdf
@dataclass
class PdfRead:
    """What a PDF read found: the text of each page (OCR where the text layer is missing), and what is still unread."""
    pages: list[str]
    ocr_pages: int = 0
    title: str = ""
    total: int = 0                                              # pages the file has (``pages`` may hold fewer: ``max_pages``)
    unread: list[int] = field(default_factory=list)             # 1-based pages still without usable text
    not_attempted: list[int] = field(default_factory=list)      # of those, the ones OCR never tried (off, unavailable, over the limit)


def _open_pdf(data: bytes) -> Any:
    try:
        import pypdfium2 as pdfium  # type: ignore
    except ImportError as exc:  # pragma: no cover - listed in requirements
        raise ReadError("pypdfium2 is not installed") from exc
    try:
        return pdfium.PdfDocument(data)
    except Exception as exc:  # noqa: BLE001 — pdfium raises PdfiumError for encrypted or corrupt files
        raise ReadError(f"PDF cannot be opened ({type(exc).__name__})") from exc


def read_pdf_detail(data: bytes, ocr: Ocr, notes: list[str], *, max_pages: int = MAX_PDF_PAGES, max_ocr: int = MAX_OCR_PAGES, mode: str = "auto",
                    dpi: int = OCR_DPI, lang: Optional[str] = None, progress: Optional[Callable[[int, int], None]] = None,
                    layout: bool = False) -> PdfRead:
    """Text of every page. ``mode``: ``auto`` runs OCR only on pages with less than ``MIN_USEFUL_CHARS`` letters and digits, ``off`` never,
    ``force`` on every page (the OCR text wins whenever it found something). At most ``max_ocr`` pages are recognised."""
    pdf = _open_pdf(data)
    out = PdfRead(pages=[])
    try:
        out.total = len(pdf)
        try:
            out.title = str((pdf.get_metadata_dict() or {}).get("Title") or "").strip()
        except Exception:  # noqa: BLE001
            out.title = ""
        count = min(out.total, max_pages)
        if out.total > max_pages:
            notes.append(f"only the first {max_pages} pages were read")
        ocr_ok, ocr_why = ocr.available()
        if mode == "off":
            ocr_ok, ocr_why = False, "OCR was switched off for this read"
        failed = 0
        for index in range(count):
            page = pdf[index]
            try:
                textpage = page.get_textpage()
                try:
                    text = clean_text(textpage.get_text_bounded(), layout=layout)
                finally:
                    textpage.close()
                wanted = mode == "force" or useful_chars(text) < MIN_USEFUL_CHARS
                if wanted:
                    if ocr_ok and out.ocr_pages + failed < max_ocr:
                        image = page.render(scale=dpi / 72.0).to_pil()
                        result = ocr.recognise(image, lang)
                        ocr_text = clean_text(result.text)
                        if result.error:
                            failed += 1
                        if (mode == "force" and ocr_text) or useful_chars(ocr_text) > useful_chars(text):
                            text = ocr_text
                            out.ocr_pages += 1
                    elif useful_chars(text) < MIN_USEFUL_CHARS:
                        out.not_attempted.append(index + 1)
                if useful_chars(text) < MIN_USEFUL_CHARS:
                    out.unread.append(index + 1)
            finally:
                page.close()
            out.pages.append(text)
            if progress is not None:
                progress(index + 1, count)
        skipped = len(out.not_attempted)
        if skipped:
            notes.append(f"{skipped} page(s) have no text layer and OCR is unavailable ({ocr_why})" if not ocr_ok
                         else f"{skipped} page(s) were not OCR'd (limit {max_ocr})")
    finally:
        pdf.close()
    return out


def read_pdf(data: bytes, ocr: Ocr, notes: list[str]) -> tuple[list[str], int]:
    read = read_pdf_detail(data, ocr, notes)
    return read.pages, read.ocr_pages


def ocr_pdf_pages(data: bytes, ocr: Ocr, numbers: list[int], *, dpi: int = OCR_DPI, lang: Optional[str] = None,
                  progress: Optional[Callable[[int, int], None]] = None) -> list[tuple[int, str, str]]:
    """OCR of the given 1-based pages, without reading their text layer: ``[(page, text, error)]``."""
    pdf = _open_pdf(data)
    done: list[tuple[int, str, str]] = []
    try:
        total = len(pdf)
        for k, number in enumerate(numbers, start=1):
            if not 1 <= number <= total:
                raise ReadError(f"page {number} does not exist (the PDF has {total})")
            page = pdf[number - 1]
            try:
                image = page.render(scale=dpi / 72.0).to_pil()
            finally:
                page.close()
            result = ocr.recognise(image, lang)
            done.append((number, clean_text(result.text), result.error))
            if progress is not None:
                progress(k, len(numbers))
    finally:
        pdf.close()
    return done


def pdf_page_count(data: bytes) -> int:
    pdf = _open_pdf(data)
    try:
        return len(pdf)
    finally:
        pdf.close()


def render_pdf_page(data: bytes, number: int, scale: float = 1.6) -> bytes:
    """PNG bytes of page ``number`` (1-based) for the preview."""
    import pypdfium2 as pdfium  # type: ignore

    pdf = pdfium.PdfDocument(data)
    try:
        if not 1 <= number <= len(pdf):
            raise ReadError("no such page")
        page = pdf[number - 1]
        try:
            image = page.render(scale=scale).to_pil()
        finally:
            page.close()
        buf = io.BytesIO()
        image.convert("RGB").save(buf, "PNG", optimize=False)
        return buf.getvalue()
    finally:
        pdf.close()


# ------------------------------------------------------------------------------------------------------------ image
def read_image(data: bytes, ocr: Ocr, notes: list[str]) -> tuple[list[str], int]:
    try:
        from PIL import Image, ImageOps
    except ImportError as exc:  # pragma: no cover
        raise ReadError("pillow is not installed") from exc
    try:
        image = Image.open(io.BytesIO(data))
        image.load()
    except Exception as exc:  # noqa: BLE001 — unsupported codec (HEIC without pillow-heif), corrupt file
        raise ReadError(f"image cannot be opened ({type(exc).__name__})") from exc
    try:
        image = ImageOps.exif_transpose(image)
    except Exception:  # noqa: BLE001
        pass
    ok, why = ocr.available()
    if not ok:
        notes.append(f"image without text: OCR is unavailable ({why})")
        return [""], 0
    text = clean_text(ocr.read(image))
    if not text:
        notes.append("OCR found no text in the image")
    return [text], 1 if text else 0


# ------------------------------------------------------------------------------------------------------------ eml
def read_eml(data: bytes) -> ReadResult:
    msg = email.message_from_bytes(data, policy=email.policy.default)
    subject = str(msg.get("Subject") or "")
    name, address = email.utils.parseaddr(str(msg.get("From") or ""))
    try:
        when = email.utils.parsedate_to_datetime(str(msg.get("Date") or ""))
        ts: Optional[float] = when.timestamp()
    except (TypeError, ValueError, IndexError):
        ts = None
    body = ""
    try:
        part = msg.get_body(preferencelist=("plain", "html"))
        if part is not None:
            raw = part.get_content()
            body = html_to_text(raw) if part.get_content_type() == "text/html" else clean_text(raw)
    except Exception:  # noqa: BLE001 — undecodable charset
        body = ""
    children: list[Child] = []
    for att in msg.iter_attachments():
        filename = att.get_filename() or ""
        ctype = att.get_content_type()
        ext = file_ext(filename) or {"application/pdf": "pdf", "image/png": "png", "image/jpeg": "jpg"}.get(ctype, "")
        if ext not in PDF_EXT | IMAGE_EXT | {"docx", "txt", "html", "htm"}:
            continue
        try:
            payload = att.get_payload(decode=True) or b""
        except Exception:  # noqa: BLE001
            continue
        if payload and len(payload) <= 15 * 1024 * 1024:
            children.append(Child(filename or f"attachment.{ext}", payload, ctype))
    result = ReadResult("eml", "eml", MIME["eml"], pages=[body] if body else [], children=children[:10],
                        mail={"subject": subject, "from_name": name, "from_address": address, "ts": ts,
                              "message_id": str(msg.get("Message-ID") or "").strip()})
    return result


# ------------------------------------------------------------------------------------------------------------ entry point
def read_bytes(name: str, data: bytes, ocr: Ocr) -> ReadResult:
    """Read one file. Raises ``ReadError`` when a supported type cannot be opened."""
    ext = sniff(name, data)
    notes: list[str] = []
    if ext in PDF_EXT:
        pages, ocr_pages = read_pdf(data, ocr, notes)
        return ReadResult("pdf", "pdf", MIME["pdf"], pages, ocr_pages, notes)
    if ext in IMAGE_EXT:
        pages, ocr_pages = read_image(data, ocr, notes)
        return ReadResult("image", ext, MIME.get(ext, "image/" + ext), pages, ocr_pages, notes)
    if ext == "eml":
        return read_eml(data)
    if ext in TEXT_EXT:
        return ReadResult("text", ext, MIME[ext], split_pages(decode_text(data)[:MAX_TEXT_CHARS]), 0, notes)
    if ext in HTML_EXT:
        return ReadResult("html", ext, MIME[ext], split_pages(html_to_text(decode_text(data)[:MAX_TEXT_CHARS])), 0, notes)
    if ext == "docx":
        return ReadResult("docx", "docx", MIME["docx"], read_docx(data), 0, notes)
    return ReadResult("unsupported", ext or "bin", "application/octet-stream", [], 0, ["unsupported type"])
