"""Turn the bytes of a file into text per page: PDF (text layer, OCR for scanned pages), images (OCR), e-mails (.eml and their
attachments), plain text, HTML and .docx. Everything else is reported as unsupported. Originals are never modified."""

from __future__ import annotations

import email
import email.policy
import email.utils
import html as _html
import io
import re
import zipfile
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import PurePath
from typing import Any, Optional

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
    """Extension of the file by its content when the name lies or is missing."""
    ext = file_ext(name)
    head = data[:16]
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"\x89PNG"):
        return "png"
    if head.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if head[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    if head.startswith(b"PK\x03\x04") and ext == "docx":
        return "docx"
    return ext


def clean_text(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "").replace(" ", " ").replace("​", "")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def useful_chars(text: str) -> int:
    return sum(1 for c in text if c.isalnum())


# ------------------------------------------------------------------------------------------------------------ html
class _HtmlText(HTMLParser):
    BLOCK = {"br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table", "section", "article", "ul", "ol", "hr"}
    SKIP = {"script", "style", "head", "title", "noscript"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in self.SKIP:
            self._skip += 1
        elif tag in self.BLOCK:
            self.parts.append("\n")
        elif tag in ("td", "th"):
            self.parts.append("  ")

    def handle_endtag(self, tag: str) -> None:
        if tag in self.SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in self.BLOCK:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)


def html_to_text(raw: str) -> str:
    parser = _HtmlText()
    try:
        parser.feed(raw[:3_000_000])
        parser.close()
    except Exception:  # noqa: BLE001 — broken markup: fall back to stripping tags
        return clean_text(_html.unescape(re.sub(r"<[^>]+>", " ", raw)))
    return clean_text("".join(parser.parts))


def decode_text(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16") if data[:2] in (b"\xff\xfe", b"\xfe\xff") else ("utf-8-sig",):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace")


def split_pages(text: str, size: int = TEXT_PAGE_CHARS) -> list[str]:
    text = clean_text(text)
    if len(text) <= size * 2:
        return [text] if text else []
    pages, current = [], ""
    pieces: list[str] = []
    for para in text.split("\n\n"):
        if len(para) <= size:
            pieces.append(para)
            continue
        chunk = ""
        for line in para.split("\n"):          # one huge paragraph (OCR output, a pasted mail): cut it on line breaks
            if chunk and len(chunk) + len(line) > size:
                pieces.append(chunk)
                chunk = ""
            chunk += line + "\n"
        if chunk.strip():
            pieces.append(chunk)
    for para in pieces:
        if current and len(current) + len(para) > size:
            pages.append(current.strip())
            current = ""
        current += para + "\n\n"
    if current.strip():
        pages.append(current.strip())
    return pages


# ------------------------------------------------------------------------------------------------------------ docx
def read_docx(data: bytes) -> list[str]:
    import xml.etree.ElementTree as ET

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            info = z.getinfo("word/document.xml")
            if info.file_size > 30_000_000:
                raise ReadError("docx too large")
            xml = z.read("word/document.xml")
    except (zipfile.BadZipFile, KeyError) as exc:
        raise ReadError("not a valid .docx") from exc
    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise ReadError("docx XML is damaged") from exc
    lines: list[str] = []
    for para in root.iter(f"{ns}p"):
        buf: list[str] = []
        for node in para.iter():
            if node.tag == f"{ns}t" and node.text:
                buf.append(node.text)
            elif node.tag == f"{ns}tab":
                buf.append("\t")
            elif node.tag == f"{ns}br":
                buf.append("\n")
        lines.append("".join(buf))
    return split_pages("\n".join(lines))


# ------------------------------------------------------------------------------------------------------------ pdf
def read_pdf(data: bytes, ocr: Ocr, notes: list[str]) -> tuple[list[str], int]:
    try:
        import pypdfium2 as pdfium  # type: ignore
    except ImportError as exc:  # pragma: no cover - listed in requirements
        raise ReadError("pypdfium2 is not installed") from exc
    try:
        pdf = pdfium.PdfDocument(data)
    except Exception as exc:  # noqa: BLE001 — pdfium raises PdfiumError for encrypted or corrupt files
        raise ReadError(f"PDF cannot be opened ({type(exc).__name__})") from exc
    pages: list[str] = []
    ocr_pages = 0
    try:
        count = min(len(pdf), MAX_PDF_PAGES)
        if len(pdf) > MAX_PDF_PAGES:
            notes.append(f"only the first {MAX_PDF_PAGES} pages were read")
        ocr_ok, ocr_why = ocr.available()
        skipped_ocr = 0
        for index in range(count):
            page = pdf[index]
            try:
                textpage = page.get_textpage()
                try:
                    text = clean_text(textpage.get_text_bounded())
                finally:
                    textpage.close()
                if useful_chars(text) < MIN_USEFUL_CHARS:
                    if ocr_ok and ocr_pages < MAX_OCR_PAGES:
                        image = page.render(scale=OCR_DPI / 72.0).to_pil()
                        ocr_text = clean_text(ocr.read(image))
                        if useful_chars(ocr_text) > useful_chars(text):
                            text = ocr_text
                            ocr_pages += 1
                    else:
                        skipped_ocr += 1
            finally:
                page.close()
            pages.append(text)
        if skipped_ocr:
            notes.append(f"{skipped_ocr} page(s) have no text layer and OCR is unavailable ({ocr_why})" if not ocr_ok
                         else f"{skipped_ocr} page(s) were not OCR'd (limit {MAX_OCR_PAGES})")
    finally:
        pdf.close()
    return pages, ocr_pages


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
