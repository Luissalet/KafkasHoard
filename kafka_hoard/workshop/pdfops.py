"""PDF operations on bytes and readers (pypdf, pypdfium2, Pillow). No database, no clock; the service decides where files go.

pypdf is imported once here and reported as missing in Spanish instead of stopping the whole app when it is not installed."""

from __future__ import annotations

import io
import math
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Optional

from ..errors import KafkaError

try:  # pragma: no cover - exercised through need_pypdf
    import pypdf
    from pypdf import PageObject, PdfReader, PdfWriter, Transformation
    from pypdf.generic import (DecodedStreamObject, DictionaryObject, FloatObject, NameObject, NumberObject)
except ImportError:  # pragma: no cover
    pypdf = None  # type: ignore[assignment]

PAGE_SIZES_PT = {"a3": (841.89, 1190.55), "a4": (595.276, 841.89), "a5": (419.53, 595.276), "letter": (612.0, 792.0), "legal": (612.0, 1008.0)}
PAGE_NAMES = {"A3": (297, 420), "A4": (210, 297), "A5": (148, 210), "Letter": (216, 279), "Legal": (216, 356)}
PRESETS = ("prepress", "printer", "ebook", "screen")
# (JPEG quality, maximum dpi) per step of the pypdf compressor, from the gentlest to the strongest; the first four match the presets
PYPDF_LEVELS: list[tuple[str, int, int]] = [("prepress", 90, 300), ("printer", 80, 200), ("ebook", 65, 150), ("screen", 45, 96),
                                             ("extra", 35, 72), ("minimo", 25, 50)]
MIN_IMAGE_PIXELS = 40_000
HELVETICA = {32: 278, 33: 278, 34: 355, 35: 556, 36: 556, 37: 889, 38: 667, 39: 191, 40: 333, 41: 333, 42: 389, 43: 584, 44: 278, 45: 333, 46: 278,
             47: 278, 58: 278, 59: 278, 60: 584, 61: 584, 62: 584, 63: 556, 64: 1015, 65: 667, 66: 667, 67: 722, 68: 722, 69: 667, 70: 611, 71: 778,
             72: 722, 73: 278, 74: 500, 75: 667, 76: 556, 77: 833, 78: 722, 79: 778, 80: 667, 81: 778, 82: 722, 83: 667, 84: 611, 85: 722, 86: 667,
             87: 944, 88: 667, 89: 667, 90: 611, 91: 278, 92: 278, 93: 278, 94: 469, 95: 556, 96: 333, 97: 556, 98: 556, 99: 500, 100: 556, 101: 556,
             102: 278, 103: 556, 104: 556, 105: 222, 106: 222, 107: 500, 108: 222, 109: 833, 110: 556, 111: 556, 112: 556, 113: 556, 114: 333,
             115: 500, 116: 278, 117: 556, 118: 500, 119: 722, 120: 500, 121: 500, 122: 500, 123: 334, 124: 260, 125: 334, 126: 584}
HELVETICA.update({c: 556 for c in range(48, 58)})
COLOURS = {"gris": "#808080", "gray": "#808080", "grey": "#808080", "rojo": "#c00000", "red": "#c00000", "azul": "#1f4eb4", "blue": "#1f4eb4",
           "negro": "#000000", "black": "#000000", "verde": "#1e8a3c", "green": "#1e8a3c", "naranja": "#e07000", "orange": "#e07000",
           "blanco": "#ffffff", "white": "#ffffff"}


def need_pypdf() -> None:
    if pypdf is None:
        raise KafkaError("not_configured", "Falta el paquete pypdf, que usa el taller de PDF.", "Ejecuta pip install -r requirements.txt y reinicia Kafka.")


def pypdf_version() -> Optional[str]:
    return getattr(pypdf, "__version__", None) if pypdf is not None else None


# ------------------------------------------------------------------ reading
def open_pdf(path: Path, password: str = "", label: str = "") -> "PdfReader":
    """Open a PDF, unlocking it with ``password`` (or the empty password). Errors are Spanish ``KafkaError``s."""
    need_pypdf()
    label = label or Path(path).name
    try:
        reader = PdfReader(str(path))
    except Exception as exc:  # noqa: BLE001 - PdfReadError, EmptyFileError, OSError…
        raise KafkaError("invalid", f"«{label}» no es un PDF válido o está dañado ({type(exc).__name__}).") from exc
    if reader.is_encrypted:
        unlocked = 0
        for pw in ([password] if password else []) + [""]:
            try:
                unlocked = int(reader.decrypt(pw))
            except pypdf.errors.DependencyError as exc:
                raise KafkaError("not_configured", "Falta el paquete cryptography para abrir PDF con cifrado AES.", "Ejecuta pip install -r requirements.txt.") from exc
            except Exception:  # noqa: BLE001
                unlocked = 0
            if unlocked:
                break
        if not unlocked:
            why = "La contraseña no es correcta." if password else "Indica la contraseña."
            raise KafkaError("invalid", f"«{label}» está protegido con contraseña. {why}", "Usa el argumento password (o current_password al proteger).")
    try:
        len(reader.pages)
    except Exception as exc:  # noqa: BLE001
        raise KafkaError("invalid", f"«{label}» no es un PDF válido o está dañado ({type(exc).__name__}).") from exc
    return reader


def page_count(reader: "PdfReader") -> int:
    return len(reader.pages)


def to_bytes(writer: "PdfWriter") -> bytes:
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def count_pages_of(data: bytes) -> int:
    need_pypdf()
    try:
        return len(PdfReader(io.BytesIO(data)).pages)
    except Exception as exc:  # noqa: BLE001
        raise KafkaError("invalid", f"El PDF resultante no se puede leer ({type(exc).__name__}).") from exc


def _meta_value(info: Any, key: str) -> str:
    try:
        value = info.get(key) if info else None
    except Exception:  # noqa: BLE001
        value = None
    return str(value).strip() if value not in (None, "") else ""


def read_metadata(reader: "PdfReader") -> dict[str, str]:
    info = None
    try:
        info = reader.metadata
    except Exception:  # noqa: BLE001
        pass
    out = {}
    for field_, key in (("title", "/Title"), ("author", "/Author"), ("subject", "/Subject"), ("keywords", "/Keywords"), ("creator", "/Creator"),
                        ("producer", "/Producer"), ("created", "/CreationDate"), ("modified", "/ModDate")):
        value = _meta_value(info, key)
        if value:
            out[field_] = value
    return out


def size_name(width_pt: float, height_pt: float) -> str:
    w, h = sorted((width_pt * 25.4 / 72, height_pt * 25.4 / 72))
    for name, (a, b) in PAGE_NAMES.items():
        if abs(w - a) <= 3 and abs(h - b) <= 3:
            return name
    return ""


def page_sizes(reader: "PdfReader") -> list[dict[str, Any]]:
    """Consecutive pages with the same size are grouped: [{pages: "1-3", width_mm, height_mm, name}]."""
    sizes: list[tuple[float, float]] = []
    for page in reader.pages:
        w, h = float(page.mediabox.width), float(page.mediabox.height)
        if (page.rotation or 0) % 180:
            w, h = h, w
        sizes.append((round(w * 25.4 / 72), round(h * 25.4 / 72)))
    out: list[dict[str, Any]] = []
    start = 0
    for i in range(1, len(sizes) + 1):
        if i == len(sizes) or sizes[i] != sizes[start]:
            w, h = sizes[start]
            out.append({"pages": f"{start + 1}" if i - start == 1 else f"{start + 1}-{i}", "width_mm": w, "height_mm": h,
                        "name": size_name(w * 72 / 25.4, h * 72 / 25.4)})
            start = i
    return out


def text_probe(path: Path, password: str = "", sample: int = 5) -> dict[str, Any]:
    """Does the PDF have a text layer? Looks at up to ``sample`` pages with pypdfium2 (the reader Kafka uses for filing)."""
    try:
        import pypdfium2 as pdfium
    except ImportError:  # pragma: no cover
        return {"has_text": None, "sampled_pages": 0}
    try:
        doc = pdfium.PdfDocument(str(path), password=password or None)
    except Exception:  # noqa: BLE001
        return {"has_text": None, "sampled_pages": 0}
    try:
        n = len(doc)
        picks = sorted({0, n // 2, n - 1, *range(min(n, sample))})[:max(sample, 3)]
        with_text = 0
        for i in picks:
            page = doc[i]
            try:
                tp = page.get_textpage()
                text = (tp.get_text_range() or "").strip()
                tp.close()
            except Exception:  # noqa: BLE001
                text = ""
            finally:
                page.close()
            if len("".join(text.split())) >= 8:
                with_text += 1
        return {"has_text": with_text > 0, "text_pages_sampled": with_text, "sampled_pages": len(picks), "total_pages": n}
    finally:
        doc.close()


# ------------------------------------------------------------------ merge, split, pages
def merge(parts: list[tuple["PdfReader", Optional[list[int]], str]]) -> tuple[bytes, int]:
    """``parts``: (reader, 0-based page indexes or None for all, label). Bookmarks of each file are kept."""
    need_pypdf()
    writer = PdfWriter()
    for reader, indexes, label in parts:
        try:
            writer.append(reader, pages=indexes)
        except Exception:  # noqa: BLE001 - odd outlines: fall back to plain pages
            for i in (indexes if indexes is not None else range(len(reader.pages))):
                writer.add_page(reader.pages[i])
    total = len(writer.pages)
    return to_bytes(writer), total


def pages_to_bytes(reader: "PdfReader", indexes: Iterable[int]) -> bytes:
    need_pypdf()
    writer = PdfWriter()
    for i in indexes:
        writer.add_page(reader.pages[i])
    return to_bytes(writer)


def rotate(reader: "PdfReader", indexes: set[int], degrees: int) -> bytes:
    need_pypdf()
    writer = PdfWriter(clone_from=reader)
    for i in indexes:
        page = writer.pages[i]
        page[NameObject("/Rotate")] = NumberObject((int(page.rotation or 0) + degrees) % 360)
    return to_bytes(writer)


# ------------------------------------------------------------------ protect
def protect(reader: "PdfReader", user_password: str, owner_password: str, *, allow_print: bool, allow_copy: bool, allow_modify: bool) -> bytes:
    need_pypdf()
    from pypdf.constants import UserAccessPermissions as P
    perms = P.all()
    if not allow_print:
        perms &= ~(P.PRINT | P.PRINT_TO_REPRESENTATION)
    if not allow_copy:
        perms &= ~P.EXTRACT_TEXT_AND_GRAPHICS
    if not allow_modify:
        perms &= ~(P.MODIFY | P.ADD_OR_MODIFY)
    writer = PdfWriter(clone_from=reader)
    try:
        writer.encrypt(user_password=user_password, owner_password=owner_password or None, permissions_flag=perms, algorithm="AES-256")
    except pypdf.errors.DependencyError as exc:
        raise KafkaError("not_configured", "Falta el paquete cryptography para cifrar con AES-256.", "Ejecuta pip install -r requirements.txt.") from exc
    return to_bytes(writer)


def unprotect(reader: "PdfReader") -> bytes:
    need_pypdf()
    writer = PdfWriter(clone_from=reader)
    return to_bytes(writer)


# ------------------------------------------------------------------ metadata
def set_metadata(reader: "PdfReader", values: dict[str, Optional[str]]) -> bytes:
    """``values``: title, author, subject, keywords, creator. None leaves a field alone; an empty string blanks it."""
    need_pypdf()
    writer = PdfWriter(clone_from=reader)
    keys = {"title": "/Title", "author": "/Author", "subject": "/Subject", "keywords": "/Keywords", "creator": "/Creator"}
    update = {keys[k]: v for k, v in values.items() if k in keys and v is not None}
    if update:
        writer.add_metadata(update)
    return to_bytes(writer)


# ------------------------------------------------------------------ watermark
def parse_colour(text: str) -> tuple[float, float, float]:
    raw = COLOURS.get(str(text or "").strip().lower(), str(text or "").strip())
    m = re.fullmatch(r"#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})", raw)
    if not m:
        raise KafkaError("invalid", f"No entiendo el color «{text}».", "Usa un nombre (gris, rojo, azul, negro, verde) o un código como #808080.")
    h = m.group(1)
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return tuple(int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))  # type: ignore[return-value]


def _glyph_width(ch: str) -> int:
    code = ord(ch)
    if code in HELVETICA:
        return HELVETICA[code]
    base = unicodedata.normalize("NFD", ch)[:1]
    if base and ord(base) in HELVETICA:
        return HELVETICA[ord(base)]
    return 556


def text_width(text: str, size: float) -> float:
    return sum(_glyph_width(c) for c in text) * size / 1000.0


def _pdf_string(text: str) -> str:
    out = []
    for byte in text.encode("cp1252", errors="replace"):
        if byte in (0x28, 0x29, 0x5C):
            out.append("\\" + chr(byte))
        elif 32 <= byte < 127:
            out.append(chr(byte))
        else:
            out.append(f"\\{byte:03o}")
    return "".join(out)


def watermark_content(text: str, *, cx: float, cy: float, width: float, height: float, size: float, angle: float, colour: tuple[float, float, float]) -> bytes:
    """Content stream that draws ``text`` centred at (cx, cy), rotated ``angle`` degrees counter-clockwise, in Helvetica."""
    theta = math.radians(angle)
    cos, sin = math.cos(theta), math.sin(theta)
    chord = min(width / abs(cos) if abs(cos) > 1e-6 else math.inf, height / abs(sin) if abs(sin) > 1e-6 else math.inf)
    textw = text_width(text, size)
    if textw > 0.9 * chord:
        size = max(6.0, size * 0.9 * chord / textw)
        textw = text_width(text, size)
    # origin of the text so that its middle (half its width, a third of its height) lands on the centre
    ox = cx - (textw / 2) * cos + (size * 0.3) * sin
    oy = cy - (textw / 2) * sin - (size * 0.3) * cos
    r, g, b = colour
    return (f"q /GSWM gs {r:.3f} {g:.3f} {b:.3f} rg BT /FWM {size:.2f} Tf {cos:.5f} {sin:.5f} {-sin:.5f} {cos:.5f} {ox:.2f} {oy:.2f} Tm "
            f"({_pdf_string(text)}) Tj ET Q").encode("ascii")


def _overlay(width: float, height: float, content: bytes, opacity: float) -> "PageObject":
    page = PageObject.create_blank_page(width=width, height=height)
    font = DictionaryObject({NameObject("/Type"): NameObject("/Font"), NameObject("/Subtype"): NameObject("/Type1"),
                             NameObject("/BaseFont"): NameObject("/Helvetica"), NameObject("/Encoding"): NameObject("/WinAnsiEncoding")})
    gs = DictionaryObject({NameObject("/Type"): NameObject("/ExtGState"), NameObject("/ca"): FloatObject(opacity), NameObject("/CA"): FloatObject(opacity)})
    page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"): DictionaryObject({NameObject("/FWM"): font}),
                                                       NameObject("/ExtGState"): DictionaryObject({NameObject("/GSWM"): gs})})
    stream = DecodedStreamObject()
    stream.set_data(content)
    page[NameObject("/Contents")] = stream
    return page


def watermark(reader: "PdfReader", indexes: set[int], *, text: str, opacity: float, angle: float, font_size: float, colour: tuple[float, float, float]) -> bytes:
    need_pypdf()
    text = re.sub(r"\s+", " ", text or "").strip()
    if not text:
        raise KafkaError("invalid", "Falta el texto de la marca de agua.")
    writer = PdfWriter(clone_from=reader)
    for i in sorted(indexes):
        page = writer.pages[i]
        box = page.mediabox
        left, bottom = float(box.left), float(box.bottom)
        w, h = float(box.width), float(box.height)
        visual = angle + (page.rotation or 0)           # the page is shown rotated clockwise by /Rotate, so the text turns the other way round
        content = watermark_content(text, cx=left + w / 2, cy=bottom + h / 2, width=w, height=h, size=font_size, angle=visual, colour=colour)
        page.merge_page(_overlay(w, h, content, opacity))
    return to_bytes(writer)


# ------------------------------------------------------------------ compress (pypdf)
def _recompress_images(writer: "PdfWriter", quality: int, max_dpi: int) -> int:
    """Re-encode the big colour and grey images as JPEG at ``quality`` and at most ``max_dpi`` (measured against the page width)."""
    from PIL import Image
    done: set[int] = set()
    changed = 0
    for page in writer.pages:
        width_in = max(0.5, float(page.mediabox.width) / 72.0)
        try:
            images = list(page.images)
        except Exception:  # noqa: BLE001 - an unreadable image list must not stop the rest
            continue
        for img in images:
            ref = getattr(img, "indirect_reference", None)
            if ref is None or ref.idnum in done:
                continue
            done.add(ref.idnum)
            try:
                obj = ref.get_object()
                if obj.get("/SMask") is not None or obj.get("/Mask") is not None or obj.get("/ImageMask"):
                    continue
                pil = img.image
                if pil is None or pil.width * pil.height < MIN_IMAGE_PIXELS:
                    continue
                if pil.mode == "CMYK":
                    pil = pil.convert("RGB")
                if pil.mode not in ("RGB", "L"):
                    continue
                dpi = pil.width / width_in
                if dpi > max_dpi:
                    factor = max_dpi / dpi
                    pil = pil.resize((max(1, int(pil.width * factor)), max(1, int(pil.height * factor))), Image.LANCZOS)
                buf = io.BytesIO()
                pil.save(buf, "JPEG", quality=quality, optimize=True)
                data = buf.getvalue()
                old = len(getattr(obj, "_data", b"") or b"")
                if old and len(data) >= old * 0.95:
                    continue
                obj._data = data
                obj[NameObject("/Filter")] = NameObject("/DCTDecode")
                for key in ("/DecodeParms", "/Decode"):
                    if key in obj:
                        del obj[key]
                obj[NameObject("/Width")] = NumberObject(pil.width)
                obj[NameObject("/Height")] = NumberObject(pil.height)
                obj[NameObject("/ColorSpace")] = NameObject("/DeviceGray" if pil.mode == "L" else "/DeviceRGB")
                obj[NameObject("/BitsPerComponent")] = NumberObject(8)
                changed += 1
            except Exception:  # noqa: BLE001 - leave that image as it was
                continue
    return changed


def compress_pypdf(reader: "PdfReader", quality: Optional[int], max_dpi: Optional[int]) -> tuple[bytes, int]:
    """Lossless clean-up (identical objects, deflated content) and, when ``quality`` is given, image re-encoding. -> (bytes, images changed)"""
    need_pypdf()
    writer = PdfWriter(clone_from=reader)
    changed = 0
    if quality:
        changed = _recompress_images(writer, quality, max_dpi or 150)
    for page in writer.pages:
        try:
            page.compress_content_streams()
        except Exception:  # noqa: BLE001
            pass
    try:
        writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)
    except Exception:  # noqa: BLE001
        pass
    return to_bytes(writer), changed


# ------------------------------------------------------------------ images <-> pdf
def _flatten(im: Any) -> Any:
    from PIL import Image
    if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
        rgba = im.convert("RGBA")
        bg = Image.new("RGB", rgba.size, (255, 255, 255))
        bg.paste(rgba, mask=rgba.split()[-1])
        return bg
    if im.mode.startswith("I") or im.mode == "F":
        return im.point(lambda v: v * (1 / 256)).convert("L")
    if im.mode not in ("RGB", "L"):
        return im.convert("RGB")
    return im


def open_image(path: Path) -> Any:
    """Open an image with the EXIF orientation applied and transparency flattened onto white."""
    from PIL import Image, ImageOps
    try:
        try:
            import pillow_heif  # type: ignore[import-not-found]
            pillow_heif.register_heif_opener()
        except Exception:  # noqa: BLE001 - HEIC is optional
            pass
        im = Image.open(path)
        im.load()
    except Exception as exc:  # noqa: BLE001
        hint = " Los HEIC necesitan el paquete pillow-heif." if path.suffix.lower() in (".heic", ".heif") else ""
        raise KafkaError("unsupported", f"No puedo abrir la imagen «{path.name}» ({type(exc).__name__}).{hint}") from exc
    try:
        im = ImageOps.exif_transpose(im)
    except Exception:  # noqa: BLE001
        pass
    return _flatten(im)


def images_to_pdf(images: list[tuple[Any, str]], *, page_size: str, margin_mm: float, orientation: str) -> bytes:
    """One page per image, the image centred at full resolution. ``page_size``: a4, letter, a3, legal or fit (the page takes the image's size)."""
    need_pypdf()
    margin = max(0.0, margin_mm) * 72 / 25.4
    writer = PdfWriter()
    for im, label in images:
        w_px, h_px = im.size
        if page_size == "fit":
            dpi = 150.0
            longest = max(w_px, h_px) * 72 / dpi
            if longest > 1440:
                dpi = max(w_px, h_px) * 72 / 1440
            dw, dh = w_px * 72 / dpi, h_px * 72 / dpi
            pw, ph = dw + 2 * margin, dh + 2 * margin
        else:
            pw, ph = PAGE_SIZES_PT[page_size]
            landscape = orientation == "landscape" or (orientation == "auto" and w_px > h_px)
            pw, ph = (max(pw, ph), min(pw, ph)) if landscape else (min(pw, ph), max(pw, ph))
            avail_w, avail_h = max(10.0, pw - 2 * margin), max(10.0, ph - 2 * margin)
            scale = min(avail_w / w_px, avail_h / h_px)
            dw, dh = w_px * scale, h_px * scale
        buf = io.BytesIO()
        try:
            im.save(buf, "PDF", resolution=72.0 * w_px / dw, quality=90)
        except Exception as exc:  # noqa: BLE001
            raise KafkaError("unsupported", f"No puedo convertir «{label}» a PDF ({type(exc).__name__}).") from exc
        image_page = PdfReader(buf).pages[0]
        blank = PageObject.create_blank_page(width=pw, height=ph)
        blank.merge_transformed_page(image_page, Transformation().translate((pw - dw) / 2, (ph - dh) / 2))
        writer.add_page(blank)
    return to_bytes(writer)


def render_pages(path: Path, indexes: list[int], *, dpi: int, password: str = "") -> Iterable[tuple[int, Any]]:
    """Yield (0-based index, PIL image) for each page, rendered by pypdfium2 at ``dpi``."""
    import pypdfium2 as pdfium
    try:
        doc = pdfium.PdfDocument(str(path), password=password or None)
    except Exception as exc:  # noqa: BLE001
        raise KafkaError("invalid", f"No puedo abrir «{path.name}» para dibujar sus páginas ({type(exc).__name__}).") from exc
    try:
        for i in indexes:
            page = doc[i]
            try:
                bitmap = page.render(scale=dpi / 72.0)
                yield i, bitmap.to_pil()
            finally:
                page.close()
    finally:
        doc.close()
