"""Compress PNG, JPEG and WEBP images until they fit a size limit: the PNG compressor of the old tool, generalised.

PNG: lossless optimise, then palette quantisation 256 … 8 colours, then downscale + 64 colours.
JPEG: lossless-style optimise (same quality), then quality 95 … 40, then downscale at quality 60.
WEBP: lossless, then quality 90 … 40, then downscale at quality 60.
``lossless_only`` stops after the first step and reports that the file is still too big instead of degrading it."""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ..errors import KafkaError

IMAGE_EXT = {"png": "png", "jpg": "jpeg", "jpeg": "jpeg", "jpe": "jpeg", "jfif": "jpeg", "webp": "webp"}
PNG_COLOURS = (256, 192, 128, 96, 64, 48, 32, 16, 8)
JPEG_QUALITIES = (95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 45, 40)
WEBP_QUALITIES = (90, 85, 80, 75, 70, 65, 60, 55, 50, 45, 40)
SCALES = (0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.30, 0.25, 0.20, 0.15, 0.10)


@dataclass
class Packed:
    ok: bool
    data: bytes
    strategy: str
    note: str = ""

    @property
    def size(self) -> int:
        return len(self.data)


def format_of(path: Path) -> Optional[str]:
    return IMAGE_EXT.get(path.suffix.lower().lstrip("."))


def _quantize(img: Any, colours: int) -> Any:
    from PIL import Image
    try:
        if img.mode == "RGBA":
            return img.quantize(colors=colours, method=Image.Quantize.FASTOCTREE)
        return img.quantize(colors=colours, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.FLOYDSTEINBERG)
    except (AttributeError, ValueError):
        return img.convert("P", palette=Image.ADAPTIVE, colors=colours)


def _encode(img: Any, fmt: str, **kw: Any) -> bytes:
    buf = io.BytesIO()
    img.save(buf, fmt, **kw)
    return buf.getvalue()


def _keep(img: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if img.info.get("icc_profile"):
        out["icc_profile"] = img.info["icc_profile"]
    if img.info.get("exif"):
        out["exif"] = img.info["exif"]
    return out


def _png(img: Any, limit: int, lossless_only: bool) -> Packed:
    keep = {k: v for k, v in _keep(img).items() if k == "icc_profile"}
    if img.info.get("dpi"):
        keep["dpi"] = img.info["dpi"]
    data = _encode(img, "PNG", optimize=True, compress_level=9, **keep)
    if len(data) <= limit:
        return Packed(True, data, "lossless")
    if lossless_only:
        return Packed(False, data, "lossless", "sigue pesando más que el límite sin perder calidad")
    has_alpha = img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info)
    base = img.convert("RGBA") if has_alpha else img.convert("RGB")
    best = data
    for n in PNG_COLOURS:
        data = _encode(_quantize(base, n), "PNG", optimize=True, compress_level=9)
        best = min(best, data, key=len)
        if len(data) <= limit:
            return Packed(True, data, f"quantize-{n}")
    for scale in SCALES:
        resized = base.resize((max(1, int(img.width * scale)), max(1, int(img.height * scale))), _lanczos())
        data = _encode(_quantize(resized, 64), "PNG", optimize=True, compress_level=9)
        best = min(best, data, key=len)
        if len(data) <= limit:
            return Packed(True, data, f"resize-{scale:.2f}+q64")
    return Packed(False, best, "exhausted", "no baja del límite ni reduciendo tamaño y colores")


def _lanczos() -> Any:
    from PIL import Image
    return getattr(Image, "Resampling", Image).LANCZOS


def _rgb_for_jpeg(img: Any) -> Any:
    if img.mode in ("RGB", "L", "CMYK"):
        return img
    if img.mode in ("RGBA", "LA", "P"):
        from PIL import Image
        rgba = img.convert("RGBA")
        bg = Image.new("RGB", rgba.size, (255, 255, 255))
        bg.paste(rgba, mask=rgba.split()[-1])
        return bg
    return img.convert("RGB")


def _jpeg(img: Any, limit: int, lossless_only: bool) -> Packed:
    img = _rgb_for_jpeg(img)
    keep = _keep(img)
    kept_quality: Any = "keep" if getattr(img, "format", None) == "JPEG" else 95
    try:
        data = _encode(img, "JPEG", quality=kept_quality, optimize=True, progressive=True, **keep)
    except (ValueError, OSError):
        data = _encode(img, "JPEG", quality=95, optimize=True, progressive=True, **keep)
    if len(data) <= limit:
        return Packed(True, data, "optimize")
    if lossless_only:
        return Packed(False, data, "optimize", "sigue pesando más que el límite sin perder calidad")
    best = data
    for q in JPEG_QUALITIES:
        data = _encode(img, "JPEG", quality=q, optimize=True, progressive=True, **keep)
        best = min(best, data, key=len)
        if len(data) <= limit:
            return Packed(True, data, f"quality-{q}")
    for scale in SCALES:
        resized = img.resize((max(1, int(img.width * scale)), max(1, int(img.height * scale))), _lanczos())
        data = _encode(resized, "JPEG", quality=60, optimize=True, progressive=True, **keep)
        best = min(best, data, key=len)
        if len(data) <= limit:
            return Packed(True, data, f"resize-{scale:.2f}+q60")
    return Packed(False, best, "exhausted", "no baja del límite ni reduciendo calidad y tamaño")


def _webp(img: Any, limit: int, lossless_only: bool) -> Packed:
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if "A" in img.mode or "transparency" in img.info else "RGB")
    keep = _keep(img)
    data = _encode(img, "WEBP", lossless=True, quality=100, method=6, **keep)
    if len(data) <= limit:
        return Packed(True, data, "lossless")
    if lossless_only:
        return Packed(False, data, "lossless", "sigue pesando más que el límite sin perder calidad")
    best = data
    for q in WEBP_QUALITIES:
        data = _encode(img, "WEBP", quality=q, method=4, **keep)
        best = min(best, data, key=len)
        if len(data) <= limit:
            return Packed(True, data, f"quality-{q}")
    for scale in SCALES:
        resized = img.resize((max(1, int(img.width * scale)), max(1, int(img.height * scale))), _lanczos())
        data = _encode(resized, "WEBP", quality=60, method=4)
        best = min(best, data, key=len)
        if len(data) <= limit:
            return Packed(True, data, f"resize-{scale:.2f}+q60")
    return Packed(False, best, "exhausted", "no baja del límite ni reduciendo calidad y tamaño")


def compress_image(path: Path, limit_bytes: int, lossless_only: bool = False) -> Packed:
    """Compress one image so it is at most ``limit_bytes``. Never touches the source. Raises ``KafkaError`` for files that are not images."""
    from PIL import Image
    fmt = format_of(path)
    if fmt is None:
        raise KafkaError("unsupported", f"«{path.name}» no es PNG, JPEG ni WEBP.")
    try:
        img = Image.open(path)
        frames = getattr(img, "n_frames", 1)
        if frames and frames > 1:
            return Packed(False, b"", "animated", "imagen animada: no se toca")
        img.load()
    except KafkaError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise KafkaError("unsupported", f"No puedo abrir «{path.name}» como imagen ({type(exc).__name__}).") from exc
    img.format = fmt.upper() if fmt != "jpeg" else "JPEG"
    return {"png": _png, "jpeg": _jpeg, "webp": _webp}[fmt](img, limit_bytes, lossless_only)


def list_images(root: Path, recursive: bool, skip_inside: Optional[Path] = None) -> list[Path]:
    """Image files under ``root`` (sorted); anything inside ``skip_inside`` (the output folder) is left out."""
    pattern = "**/*" if recursive else "*"
    out = []
    for p in sorted(root.glob(pattern), key=lambda x: str(x).lower()):
        if not p.is_file() or format_of(p) is None:
            continue
        if skip_inside is not None:
            try:
                p.resolve().relative_to(skip_inside.resolve())
                continue
            except ValueError:
                pass
        out.append(p)
    return out
