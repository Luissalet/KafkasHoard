"""Compress PNG, JPEG and WEBP images until they fit a size limit: the PNG compressor of the old tool, generalised.

The ladder itself (lossless first, then fewer colours or lower quality, then smaller) lives in Hoard Link
(``hoard_link.docs.imaging.compress_to_limit``, which began as this module); what stays here is the file-type check by extension,
the Spanish wording of the notes and the folder listing.

PNG: lossless optimise, then palette quantisation 256 … 8 colours, then downscale + 64 colours.
JPEG: lossless-style optimise (same quality), then quality 95 … 40, then downscale at quality 60.
WEBP: lossless, then quality 90 … 40, then downscale at quality 60.
``lossless_only`` stops after the first step and reports that the file is still too big instead of degrading it."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..errors import KafkaError
from ..hoard_link.docs import imaging as _imaging
from ..hoard_link.docs.imaging import Packed

IMAGE_EXT = {"png": "png", "jpg": "jpeg", "jpeg": "jpeg", "jpe": "jpeg", "jfif": "jpeg", "webp": "webp"}

__all__ = ["IMAGE_EXT", "Packed", "format_of", "compress_image", "list_images"]

_NOTES_ES = {
    "still over the limit without losing quality": "sigue pesando más que el límite sin perder calidad",
    "does not fit even after reducing size and colours": "no baja del límite ni reduciendo tamaño y colores",
    "does not fit even after reducing quality and size": "no baja del límite ni reduciendo calidad y tamaño",
    "animated image: left untouched": "imagen animada: no se toca",
}


def format_of(path: Path) -> Optional[str]:
    return IMAGE_EXT.get(path.suffix.lower().lstrip("."))


def compress_image(path: Path, limit_bytes: int, lossless_only: bool = False) -> Packed:
    """Compress one image so it is at most ``limit_bytes``. Never touches the source. Raises ``KafkaError`` for files that are not images."""
    if format_of(path) is None:
        raise KafkaError("unsupported", f"«{path.name}» no es PNG, JPEG ni WEBP.")
    try:
        packed = _imaging.compress_to_limit(path, limit_bytes, lossless_only=lossless_only)
    except ValueError as exc:  # unreadable, or a name that lies about its content (a GIF called .png)
        raise KafkaError("unsupported", f"No puedo abrir «{path.name}» como imagen ({str(exc)[:80]}).") from exc
    packed.note = _NOTES_ES.get(packed.note, packed.note)
    return packed


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
