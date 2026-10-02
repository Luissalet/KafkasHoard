"""Output files of the workshop: Spanish suffixes, never overwrite (« (2)», « (3)»), safe names on Windows and Linux.

Safe names and the exclusive, never-overwrite write are the shared ones of Hoard Link (``hoard_link.docs.archives``); what stays
here is the workshop's naming convention (``<stem><suffix>.<ext>``, Spanish defaults) and its folder helpers."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from ..hoard_link import text as _text
from ..hoard_link.docs import archives

MAX_STEM = archives.MAX_STEM


def safe_stem(name: str, default: str = "documento") -> str:
    """A file name part that is valid on Windows: no reserved characters, no trailing dots or spaces, no device names."""
    # ``text.safe_filename`` (not ``archives.safe_stem``): a title such as «Seguro 1/2» must become «Seguro 1_2», not «2».
    return _text.safe_filename(name, max_len=MAX_STEM, fallback=default, keep_extension=False)


def safe_filename(name: str, default: str = "fichero") -> str:
    """A whole file name (stem and extension) made safe; keeps the extension (lower-cased, letters and digits only)."""
    base = os.path.basename(str(name or "").replace("\\", "/"))
    stem, ext = os.path.splitext(base)
    ext = re.sub(r"[^A-Za-z0-9]", "", ext)[:8]
    return safe_stem(stem, default) + (f".{ext.lower()}" if ext else "")


def candidate(directory: Path, stem: str, suffix: str, ext: str, n: int) -> Path:
    base = f"{stem}{suffix}"
    return directory / (f"{base}.{ext}" if n == 1 else f"{base} ({n}).{ext}")


def write_unique(directory: Path, stem: str, suffix: str, ext: str, data: bytes) -> Path:
    """Write ``data`` to ``<stem><suffix>.<ext>``, or ``<stem><suffix> (2).<ext>`` and so on when that name is taken.

    The file is created exclusively, so nothing that exists can be overwritten even when two jobs race."""
    return archives.write_unique(directory, f"{stem}{suffix}.{ext.lstrip('.')}", data)


def unique_dir(parent: Path, name: str) -> Path:
    """Create ``parent/name`` (or ``name (2)``…) as a new folder and return it."""
    parent.mkdir(parents=True, exist_ok=True)
    for n in range(1, 10_000):
        target = parent / (name if n == 1 else f"{name} ({n})")
        try:
            target.mkdir()
        except FileExistsError:
            continue
        return target
    raise OSError(f"No free folder name for {name} in {parent}")


def split_output(output: str, ext: str) -> tuple[Path, str]:
    """An explicit ``output`` file path -> (folder, stem). The extension is dropped when it is the expected one."""
    p = Path(output)
    stem = p.name
    if stem.lower().endswith("." + ext.lower()):
        stem = stem[: -(len(ext) + 1)]
    return p.parent, safe_stem(stem, "salida")


def human_size(n: Optional[int]) -> str:
    if n is None:
        return ""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.2f} MB"
