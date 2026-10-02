"""Workshop folders under ``<data>/workshop``: ``in/<job>/`` holds uploaded files, ``out/<job>/`` their results. Jobs older than
a week are deleted by housekeeping. Only this module and the upload route know the layout."""

from __future__ import annotations

import re
import secrets
import shutil
import time
from pathlib import Path
from typing import Optional

from .names import safe_filename

JOB_RE = re.compile(r"^j_[0-9a-f]{12}$")
KEEP_DAYS = 7


def new_job() -> str:
    return "j_" + secrets.token_hex(6)


def valid_job(job: str) -> bool:
    return bool(JOB_RE.match(job or ""))


def in_dir(root: Path, job: str) -> Path:
    return root / "in" / job


def out_dir(root: Path, job: str) -> Path:
    return root / "out" / job


def job_of(root: Path, path: Path) -> Optional[str]:
    """The job id when ``path`` is inside ``<root>/in/<job>/`` (an uploaded file), else None."""
    try:
        rel = path.resolve().relative_to((root / "in").resolve())
    except (ValueError, OSError):
        return None
    job = rel.parts[0] if rel.parts else ""
    return job if valid_job(job) else None


def unique_upload_name(folder: Path, name: str) -> Path:
    name = safe_filename(name, "fichero")
    stem, dot, ext = name.rpartition(".")
    if not dot:
        stem, ext = name, ""
    for n in range(1, 1000):
        candidate = folder / (name if n == 1 else f"{stem} ({n}){'.' + ext if ext else ''}")
        if not candidate.exists():
            return candidate
    return folder / f"{secrets.token_hex(4)}-{name}"


def purge(root: Path, now: Optional[float] = None, days: int = KEEP_DAYS) -> int:
    """Delete job folders (in and out) that have not changed for ``days`` days. Returns how many were removed."""
    now = time.time() if now is None else now
    removed = 0
    for side in ("in", "out"):
        base = root / side
        if not base.is_dir():
            continue
        for job in base.iterdir():
            try:
                if job.is_dir() and valid_job(job.name) and now - job.stat().st_mtime > days * 86400:
                    shutil.rmtree(job, ignore_errors=True)
                    removed += 1
            except OSError:
                continue
    return removed
