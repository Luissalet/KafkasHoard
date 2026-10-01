"""Content-addressed store of the original documents: ``files/<sha[:2]>/<sha256>.<ext>``. Originals are never modified."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

SAFE_EXT = re.compile(r"^[a-z0-9]{1,8}$")


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def clean_ext(ext: str) -> str:
    ext = (ext or "").lower().lstrip(".")
    return ext if SAFE_EXT.match(ext) else "bin"


class FileStore:
    def __init__(self, root: Path):
        self.root = Path(root)

    def path_of(self, sha: str, ext: str) -> Path:
        if not re.fullmatch(r"[0-9a-f]{64}", sha or ""):
            raise ValueError("not a sha256")
        return self.root / sha[:2] / f"{sha}.{clean_ext(ext)}"

    def exists(self, sha: str, ext: str) -> bool:
        try:
            return self.path_of(sha, ext).is_file()
        except ValueError:
            return False

    def put(self, data: bytes, ext: str, sha: Optional[str] = None) -> tuple[str, Path]:
        """Write the bytes once (atomically); a second put of the same content changes nothing."""
        sha = sha or sha256_of(data)
        target = self.path_of(sha, ext)
        if target.is_file():
            return sha, target
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(target.parent), prefix=".part-")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp, target)
        finally:
            if os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
        return sha, target

    def read(self, sha: str, ext: str) -> bytes:
        return self.path_of(sha, ext).read_bytes()

    def remove(self, sha: str, ext: str) -> bool:
        """Delete the stored original. The caller checks that no other document uses it."""
        try:
            path = self.path_of(sha, ext)
        except ValueError:
            return False
        try:
            path.unlink()
        except OSError:
            return False
        try:
            path.parent.rmdir()          # only succeeds when the bucket is empty
        except OSError:
            pass
        return True
