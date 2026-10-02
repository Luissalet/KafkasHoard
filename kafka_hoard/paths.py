"""Which folders and files Kafka may read on behalf of the user (or of the assistant)."""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Optional

SYSTEM_PARTS_WIN = {"windows", "program files", "program files (x86)", "programdata", "$recycle.bin", "system volume information"}
SYSTEM_PARTS_POSIX = {"etc", "usr", "bin", "sbin", "lib", "lib64", "proc", "sys", "dev", "boot", "root", "var", "run", "opt", "snap"}
SECRET_NAMES = re.compile(r"(^\.env($|\.)|mcp-token|^id_(rsa|ed25519|ecdsa|dsa)|\.pem$|\.key$|\.p12$|\.pfx$|\.kdbx$|credentials|secrets?\.|\.ssh$|\.htpasswd)", re.I)
HIDDEN_PARTS = {".git", ".ssh", ".gnupg", ".aws", ".config", "appdata", "node_modules", "__pycache__"}


def _hidden_parts(path: Path, parts_of: tuple[str, ...]) -> set[str]:
    """Configuration-like folder names in ``parts_of``; inside the system temp folder (which lives under AppData on
    Windows) only the parts below it count, so a document saved to Temp can be filed."""
    try:
        temp = Path(tempfile.gettempdir()).resolve()
        rel = path.relative_to(temp)
        names = {x.lower() for x in rel.parts[: len(rel.parts) - (len(path.parts) - len(parts_of))]}
    except (ValueError, OSError):
        names = {x.lower() for x in parts_of}
    return names & HIDDEN_PARTS


def _is_root(path: Path) -> bool:
    return path == path.parent or len(path.parts) <= 1 or (path.drive and len(path.parts) == 1)


def unsafe_folder(path: str | Path, data_dir: Optional[Path] = None) -> str:
    """Empty when the folder may be watched; otherwise the reason it must not."""
    try:
        p = Path(path).expanduser()
    except (OSError, ValueError, RuntimeError):
        return "not a valid path"
    if not p.is_absolute():
        return "the path must be absolute"
    try:
        p = p.resolve()
    except OSError:
        return "the path cannot be resolved"
    if not p.is_dir():
        return "the folder does not exist"
    if _is_root(p):
        return "a drive root is too broad"
    try:
        home = Path.home().resolve()
    except (OSError, RuntimeError):
        home = None
    if home is not None and p == home:
        return "the user profile folder is too broad: pick a subfolder"
    parts = {x.lower() for x in p.parts}
    if parts & SYSTEM_PARTS_WIN and (p.drive or os.name == "nt"):
        return "system folders are not allowed"
    if os.name != "nt" and len(p.parts) > 1 and p.parts[1].lower() in SYSTEM_PARTS_POSIX and not (data_dir and _inside(p, data_dir)):
        # /var/tmp and the like are fine for tests; the real system trees are not
        if p.parts[1].lower() != "var" or len(p.parts) < 3 or p.parts[2].lower() not in ("tmp", "folders"):
            return "system folders are not allowed"
    if data_dir is not None and _inside(p, data_dir) and p != (Path(data_dir) / "inbox").resolve() and not _inside(p, Path(data_dir) / "inbox"):
        return "Kafka's own data folder cannot be watched (except its inbox)"
    if _hidden_parts(p, p.parts):
        return "configuration and hidden folders are not allowed"
    return ""


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(Path(parent).resolve())
        return True
    except (ValueError, OSError):
        return False


def unsafe_file(path: str | Path, data_dir: Optional[Path] = None) -> str:
    """Empty when the file may be read as a document; otherwise the reason."""
    try:
        p = Path(path).expanduser()
    except (OSError, ValueError, RuntimeError):
        return "not a valid path"
    if not p.is_absolute():
        return "the path must be absolute"
    try:
        p = p.resolve()
    except OSError:
        return "the path cannot be resolved"
    if not p.is_file():
        return "the file does not exist"
    if SECRET_NAMES.search(p.name):
        return "that looks like a credentials file"
    parts = {x.lower() for x in p.parts[:-1]}
    if parts & SYSTEM_PARTS_WIN and (p.drive or os.name == "nt"):
        return "system folders are not allowed"
    if os.name != "nt" and len(p.parts) > 1 and p.parts[1].lower() in SYSTEM_PARTS_POSIX:
        if p.parts[1].lower() != "var" or len(p.parts) < 3 or p.parts[2].lower() not in ("tmp", "folders"):
            if not (data_dir and _inside(p, data_dir)):
                return "system folders are not allowed"
    if data_dir is not None and _inside(p, data_dir) and not _inside(p, Path(data_dir) / "inbox") and not _inside(p, Path(data_dir) / "workshop"):
        return "Kafka's own data folder is off limits (except its inbox and the workshop folder)"
    if _hidden_parts(p, p.parts[:-1]):
        return "configuration and hidden folders are not allowed"
    return ""


def unsafe_output_dir(path: str | Path, data_dir: Optional[Path] = None) -> str:
    """Empty when the workshop may write files into this folder (it need not exist yet); otherwise the reason it must not."""
    try:
        p = Path(path).expanduser()
    except (OSError, ValueError, RuntimeError):
        return "not a valid path"
    if not p.is_absolute():
        return "the path must be absolute"
    try:
        p = p.resolve()
    except OSError:
        return "the path cannot be resolved"
    if p.exists() and not p.is_dir():
        return "that path is a file, not a folder"
    parts = {x.lower() for x in p.parts}
    if parts & SYSTEM_PARTS_WIN and (p.drive or os.name == "nt"):
        return "system folders are not allowed"
    if os.name != "nt" and len(p.parts) > 1 and p.parts[1].lower() in SYSTEM_PARTS_POSIX and not (data_dir and _inside(p, data_dir)):
        if p.parts[1].lower() != "var" or len(p.parts) < 3 or p.parts[2].lower() not in ("tmp", "folders"):
            return "system folders are not allowed"
    if data_dir is not None and _inside(p, data_dir) and not _inside(p, Path(data_dir) / "workshop") and not _inside(p, Path(data_dir) / "inbox"):
        return "Kafka's own data folder is off limits (except its inbox and the workshop folder)"
    if _hidden_parts(p, p.parts):
        return "configuration and hidden folders are not allowed"
    return ""
