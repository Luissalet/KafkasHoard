"""Which folders and files Kafka may read on behalf of the user (or of the assistant).

The checks are the family's (``hoard_link.paths``: drive roots, the profile folder, system, hidden and credential paths, Windows paths
analysed as such on any platform). This module fixes Kafka's own data folder rule: the inbox is open to watching and reading, and the
workshop folder is open to reading and writing. Every function returns ``""`` when the path may be used, else the reason (in English).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from .hoard_link import paths as shared

INBOX = "inbox"
WORKSHOP = "workshop"


def _text(reason: Optional[str]) -> str:
    return reason or ""


def unsafe_folder(path: str | Path, data_dir: Optional[Path] = None) -> str:
    """Empty when the folder may be watched; otherwise the reason it must not."""
    return _text(shared.unsafe_folder(path, data_dir=data_dir, allow_data_subdir=INBOX, lang="en"))


def unsafe_file(path: str | Path, data_dir: Optional[Path] = None) -> str:
    """Empty when the file may be read as a document; otherwise the reason."""
    return _text(shared.unsafe_file(path, data_dir=data_dir, allow_data_subdir=(INBOX, WORKSHOP), lang="en"))


def unsafe_output_dir(path: str | Path, data_dir: Optional[Path] = None) -> str:
    """Empty when the workshop may write files into this folder (it need not exist yet); otherwise the reason it must not."""
    return _text(shared.unsafe_output_dir(path, data_dir=data_dir, allow_data_subdir=(WORKSHOP, INBOX), lang="en"))
