"""Running external programs (Ghostscript, LibreOffice, PowerShell) and finding them. Everything goes through ``Env`` so the
tests can inject a fake runner and a fake ``which`` and never start a real program."""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional

CREATE_NO_WINDOW = 0x08000000


def run_process(cmd: list[str], *, timeout: float = 180.0, env: Optional[Mapping[str, str]] = None, cwd: Optional[str] = None) -> Any:
    """``subprocess.run`` with no console window on Windows, no stdin and text output that never raises on odd bytes."""
    kwargs: dict[str, Any] = {}
    if os.name == "nt":
        kwargs["creationflags"] = CREATE_NO_WINDOW
    return subprocess.run(cmd, capture_output=True, text=True, errors="replace", timeout=timeout, env=dict(env) if env is not None else None,
                          cwd=cwd, stdin=subprocess.DEVNULL, check=False, **kwargs)


@dataclass
class Env:
    runner: Callable[..., Any] = run_process
    which: Callable[[str], Optional[str]] = shutil.which
    windows: bool = field(default_factory=lambda: os.name == "nt")
    environ: Mapping[str, str] = field(default_factory=lambda: os.environ)
    glob: Callable[[str], list[str]] = glob.glob
    exists: Callable[[str], bool] = os.path.isfile

    def find(self, names: tuple[str, ...], patterns: tuple[str, ...] = ()) -> Optional[str]:
        """The first of ``names`` on PATH, then the first existing match of ``patterns`` (newest name last-sorted wins)."""
        for name in names:
            found = self.which(name)
            if found:
                return found
        for pattern in patterns:
            matches = sorted(self.glob(pattern), reverse=True)
            for match in matches:
                if self.exists(match):
                    return match
        return None

    def program_dirs(self) -> list[str]:
        out = []
        for var in ("ProgramFiles", "ProgramFiles(x86)", "ProgramW6432", "LOCALAPPDATA"):
            value = self.environ.get(var)
            if value and value not in out:
                out.append(value)
        return out

    def ghostscript(self) -> Optional[str]:
        names = ("gswin64c", "gswin32c", "gs") if self.windows else ("gs",)
        patterns = tuple(f"{base}\\gs\\gs*\\bin\\gswin64c.exe" for base in self.program_dirs()) + \
                   tuple(f"{base}\\gs\\gs*\\bin\\gswin32c.exe" for base in self.program_dirs()) if self.windows else ()
        return self.find(names, patterns)

    def soffice(self) -> Optional[str]:
        patterns = tuple(f"{base}\\LibreOffice*\\program\\soffice.exe" for base in self.program_dirs()) if self.windows else ()
        found = self.find(("soffice", "libreoffice", "soffice.exe"), patterns)
        if found is None and self.windows:
            for base in self.program_dirs():
                candidate = f"{base}\\LibreOffice\\program\\soffice.exe"
                if self.exists(candidate):
                    return candidate
        return found

    def powershell(self) -> Optional[str]:
        return self.find(("powershell", "powershell.exe", "pwsh")) if self.windows else None
