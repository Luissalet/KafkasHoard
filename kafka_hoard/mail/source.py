"""Run ``faustus_mail.py`` with Faustus's Python so the mail password never leaves Faustus.

The Faustus folder comes from the ``mail.faustus_dir`` setting, ``KAFKA_FAUSTUS_DIR`` / ``FAUSTUS_DIR``, or a
``faustus`` folder next to this app. ``mail.faustus_owner`` picks the Faustus user when several have accounts.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Optional

from ..config import REPO_ROOT
from .faustus_mail import GMAIL_QUERY, SUBJECT_TERMS

HELPER = Path(__file__).with_name("faustus_mail.py")
TIMEOUT_S = 180
STATUS_TTL_S = 300.0


class FaustusMail:
    def __init__(self, setting: Callable[[str, str], str], secret: Callable[[str], str], *,
                 runner: Optional[Callable[..., Any]] = None, clock: Callable[[], float] = time.time):
        self.setting = setting
        self.secret = secret
        self.runner = runner or subprocess.run
        self.clock = clock
        self._status: Optional[tuple[float, str, dict[str, Any]]] = None

    def faustus_dir(self) -> Optional[Path]:
        raw = [self.setting("mail.faustus_dir", ""), self.secret("FAUSTUS_DIR"), os.environ.get("FAUSTUS_DIR", "")]
        candidates = [Path(r).expanduser() for r in raw if r and r.strip()]
        if not any(r and r.strip() for r in raw):
            candidates += [REPO_ROOT.parent / "faustus", REPO_ROOT.parent.parent / "faustus"]
        for path in candidates:
            try:
                if (path / "mcp_servers" / "email_server.py").is_file():
                    return path.resolve()
            except OSError:
                continue
        return None

    @staticmethod
    def python_of(root: Path) -> Optional[str]:
        for rel in ("venv/Scripts/python.exe", ".venv/Scripts/python.exe", "venv/bin/python", ".venv/bin/python"):
            if (root / rel).is_file():
                return str(root / rel)
        return None

    def call(self, request: dict[str, Any], timeout: float = TIMEOUT_S) -> dict[str, Any]:
        root = self.faustus_dir()
        if root is None:
            return {"ok": False, "error": "Faustus folder not found (set it in Settings → Mail)"}
        python = self.python_of(root)
        if python is None:
            return {"ok": False, "error": "Faustus has no venv with Python"}
        owner = self.setting("mail.faustus_owner", "")
        if owner:
            request = {**request, "owner": owner}
        env = {k: v for k, v in os.environ.items() if not k.startswith("KAFKA_")}
        env["PYTHONIOENCODING"] = "utf-8"
        try:
            done = self.runner([python, str(HELPER), str(root)], input=json.dumps(request), capture_output=True, text=True,
                               encoding="utf-8", timeout=timeout, cwd=str(root), env=env,
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "the mail read took too long"}
        except (OSError, subprocess.SubprocessError) as exc:
            return {"ok": False, "error": f"mail helper: {type(exc).__name__}"}
        lines = [line for line in (getattr(done, "stdout", "") or "").splitlines() if line.strip().startswith("{")]
        try:
            answer = json.loads(lines[-1]) if lines else {}
        except ValueError:
            answer = {}
        if not isinstance(answer, dict) or "ok" not in answer:
            return {"ok": False, "error": f"mail helper exit {getattr(done, 'returncode', '?')}"}
        return answer

    def status(self, refresh: bool = False) -> dict[str, Any]:
        key = str(self.faustus_dir() or "") + "|" + self.setting("mail.faustus_owner", "")
        cached = self._status
        if cached and not refresh and cached[1] == key and self.clock() - cached[0] < (STATUS_TTL_S if cached[2].get("ok") else 30):
            return cached[2]
        answer = self.call({"action": "status"}, timeout=60)
        answer["faustus_dir"] = str(self.faustus_dir() or "")
        self._status = (self.clock(), key, answer)
        return answer

    def scan(self, *, since_days: int, limit: int, skip: list[str], query: str = "", attachments_dir: str = "") -> dict[str, Any]:
        """Paperwork mail of the last ``since_days`` days; PDF / image attachments are written to ``attachments_dir`` by the helper."""
        return self.call({"action": "scan", "since_days": since_days, "max": limit, "skip": skip, "query": query,
                          "gmail_query": GMAIL_QUERY, "subject_terms": SUBJECT_TERMS, "attachments_dir": attachments_dir})
