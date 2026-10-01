"""Process-level configuration read from the environment (never from the DB)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from .guard import parse_allowed_hosts

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PORT = 5200


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _int(raw: str, default: int, low: int, high: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if low <= value <= high else default


def _float(raw: str, default: float, low: float, high: float) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if low <= value <= high else default


def load_dotenv(path: Path) -> dict[str, str]:
    """Minimal .env reader (KEY=VALUE, # comments, optional quotes). Never overrides the real environment."""
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key:
            values[key] = value
    return values


@dataclass
class Config:
    """Everything the process needs before the database exists."""

    data_dir: Path = field(default_factory=lambda: REPO_ROOT / "data")
    port: int = DEFAULT_PORT
    port_strict: bool = False
    allowed_hosts: tuple[str, ...] = ()
    data_dir_configured: bool = False
    http_timeout_s: float = 25.0
    offline: bool = False  # never touch the network (tests): the mail source and the notification channels answer "offline"
    scheduler: bool = True  # background folder and mail scans and reminders; tests and the MCP-only mode switch it off
    ocr: bool = True  # allow OCR of scanned pages and images (needs rapidocr; reported as unavailable when missing)
    secrets: dict[str, str] = field(default_factory=dict)  # from .env (+ environment); never written back

    @property
    def db_path(self) -> Path:
        return self.data_dir / "kafka.db"

    @property
    def token_path(self) -> Path:
        return self.data_dir / "mcp-token"

    @property
    def url_path(self) -> Path:
        return self.data_dir / "url"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def files_dir(self) -> Path:
        """Original documents, content-addressed: files/<sha[:2]>/<sha256>.<ext>. Never modified."""
        return self.data_dir / "files"

    @property
    def inbox_dir(self) -> Path:
        """The drop folder that is watched by default."""
        return self.data_dir / "inbox"

    @property
    def mail_cache_dir(self) -> Path:
        """Mail attachments written by the Faustus helper until they are filed."""
        return self.data_dir / "cache" / "mail-attachments"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def backend_json_path(self) -> Path:
        return self.data_dir / "backend.json"

    def secret(self, name: str) -> str:
        """Environment first, then .env. ``name`` without the KAFKA_ prefix (e.g. TELEGRAM_TOKEN)."""
        key = f"KAFKA_{name}"
        return (os.environ.get(key) or self.secrets.get(key) or "").strip()

    @classmethod
    def from_env(cls) -> "Config":
        raw_dir = _env("KAFKA_DATA_DIR")
        port = _int(_env("KAFKA_PORT") or _env("PORT") or str(DEFAULT_PORT), DEFAULT_PORT, 1, 65535)
        return cls(
            data_dir=Path(raw_dir).expanduser() if raw_dir else REPO_ROOT / "data",
            port=port,
            port_strict=_env("PORT_STRICT") == "1",
            allowed_hosts=parse_allowed_hosts(_env("KAFKA_ALLOWED_HOSTS")),
            data_dir_configured=bool(raw_dir),
            http_timeout_s=_float(_env("KAFKA_HTTP_TIMEOUT_S"), 25.0, 2.0, 120.0),
            offline=_env("KAFKA_OFFLINE") == "1",
            scheduler=_env("KAFKA_SCHEDULER", "1") != "0",
            ocr=_env("KAFKA_OCR", "1") != "0",
            secrets=load_dotenv(REPO_ROOT / ".env"),
        )
