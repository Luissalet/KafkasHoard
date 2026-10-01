from __future__ import annotations

import sys
import warnings
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
for entry in (str(ROOT), str(ROOT / "tests")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastapi.testclient import TestClient  # noqa: E402

from kafka_hoard.agent_tools import call_tool  # noqa: E402
from kafka_hoard.config import Config  # noqa: E402
from kafka_hoard.main import create_app  # noqa: E402
from kafka_hoard.ocr import Ocr  # noqa: E402
from kafka_hoard.services import Services  # noqa: E402

from docs import T0  # noqa: E402


def _no_network():
    import httpx

    def handler(request):
        raise httpx.ConnectError(f"network disabled in tests: {request.url}")
    return httpx.MockTransport(handler)


class FakeNotifier:
    def __init__(self):
        self.sent: list[tuple[dict, list[str]]] = []

    def send(self, event, channels):
        self.sent.append((event, list(channels)))
        return [{"channel": c, "ok": True, "error": ""} for c in channels if c in ("hub", "toast")]

    def test(self, channel):
        return {"channel": channel, "ok": True, "error": ""}

    def channels_status(self):
        return {"toast": {"configured": True, "enabled": True}, "hub": {"configured": True, "enabled": True}}

    def telegram_discover_chat_id(self):
        return {"ok": False}

    def types(self):
        return [e["type"] for e, _ in self.sent]

    def titles(self):
        return [e["title"] for e, _ in self.sent]


class FakeMail:
    """Stands in for the Faustus mail helper: hands back the messages it was given, minus the skipped ids."""

    def __init__(self, messages: list[dict[str, Any]] | None = None):
        self.messages = list(messages or [])
        self.calls: list[dict[str, Any]] = []

    def scan(self, *, since_days, limit, skip, query="", attachments_dir=""):
        self.calls.append({"since_days": since_days, "limit": limit, "skip": len(skip), "query": query, "attachments_dir": attachments_dir})
        skip = set(skip)
        return {"ok": True, "accounts": [{"account": "test", "folder": "INBOX", "matches": len(self.messages)}],
                "messages": [m for m in self.messages if m["message_id"] not in skip][:limit]}

    def status(self, refresh=False):
        return {"ok": True, "accounts": [{"account": "test"}]}

    def faustus_dir(self):
        return None


class FakeLlm:
    """A model pass that answers from a canned JSON string (or is unavailable)."""

    def __init__(self, answer: str | None = None, available: bool = True):
        self.answer, self.ok, self.last_error, self.calls = answer, available, "", 0

    def available(self):
        return (True, "fake model") if self.ok else (False, "no model")

    def hints(self, text):
        from kafka_hoard.extract.llm import LlmPass
        self.calls += 1
        return LlmPass(chat=lambda messages: self.answer or "").hints(text)

    def close(self):
        pass


class Clock:
    def __init__(self, t: float = T0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def make_config(tmp_path: Path, **overrides) -> Config:
    base = dict(data_dir=tmp_path / "data", port=0, port_strict=False, data_dir_configured=True, scheduler=False,
                offline=False, ocr=False, secrets={})
    base.update(overrides)
    return Config(**base)


@pytest.fixture
def config(tmp_path):
    return make_config(tmp_path)


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def fake_mail():
    return FakeMail()


def build(config, clock, fake_mail, transport=None, llm=None, family_call=None, ocr=None):
    return Services(config, clock_fn=clock, http_transport=transport or _no_network(), notifier=FakeNotifier(), mail_source=fake_mail,
                    ocr=ocr or Ocr(False), llm=llm or FakeLlm(available=False), family_call=family_call or (lambda *a, **k: {"ok": False, "error": "off in tests"}))


@pytest.fixture
def svc(config, clock, fake_mail):
    s = build(config, clock, fake_mail)
    yield s
    s.stop()


@pytest.fixture
def client(config, clock, fake_mail):
    services = build(config, clock, fake_mail)
    app = create_app(config, services=services)
    with TestClient(app, base_url="http://127.0.0.1") as c:
        c.svc = services
        c.bearer = {"Authorization": f"Bearer {services.token}"}
        yield c


def tool(svc: Services, tool_name: str, /, **arguments) -> Any:
    return call_tool(svc, tool_name, arguments)
