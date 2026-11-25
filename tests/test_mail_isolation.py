"""Kafka in a test hub or on test data has no mail without a gateway: the Faustus helper is never started."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from kafka_hoard.hoard_link import fam_mail, family
from kafka_hoard.mail.source import MailSource

REAL_HUB = "http://127.0.0.1:8810"
REPO = Path(fam_mail.__file__).resolve().parent.parent.parent


class CountingRunner:
    def __init__(self):
        self.calls = 0

    def __call__(self, cmd, **kw):
        self.calls += 1
        return subprocess.CompletedProcess(cmd, 0, '{"ok": true, "messages": [], "accounts": []}\n', "")


class NoGateway:
    def available(self, *a, **k):
        return False


@pytest.fixture
def faustus(tmp_path):
    root = tmp_path / "faustus"
    (root / "mcp_servers").mkdir(parents=True)
    (root / "mcp_servers" / "email_server.py").write_text("", encoding="utf-8")
    (root / "venv" / "bin").mkdir(parents=True)
    (root / "venv" / "bin" / "python").write_text("", encoding="utf-8")
    return root


@pytest.fixture
def configure(monkeypatch):
    saved = dict(family._state)
    for key in ("HOARD_HUB_URL", "HOARD_TOKEN_FILE", "HOARD_HUB_DATA_DIR",
                "HOARD_MAIL_REAL_HUB_URLS", "HOARD_MAIL_REAL_DATA_DIRS"):
        monkeypatch.delenv(key, raising=False)
    fam_mail.forget_availability()
    yield lambda hub, token: family.configure("kafka", None, token_file=token, hub=hub)
    family._state.clear()
    family._state.update(saved)
    fam_mail.forget_availability()


def source_for(faustus, runner):
    settings = {"mail.faustus_dir": str(faustus), "mail.source": "auto"}
    return MailSource(lambda k, d="": settings.get(k, d), lambda k: "", runner=runner, hub_api=NoGateway())


@pytest.mark.parametrize("hub, own_data, code", [("http://127.0.0.1:58778", True, "hub"), (REAL_HUB, False, "data")], ids=["test-hub", "test-data"])
def test_a_scan_in_a_test_install_never_starts_the_helper(configure, faustus, tmp_path, hub, own_data, code):
    configure(hub, str((REPO if own_data else tmp_path) / "data" / "mcp-token"))
    runner = CountingRunner()
    source = source_for(faustus, runner)
    answer = source.scan(since_days=30, limit=10, skip=[])
    assert answer["ok"] is False and "sin pasarela" in answer["error"] and "mail is off" in answer["error"]
    state = source.status(refresh=True)
    assert state["ok"] is False and state["isolated"] == code and "correo desactivado" in state["error"]
    assert source.isolated() == code
    assert runner.calls == 0


def test_the_real_install_still_runs_the_helper(configure, faustus):
    configure(REAL_HUB, str(REPO / "data" / "mcp-token"))
    runner = CountingRunner()
    source = source_for(faustus, runner)
    assert source.scan(since_days=30, limit=10, skip=[])["ok"] is True and runner.calls == 1
    assert source.status(refresh=True)["isolated"] == "" and source.isolated() == ""


def test_a_declared_install_uses_the_same_guard(configure, faustus, tmp_path, monkeypatch):
    hub = "https://hub.example.test"
    roots = [tmp_path / "other", tmp_path / "install"]
    monkeypatch.setenv("HOARD_MAIL_REAL_HUB_URLS", hub)
    monkeypatch.setenv("HOARD_MAIL_REAL_DATA_DIRS", os.pathsep.join(map(str, roots)))
    runner = CountingRunner()
    source = source_for(faustus, runner)

    configure(hub, str(roots[1] / "mcp-token"))
    assert source.scan(since_days=30, limit=10, skip=[])["ok"] and runner.calls == 1
    configure(hub, str(tmp_path / "scratch" / "mcp-token"))
    assert source.status()["isolated"] == "data"
    assert not source.status()["ok"] and runner.calls == 1
