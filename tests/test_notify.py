"""Notification channels: gating by setting and severity, ntfy and Telegram payloads, secrets never leaked."""

from __future__ import annotations

import json

import httpx
import pytest

from conftest import make_config
from kafka_hoard.notify import Notifier, build_toast_ps1, xml_escape
from kafka_hoard.notify.labels import compose, format_price, label

EVENT = {"id": "x", "type": "deadline_soon", "severity": "high", "title": "Mañana: Pago de Demo", "summary": "2 oct · 100,00 €", "url": "http://127.0.0.1:5200/#/documentos/d_1",
         "bus": "kafka.deadline.soon", "data": {"deadline_id": "t_1"}}


class Recorder:
    def __init__(self, status=200, body=None):
        self.requests, self.status, self.body = [], status, body or {"ok": True}

    def __call__(self, request: httpx.Request):
        self.requests.append(request)
        return httpx.Response(self.status, json=self.body)


def make(tmp_path, settings=None, secrets=None, recorder=None, **kw):
    cfg = make_config(tmp_path, secrets=dict(secrets or {}))
    store = dict(settings or {})
    n = Notifier(cfg, lambda k, d=None: store.get(k, d), transport=httpx.MockTransport(recorder or Recorder()), platform="linux", **kw)
    return n, store


def test_disabled_and_unconfigured_channels_are_skipped_with_a_reason(tmp_path):
    n, _ = make(tmp_path)
    out = {r["channel"]: r for r in n.send(EVENT, ["ntfy", "telegram", "email", "toast"])}
    assert out["ntfy"]["error"] == "disabled" and out["telegram"]["error"] == "disabled" and out["email"]["skipped"]
    assert out["toast"]["ok"] is False and out["toast"]["error"] == "not windows"


def test_ntfy_posts_json_with_priority_click_and_token(tmp_path):
    rec = Recorder()
    n, _ = make(tmp_path, {"notify.ntfy.enabled": "1"}, {"KAFKA_NTFY_TOPIC": "mi-tema-privado", "KAFKA_NTFY_TOKEN": "tk_secret"}, rec)
    r = n.send(EVENT, ["ntfy"])[0]
    assert r["ok"] and len(rec.requests) == 1
    req = rec.requests[0]
    body = json.loads(req.content)
    assert str(req.url).startswith("https://ntfy.sh") and body["topic"] == "mi-tema-privado" and body["priority"] == 4 and body["click"] == EVENT["url"]
    assert req.headers["authorization"] == "Bearer tk_secret" and body["title"] == EVENT["title"]


def test_ntfy_server_must_be_http(tmp_path):
    n, _ = make(tmp_path, {"notify.ntfy.enabled": "1", "notify.ntfy.server": "file:///etc"}, {"KAFKA_NTFY_TOPIC": "t"})
    assert n.send(EVENT, ["ntfy"])[0]["error"] == "invalid ntfy server"


def test_telegram_message_is_html_escaped_and_secrets_are_scrubbed_from_errors(tmp_path):
    rec = Recorder()
    n, _ = make(tmp_path, {"notify.telegram.enabled": "1"}, {"KAFKA_TELEGRAM_TOKEN": "123:ABCDEF", "KAFKA_TELEGRAM_CHAT_ID": "987654"}, rec)
    event = {**EVENT, "title": "Pago <b>& más</b>"}
    assert n.send(event, ["telegram"])[0]["ok"]
    sent = json.loads(rec.requests[0].content)
    assert "&lt;b&gt;" in sent["text"] and sent["chat_id"] == "987654" and "bot123:ABCDEF" in str(rec.requests[0].url)
    bad = Recorder(status=400, body={"ok": False, "description": "chat 987654 not found for 123:ABCDEF"})
    n2, _ = make(tmp_path, {"notify.telegram.enabled": "1"}, {"KAFKA_TELEGRAM_TOKEN": "123:ABCDEF", "KAFKA_TELEGRAM_CHAT_ID": "987654"}, bad)
    err = n2.send(EVENT, ["telegram"])[0]["error"]
    assert "987654" not in err and "123:ABCDEF" not in err and err


def test_severity_floor_per_channel(tmp_path):
    rec = Recorder()
    n, _ = make(tmp_path, {"notify.ntfy.enabled": "1", "notify.ntfy.min_severity": "high"}, {"KAFKA_NTFY_TOPIC": "t"}, rec)
    low = n.send({**EVENT, "severity": "low"}, ["ntfy"])[0]
    assert low["skipped"] and not rec.requests
    assert n.send({**EVENT, "severity": "high"}, ["ntfy"])[0]["ok"]


def test_network_errors_become_a_short_error_not_an_exception(tmp_path):
    def boom(request):
        raise httpx.ConnectError("down")
    cfg = make_config(tmp_path, secrets={"KAFKA_NTFY_TOPIC": "t"})
    n = Notifier(cfg, lambda k, d=None: {"notify.ntfy.enabled": "1"}.get(k, d), transport=httpx.MockTransport(boom), platform="linux")
    r = n.send(EVENT, ["ntfy"])[0]
    assert not r["ok"] and r["error"] == "ConnectError"


def test_hub_channel_emits_the_bus_event_with_its_data(tmp_path, monkeypatch):
    from kafka_hoard.hoard_link import family
    seen = []
    monkeypatch.setattr(family, "emit", lambda event, payload: seen.append((event, payload)) or True)
    n, _ = make(tmp_path)
    assert n.send(EVENT, ["hub"])[0]["ok"]
    event, payload = seen[0]
    assert event == "kafka.deadline.soon" and payload["deadline_id"] == "t_1" and payload["title"] == EVENT["title"] and payload["severity"] == "high"
    monkeypatch.setattr(family, "emit", lambda event, payload: False)
    assert n.send(EVENT, ["hub"])[0]["error"] == "hub not configured"


def test_unknown_channel_and_test_message(tmp_path):
    n, _ = make(tmp_path, {"notify.ntfy.enabled": "0"}, {"KAFKA_NTFY_TOPIC": "t"})
    assert n.send(EVENT, ["pigeon"])[0]["error"] == "unknown channel"
    assert n.test("ntfy")["ok"]                       # a test ignores the enabled switch
    assert n.test("pigeon")["ok"] is False


def test_email_over_smtp_uses_the_given_factory(tmp_path):
    sent = []

    class Smtp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def login(self, user, password):
            sent.append(("login", user))

        def send_message(self, msg):
            sent.append(("msg", msg["To"], msg["Subject"]))

    secrets = {"KAFKA_SMTP_USER": "yo@example.org", "KAFKA_SMTP_PASSWORD": "pw", "KAFKA_SMTP_TO": "yo@example.org"}
    n, _ = make(tmp_path, {"notify.email.enabled": "1", "notify.email.backend": "smtp"}, secrets, smtp_factory=lambda *a, **k: Smtp())
    assert n.send(EVENT, ["email"])[0]["ok"]
    assert ("login", "yo@example.org") in sent and ("msg", "yo@example.org", EVENT["title"]) in sent


def test_toast_xml_is_escaped():
    assert xml_escape("a<b>&\"'") == "a&lt;b&gt;&amp;&quot;&#x27;"
    ps = build_toast_ps1("Plazo <hoy>", "Pago & más", "http://127.0.0.1:5200/")
    assert "<hoy>" not in ps and "&lt;hoy&gt;" in ps


def test_labels_and_formatting():
    assert label("deadline_soon", "es") == "Plazo próximo" and label("deadline_soon", "en") == "Upcoming deadline"
    assert format_price(1234.5, "EUR", "es") == "1.234,50 €" and format_price("x", "EUR") == ""
    assert compose({"type": "price_change", "summary": "s"}, "es") == ("Cambio de precio", "s")


@pytest.mark.parametrize("severity,expected", [("low", 2), ("medium", 3), ("high", 4)])
def test_ntfy_priority_by_severity(tmp_path, severity, expected):
    rec = Recorder()
    n, _ = make(tmp_path, {"notify.ntfy.enabled": "1"}, {"KAFKA_NTFY_TOPIC": "t"}, rec)
    n.send({**EVENT, "severity": severity}, ["ntfy"])
    assert json.loads(rec.requests[0].content)["priority"] == expected
