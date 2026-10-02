"""Request guard wiring: host allow-list, Origin rule and Fetch Metadata rules as the app applies them (the rules live in the commons)."""

import pytest
from conftest import make_config
from fastapi import FastAPI
from fastapi.testclient import TestClient

from kafka_hoard.hoard_link.guard import install_guard, parse_allowed_hosts
from kafka_hoard.main import create_app

NAV = {"sec-fetch-site": "cross-site", "sec-fetch-mode": "navigate", "sec-fetch-dest": "document"}
CORS = {"sec-fetch-site": "cross-site", "sec-fetch-mode": "cors", "sec-fetch-dest": "empty"}
IFRAME = {"sec-fetch-site": "cross-site", "sec-fetch-mode": "navigate", "sec-fetch-dest": "iframe"}


def test_middleware_blocks_cross_site_embeds_and_fetches():
    app = FastAPI()
    install_guard(app, port_getter=lambda: 5200, allowed_hosts="*.ts.net")

    @app.get("/")
    def home():
        return {"ok": True}

    with TestClient(app, base_url="http://127.0.0.1") as client:
        assert client.get("/", headers=NAV).status_code == 200
        assert client.get("/", headers=IFRAME).status_code == 403
        assert client.get("/", headers=CORS).status_code == 403


@pytest.fixture
def guarded(tmp_path):
    app = create_app(make_config(tmp_path, allowed_hosts=parse_allowed_hosts("pc.example, *.ts.net")))
    with TestClient(app, base_url="http://127.0.0.1") as client:
        yield client


def test_app_host_origin_and_cross_site_rules(guarded):
    get = lambda **headers: guarded.get("/api/health", headers=headers).status_code  # noqa: E731
    assert get() == 200
    assert get(host="pc.example") == 200
    assert get(host="My-PC.ts.net:8443") == 200
    assert get(host="evil.example") == 403
    assert get(origin="https://my-pc.ts.net:8443") == 200
    assert get(origin="https://evil.example") == 403
    assert get(**NAV) == 200
    assert get(**CORS) == 403
    assert get(**IFRAME) == 403


def test_app_without_allowed_hosts_is_local_only(client):
    assert client.get("/api/health", headers={"host": "my-pc.ts.net"}).status_code == 403
    assert client.get("/api/health", headers={"host": "[::1]:5200"}).status_code == 200
