"""FastAPI application factory: request guard, API routers, PWA files, static SPA."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from . import __version__
from . import agenda
from .api import ROUTERS
from .config import Config
from .hoard_link import fam_agenda, family
from .hoard_link.agentkit import format_issues, issues_of
from .hoard_link.guard import install_guard
from .hoard_link.service import install_error_handlers, install_pwa, install_spa
from .services import Services

STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_app(config: Config | None = None, services: Services | None = None) -> FastAPI:
    """``services`` lets tests inject a pre-built instance (fake transport, fake link)."""
    config = config or (services.config if services else Config.from_env())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        svc = services or Services(config)
        app.state.services = svc
        svc.start()
        logging.getLogger("kafka").info("Kafka's Hoard %s - data in %s", __version__, config.data_dir)
        try:
            yield
        finally:
            svc.stop()

    app = FastAPI(title="Kafka's Hoard", version=__version__, lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.config = config
    family.configure("kafka", str(config.data_dir), token_file=str(config.token_path))

    install_guard(app, port_getter=lambda: config.port, allowed_env="KAFKA_ALLOWED_HOSTS", allowed_hosts=config.allowed_hosts)
    fam_agenda.install_fastapi(app, lambda start, end, sphere: agenda.items(app.state.services, start, end, sphere))

    install_error_handlers(app)

    # Bad tool arguments and plain ValueErrors raised by handlers are the caller's mistake: 400, never a 500.
    @app.exception_handler(ValidationError)
    async def invalid_arguments(_: Request, exc: ValidationError):
        return JSONResponse({"error": format_issues(exc), "code": "invalid_arguments", "issues": issues_of(exc)}, status_code=400)

    @app.exception_handler(ValueError)
    async def invalid_value(_: Request, exc: ValueError):
        return JSONResponse({"error": str(exc), "code": "invalid"}, status_code=400)

    for router in ROUTERS:
        app.include_router(router)

    install_pwa(app, name="Kafka's Hoard", short_name="Kafka", theme="#1c1c14", background="#1c1c14", cache="kafka-hoard", lang="es",
                static_dir=STATIC_DIR, version=__version__)
    install_spa(app, STATIC_DIR)

    return app
