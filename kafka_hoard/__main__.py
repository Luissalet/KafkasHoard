"""`python -m kafka_hoard` — run the app with uvicorn on 127.0.0.1 (the shared launcher)."""

from __future__ import annotations

import logging

from .hoard_link.service import run_main


def main() -> int:
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request would drown the log
    return run_main(service="kafka-hoard", package="kafka_hoard", default_port=5200, app_factory="kafka_hoard.main:create_app",
                    data_dir_env="KAFKA_DATA_DIR", port_env="KAFKA_PORT", open_browser_default=False, title="Kafka's Hoard")


if __name__ == "__main__":
    raise SystemExit(main())
