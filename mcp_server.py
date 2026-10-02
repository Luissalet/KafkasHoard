"""Stdio MCP bridge for Kafka's Hoard.

It never opens the database: every tool call is proxied to the running app (`POST /api/agent/call`) with the Bearer token
from `<DATA_DIR>/mcp-token`. The tool list comes from `GET /api/agent/tools`, so the bridge and the app can never disagree.
When nothing answers, the bridge starts the app itself (`python -m kafka_hoard`, detached, on the port of KAFKA_URL);
KAFKA_BRIDGE_AUTOSTART=0 turns that off. The bridge itself is the shared catalogue bridge of the family commons.
"""

from __future__ import annotations

import sys

from kafka_hoard.hoard_link.bridge import CatalogBridge

BRIDGE = CatalogBridge(app="kafka", service="kafka-hoard", package="kafka_hoard", default_port=5200, data_dir_env="KAFKA_DATA_DIR",
                       title="Kafka's Hoard", root=__file__,
                       # Workshop tools (PDF and image work) may run Ghostscript or Word: give them most of the assistant's 180 s.
                       tool_timeouts={"pdf_*": 175, "images_*": 175})


def main() -> int:
    BRIDGE.run_bridge()
    return 0


if __name__ == "__main__":
    sys.exit(main())
