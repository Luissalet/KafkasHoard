"""/api/agent/* — the bridge used by mcp_server.py (Bearer token from <DATA_DIR>/mcp-token). The routes come from the shared agent kit.

An assistant says why for every write (``reason``), its writes are journaled and a whole session can be undone (``POST /api/agent/undo``,
hooks in ``agent_undo.py``). The web UI is not an agent: it calls ``/api/ui/call`` and needs no reason."""

from __future__ import annotations

from typing import Any

from fastapi import Request

from ..agent_tools import AGENT_INSTRUCTIONS, TOOLS, call_tool, tool_catalog
from ..errors import KafkaError
from ..hoard_link.agentkit import make_agent_router
from .deps import services

#: arguments whose value is a secret although the key does not say so (the journal masks by key name and by pattern only)
_SECRET_VALUES = {"secret_set": "value"}


def _call(name: str, arguments: dict[str, Any], request: Request) -> Any:
    try:
        return call_tool(services(request), name, arguments)
    finally:
        field = _SECRET_VALUES.get(name)
        if field and arguments.get(field):
            arguments[field] = "***"          # the router summarises this dict for the journal after the call


router = make_agent_router(
    tools_fn=tool_catalog,
    call_fn=_call,
    token_fn=lambda request: services(request).token,
    instructions=AGENT_INSTRUCTIONS,
    app_name="kafka",
    error_types=(KafkaError,),
    reasons=True,                                          # an agent says why for every write; the web UI is not an agent and is exempt
    data_dir=lambda request: services(request).config.data_dir,
    tools=TOOLS,
    ctx_fn=lambda request: services(request),
)
