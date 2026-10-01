"""One error type for every expected failure, so the API, the agent tools and the UI report it the same way."""

from __future__ import annotations

from typing import Any


class KafkaError(Exception):
    """An expected, explainable failure: a stable ``code``, a human ``message`` and an actionable ``hint``."""

    STATUS = {
        "not_found": 404,
        "confirm_required": 400,
        "invalid": 400,
        "not_configured": 400,
        "mail_unavailable": 503,
        "offline": 503,
        "unsupported": 415,
        "too_large": 413,
        "forbidden": 403,
        "channel_not_configured": 400,
    }

    def __init__(self, code: str, message: str, hint: str = "", *, status: int | None = None, **details: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.status = status or self.STATUS.get(code, 400)
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"error": self.message, "code": self.code}
        if self.hint:
            body["hint"] = self.hint
        body.update(self.details)
        return body
