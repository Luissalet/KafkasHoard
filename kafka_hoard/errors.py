"""One error type for every expected failure, so the API, the agent tools and the UI report it the same way."""

from __future__ import annotations

from typing import Any

from .hoard_link.agentkit import AppError


class KafkaError(AppError):
    """An expected, explainable failure: a stable ``code``, a human ``message`` and an actionable ``hint``.

    Keeps Kafka's call shape (``KafkaError(code, message, hint, **details)``) on top of the shared ``AppError``."""

    STATUS = {**AppError.STATUS, "mail_unavailable": 503, "channel_not_configured": 400}

    def __init__(self, code: str, message: str, hint: str = "", *, status: int | None = None, **details: Any):
        super().__init__(code, message, hint=hint, status=status, details=details)
