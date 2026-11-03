"""Dossiers over HTTP: build, read, status, refresh, delete, and the requirements diff. Same handlers as the assistant tools."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from .deps import tool

router = APIRouter(prefix="/api")


class DossierBody(BaseModel):
    question: str = Field(..., min_length=3, max_length=400)
    doc_ids: Optional[list[str]] = None
    lang: str = "es"
    max_passages: int = Field(6, ge=1, le=12)


@router.get("/dossiers")
def dossiers(request: Request) -> Any:
    return tool(request, "kafka_dossier_status", {})


@router.post("/dossiers")
def create(request: Request, body: DossierBody) -> Any:
    return tool(request, "kafka_dossier", {**body.model_dump(exclude_none=True), "detail": "full"})


@router.get("/dossiers/{dossier_id}")
def read(request: Request, dossier_id: str, detail: str = "full") -> Any:
    return tool(request, "kafka_dossier_status", {"dossier": dossier_id, "detail": detail})


@router.post("/dossiers/{dossier_id}/refresh")
def refresh(request: Request, dossier_id: str) -> Any:
    return tool(request, "kafka_dossier_refresh", {"dossier": dossier_id, "detail": "full"})


@router.delete("/dossiers/{dossier_id}")
def delete(request: Request, dossier_id: str) -> Any:
    return tool(request, "kafka_dossier_delete", {"dossier": dossier_id, "confirm": True})


@router.get("/requirements-diff")
def requirements_diff(request: Request, before: str, after: str, lang: str = "es", offset: int = 0, limit: int = 30) -> Any:
    return tool(request, "kafka_requirements_diff", {"before": before, "after": after, "lang": lang, "offset": offset, "limit": limit})
