"""The workshop over HTTP: upload files into a job folder, download what the tools produced, and report what is installed.

The operations themselves are tools (``pdf_merge``, ``pdf_compress``, …) called through ``POST /api/ui/call`` with the uploaded paths."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from ..errors import KafkaError
from ..workshop import external, jobs, pdfops
from ..workshop.names import human_size
from ..workshop.service import IMAGE_INPUT_EXT
from .deps import services

router = APIRouter(prefix="/api/workshop")

MAX_UPLOAD_FILES = 60
MAX_UPLOAD_BYTES = 400 * 1024 * 1024
ALLOWED_EXT = {"pdf"} | IMAGE_INPUT_EXT | external.OFFICE_EXT
SAFE_HEADERS = {"X-Content-Type-Options": "nosniff", "Cache-Control": "private, no-store", "Cross-Origin-Resource-Policy": "same-origin"}
INLINE_TYPES = {"application/pdf", "image/png", "image/jpeg", "image/webp"}


def _same_origin(request: Request) -> None:
    site = request.headers.get("sec-fetch-site")
    if site not in (None, "same-origin", "none"):
        raise HTTPException(403, "Los ficheros del taller solo se sirven a la propia aplicación.")


@router.get("/status")
def status(request: Request) -> dict[str, Any]:
    return services(request).workshop.status()


@router.post("/upload")
async def upload(request: Request, files: list[UploadFile] = File(...), job: str = Form("")) -> dict[str, Any]:
    ws = services(request).workshop
    if len(files) > MAX_UPLOAD_FILES:
        raise KafkaError("invalid", f"Como mucho {MAX_UPLOAD_FILES} ficheros por subida.")
    job_id = job if jobs.valid_job(job) else jobs.new_job()
    folder = jobs.in_dir(ws.root, job_id)
    folder.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    saved: list[dict[str, Any]] = []
    for item in files:
        name = item.filename or "fichero"
        ext = Path(name).suffix.lower().lstrip(".")
        if ext not in ALLOWED_EXT:
            results.append({"name": name, "ok": False, "error": f"Un .{ext or '?'} no se puede usar en el taller (PDF, imágenes y documentos de Word)."})
            await item.close()
            continue
        dest = jobs.unique_upload_name(folder, name)
        size = 0
        try:
            with open(dest, "wb") as handle:
                while True:
                    chunk = await item.read(1 << 20)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_UPLOAD_BYTES:
                        raise KafkaError("too_large", f"«{name}» pesa más de {MAX_UPLOAD_BYTES // (1024 * 1024)} MB: usa «Ruta en este equipo».", status=413)
                    handle.write(chunk)
        except KafkaError as exc:
            dest.unlink(missing_ok=True)
            results.append({"name": name, "ok": False, "error": exc.message})
            continue
        except OSError as exc:
            dest.unlink(missing_ok=True)
            results.append({"name": name, "ok": False, "error": f"No se pudo guardar ({type(exc).__name__})."})
            continue
        finally:
            await item.close()
        entry: dict[str, Any] = {"name": dest.name, "path": str(dest.resolve()), "size": size, "size_text": human_size(size), "ext": ext}
        if ext == "pdf":
            entry.update(ws.pdf_summary(dest))
        saved.append(entry)
        results.append({"name": name, "ok": True, "path": entry["path"]})
    return {"job": job_id, "files": saved, "results": results}


@router.get("/file")
def download(request: Request, path: str, inline: bool = False):
    _same_origin(request)
    ws = services(request).workshop
    if not ws.is_servable(path):
        raise KafkaError("forbidden", "Solo se sirven los ficheros que ha producido el taller.", status=403)
    target = Path(path).resolve()
    mime = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
    disposition = "inline" if inline and mime in INLINE_TYPES else "attachment"
    return FileResponse(target, media_type=mime, filename=target.name, content_disposition_type=disposition, headers=SAFE_HEADERS)
