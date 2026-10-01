"""Documents over HTTP: upload (multipart), the stored original, page previews and the detail view."""

from __future__ import annotations

import mimetypes
from typing import Any

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, Response

from .. import readers
from ..errors import KafkaError
from .deps import services

router = APIRouter(prefix="/api/documents")

MAX_UPLOAD_FILES = 40
TEXTUAL = {"txt", "md", "text", "csv", "html", "htm", "xhtml", "eml"}
INLINE_TYPES = {"application/pdf", "image/png", "image/jpeg", "image/webp", "image/gif", "image/bmp", "image/tiff"}
SAFE_HEADERS = {"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=3600", "Cross-Origin-Resource-Policy": "same-origin"}


def _same_origin(request: Request) -> None:
    site = request.headers.get("sec-fetch-site")
    if site not in (None, "same-origin", "none"):
        raise HTTPException(403, "Documents are only served to the app itself.")


@router.post("/upload")
async def upload(request: Request, files: list[UploadFile] = File(...)):
    svc = services(request)
    if len(files) > MAX_UPLOAD_FILES:
        raise KafkaError("invalid", f"At most {MAX_UPLOAD_FILES} files per upload.")
    results: list[dict[str, Any]] = []
    for item in files:
        name = item.filename or "document"
        try:
            data = await item.read(readers.MAX_FILE_BYTES + 1)
            out = svc.engine.ingest_bytes(data, name, source="upload")
            results.append({"name": name, "ok": True, "created": out["created"], "duplicate_of": out.get("duplicate_of"),
                            "notes": out.get("notes") or [],
                            "documents": [svc.doc_card(svc.store.document(d["id"])) for d in out["documents"]]})
        except KafkaError as exc:
            results.append({"name": name, "ok": False, "error": exc.message, "code": exc.code})
        except Exception as exc:  # noqa: BLE001 — one bad file must not lose the others
            results.append({"name": name, "ok": False, "error": f"{type(exc).__name__}", "code": "failed"})
        finally:
            await item.close()
    return {"results": results, "created": sum(len(r.get("documents") or []) for r in results if r.get("created")),
            "duplicates": sum(1 for r in results if r.get("ok") and not r.get("created"))}


@router.get("/{doc_id}")
def detail(request: Request, doc_id: str):
    return services(request).detail(doc_id)


@router.get("/{doc_id}/file")
def original(request: Request, doc_id: str):
    _same_origin(request)
    svc = services(request)
    doc = svc.store.document(doc_id)
    if not doc.get("file_sha"):
        raise KafkaError("not_found", "This document has no stored file.", "Documents created from Phileas have none.")
    path = svc.files.path_of(doc["file_sha"], doc["file_ext"])
    if not path.is_file():
        raise KafkaError("not_found", "The stored file is missing.")
    ext = (doc.get("file_ext") or "").lower()
    mime = doc.get("mime") or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    filename = (doc.get("file_name") or path.name).replace('"', "")
    if mime in INLINE_TYPES:
        return FileResponse(path, media_type=mime, headers={**SAFE_HEADERS, "Content-Disposition": f'inline; filename="{_ascii(filename)}"'})
    if ext in TEXTUAL:
        # never let the browser render stored HTML or mail: show it as plain text
        return Response(path.read_bytes(), media_type="text/plain; charset=utf-8",
                        headers={**SAFE_HEADERS, "Content-Disposition": f'inline; filename="{_ascii(filename)}.txt"', "Content-Security-Policy": "sandbox"})
    return FileResponse(path, media_type="application/octet-stream", headers={**SAFE_HEADERS, "Content-Disposition": f'attachment; filename="{_ascii(filename)}"'})


@router.get("/{doc_id}/page/{number}.png")
def page_png(request: Request, doc_id: str, number: int):
    _same_origin(request)
    svc = services(request)
    doc = svc.store.document(doc_id)
    if not doc.get("file_sha"):
        raise KafkaError("not_found", "This document has no stored file.")
    if number < 1 or number > max(1, int(doc.get("pages") or 1)):
        raise KafkaError("not_found", f"The document has {doc.get('pages') or 1} page(s).")
    path = svc.files.path_of(doc["file_sha"], doc["file_ext"])
    if not path.is_file():
        raise KafkaError("not_found", "The stored file is missing.")
    ext = (doc.get("file_ext") or "").lower()
    cache = svc.config.cache_dir / "pages"
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / f"{doc['id']}-{number}.png"
    if target.is_file():
        return FileResponse(target, media_type="image/png", headers=SAFE_HEADERS)
    if ext == "pdf":
        try:
            png = readers.render_pdf_page(path.read_bytes(), number)
        except Exception as exc:  # noqa: BLE001
            raise KafkaError("unsupported", f"The page could not be drawn ({type(exc).__name__}).") from exc
        target.write_bytes(png)
        return Response(png, media_type="image/png", headers=SAFE_HEADERS)
    if ext in readers.IMAGE_EXT and number == 1:
        try:
            from PIL import Image
            with Image.open(path) as im:
                im.convert("RGB").save(target, "PNG")
            return FileResponse(target, media_type="image/png", headers=SAFE_HEADERS)
        except Exception as exc:  # noqa: BLE001
            raise KafkaError("unsupported", f"The image could not be drawn ({type(exc).__name__}).") from exc
    raise KafkaError("unsupported", "Only PDFs and images have page previews.")


def _ascii(name: str) -> str:
    return name.encode("ascii", "replace").decode("ascii").replace("?", "_")[:120] or "document"

