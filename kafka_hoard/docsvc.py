"""The family's document extraction and OCR service (``doc_extract``, ``ocr_image``, ``ocr_pdf``, ``ocr_status``).

Stateless: a caller names a file by its absolute path on the shared disk and gets text back; nothing is filed in Kafka, nothing is copied.
Long work (a scan of many pages) runs as a job in memory (kept one hour, the newest 50) that ``ocr_status`` follows, so a tool call never
waits more than ``hoard_link.waiting.MAX_WAIT_S``. The contract is ``docs/commons/services.md`` section 5 of Hoard Link.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

from . import paths, readers
from .errors import KafkaError
from .hoard_link import waiting
from .hoard_link.docs import readers_lite, textclean
from .hoard_link.docs.sniff import sniff as _sniff
from .hoard_link.ids import new_id
from .ocr import Ocr, lang_code
from .workshop import ranges

log = logging.getLogger("kafka.docsvc")

MAX_BYTES = 200 * 1024 * 1024           # the same ceiling the clients use for the local fallback
MAX_IMAGE_PIXELS = 150_000_000
KEEP_S = 3600.0
KEEP_N = 50
OCR_MODES = ("auto", "off", "force")
ACTIVE = ("queued", "running")


class Cancelled(Exception):
    pass


class JobTable:
    """Jobs in memory, run by a couple of daemon threads (OCR itself is serialised by the engine's lock). ``fn(job)`` returns the result
    fields; ``job.progress(done, total)`` updates the view."""

    def __init__(self, workers: int = 2, *, keep_s: float = KEEP_S, keep_n: int = KEEP_N, clock: Callable[[], float] = time.time):
        self.workers, self.keep_s, self.keep_n, self.clock = workers, keep_s, keep_n, clock
        self._jobs: dict[str, dict[str, Any]] = {}
        self._queue: "queue.Queue[tuple[str, Callable[..., Any]]]" = queue.Queue()
        self._lock = threading.RLock()
        self._threads: list[threading.Thread] = []
        self._stopped = False

    # ----------------------------------------------------------------------------------------------- running
    def _ensure_threads(self) -> None:
        with self._lock:
            self._threads = [t for t in self._threads if t.is_alive()]
            while len(self._threads) < self.workers and not self._stopped:
                t = threading.Thread(target=self._loop, name=f"kafka-docs-{len(self._threads)}", daemon=True)
                t.start()
                self._threads.append(t)

    def _loop(self) -> None:
        while not self._stopped:
            try:
                job_id, fn = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            with self._lock:
                job = self._jobs.get(job_id)
                if job is None or job["status"] != "queued":
                    continue
                job["status"] = "running"
                job["started_at"] = self.clock()
            try:
                result = fn(_Handle(self, job_id))
                with self._lock:
                    job["result"] = result
                    job["status"] = "done"
                    job["progress"] = 1.0
            except Cancelled:
                with self._lock:
                    job["status"] = "cancelled"
            except KafkaError as exc:
                with self._lock:
                    job.update(status="error", error=exc.message, code=exc.code)
            except Exception as exc:  # noqa: BLE001 - the job table must never die with a job
                log.exception("job %s failed", job_id)
                with self._lock:
                    job.update(status="error", error=f"{type(exc).__name__}: {str(exc)[:200]}", code="failed")
            finally:
                with self._lock:
                    job["finished_at"] = self.clock()

    def submit(self, kind: str, fn: Callable[["_Handle"], dict[str, Any]], *, label: str = "") -> str:
        self._ensure_threads()
        job_id = new_id("ocr")
        with self._lock:
            self._prune()
            self._jobs[job_id] = {"job_id": job_id, "kind": kind, "label": label, "status": "queued", "progress": 0.0, "pages_done": 0,
                                  "pages_total": 0, "created_at": self.clock()}
        self._queue.put((job_id, fn))
        return job_id

    def stop(self) -> None:
        self._stopped = True

    def _prune(self) -> None:
        now = self.clock()
        for job_id, job in list(self._jobs.items()):
            if job["status"] not in ACTIVE and now - job.get("finished_at", now) > self.keep_s:
                del self._jobs[job_id]
        finished = sorted((j for j in self._jobs.values() if j["status"] not in ACTIVE), key=lambda j: j.get("finished_at", 0))
        for job in finished[: max(0, len(finished) - self.keep_n)]:
            self._jobs.pop(job["job_id"], None)

    # ----------------------------------------------------------------------------------------------- reading
    def view(self, job_id: str) -> Optional[dict[str, Any]]:
        """The job as the tools answer it: while it runs only ``job_id``, ``status``, ``progress`` (and the page counters); done, the result
        fields too; failed, ``error``."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            out: dict[str, Any] = {"job_id": job_id, "status": job["status"], "progress": round(float(job["progress"]), 3),
                                   "pages_done": job["pages_done"], "pages_total": job["pages_total"], "tool": job["kind"]}
            if job["status"] == "done":
                out.update(job.get("result") or {})
                out["status"] = "done"
            elif job["status"] == "error":
                out["error"] = job.get("error") or "failed"
                out["ok"] = False
            return out

    def wait(self, job_id: str, wait_s: Any) -> Optional[dict[str, Any]]:
        if self.view(job_id) is None:
            return None
        out = waiting.wait_for(lambda: self.view(job_id), wait_s, poll=0.1)
        if out.get("state") == "missing":
            return None
        out.pop("waited_s", None)
        if out.get("still_running"):
            out["status"] = out["status"] if out["status"] in ACTIVE else "running"
        return out


class _Handle:
    def __init__(self, table: JobTable, job_id: str):
        self._table, self.id = table, job_id

    def progress(self, done: int, total: int) -> None:
        with self._table._lock:
            job = self._table._jobs.get(self.id)
            if job is not None:
                job["pages_done"], job["pages_total"] = int(done), int(total)
                job["progress"] = (done / total) if total else 0.0


class DocService:
    def __init__(self, ocr: Ocr, data_dir: Optional[Path] = None, *, jobs: Optional[JobTable] = None):
        self.ocr = ocr
        self.data_dir = Path(data_dir) if data_dir else None
        self.jobs = jobs or JobTable()

    def stop(self) -> None:
        self.jobs.stop()

    # ----------------------------------------------------------------------------------------------- inputs
    def _read_file(self, path: str) -> tuple[Path, bytes]:
        p = Path(str(path or "")).expanduser()
        if not p.is_absolute():
            raise KafkaError("invalid", "The path must be absolute.", "Send the full path of the file on this computer.")
        why = paths.unsafe_file(p, self.data_dir)
        if why:
            raise KafkaError("not_found" if why == "the file does not exist" else "forbidden", f"Cannot read «{p.name}»: {why}.")
        size = p.stat().st_size
        if size == 0:
            raise KafkaError("invalid", f"«{p.name}» is empty.")
        if size > MAX_BYTES:
            raise KafkaError("too_large", f"«{p.name}» is {size // (1024 * 1024)} MB; the limit is {MAX_BYTES // (1024 * 1024)} MB.")
        try:
            return p, p.read_bytes()
        except OSError as exc:
            raise KafkaError("invalid", f"Cannot read «{p.name}»: {exc.strerror or type(exc).__name__}.") from exc

    @staticmethod
    def _mode(value: str) -> str:
        mode = str(value or "auto").strip().lower()
        if mode not in OCR_MODES:
            raise KafkaError("invalid", f"ocr must be one of {', '.join(OCR_MODES)}, not «{value}».")
        return mode

    # ----------------------------------------------------------------------------------------------- doc_extract
    def extract(self, path: str, *, ocr: str = "auto", max_pages: int = 400, lang: str = "es", wait_s: Any = 150) -> dict[str, Any]:
        mode = self._mode(ocr)
        src, data = self._read_file(path)
        code = lang_code(lang)
        pages_cap = max(1, min(int(max_pages or 400), 2000))
        job_id = self.jobs.submit("doc_extract", lambda job: self.extract_bytes(src.name, data, mode=mode, max_pages=pages_cap, lang=code,
                                                                                   progress=job.progress), label=src.name)
        return self.jobs.wait(job_id, wait_s) or {"job_id": job_id, "status": "running"}

    def extract_bytes(self, name: str, data: bytes, *, mode: str = "auto", max_pages: int = 400, lang: str = "es",
                      progress: Optional[Callable[[int, int], None]] = None) -> dict[str, Any]:
        """The ``readers_lite.read_any`` shape plus ``pages_ocr`` (PDFs and images get Kafka's OCR pass)."""
        found = _sniff(name, data)
        stem = Path(str(name).replace("\\", "/")).stem
        try:
            return self._extract_bytes(found, stem, name, data, mode, max_pages, lang, progress)
        except (readers.ReadError, KafkaError) as exc:      # a damaged file is an answer, not a failed call
            message = exc.message if isinstance(exc, KafkaError) else str(exc)
            return {"kind": found.kind, "title": stem, "text": "", "units": [], "needs_ocr": False, "notes": [message], "mime": found.mime,
                    "error": message, "pages_ocr": 0}

    def _extract_bytes(self, found: Any, stem: str, name: str, data: bytes, mode: str, max_pages: int, lang: str,
                       progress: Optional[Callable[[int, int], None]]) -> dict[str, Any]:
        notes: list[str] = []
        if found.kind == "pdf":
            read = readers.read_pdf_detail(data, self.ocr, notes, max_pages=max_pages, max_ocr=max_pages, mode=mode, lang=lang, progress=progress, layout=True)
            pages = textclean.strip_repeated_lines(read.pages)
            skipped = set(read.not_attempted)
            units = []
            for i, text in enumerate(pages, start=1):
                unit: dict[str, Any] = {"kind": "page", "number": i, "title": "", "text": text}
                if i in skipped:
                    unit["needs_ocr"] = True                    # OCR never looked at this page
                units.append(unit)
            empty = sum(1 for i in read.unread if i not in skipped)
            needs = bool(skipped) and len(skipped) >= max(1, len(units) / 2)
            if empty:
                notes.append(f"{empty} page(s) have no text even after OCR")
            return self._shape("pdf", read.title or stem, units, needs, notes, read.ocr_pages, found.mime)
        if found.kind == "image" and found.ext != "svg":
            image = self._open_image(data)
            ok, why = self.ocr.available()
            if mode == "off" or not ok:
                notes.append("OCR was not run" + ("" if mode == "off" else f": {why}"))
                return self._shape("image", stem, [], True, notes, 0, found.mime)
            result = self.ocr.recognise(image, lang)
            if progress:
                progress(1, 1)
            text = readers.clean_text(result.text)
            if result.error:
                notes.append(f"OCR failed: {result.error}")
            elif not text:
                notes.append("OCR found no text in the image")
            units = [{"kind": "page", "number": 1, "title": "", "text": text}] if text else []
            return self._shape("image", stem, units, not text, notes, 1 if text else 0, found.mime)
        result = readers_lite.read_any(name, data, max_pages=max_pages)
        result["pages_ocr"] = 0
        if progress:
            progress(1, 1)
        return result

    @staticmethod
    def _shape(kind: str, title: str, units: list[dict[str, Any]], needs_ocr: bool, notes: list[str], pages_ocr: int, mime: str) -> dict[str, Any]:
        text = "\n\n".join(u["text"] for u in units if u.get("text"))
        return {"kind": kind, "title": title, "text": text, "units": units, "needs_ocr": needs_ocr, "notes": notes, "mime": mime, "error": None,
                "pages_ocr": pages_ocr}

    # ----------------------------------------------------------------------------------------------- images
    @staticmethod
    def _open_image(data: bytes) -> Any:
        from .hoard_link.docs import imaging

        try:
            return imaging.open_oriented(data, max_pixels=MAX_IMAGE_PIXELS)
        except ValueError as exc:
            raise KafkaError("invalid", f"The file is not a readable image ({str(exc)[:120]}).") from exc

    def ocr_image(self, path: str, *, lang: str = "es", blocks: bool = False) -> dict[str, Any]:
        src, data = self._read_file(path)
        image = self._open_image(data)
        ok, why = self.ocr.available()
        if not ok:
            raise KafkaError("ocr_unavailable", f"OCR is not available: {why}", self._install_hint(), status=503)
        result = self.ocr.recognise(image, lang_code(lang))
        if result.error and not result.text:
            raise KafkaError("ocr_failed", f"OCR failed on «{src.name}»: {result.error}", status=500)
        out: dict[str, Any] = {"text": readers.clean_text(result.text), "backend": result.backend or self.ocr.available()[1]}
        if blocks:
            out["blocks"] = [b.public() for b in result.blocks]
        return out

    @staticmethod
    def _install_hint() -> str:
        return "Install an OCR engine: pip install rapidocr onnxruntime (any system), winocr (Windows) or pytesseract with Tesseract."

    # ----------------------------------------------------------------------------------------------- ocr_pdf
    def ocr_pdf(self, path: str, *, pages: str = "", dpi: int = 200, lang: str = "es", max_pages: int = 40, wait_s: Any = 150) -> dict[str, Any]:
        src, data = self._read_file(path)
        if readers.sniff(src.name, data) != "pdf":
            raise KafkaError("invalid", f"«{src.name}» is not a PDF.")
        ok, why = self.ocr.available()
        if not ok:
            raise KafkaError("ocr_unavailable", f"OCR is not available: {why}", self._install_hint(), status=503)
        cap = max(1, min(int(max_pages or 40), 400))
        dpi = max(72, min(int(dpi or 200), 400))
        total = readers.pdf_page_count(data)
        numbers = ranges.parse_ranges(pages, total, default_all=True, unique=True) if str(pages or "").strip() else list(range(1, total + 1))
        notes: list[str] = []
        if len(numbers) > cap:
            notes.append(f"only {cap} of {len(numbers)} pages were recognised (max_pages)")
            numbers = numbers[:cap]
        code = lang_code(lang)

        def run(job: _Handle) -> dict[str, Any]:
            results = readers.ocr_pdf_pages(data, self.ocr, numbers, dpi=dpi, lang=code, progress=job.progress)
            failed = [n for n, _t, err in results if err]
            if failed:
                notes.append(f"OCR failed on page(s) {ranges.describe(failed)}")
            return {"text": "\n\n".join(t for _n, t, _e in results if t), "pages": [{"page": n, "text": t} for n, t, _e in results],
                    "pages_done": len(results), "pages_total": len(numbers), "backend": self.ocr.available()[1], "notes": notes}

        job_id = self.jobs.submit("ocr_pdf", run, label=src.name)
        return self.jobs.wait(job_id, wait_s) or {"job_id": job_id, "status": "running"}

    # ----------------------------------------------------------------------------------------------- ocr_status
    def status(self, job_id: str = "", wait_s: Any = 0) -> dict[str, Any]:
        if not str(job_id or "").strip():
            return self.ocr.status()
        view = self.jobs.wait(str(job_id).strip(), wait_s)
        if view is None:
            raise KafkaError("not_found", f"no such job: {job_id}", "Finished jobs are kept for one hour.")
        return view
