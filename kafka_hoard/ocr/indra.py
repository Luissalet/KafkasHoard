"""Indra OCR (ONNX Runtime): line detection and recognition in 80+ languages, reading order, layout blocks and Markdown with tables.

``pip install indra-ocr``. The code is Apache-2.0; its weights (about 76 MB, OpenRAIL-M) are downloaded from Hugging Face the first time
the engine starts. Besides one image (``recognise``), a PDF goes through Indra's own document path (``convert_pdf``): the text layer where it
can be trusted, OCR where it cannot (``mode`` auto | ocr | text), so a digital PDF gets its Markdown and tables too. Options per call:
``furniture`` keeps page headers and footers in the Markdown, ``math`` writes equations as LaTeX (its models download on first use).

Speed: ``threads`` (KAFKA_OCR_THREADS, default 4: more does not help a single page), ``device`` cpu or gpu (KAFKA_OCR_DEVICE; gpu needs
onnxruntime-gpu and uses the CUDA device KAFKA_OCR_GPU, default 3) and ``workers``: a PDF of POOL_MIN_PAGES pages or more is spread over
that many worker processes (KAFKA_OCR_WORKERS, default 4; 0 or 1 reads it in this process). The pool starts on the first long PDF and
stops after five idle minutes.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from .base import Block, OcrBackend, OcrResult, rect_box

log = logging.getLogger("kafka.ocr.indra")

INSTALL = "indra-ocr is not installed (pip install indra-ocr)"
MODES = ("auto", "ocr", "text")
POOL_MIN_PAGES = 6                 # fewer pages: one process is as fast (each worker takes ~3 s to load its models)
POOL_IDLE_S = 300.0


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.environ.get(name, "") or default)))
    except ValueError:
        return default


def providers_for(device: str, gpu_id: int) -> tuple[Optional[list], str]:
    """ONNX Runtime providers for ``device`` (cpu | gpu) and a note when the GPU was asked for but cannot be used."""
    if (device or "cpu").lower() != "gpu":
        return None, ""
    try:
        import onnxruntime as ort  # type: ignore

        found = set(ort.get_available_providers())
    except Exception as exc:  # noqa: BLE001
        return None, f"gpu unavailable ({type(exc).__name__}); using the CPU"
    if "CUDAExecutionProvider" in found:
        try:
            ort.preload_dlls()                 # the CUDA and cuDNN DLLs of the nvidia-* wheels, when installed that way
        except Exception:  # noqa: BLE001
            pass
        return [("CUDAExecutionProvider", {"device_id": int(gpu_id)}), "CPUExecutionProvider"], ""
    return None, "gpu unavailable: onnxruntime-gpu is not installed; using the CPU"


class IndraBackend(OcrBackend):
    name = "indra"
    accepts_options = True                 # recognise(..., options={furniture, math})

    def __init__(self, language: str = "es", factory: Optional[Callable[[], Any]] = None, *, threads: Optional[int] = None,
                 device: Optional[str] = None, gpu_id: Optional[int] = None, workers: Optional[int] = None):
        """``factory`` builds the converter (an object with ``ocr(gray) -> Page``); tests inject a fake one."""
        self.threads = threads or _env_int("KAFKA_OCR_THREADS", 4, 1, 32)
        self.device = (device or os.environ.get("KAFKA_OCR_DEVICE") or "cpu").strip().lower()
        self.gpu_id = gpu_id if gpu_id is not None else _env_int("KAFKA_OCR_GPU", 3, 0, 15)
        self.workers = workers if workers is not None else _env_int("KAFKA_OCR_WORKERS", 4, 0, 32)
        self.providers, self.device_note = providers_for(self.device, self.gpu_id)
        self._lock = threading.RLock()
        self._extra: dict[str, Any] = {}
        self._pool: Any = None
        self._pool_key: tuple = ()
        self._pool_used = 0.0
        self._factory = factory
        if factory is not None:
            self._conv = factory()
            return
        if importlib.util.find_spec("indra_ocr") is None:
            raise ImportError(INSTALL)
        self._conv = self._load(math=False)                # downloads the models on the first run

    def _load(self, *, math: bool) -> Any:
        import indra_ocr  # type: ignore

        return indra_ocr.load(threads=self.threads, providers=self.providers, math=math)

    @classmethod
    def available(cls) -> tuple[bool, str]:
        try:
            found = importlib.util.find_spec("indra_ocr") is not None
        except (ImportError, ValueError):
            found = False
        return (True, "ok") if found else (False, INSTALL)

    @classmethod
    def languages(cls) -> list[str]:
        return ["es", "en", "fr", "pt", "it", "de", "ca"]

    @property
    def device_used(self) -> str:
        return f"gpu:{self.gpu_id}" if self.providers else "cpu"

    # ------------------------------------------------------------------------------------------------ converters
    def _converter(self, options: Optional[dict[str, Any]] = None) -> Any:
        """The converter for these options: math needs its own (its models load once), furniture is a switch on the shared one."""
        opts = options or {}
        conv = self._conv
        if opts.get("math") and self._factory is None:
            if "math" not in self._extra:
                self._extra["math"] = self._load(math=True)
            conv = self._extra["math"]
        inner = getattr(conv, "ocr", None)
        if inner is not None and hasattr(inner, "furniture"):
            inner.furniture = bool(opts.get("furniture"))
        return conv

    # ------------------------------------------------------------------------------------------------ one image
    def recognise(self, image: Any, lang: str = "es", options: Optional[dict[str, Any]] = None) -> OcrResult:
        import numpy as np

        gray = np.asarray(image.convert("L"), dtype=np.uint8)
        with self._lock:
            conv = self._converter(options)
            page = conv.ocr(gray)
        if getattr(page, "source", "") == "error":
            raise RuntimeError(f"indra could not read the image: {getattr(page, 'note', '')[:120]}")
        return self.to_result(page, size=(int(gray.shape[1]), int(gray.shape[0])))

    def to_result(self, page: Any, *, size: Optional[tuple[int, int]] = None) -> OcrResult:
        """An Indra ``Page`` as Kafka's ``OcrResult`` (lines, Markdown, layout blocks and what Indra says about the page)."""
        blocks: list[Block] = []
        for line in getattr(page, "lines", None) or []:
            text = str(getattr(line, "text", "") or "").strip()
            if not text:
                continue
            try:
                x0, y0, x1, y1 = (float(v) for v in line.box)
            except (TypeError, ValueError):
                continue
            blocks.append(Block(text, rect_box(x0, y0, x1 - x0, y1 - y0), round(float(getattr(line, "conf", 0.0) or 0.0), 3)))
        layout = []
        for b in getattr(page, "blocks", None) or []:
            try:
                layout.append({"label": str(b["label"]), "bbox": [round(float(v), 1) for v in b["bbox"]], "text": str(b.get("text") or "")})
            except (KeyError, TypeError, ValueError):
                continue
        meta: dict[str, Any] = {"source": str(getattr(page, "source", "") or "ocr"), "rotation": int(getattr(page, "rotation", 0) or 0),
                                "skew": float(getattr(page, "skew", 0.0) or 0.0), "vertical": bool(getattr(page, "vertical", False)),
                                "device": self.device_used}
        timings = getattr(page, "timings", None)
        if isinstance(timings, dict) and timings.get("total_ms") is not None:
            meta["ms"] = round(float(timings["total_ms"]), 1)
        if size:
            meta["size"] = [int(size[0]), int(size[1])]
        text = "\n".join(b.text for b in blocks)             # Indra's own reading order
        return OcrResult(blocks=blocks, text=text, backend=self.name, markdown=str(getattr(page, "markdown", "") or ""), layout=layout, meta=meta)

    # ------------------------------------------------------------------------------------------------ a PDF
    def convert_pdf(self, data: bytes, numbers: list[int], *, dpi: int = 200, mode: str = "ocr", password: str = "",
                    options: Optional[dict[str, Any]] = None) -> Iterator[tuple[int, OcrResult]]:
        """``(page, OcrResult)`` for the 1-based ``numbers``, in order. ``mode``: ocr (never the text layer), auto (the layer where it is sound,
        OCR elsewhere), text (the layer even when its checks fail). A page Indra cannot read comes back with ``error`` set."""
        mode = mode if mode in MODES else "ocr"
        if self._factory is None and self.workers > 1 and len(numbers) >= POOL_MIN_PAGES:
            pooled = self._convert_pooled(data, numbers, dpi=dpi, mode=mode, password=password, options=options)
            if pooled is not None:
                yield from pooled
                return
        import pypdfium2 as pdfium  # type: ignore

        doc = pdfium.PdfDocument(data, password=password or None)
        try:
            total = len(doc)
            for number in numbers:
                if not 1 <= number <= total:
                    raise ValueError(f"page {number} does not exist (the PDF has {total})")
                with self._lock:
                    conv = self._converter(options)
                    old_dpi, old_mode = conv.dpi, conv.mode
                    conv.dpi, conv.mode = float(dpi), mode
                    try:
                        page = conv._safe(lambda: conv.pdf_page("", doc, number - 1))
                    finally:
                        conv.dpi, conv.mode = old_dpi, old_mode
                yield number, self._page_result(page)
        finally:
            doc.close()

    def _page_result(self, page: Any) -> OcrResult:
        if getattr(page, "source", "") == "error":
            return OcrResult(backend=self.name, error=f"indra: {str(getattr(page, 'note', '') or 'failed')[:160]}")
        return self.to_result(page)

    def _convert_pooled(self, data: bytes, numbers: list[int], *, dpi: int, mode: str, password: str,
                        options: Optional[dict[str, Any]]) -> Optional[Iterator[tuple[int, OcrResult]]]:
        """The pages on the worker pool; None when the pool cannot start (the caller then reads them here)."""
        opts = options or {}
        key = (float(dpi), mode, bool(opts.get("furniture")), bool(opts.get("math")))
        try:
            pool = self._ensure_pool(key)
        except Exception as exc:  # noqa: BLE001
            log.info("OCR worker pool unavailable: %s", type(exc).__name__)
            return None
        handle, tmp = tempfile.mkstemp(prefix="kafka-ocr-", suffix=".pdf")
        os.close(handle)
        Path(tmp).write_bytes(data)
        units = [(tmp, n - 1, password or None) for n in numbers]

        def run() -> Iterator[tuple[int, OcrResult]]:
            from concurrent.futures.process import BrokenProcessPool

            try:
                for number, res in zip(numbers, pool.map(_pool_page, units)):
                    self._pool_used = time.time()
                    if isinstance(res, Exception):
                        yield number, OcrResult(backend=self.name, error=f"indra: {type(res).__name__}: {str(res)[:120]}")
                    else:
                        yield number, self._page_result(res)
            except BrokenProcessPool:
                self._stop_pool()
                raise RuntimeError("the OCR worker processes stopped") from None
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
        return run()

    def _ensure_pool(self, key: tuple) -> Any:
        with self._lock:
            if self._pool is not None and self._pool_key == key:
                self._pool_used = time.time()
                return self._pool
            self._stop_pool()
            from concurrent.futures import ProcessPoolExecutor
            from multiprocessing import get_context

            from indra_ocr.models import model_dir  # type: ignore

            dpi, mode, furniture, math = key
            kwargs = {"path": str(model_dir(None, math=math)), "threads": 1, "dpi": dpi, "mode": mode, "furniture": furniture, "math": math,
                      "providers": self.providers}
            self._pool = ProcessPoolExecutor(self.workers, mp_context=get_context("spawn"), initializer=_pool_init, initargs=(kwargs,))
            self._pool_key = key
            self._pool_used = time.time()
            threading.Thread(target=self._reaper, args=(self._pool,), name="kafka-ocr-pool-reaper", daemon=True).start()
            log.info("OCR worker pool started: %d workers", self.workers)
            return self._pool

    def _reaper(self, pool: Any) -> None:
        while self._pool is pool:
            time.sleep(15)
            if self._pool is pool and time.time() - self._pool_used > POOL_IDLE_S:
                with self._lock:
                    if self._pool is pool:
                        self._stop_pool()
                return

    def _stop_pool(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    def pool_running(self) -> int:
        return self.workers if self._pool is not None else 0

    def close(self) -> None:
        self._stop_pool()


# ---------------------------------------------------------------------------------------------------- worker processes
_worker_conv: Any = None


def _pool_init(kwargs: dict[str, Any]) -> None:
    global _worker_conv
    import cv2  # type: ignore
    import indra_ocr  # type: ignore

    cv2.setNumThreads(1)
    _worker_conv = indra_ocr.load(**kwargs)


def _pool_page(unit: tuple) -> Any:
    """One PDF page in a worker: Indra's ``Page`` (picklable), or the exception. The file is opened and closed per page so the
    temporary copy can be deleted on Windows as soon as the job ends."""
    path, index, password = unit
    try:
        import pypdfium2 as pdfium  # type: ignore

        doc = pdfium.PdfDocument(path, password=password)
        try:
            return _worker_conv._safe(lambda: _worker_conv.pdf_page(path, doc, index))
        finally:
            doc.close()
    except Exception as exc:  # noqa: BLE001
        return exc
