"""Optional OCR with rapidocr (+ onnxruntime). When it is not installed OCR is reported as unavailable; nothing ever crashes."""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional

log = logging.getLogger("kafka.ocr")

MAX_SIDE = 3200   # larger scans are shrunk before recognition (speed); the stored original is never touched


def _rows(boxes: Any, texts: Any) -> str:
    """Join recognised fragments into reading-order lines: group by vertical position, left to right inside a line."""
    items: list[tuple[float, float, float, str]] = []
    for box, text in zip(boxes if boxes is not None else [], texts if texts is not None else []):
        text = str(text or "").strip()
        if not text:
            continue
        try:
            xs = [float(p[0]) for p in box]
            ys = [float(p[1]) for p in box]
        except (TypeError, ValueError, IndexError):
            continue
        items.append((sum(ys) / len(ys), min(xs), max(ys) - min(ys), text))
    if not items:
        return ""
    items.sort(key=lambda it: (it[0], it[1]))
    heights = sorted(h for _, _, h, _ in items if h > 0)
    tolerance = (heights[len(heights) // 2] * 0.6) if heights else 10.0
    lines: list[list[tuple[float, float, str]]] = []
    for yc, x, _h, text in items:
        if lines and abs(lines[-1][0][0] - yc) <= tolerance:
            lines[-1].append((yc, x, text))
        else:
            lines.append([(yc, x, text)])
    return "\n".join("  ".join(t for _, _, t in sorted(line, key=lambda it: it[1])) for line in lines)


class Ocr:
    """``read(image)`` -> text. ``factory`` builds the engine (a callable taking a numpy BGR image and returning an object with
    ``boxes`` and ``txts``); tests inject a fake one."""

    def __init__(self, enabled: bool = True, factory: Optional[Callable[[], Any]] = None):
        self.enabled = enabled
        self._factory = factory
        self._engine: Any = None
        self._error = ""
        self._lock = threading.Lock()

    def _load(self) -> Any:
        if self._engine is not None or self._error:
            return self._engine
        try:
            if self._factory is not None:
                self._engine = self._factory()
            else:
                logging.getLogger("RapidOCR").setLevel(logging.ERROR)
                from rapidocr import RapidOCR  # type: ignore

                try:
                    self._engine = RapidOCR(params={"Global.log_level": "error"})
                except Exception:  # noqa: BLE001 — older parameter names
                    self._engine = RapidOCR()
        except Exception as exc:  # noqa: BLE001 — missing package, missing model, broken runtime
            self._error = f"{type(exc).__name__}: {str(exc)[:120]}"
            log.info("OCR unavailable: %s", self._error)
        return self._engine

    def available(self) -> tuple[bool, str]:
        if not self.enabled:
            return False, "OCR is switched off (KAFKA_OCR=0)"
        if self._factory is None:
            try:
                import importlib.util
                if importlib.util.find_spec("rapidocr") is None:
                    return False, "rapidocr is not installed (pip install rapidocr onnxruntime)"
            except (ImportError, ValueError):
                return False, "rapidocr is not installed (pip install rapidocr onnxruntime)"
        if self._error:
            return False, self._error
        return True, "rapidocr"

    def read(self, image: Any) -> str:
        """Text of a PIL image; empty when OCR is unavailable or fails."""
        if not self.enabled:
            return ""
        with self._lock:
            engine = self._load()
            if engine is None:
                return ""
            try:
                import numpy as np

                rgb = image.convert("RGB")
                if max(rgb.size) > MAX_SIDE:
                    scale = MAX_SIDE / max(rgb.size)
                    rgb = rgb.resize((max(1, int(rgb.width * scale)), max(1, int(rgb.height * scale))))
                array = np.asarray(rgb)[:, :, ::-1].copy()      # RGB -> BGR
                out = engine(array)
                boxes = getattr(out, "boxes", None)
                texts = getattr(out, "txts", None)
                return _rows(boxes, texts)
            except Exception as exc:  # noqa: BLE001
                self._error = ""    # one bad image must not disable OCR for the rest
                log.info("OCR failed on one image: %s", type(exc).__name__)
                return ""
