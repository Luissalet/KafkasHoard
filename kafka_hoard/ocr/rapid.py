"""RapidOCR (ONNX runtime, CPU): the cross-platform default. Reads Latin script; Spanish and English are what Kafka uses it for."""

from __future__ import annotations

import importlib.util
import logging
from typing import Any, Callable, Optional

from .base import Block, OcrBackend, OcrResult, join_blocks

INSTALL = "rapidocr is not installed (pip install rapidocr onnxruntime)"


class RapidBackend(OcrBackend):
    name = "rapidocr"

    # Two packages expose the same engine: ``rapidocr`` (v2+, Python 3.13 wheels) and the older ``rapidocr_onnxruntime``.
    @staticmethod
    def _package() -> Optional[str]:
        for name in ("rapidocr", "rapidocr_onnxruntime"):
            try:
                if importlib.util.find_spec(name) is not None:
                    return name
            except (ImportError, ValueError):
                continue
        return None

    def __init__(self, language: str = "es", factory: Optional[Callable[[], Any]] = None):
        """``factory`` builds the engine (a callable taking a numpy BGR image); tests inject a fake one."""
        self._modern = True
        if factory is not None:
            self._engine = factory()
            return
        logging.getLogger("RapidOCR").setLevel(logging.ERROR)
        package = self._package()
        if package is None:
            raise ImportError(INSTALL)
        if package == "rapidocr":
            from rapidocr import RapidOCR  # type: ignore

            try:
                self._engine = RapidOCR(params={"Global.log_level": "error"})
            except Exception:  # noqa: BLE001 - older parameter names
                self._engine = RapidOCR()
        else:
            from rapidocr_onnxruntime import RapidOCR  # type: ignore

            self._engine = RapidOCR()
            self._modern = False

    @classmethod
    def available(cls) -> tuple[bool, str]:
        if cls._package() is None:
            return False, INSTALL
        return True, "ok"

    @classmethod
    def languages(cls) -> list[str]:
        return ["es", "en"]

    def _items(self, array: Any) -> list[tuple[Any, str, float]]:
        if self._modern:
            out = self._engine(array)
            boxes = getattr(out, "boxes", None)
            texts = getattr(out, "txts", None)
            scores = getattr(out, "scores", None)
            if boxes is None or texts is None:
                return []
            scores = list(scores) if scores is not None else [0.0] * len(texts)
            return [(b, str(t or ""), float(s)) for b, t, s in zip(boxes, texts, scores)]
        result, _elapse = self._engine(array)
        return [(item[0], str(item[1] or ""), float(item[2])) for item in (result or [])]

    def recognise(self, image: Any, lang: str = "es") -> OcrResult:
        import numpy as np

        rgb = image.convert("RGB")
        array = np.asarray(rgb)[:, :, ::-1].copy()      # RGB -> BGR
        blocks: list[Block] = []
        for quad, text, score in self._items(array):
            text = text.strip()
            if not text:
                continue
            try:
                box = [[float(p[0]), float(p[1])] for p in quad]
            except (TypeError, ValueError, IndexError):
                continue
            blocks.append(Block(text, box, round(score, 3)))
        return OcrResult(blocks=blocks, text=join_blocks(blocks), backend=self.name)
