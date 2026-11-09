"""Indra OCR (ONNX Runtime, CPU): a fast line detector and recogniser that also gives reading order and Markdown.

``pip install indra-ocr``. The code is Apache-2.0; its weights (about 76 MB) are under a personal-use licence and are downloaded from
Hugging Face the first time the engine starts. Latin script is what Kafka needs; the engine reads many more scripts on its own.
"""

from __future__ import annotations

import importlib.util
import os
from typing import Any, Callable, Optional

from .base import Block, OcrBackend, OcrResult, rect_box

INSTALL = "indra-ocr is not installed (pip install indra-ocr)"


class IndraBackend(OcrBackend):
    name = "indra"

    def __init__(self, language: str = "es", factory: Optional[Callable[[], Any]] = None):
        """``factory`` builds the converter (an object with ``ocr(gray) -> Page``); tests inject a fake one."""
        if factory is not None:
            self._conv = factory()
            return
        if importlib.util.find_spec("indra_ocr") is None:
            raise ImportError(INSTALL)
        import indra_ocr  # type: ignore

        threads = max(1, min(4, (os.cpu_count() or 2)))
        self._conv = indra_ocr.load(threads=threads)          # downloads the models on the first run

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

    def recognise(self, image: Any, lang: str = "es") -> OcrResult:
        import numpy as np

        gray = np.asarray(image.convert("L"), dtype=np.uint8)
        page = self._conv.ocr(gray)
        if getattr(page, "source", "") == "error":
            raise RuntimeError(f"indra could not read the image: {getattr(page, 'note', '')[:120]}")
        blocks: list[Block] = []
        for line in page.lines or []:
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
        text = "\n".join(b.text for b in blocks)             # Indra's own reading order
        return OcrResult(blocks=blocks, text=text, backend=self.name, markdown=str(getattr(page, "markdown", "") or ""), layout=layout)
