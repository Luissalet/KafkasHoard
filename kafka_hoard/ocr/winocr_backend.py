"""Windows.Media.Ocr through the ``winocr`` package (Windows 10/11 only, very fast, no model to download).

Not verifiable off Windows: the result is read defensively (a dict or a WinRT object). It reports no confidence, so every block says 1.0.
"""

from __future__ import annotations

import importlib.util
import sys
from typing import Any

from .base import Block, OcrBackend, OcrResult, join_blocks, rect_box


def _load_onnxruntime_first() -> None:
    """Measured on Windows 11 / Python 3.13: importing ``winocr`` (WinRT) and then ``onnxruntime`` dies with an access violation, while
    the other order is fine. So whenever onnxruntime is installed it is imported before WinRT touches the process."""
    try:
        if importlib.util.find_spec("onnxruntime") is None:
            return
        import onnxruntime  # noqa: F401
    except Exception:  # noqa: BLE001 - a broken onnxruntime must not take winocr down with it
        pass


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class WinOcrBackend(OcrBackend):
    name = "winocr"

    def __init__(self, language: str = "es"):
        _load_onnxruntime_first()
        import winocr  # type: ignore

        self._winocr = winocr

    @classmethod
    def available(cls) -> tuple[bool, str]:
        if sys.platform != "win32":
            return False, "winocr only works on Windows"
        try:
            if importlib.util.find_spec("winocr") is None:
                return False, "winocr is not installed (pip install winocr)"
        except (ImportError, ValueError):
            return False, "winocr is not installed (pip install winocr)"
        return True, "ok"

    @classmethod
    def languages(cls) -> list[str]:
        return []                       # the installed Windows language packs are not listed here

    def recognise(self, image: Any, lang: str = "es") -> OcrResult:
        rgb = image.convert("RGB")
        try:
            result = self._winocr.recognize_pil_sync(rgb, lang)
        except Exception:  # noqa: BLE001 - the language pack may be missing: retry with English, which Windows ships
            result = self._winocr.recognize_pil_sync(rgb, "en")
        blocks: list[Block] = []
        for line in _get(result, "lines", []) or []:
            words = _get(line, "words", []) or []
            rects = [r for r in (_get(w, "bounding_rect") for w in words) if r is not None]
            text = " ".join(str(_get(w, "text", "")) for w in words).strip() or str(_get(line, "text", "")).strip()
            if not text:
                continue
            if rects:
                x = min(float(_get(r, "x", 0)) for r in rects)
                y = min(float(_get(r, "y", 0)) for r in rects)
                right = max(float(_get(r, "x", 0)) + float(_get(r, "width", 0)) for r in rects)
                bottom = max(float(_get(r, "y", 0)) + float(_get(r, "height", 0)) for r in rects)
                blocks.append(Block(text, rect_box(x, y, right - x, bottom - y), 1.0))
            else:
                blocks.append(Block(text, rect_box(0, len(blocks) * 20, image.width, 20), 1.0))
        return OcrResult(blocks=blocks, text=join_blocks(blocks), backend=self.name)
