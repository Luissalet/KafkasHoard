"""Tesseract through ``pytesseract`` (only when it is importable and the program is installed)."""

from __future__ import annotations

import importlib.util
from typing import Any

from .base import LANG_TAGS, Block, OcrBackend, OcrResult, join_blocks, rect_box


class TesseractBackend(OcrBackend):
    name = "tesseract"

    def __init__(self, language: str = "es"):
        import pytesseract  # type: ignore

        self._pt = pytesseract

    @classmethod
    def available(cls) -> tuple[bool, str]:
        try:
            if importlib.util.find_spec("pytesseract") is None:
                return False, "pytesseract is not installed (pip install pytesseract) or the tesseract program is missing"
        except (ImportError, ValueError):
            return False, "pytesseract is not installed"
        try:
            import pytesseract  # type: ignore

            pytesseract.get_tesseract_version()
        except Exception as exc:  # noqa: BLE001
            return False, f"the tesseract program is not usable: {str(exc)[:100]}"
        return True, "ok"

    @classmethod
    def languages(cls) -> list[str]:
        try:
            import pytesseract  # type: ignore

            back = {v: k for k, v in LANG_TAGS.items()}
            return sorted({back.get(code, code) for code in pytesseract.get_languages(config="") if code != "osd"})
        except Exception:  # noqa: BLE001
            return []

    def recognise(self, image: Any, lang: str = "es") -> OcrResult:
        tag = LANG_TAGS.get(lang, lang)
        rgb = image.convert("RGB")
        try:
            data = self._pt.image_to_data(rgb, lang=tag, output_type=self._pt.Output.DICT)
        except self._pt.TesseractError:                  # the language pack is not installed: the default one
            data = self._pt.image_to_data(rgb, output_type=self._pt.Output.DICT)
        lines: dict[tuple, list[int]] = {}
        for i, word in enumerate(data["text"]):
            word = (word or "").strip()
            if not word or float(data["conf"][i]) < 0:
                continue
            lines.setdefault((data["block_num"][i], data["par_num"][i], data["line_num"][i]), []).append(i)
        blocks: list[Block] = []
        for idx in lines.values():
            left = min(data["left"][i] for i in idx)
            top = min(data["top"][i] for i in idx)
            right = max(data["left"][i] + data["width"][i] for i in idx)
            bottom = max(data["top"][i] + data["height"][i] for i in idx)
            conf = sum(float(data["conf"][i]) for i in idx) / len(idx) / 100
            blocks.append(Block(" ".join(data["text"][i].strip() for i in idx), rect_box(left, top, right - left, bottom - top), round(conf, 3)))
        return OcrResult(blocks=blocks, text=join_blocks(blocks), backend=self.name)
