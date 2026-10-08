"""OCR for scanned pages and photos, the family's one engine: Kafka owns it and other apps reach it through ``ocr_image`` / ``ocr_pdf`` /
``doc_extract``. Backends are picked in the order winocr (Windows, fast), rapidocr (any system, the default install), light-ocr, tesseract; the first
one that is available runs, ``KAFKA_OCR_BACKEND`` forces one. When nothing is installed OCR is reported as unavailable and nothing crashes.

``Ocr(enabled, factory=...)`` is what the rest of Kafka uses: ``read(image) -> text``, ``recognise(image, lang) -> OcrResult`` (blocks with
boxes and confidences), ``available() -> (ok, why)`` and ``status()`` for settings pages. ``factory`` builds a rapidocr-like engine and is
how the tests plug a fake one in.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional

from .base import Block, OcrBackend, OcrResult, join_blocks, lang_code, rows

log = logging.getLogger("kafka.ocr")

MAX_SIDE = 3200                       # larger scans are shrunk before recognition (speed); the stored original is never touched
ORDER = ("winocr", "rapidocr", "light-ocr", "tesseract")
_rows = rows                          # the old private name, kept for the tests that group boxes into lines

__all__ = ["Ocr", "OcrResult", "Block", "OcrBackend", "ORDER", "MAX_SIDE", "join_blocks", "select_ocr", "ocr_availability"]


def _classes() -> dict[str, type[OcrBackend]]:
    from .rapid import RapidBackend
    from .tesseract import TesseractBackend
    from .winocr_backend import WinOcrBackend
    from .light import LightBackend

    return {"winocr": WinOcrBackend, "rapidocr": RapidBackend, "light-ocr": LightBackend, "tesseract": TesseractBackend}


def ocr_availability() -> dict[str, dict[str, Any]]:
    """For a settings page: which engines could run here and why not."""
    out: dict[str, dict[str, Any]] = {}
    for name, cls in _classes().items():
        ok, reason = cls.available()
        out[name] = {"available": ok, "reason": reason}
    return out


def select_ocr(requested: str = "auto", language: str = "es", *, skip: tuple[str, ...] = ()) -> tuple[Optional[OcrBackend], list[str]]:
    """Start the requested engine or the first available one: ``(engine | None, notes)``. ``skip`` names engines that already failed."""
    classes = _classes()
    notes: list[str] = []
    requested = (requested or "auto").strip().lower()
    candidates = list(ORDER) if requested == "auto" else [requested, *[n for n in ORDER if n != requested]]
    for name in candidates:
        cls = classes.get(name)
        if cls is None or name in skip:
            continue
        ok, reason = cls.available()
        if not ok:
            notes.append(f"{name}: {reason}")
            continue
        try:
            engine = cls(language)
        except Exception as exc:  # noqa: BLE001 - a broken install
            notes.append(f"{name}: failed to start ({type(exc).__name__}: {str(exc)[:100]})")
            continue
        if requested not in ("auto", name):
            notes.append(f"requested {requested} unavailable, using {name}")
        return engine, notes
    notes.append("no OCR engine available")
    return None, notes


class Ocr:
    """``read(image)`` -> text; ``recognise(image, lang)`` -> blocks. One engine in memory, one image at a time."""

    def __init__(self, enabled: bool = True, factory: Optional[Callable[[], Any]] = None, *, backend: str = "auto", lang: str = "es"):
        self.enabled = enabled
        self.lang = lang_code(lang)
        self._factory = factory
        self._requested = (backend or "auto").strip().lower()
        self._engine: Optional[OcrBackend] = None
        self._broken: dict[str, str] = {}
        self._error = ""
        self._notes: list[str] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------------------------------------ loading
    def _load(self) -> Optional[OcrBackend]:
        if self._engine is not None:
            return self._engine
        if self._factory is not None:                              # tests: a fake rapidocr-like engine
            if self._error:
                return None
            try:
                from .rapid import RapidBackend

                self._engine = RapidBackend(self.lang, factory=self._factory)
            except Exception as exc:  # noqa: BLE001
                self._error = f"{type(exc).__name__}: {str(exc)[:120]}"
                log.info("OCR unavailable: %s", self._error)
            return self._engine
        engine, notes = select_ocr(self._requested, self.lang, skip=tuple(self._broken))
        self._notes = notes
        if engine is None:
            self._error = "; ".join(notes)[:300]
        else:
            self._error = ""
            self._engine = engine
            log.info("OCR engine: %s", engine.name)
        return self._engine

    def available(self) -> tuple[bool, str]:
        """``(usable, backend name or the reason it is not)``. Cheap: it does not load a model."""
        if not self.enabled:
            return False, "OCR is switched off (KAFKA_OCR=0)"
        if self._factory is not None:
            return (False, self._error) if self._error else (True, "rapidocr")
        if self._engine is not None:
            return True, self._engine.name
        classes = _classes()
        requested = self._requested
        order = list(ORDER) if requested == "auto" else [requested, *[n for n in ORDER if n != requested]]
        reasons: list[str] = []
        for name in order:
            cls = classes.get(name)
            if cls is None or name in self._broken:
                if name in self._broken:
                    reasons.append(f"{name}: {self._broken[name]}")
                continue
            ok, why = cls.available()
            if ok:
                return True, name
            reasons.append(f"{name}: {why}")
        if reasons and all("not installed" in r or "only works on Windows" in r or "not usable" in r for r in reasons):
            rapid = next((r for r in reasons if r.startswith("rapidocr")), "")
            return False, rapid.split(": ", 1)[1] if rapid else "; ".join(reasons)
        return False, "; ".join(reasons) or "no OCR engine"

    def status(self, lang: Optional[str] = None) -> dict[str, Any]:
        """``{available, backend, languages, reason?, backends, requested, enabled}`` for ``ocr_status`` and the settings page."""
        ok, why = self.available()
        classes = _classes()
        out: dict[str, Any] = {"available": ok, "backend": why if ok else "", "enabled": self.enabled, "requested": self._requested,
                               "default_language": self.lang, "languages": []}
        if ok:
            cls = classes.get(why)
            out["languages"] = cls.languages() if cls is not None else ["es", "en"]
        else:
            out["reason"] = why
        out["backends"] = ocr_availability()
        return out

    # ------------------------------------------------------------------------------------------------ recognition
    def recognise(self, image: Any, lang: Optional[str] = None) -> OcrResult:
        """Blocks of a PIL image in the coordinates of ``image``; an empty result (with ``error`` when the engine failed) otherwise."""
        if not self.enabled:
            return OcrResult(error="OCR is switched off")
        code = lang_code(lang, self.lang)
        with self._lock:
            for _ in range(len(ORDER) + 1):
                engine = self._load()
                if engine is None:
                    return OcrResult(error=self._error or "OCR is unavailable")
                try:
                    return self._run(engine, image, code)
                except Exception as exc:  # noqa: BLE001
                    detail = f"{type(exc).__name__}: {str(exc)[:120]}"
                    log.info("OCR failed on one image (%s): %s", engine.name, type(exc).__name__)
                    if self._factory is None and self._requested == "auto" and _is_load_failure(exc):
                        self._broken[engine.name] = detail          # an engine that cannot run at all: the next one
                        self._engine = None
                        continue
                    return OcrResult(backend=engine.name, error=detail)     # one bad image must not disable OCR for the rest
            return OcrResult(error="no OCR engine could run")

    @staticmethod
    def _run(engine: OcrBackend, image: Any, lang: str) -> OcrResult:
        rgb = image.convert("RGB")
        scale = 1.0
        if max(rgb.size) > MAX_SIDE:
            scale = MAX_SIDE / max(rgb.size)
            rgb = rgb.resize((max(1, int(rgb.width * scale)), max(1, int(rgb.height * scale))))
        result = engine.recognise(rgb, lang)
        if scale != 1.0:                                   # boxes in the coordinates of the original image
            for block in result.blocks:
                block.box = [[p[0] / scale, p[1] / scale] for p in block.box]
        result.backend = result.backend or engine.name
        return result

    def read(self, image: Any) -> str:
        """Text of a PIL image; empty when OCR is unavailable or fails."""
        return self.recognise(image).text


def _is_load_failure(exc: Exception) -> bool:
    """An error that says the engine cannot work at all (a missing DLL, a missing model), not that one image was odd."""
    return isinstance(exc, (ImportError, OSError, NotImplementedError)) and not isinstance(exc, (FileNotFoundError, PermissionError))
