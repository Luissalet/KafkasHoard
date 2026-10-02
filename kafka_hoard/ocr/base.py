"""OCR backend interface and the shared result types.

The structure (a backend per engine, blocks with a confidence, an availability report) comes from the screen-memory app's OCR
package; Kafka is the family's OCR owner, so it lives here now. A block is one recognised line: its text, the four corners of its
box in the coordinates of the image that was given, and a confidence from 0 to 1.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

LANG_TAGS = {"es": "spa", "en": "eng", "fr": "fra", "de": "deu", "pt": "por", "it": "ita", "ca": "cat"}


def lang_code(language: Optional[str], default: str = "es") -> str:
    """``"es"``, ``"es-ES"``, ``"spa"``, ``"ES"`` -> ``"es"``; anything unknown is cut to its first two letters."""
    text = str(language or default).strip().lower().replace("_", "-")
    if not text:
        return default
    if text in LANG_TAGS.values():
        return next(k for k, v in LANG_TAGS.items() if v == text)
    return text.split("-")[0][:2] or default


@dataclass
class Block:
    text: str
    box: list[list[float]]              # four corners [[x, y], ...], clockwise from the top left
    score: float = 0.0                  # 0..1; engines that do not say (Windows OCR) report 1.0

    @property
    def y(self) -> float:
        ys = [p[1] for p in self.box] or [0.0]
        return sum(ys) / len(ys)

    @property
    def x(self) -> float:
        return min((p[0] for p in self.box), default=0.0)

    def public(self) -> dict[str, Any]:
        return {"text": self.text, "box": [[round(float(p[0]), 1), round(float(p[1]), 1)] for p in self.box], "score": round(float(self.score), 3)}


@dataclass
class OcrResult:
    blocks: list[Block] = field(default_factory=list)
    text: str = ""
    backend: str = ""
    error: str = ""                      # set when the engine itself failed (an image without text has no error)


class OcrBackend:
    """One engine. ``available()`` is cheap and never raises; ``recognise`` takes a PIL image."""

    name = "base"

    @classmethod
    def available(cls) -> tuple[bool, str]:
        return False, "not implemented"

    @classmethod
    def languages(cls) -> list[str]:
        return []

    def recognise(self, image: Any, lang: str = "es") -> OcrResult:  # pragma: no cover - interface
        raise NotImplementedError


def rect_box(x: float, y: float, w: float, h: float) -> list[list[float]]:
    return [[x, y], [x + w, y], [x + w, y + h], [x, y + h]]


def join_blocks(blocks: Sequence[Block]) -> str:
    """The blocks as text in reading order: lines by vertical position (tolerance from the median height), left to right inside a line."""
    return rows([b.box for b in blocks], [b.text for b in blocks])


def rows(boxes: Any, texts: Any) -> str:
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
