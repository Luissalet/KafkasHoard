"""Optional light-ocr native engine through Node, shared by the whole family.

CPU execution avoids taking the chat model's GPU. Package installation brings
its models; this backend never downloads anything at recognition time.
"""
from __future__ import annotations

import io
import json
import math
from pathlib import Path

from ..hoard_link import proc
from .base import Block, OcrBackend, OcrResult, join_blocks

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "node_modules" / "@arcships" / "light-ocr" / "package.json"
WORKER = ROOT / "scripts" / "light_ocr.mjs"
INSTALL = "light-ocr is not installed (npm install --include=optional)"


class LightBackend(OcrBackend):
    name = "light-ocr"

    @classmethod
    def available(cls):
        if not PACKAGE.is_file() or not WORKER.is_file():
            return False, INSTALL
        if not proc.find_exe("node"):
            return False, "Node.js 22 or newer is not installed"
        return True, "ok"

    @classmethod
    def languages(cls):
        return ["es", "en", "fr", "de", "pt", "it", "zh"]

    def __init__(self, language="es"):
        ok, reason = self.available()
        if not ok:
            raise ImportError(reason)
        self.node = proc.find_exe("node")

    def recognise(self, image, lang="es"):
        buffer = io.BytesIO()
        image.convert("RGB").save(buffer, format="PNG")
        got = proc.run([self.node, str(WORKER)], input=buffer.getvalue(), text=False, timeout=20, cwd=ROOT)
        if got.returncode:
            # The native runtime's stderr can carry paths. Keep it out of
            # public results and let auto selection try another local engine.
            raise OSError("light-ocr native engine failed")
        data = json.loads(got.stdout)
        if not isinstance(data, dict) or not isinstance(data.get("lines"), list):
            raise ValueError("invalid light-ocr result")
        blocks = []
        for line in data["lines"]:
            text = str(line.get("text") or "").strip()
            if not text:
                continue
            quad = line["box"]
            score = float(line["confidence"])
            if len(quad) != 4 or any(not isinstance(p, dict) or not {"x", "y"} <= p.keys() for p in quad):
                raise ValueError("invalid light-ocr quadrilateral")
            box = [[float(p["x"]), float(p["y"])] for p in quad]
            if not math.isfinite(score) or not 0 <= score <= 1 or any(not math.isfinite(c) for p in box for c in p):
                raise ValueError("invalid light-ocr confidence or coordinates")
            blocks.append(Block(text, box, round(score, 3)))
        return OcrResult(blocks=blocks, text=join_blocks(blocks), backend=self.name)
