"""Native bridge, coordinates and fallback; no real OCR or network required."""
import json
import subprocess

from PIL import Image
import pytest

from kafka_hoard.ocr import Ocr, _classes
from kafka_hoard.ocr.light import LightBackend
from kafka_hoard.ocr.base import OcrResult


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.setattr(LightBackend, "available", classmethod(lambda cls: (True, "ok")))
    monkeypatch.setattr("kafka_hoard.ocr.light.proc.find_exe", lambda *a, **k: "node.exe")
    return LightBackend()


def test_bridge_passes_png_bytes_and_preserves_confidence_boxes(backend, monkeypatch):
    def run(args, **kw):
        assert args[0] == "node.exe" and args[1].endswith("light_ocr.mjs")
        assert kw["input"].startswith(b"\x89PNG")
        assert kw["timeout"] == 20 and not kw["text"]
        return subprocess.CompletedProcess(args, 0, json.dumps({"lines": [
            {"text": "Factura", "confidence": 0.975, "box": [{"x": 1, "y": 2}, {"x": 11, "y": 2}, {"x": 11, "y": 6}, {"x": 1, "y": 6}]}]}).encode(), b"")
    monkeypatch.setattr("kafka_hoard.ocr.light.proc.run", run)
    result = backend.recognise(Image.new("RGB", (30, 20)))
    assert result.text == "Factura" and result.backend == "light-ocr"
    assert result.blocks[0].score == 0.975 and result.blocks[0].box[0] == [1.0, 2.0]


def test_native_failure_auto_falls_back_without_private_stderr(backend, monkeypatch):
    class Broken(LightBackend):
        def __init__(self, *a):
            self.node = "node.exe"
    class Other:
        name = "tesseract"
        @classmethod
        def available(cls): return True, "ok"
        def __init__(self, *a): pass
        def recognise(self, *a): return OcrResult(text="fallback", backend=self.name)
    monkeypatch.setattr("kafka_hoard.ocr._classes", lambda: {"light-ocr": Broken, "tesseract": Other})
    monkeypatch.setattr("kafka_hoard.ocr.light.proc.run", lambda *a, **k: subprocess.CompletedProcess([], 1, b"", b"PRIVATE PATH"))
    result = Ocr().recognise(Image.new("RGB", (10, 10)))
    assert result.text == "fallback" and result.backend == "tesseract"


def test_invalid_probability_is_not_presented_as_a_confidence(backend, monkeypatch):
    monkeypatch.setattr("kafka_hoard.ocr.light.proc.run", lambda *a, **k: subprocess.CompletedProcess([], 0,
        json.dumps({"lines": [{"text": "bad", "confidence": 3, "box": [{"x": 0, "y": 0}] * 4}]}).encode(), b""))
    with pytest.raises(ValueError, match="confidence"):
        backend.recognise(Image.new("RGB", (10, 10)))


def test_engine_is_registered_once():
    assert _classes()["light-ocr"] is LightBackend
