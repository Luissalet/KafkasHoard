"""Compare OCR on invented scanned PDFs; never reads or files user documents.

Generate fixed 1/5/20-page fixtures, or score external per-page predictions.
All inputs, raw results and reports stay in the chosen directory outside the repo.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import time
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text).casefold()).strip()


def quantities(text: str) -> list[str]:
    found = re.findall(r"[+\-−]?\s*\d+(?:[.,/:]\d+)*(?:\s*[%‰])?", text)
    return [re.sub(r"\s+", "", value).replace("−", "-").replace(",", ".") for value in found]


def edit_distance(expected: str, actual: str) -> int:
    previous = list(range(len(actual) + 1))
    for i, char in enumerate(expected, 1):
        row = [i]
        for j, other in enumerate(actual, 1):
            row.append(min(row[-1] + 1, previous[j] + 1, previous[j - 1] + (char != other)))
        previous = row
    return previous[-1]


def score(expected: list[dict], units: list[dict]) -> dict:
    numbers = [unit["number"] for unit in units]
    wanted = [page["number"] for page in expected]
    by_number = {unit["number"]: unit["text"] for unit in units}
    pages, errors, characters = [], 0, 0
    for page in expected:
        reference = normalized(page["text"])
        got = normalized(by_number.get(page["number"], ""))
        distance = edit_distance(reference, got)
        errors += distance
        characters += len(reference)
        pages.append({"number": page["number"], "cer": round(distance / max(1, len(reference)), 6),
                      "quantities_match": quantities(reference) == quantities(got),
                      "empty": not bool(got), "expected_chars": len(reference), "actual_chars": len(got)})
    return {"cer": round(errors / max(1, characters), 6), "page_order_complete": numbers == wanted,
            "empty_pages": sum(page["empty"] for page in pages),
            "numeric_mismatch_pages": sum(not page["quantities_match"] for page in pages), "pages": pages}


def prepare(directory: Path, count: int, font_path: str) -> tuple[Path, list[dict]]:
    from PIL import Image, ImageDraw, ImageFont, JpegImagePlugin  # ensure the PDF's JPEG encoder is registered
    font = ImageFont.truetype(font_path, 32)
    folder = directory / f"pages-{count}"
    folder.mkdir(parents=True, exist_ok=True)
    images, expected = [], []
    for number in range(1, count + 1):
        # Repeated table rows with different quantities must survive extraction.
        lines = [f"Inventario de prueba. Página {number} de {count}.",
                 "Artículo    Unidades    Importe",
                 f"Avena          {number}            {number * 12},50 EUR",
                 f"Leche          {number + 1}            -{number * 4},75 EUR",
                 "La tabla continúa en la página siguiente." if number < count else "Fin del inventario de prueba."]
        image = Image.new("RGB", (1600, 600), "white")
        draw = ImageDraw.Draw(image)
        for line, text in enumerate(lines):
            draw.text((80, 60 + line * 65), text, font=font, fill="black")
        path = folder / f"page-{number:03}.png"
        image.save(path)
        expected.append({"number": number, "text": "\n".join(lines), "image": path.name,
                         "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        images.append(image)
    pdf_path = folder / "scanned.pdf"
    images[0].save(pdf_path, save_all=True, append_images=images[1:], resolution=150)
    for image in images:
        image.close()
    (folder / "manifest.json").write_text(json.dumps({"source": "invented synthetic printed pages",
        "font": font_path, "pages": expected}, ensure_ascii=False, indent=2), encoding="utf-8")
    return pdf_path, expected


def run(directory: Path, count: int, backend: str, font_path: str) -> dict:
    from kafka_hoard.docsvc import DocService
    from kafka_hoard.ocr import Ocr
    path, expected = prepare(directory, count, font_path)
    engine = Ocr(backend=backend, lang="es")
    service = DocService(engine)
    started = time.perf_counter()
    try:
        result = service.extract_bytes(path.name, path.read_bytes(), mode="force", max_pages=count, lang="es")
    finally:
        service.stop()
    served = engine.status()["backend"]
    report = {"stage": "synthetic_scanned_pdf_service", "requested_backend": backend, "served_backend": served,
              "seconds": round(time.perf_counter() - started, 3), "pages_ocr": result["pages_ocr"],
              "error": result.get("error"), **score(expected, result["units"])}
    report["passed"] = bool(not report["error"] and report["pages_ocr"] == count
        and report["page_order_complete"] and report["empty_pages"] == 0 and report["cer"] <= .05
        and report["numeric_mismatch_pages"] == 0
        and (backend == "auto" or served == backend))
    folder = path.parent
    (folder / f"{backend}-raw.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (folder / f"{backend}-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--backend", choices=["auto", "rapidocr", "light-ocr", "winocr", "tesseract"], default="rapidocr")
    parser.add_argument("--counts", type=int, nargs="+", default=[1, 5, 20])
    parser.add_argument("--font", default="C:/Windows/Fonts/arial.ttf")
    parser.add_argument("--manifest", type=Path, help="Score external predictions instead of running an OCR engine")
    parser.add_argument("--predictions", type=Path, help='JSON with {"units": [{"number": 1, "text": "..."}]}')
    args = parser.parse_args()
    directory = args.directory.resolve()
    if directory == ROOT or ROOT in directory.parents:
        parser.error("Use an isolated directory outside the repository")
    directory.mkdir(parents=True, exist_ok=True)
    if args.manifest or args.predictions:
        if not args.manifest or not args.predictions:
            parser.error("Both --manifest and --predictions are required")
        expected = json.loads(args.manifest.read_text(encoding="utf-8"))["pages"]
        units = json.loads(args.predictions.read_text(encoding="utf-8"))["units"]
        report = score(expected, units)
        (directory / "external-score.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report))
        return int(bool(not report["page_order_complete"] or report["empty_pages"]
                        or report["numeric_mismatch_pages"] or report["cer"] > .05))
    if any(count < 1 or count > 20 for count in args.counts):
        parser.error("Counts must be between 1 and 20")
    reports = []
    for count in args.counts:
        report = run(directory, count, args.backend, args.font)
        reports.append(report)
        print(json.dumps(report, ensure_ascii=True), flush=True)
    return int(any(not report["passed"] for report in reports))


if __name__ == "__main__":
    raise SystemExit(main())
