"""The workshop: PDF and image tools, page ranges, never-overwrite naming, fake Ghostscript / Word / LibreOffice, the REST routes and the tools."""

from __future__ import annotations

import base64
import io
import json
import os
import random
import types
from pathlib import Path

import pypdfium2 as pdfium
import pytest
from PIL import Image
from pypdf import PdfReader, PdfWriter

import docs
from conftest import tool
from kafka_hoard.agent_tools import TOOLS_BY_NAME
from kafka_hoard.errors import KafkaError
from kafka_hoard.workshop import Env, jobs, ranges
from kafka_hoard.workshop.names import safe_filename, safe_stem, write_unique
from kafka_hoard.workshop.service import redact
from pdfmaker import make_pdf


# ------------------------------------------------------------------ helpers
def done(code=0, out="", err=""):
    return types.SimpleNamespace(returncode=code, stdout=out, stderr=err)


class FakeRun:
    """Stands in for subprocess: records every call and lets a handler write the output files."""

    def __init__(self, handler=None):
        self.calls: list[dict] = []
        self.handler = handler

    def __call__(self, cmd, *, timeout=None, env=None, cwd=None):
        self.calls.append({"cmd": list(cmd), "env": dict(env) if env else None})
        return self.handler(list(cmd), env) if self.handler else done()


def nothing_installed() -> Env:
    def boom(*a, **k):
        raise AssertionError("no external program may run in this test")
    return Env(runner=boom, which=lambda name: None, windows=False, environ={}, glob=lambda pattern: [], exists=lambda path: False)


@pytest.fixture
def ws(svc, tmp_path):
    svc.workshop.env = nothing_installed()
    svc.workshop.home = tmp_path / "home"
    return svc.workshop


def write_pdf(folder: Path, name: str, pages: list[str] | str | int = 3) -> Path:
    if isinstance(pages, int):
        pages = [f"Página {i + 1} de {name}" for i in range(pages)]
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(make_pdf(pages))
    return path


def noise_image(size=(300, 300), seed=1, mode="RGB") -> Image.Image:
    return Image.frombytes(mode, size, random.Random(seed).randbytes(size[0] * size[1] * len(mode)))


def photo_pdf(path: Path, size=(800, 600), dpi=200) -> Path:
    """A one-page PDF holding a noisy JPEG (the kind of file that is worth compressing)."""
    buf = io.BytesIO()
    noise_image(size).save(buf, "PDF", resolution=dpi, quality=100)
    path.write_bytes(buf.getvalue())
    return path


def page_texts(path: Path) -> list[str]:
    doc = pdfium.PdfDocument(str(path))
    try:
        out = []
        for i in range(len(doc)):
            tp = doc[i].get_textpage()
            out.append(tp.get_text_range().strip())
        return out
    finally:
        doc.close()


def pages_of(path: Path) -> int:
    return len(PdfReader(str(path)).pages)


# ------------------------------------------------------------------ ranges
def test_ranges_parser_understands_the_ways_people_write_pages():
    assert ranges.parse_ranges("1-3,5,8-", 10) == [1, 2, 3, 5, 8, 9, 10]
    assert ranges.parse_ranges("last", 7) == [7]
    assert ranges.parse_ranges("-1", 7) == [7] and ranges.parse_ranges("-2", 7) == [6]
    assert ranges.parse_ranges("3-last", 5) == [3, 4, 5]
    assert ranges.parse_ranges("-3--1", 6) == [4, 5, 6]
    assert ranges.parse_ranges("impares", 5) == [1, 3, 5] and ranges.parse_ranges("pares", 5) == [2, 4]
    assert ranges.parse_ranges("todas", 3) == [1, 2, 3]
    assert ranges.parse_ranges("1 a 3 y 5", 6) == [1, 2, 3, 5]
    assert ranges.parse_ranges("pág. 2", 3) == [2]
    assert ranges.parse_ranges("", 4, default_all=True) == [1, 2, 3, 4]
    assert ranges.parse_ranges("2,2,1", 3, unique=True) == [2, 1]
    assert ranges.parse_groups("1-2, 4", 5) == [[1, 2], [4]]
    assert ranges.describe([1, 2, 3, 5, 8, 9]) == "1-3,5,8-9"


@pytest.mark.parametrize("text,fragment", [("0", "empiezan en 1"), ("12", "La página 12 no existe"), ("3-2", "al revés"), ("abc", "No entiendo"),
                                           ("-9", "antes de la primera"), ("", "Indica las páginas"), ("1-99", "no existe")])
def test_ranges_errors_are_clear_spanish(text, fragment):
    with pytest.raises(KafkaError) as e:
        ranges.parse_ranges(text, 4)
    assert fragment in e.value.message and e.value.code == "invalid"


# ------------------------------------------------------------------ names
def test_never_overwrite_adds_a_number(tmp_path):
    first = write_unique(tmp_path, "informe", "_unido", "pdf", b"1")
    second = write_unique(tmp_path, "informe", "_unido", "pdf", b"2")
    third = write_unique(tmp_path, "informe", "_unido", "pdf", b"3")
    assert [p.name for p in (first, second, third)] == ["informe_unido.pdf", "informe_unido (2).pdf", "informe_unido (3).pdf"]
    assert first.read_bytes() == b"1" and second.read_bytes() == b"2"


def test_names_are_safe_on_windows():
    assert safe_stem('a<b>:c"d/e\\f|g?h*') == "a_b__c_d_e_f_g_h_"
    assert safe_stem("CON") == "_CON" and safe_stem("   ") == "documento" and safe_stem("fin. ") == "fin"
    assert safe_filename("..\\..\\evil name.PDF") == "evil name.pdf"


def test_the_same_operation_twice_never_overwrites(ws, tmp_path):
    src = write_pdf(tmp_path / "docs", "informe.pdf", 3)
    a = ws.pages(action="extract", file=str(src), pages="1")
    b = ws.pages(action="extract", file=str(src), pages="2")
    assert Path(a["output"]).name == "informe_paginas.pdf" and Path(b["output"]).name == "informe_paginas (2).pdf"
    assert page_texts(Path(a["output"]))[0].startswith("Página 1") and page_texts(Path(b["output"]))[0].startswith("Página 2")


def test_explicit_output_is_respected_but_still_never_overwrites(ws, tmp_path):
    src = write_pdf(tmp_path / "docs", "informe.pdf", 2)
    target = tmp_path / "salida" / "mio.pdf"
    r1 = ws.pages(action="extract", file=str(src), pages="1", output=str(target))
    r2 = ws.pages(action="extract", file=str(src), pages="2", output=str(target))
    assert Path(r1["output"]) == target and Path(r2["output"]).name == "mio (2).pdf"
    r3 = ws.pages(action="extract", file=str(src), pages="1", out_dir=str(tmp_path / "otra"))
    assert Path(r3["output"]).parent == tmp_path / "otra"


# ------------------------------------------------------------------ merge, split, pages
def test_merge_with_per_file_ranges(ws, tmp_path):
    a = write_pdf(tmp_path / "in", "a.pdf", 3)
    b = write_pdf(tmp_path / "in", "b.pdf", 4)
    r = ws.merge(files=[str(a), str(b)], ranges=["", "2-3"])
    out = Path(r["output"])
    assert out.name == "a_unido.pdf" and out.parent == a.parent
    assert r["pages"] == 5 and pages_of(out) == 5
    assert [t.split("\n")[0] for t in page_texts(out)] == ["Página 1 de a.pdf", "Página 2 de a.pdf", "Página 3 de a.pdf", "Página 2 de b.pdf", "Página 3 de b.pdf"]
    assert r["size_before"] == a.stat().st_size + b.stat().st_size and r["size_after"] == out.stat().st_size


def test_merge_validates_its_input(ws, tmp_path):
    a = write_pdf(tmp_path / "in", "a.pdf", 1)
    with pytest.raises(KafkaError) as e:
        ws.merge(files=[str(a)])
    assert "al menos dos" in e.value.message
    with pytest.raises(KafkaError) as e:
        ws.merge(files=[str(a), str(a)], ranges=["1"])
    assert "deben coincidir" in e.value.message
    with pytest.raises(KafkaError) as e:
        ws.merge(files=[str(a), str(a)], ranges=["", "7"])
    assert "a.pdf" in e.value.message and "no existe" in e.value.message
    notpdf = tmp_path / "in" / "x.png"
    noise_image((10, 10)).save(notpdf)
    with pytest.raises(KafkaError) as e:
        ws.merge(files=[str(a), str(notpdf)])
    assert e.value.code == "unsupported" and "pdf_from_images" in e.value.hint
    with pytest.raises(KafkaError) as e:
        ws.merge(files=[str(a), "relativo.pdf"])
    assert "absoluta" in e.value.message
    with pytest.raises(KafkaError) as e:
        ws.merge(files=[str(a), str(tmp_path / "no-existe.pdf")])
    assert e.value.code == "not_found"


def test_damaged_pdf_is_reported(ws, tmp_path):
    bad = tmp_path / "in" / "roto.pdf"
    bad.parent.mkdir()
    bad.write_bytes(b"%PDF-1.4\nesto no es un pdf")
    with pytest.raises(KafkaError) as e:
        ws.pages(action="extract", file=str(bad), pages="1")
    assert "no es un PDF válido" in e.value.message


def test_split_every_page_in_a_new_folder(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "libro.pdf", 3)
    r = ws.split(file=str(src), mode="pages")
    folder = Path(r["out_dir"])
    assert folder == src.parent / "libro_dividido"
    assert sorted(p.name for p in folder.iterdir()) == ["libro_p1.pdf", "libro_p2.pdf", "libro_p3.pdf"]
    assert page_texts(folder / "libro_p2.pdf")[0].startswith("Página 2")
    again = ws.split(file=str(src), mode="pages")
    assert Path(again["out_dir"]).name == "libro_dividido (2)"


def test_split_by_ranges_and_every_n(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "doc.pdf", 6)
    r = ws.split(file=str(src), mode="ranges", ranges="1-3,4-6")
    assert [o["name"] for o in r["outputs"]] == ["doc_p1-3.pdf", "doc_p4-6.pdf"] and [o["pages"] for o in r["outputs"]] == [3, 3]
    r = ws.split(file=str(src), mode="every", every=4, out_dir=str(tmp_path / "partes"))
    assert [o["pages"] for o in r["outputs"]] == [4, 2] and Path(r["out_dir"]) == tmp_path / "partes"
    with pytest.raises(KafkaError):
        ws.split(file=str(src), mode="ranges", ranges="")
    with pytest.raises(KafkaError):
        ws.split(file=str(src), mode="ranges", ranges="1-9")


def test_pages_extract_delete_rotate_reorder(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "p.pdf", 5)
    ext = ws.pages(action="extract", file=str(src), pages="2-3,last")
    assert [t.split()[1] for t in page_texts(Path(ext["output"]))] == ["2", "3", "5"] and ext["pages"] == 3
    dele = ws.pages(action="delete", file=str(src), pages="3")
    assert Path(dele["output"]).name == "p_sin_paginas.pdf" and dele["pages"] == 4
    assert [t.split()[1] for t in page_texts(Path(dele["output"]))] == ["1", "2", "4", "5"]
    with pytest.raises(KafkaError) as e:
        ws.pages(action="delete", file=str(src), pages="todas")
    assert "vacío" in e.value.message
    rot = ws.pages(action="rotate", file=str(src), pages="1,3", degrees=90)
    reader = PdfReader(rot["output"])
    assert [p.rotation for p in reader.pages] == [90, 0, 90, 0, 0] and Path(rot["output"]).name == "p_rotado.pdf"
    again = ws.pages(action="rotate", file=rot["output"], degrees=-90)
    assert [p.rotation for p in PdfReader(again["output"]).pages] == [0, 270, 0, 270, 270]
    with pytest.raises(KafkaError):
        ws.pages(action="rotate", file=str(src), degrees=45)
    order = ws.pages(action="reorder", file=str(src), order="5,4,3,2,1")
    assert [t.split()[1] for t in page_texts(Path(order["output"]))] == ["5", "4", "3", "2", "1"]
    rev = ws.pages(action="reorder", file=str(src), order="reverse")
    assert [t.split()[1] for t in page_texts(Path(rev["output"]))] == ["5", "4", "3", "2", "1"]
    with pytest.raises(KafkaError) as e:
        ws.pages(action="reorder", file=str(src), order="1,2,3")
    assert "faltan las páginas 4-5" in e.value.message
    with pytest.raises(KafkaError) as e:
        ws.pages(action="reorder", file=str(src), order="1,1,2,3,4,5")
    assert "se repiten" in e.value.message


# ------------------------------------------------------------------ compress
def test_compress_without_ghostscript_uses_pypdf_and_shrinks_images(ws, tmp_path):
    src = photo_pdf(tmp_path / "in" / "foto.pdf") if (tmp_path / "in").mkdir() is None else None
    r = ws.compress(file=str(src))
    out = Path(r["output"])
    assert out.name == "foto_comprimido.pdf" and r["engine"] == "pypdf" and r["step"] == "ebook"
    assert r["size_after"] < r["size_before"] / 3 and r["reduction_pct"] > 60
    assert pages_of(out) == 1 and r["reached_target"] is None


def test_compress_with_target_tries_stronger_settings_until_it_fits(ws, tmp_path):
    (tmp_path / "in").mkdir()
    src = photo_pdf(tmp_path / "in" / "foto.pdf")
    r = ws.compress(file=str(src), preset="prepress", target_mb=0.2)
    assert r["reached_target"] is True and r["size_after"] <= 0.2 * 1024 * 1024
    steps = [a["step"] for a in r["attempts"]]
    assert steps == ["prepress", "printer", "ebook"] and r["step"] == "ebook"
    assert r["attempts"][0]["size"] > 0.2 * 1024 * 1024


def test_compress_says_when_the_target_is_impossible(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "texto.pdf", 3)
    r = ws.compress(file=str(src), target_mb=0.0001)
    assert r["ok"] is True and r["reached_target"] is False
    assert any("No se ha podido bajar de 0.0001 MB" in n for n in r["notes"])
    assert Path(r["output"]).is_file() and pages_of(Path(r["output"])) == 3


def test_compress_that_cannot_reduce_keeps_a_copy_and_says_so(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "texto.pdf", 1)
    r = ws.compress(file=str(src), preset="screen")
    assert r["size_after"] <= r["size_before"] or r["engine"] == "original"
    if r["engine"] == "original":
        assert any("ya está optimizado" in n for n in r["notes"])


def _fake_gs(sizes: dict[str, int], source_pages: int, log: list):
    """Ghostscript stand-in: writes a valid PDF padded to the size wanted for the preset in -dPDFSETTINGS."""
    def handler(cmd, env):
        preset = next(a.split("/")[-1] for a in cmd if a.startswith("-dPDFSETTINGS="))
        out = Path(next(a.split("=", 1)[1] for a in cmd if a.startswith("-sOutputFile=")))
        log.append(preset)
        base = make_pdf([f"Página {i + 1} de x" for i in range(source_pages)])
        out.write_bytes(base + b"\n%" + b"x" * max(0, sizes[preset] - len(base) - 2))
        return done()
    return handler


def gs_env(handler):
    runner = FakeRun(handler)
    env = Env(runner=runner, which=lambda n: "/usr/bin/gs" if n == "gs" else None, windows=False, environ={}, glob=lambda p: [], exists=lambda p: False)
    return env, runner


def test_compress_with_a_fake_ghostscript_walks_the_presets_down(ws, tmp_path):
    src = tmp_path / "in" / "grande.pdf"
    src.parent.mkdir()
    src.write_bytes(make_pdf([f"Página {i + 1} de x" for i in range(3)]) + b"\n%" + b"o" * 60_000)
    log: list[str] = []
    ws.env, runner = gs_env(_fake_gs({"printer": 40_000, "ebook": 20_000, "screen": 5_000, "prepress": 50_000}, 3, log))
    r = ws.compress(file=str(src), preset="printer", target_mb=0.01)
    assert log == ["printer", "ebook", "screen"] and r["engine"] == "ghostscript" and r["step"] == "screen" and r["reached_target"] is True
    assert [a["size"] for a in r["attempts"]] == [40_000, 20_000, 5_000]
    cmd = runner.calls[0]["cmd"]
    assert cmd[0] == "/usr/bin/gs" and "-sDEVICE=pdfwrite" in cmd and "-dSAFER" in cmd
    assert str(src) not in " ".join(cmd) and not any("grande" in a for a in cmd)        # works on an ASCII temp copy
    assert Path(r["output"]).stat().st_size == 5_000


def test_compress_with_ghostscript_without_target_runs_only_the_chosen_preset(ws, tmp_path):
    src = tmp_path / "in" / "g.pdf"
    src.parent.mkdir()
    src.write_bytes(make_pdf(["uno", "dos"]) + b"\n%" + b"o" * 30_000)
    log: list[str] = []
    ws.env, _ = gs_env(_fake_gs({"ebook": 9_000, "screen": 3_000, "printer": 12_000, "prepress": 14_000}, 2, log))
    r = ws.compress(file=str(src), preset="ebook")
    assert log == ["ebook"] and r["engine"] == "ghostscript" and r["size_after"] == 9_000


def test_a_failing_ghostscript_falls_back_to_the_built_in_compressor(ws, tmp_path):
    (tmp_path / "in").mkdir()
    src = photo_pdf(tmp_path / "in" / "foto.pdf")
    ws.env, _ = gs_env(lambda cmd, env: done(1, "", "boom"))
    r = ws.compress(file=str(src))
    assert r["engine"] == "pypdf" and any("Ghostscript falló" in n for n in r["notes"])
    with pytest.raises(KafkaError) as e:
        ws.env = nothing_installed()
        ws.compress(file=str(src), engine="ghostscript")
    assert e.value.code == "not_configured"


def test_ghostscript_output_with_the_wrong_page_count_is_discarded(ws, tmp_path):
    src = tmp_path / "in" / "g.pdf"
    src.parent.mkdir()
    src.write_bytes(make_pdf(["a", "b", "c"]) + b"\n%" + b"o" * 30_000)
    ws.env, _ = gs_env(lambda cmd, env: (Path(next(a.split("=", 1)[1] for a in cmd if a.startswith("-sOutputFile="))).write_bytes(make_pdf(["solo una"])), done())[1])
    r = ws.compress(file=str(src), preset="ebook")
    assert any("cambió el número de páginas" in n for n in r["notes"]) and pages_of(Path(r["output"])) == 3


# ------------------------------------------------------------------ protect
def test_protect_unprotect_round_trip_never_leaks_the_password(ws, tmp_path, caplog):
    src = write_pdf(tmp_path / "in", "secreto.pdf", 2)
    secret = "clave-Muy-Secreta-77"
    r = ws.protect(action="protect", file=str(src), password=secret)
    locked = Path(r["output"])
    assert locked.name == "secreto_protegido.pdf" and r["pages"] == 2
    reader = PdfReader(str(locked))
    assert reader.is_encrypted and int(reader.decrypt("otra")) == 0 and int(reader.decrypt(secret)) > 0
    assert b"/AESV3" in locked.read_bytes()
    assert secret not in json.dumps(r)
    with pytest.raises(KafkaError) as e:
        ws.pages(action="extract", file=str(locked), pages="1")
    assert "protegido con contraseña" in e.value.message and secret not in e.value.message
    with pytest.raises(KafkaError) as e:
        ws.protect(action="unprotect", file=str(locked), password="mal")
    assert "no es correcta" in e.value.message
    ok = ws.protect(action="unprotect", file=str(locked), password=secret)
    assert Path(ok["output"]).name == "secreto_protegido_sin_clave.pdf" and not PdfReader(ok["output"]).is_encrypted
    assert page_texts(Path(ok["output"]))[1].startswith("Página 2")
    # other operations take the password and write an unprotected result
    extra = ws.pages(action="extract", file=str(locked), pages="2", password=secret)
    assert not PdfReader(extra["output"]).is_encrypted
    assert secret not in caplog.text
    with pytest.raises(KafkaError):
        ws.protect(action="unprotect", file=str(src), password="x")


def test_protect_permissions_and_owner_password(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "perm.pdf", 1)
    r = ws.protect(action="protect", file=str(src), password="abrir", owner_password="dueño", allow_print=False, allow_copy=False)
    reader = PdfReader(r["output"])
    assert int(reader.decrypt("abrir")) == 1 and int(PdfReader(r["output"]).decrypt("dueño")) == 2
    assert any("permisos" in n for n in r["notes"])


def test_protect_a_protected_input_needs_its_password(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "x.pdf", 1)
    locked = ws.protect(action="protect", file=str(src), password="uno")["output"]
    with pytest.raises(KafkaError):
        ws.protect(action="protect", file=locked, password="dos")
    again = ws.protect(action="protect", file=locked, password="dos", current_password="uno")
    assert int(PdfReader(again["output"]).decrypt("dos")) > 0


# ------------------------------------------------------------------ watermark
def test_watermark_text_is_in_the_content_stream_and_in_the_rendered_text(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "contrato.pdf", 3)
    r = ws.watermark(file=str(src), text="CONFIDENCIAL", opacity=0.25, pages="2-3")
    out = Path(r["output"])
    assert out.name == "contrato_marca.pdf" and r["watermarked_pages"] == "2-3" and r["pages"] == 3
    reader = PdfReader(str(out))
    streams = [p.get_contents().get_data() for p in reader.pages]
    assert b"(CONFIDENCIAL) Tj" not in streams[0] and all(b"(CONFIDENCIAL) Tj" in s for s in streams[1:])
    assert all(b"Tf" in s and b"Tm" in s for s in streams[1:])
    states = [v.get_object() for v in reader.pages[1]["/Resources"]["/ExtGState"].values()]
    assert any(abs(float(v.get("/ca", 1)) - 0.25) < 1e-6 for v in states)
    texts = page_texts(out)
    assert "CONFIDENCIAL" not in texts[0] and "CONFIDENCIAL" in texts[1] and texts[1].startswith("Página 2")


def test_watermark_is_diagonal_by_default_and_follows_the_angle(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "a.pdf", 1)
    def matrix(path):
        import re
        data = PdfReader(path).pages[0].get_contents().get_data().decode("latin-1")
        found = re.findall(r"([-\d.]+) ([-\d.]+) ([-\d.]+) ([-\d.]+) ([-\d.]+) ([-\d.]+) Tm\s*\(BORRADOR\)", data)
        assert found
        return [float(v) for v in found[-1]]
    diag = matrix(ws.watermark(file=str(src), text="BORRADOR")["output"])
    flat = matrix(ws.watermark(file=str(src), text="BORRADOR", angle=0)["output"])
    assert abs(diag[0] - 0.7071) < 1e-3 and abs(diag[1] - 0.7071) < 1e-3 and abs(diag[2] + 0.7071) < 1e-3
    assert flat[:4] == [1.0, 0.0, 0.0, 1.0]
    centre = (595.276 / 2, 841.89 / 2)
    assert abs(flat[5] - (centre[1] - 60 * 0.3)) < 1 and 100 < flat[4] < centre[0]          # the text starts left of the centre, vertically centred


def test_watermark_handles_accents_parentheses_long_text_and_rotated_pages(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "a.pdf", 1)
    rotated = ws.pages(action="rotate", file=str(src), degrees=90)["output"]
    r = ws.watermark(file=rotated, text="Anulación (copia) " + "muy largo " * 12, font_size=80, color="#c00000")
    assert pages_of(Path(r["output"])) == 1
    data = PdfReader(r["output"]).pages[0].get_contents().get_data().decode("latin-1")
    assert "Anulaci\\363n \\050copia\\051" in data and "0.753 0.0 0.0 rg" in data
    with pytest.raises(KafkaError):
        ws.watermark(file=str(src), text="x", color="fucsia raro")
    with pytest.raises(KafkaError):
        ws.watermark(file=str(src), text="   ")


# ------------------------------------------------------------------ info and metadata
def test_info_reports_pages_sizes_text_and_password(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "i.pdf", 2)
    info = ws.info(file=str(src))
    assert info["pages"] == 2 and info["has_text"] is True and info["scanned"] is False and info["encrypted"] is False
    assert info["page_sizes"] == [{"pages": "1-2", "width_mm": 210, "height_mm": 297, "name": "A4"}] and info["size"] == src.stat().st_size
    blank = tmp_path / "in" / "escaneo.pdf"
    blank.write_bytes(make_pdf(["", ""], blank_pages=(0, 1)))
    scanned = ws.info(file=str(blank))
    assert scanned["has_text"] is False and scanned["scanned"] is True and "OCR" in scanned["notes"][0]
    locked = ws.protect(action="protect", file=str(src), password="abc")["output"]
    closed = ws.info(file=locked)
    assert closed["encrypted"] is True and closed["locked"] is True and "pages" not in closed
    opened = ws.info(file=locked, password="abc")
    assert opened["locked"] is False and opened["pages"] == 2


def test_metadata_set_and_clear(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "m.pdf", 1)
    r = ws.metadata_set(file=str(src), title="Informe anual", author="Ana Ejemplo", keywords="cuentas, 2026")
    assert Path(r["output"]).name == "m_metadatos.pdf" and r["metadata"]["title"] == "Informe anual" and r["metadata"]["author"] == "Ana Ejemplo"
    info = ws.info(file=r["output"])
    assert info["metadata"]["keywords"] == "cuentas, 2026"
    cleared = ws.metadata_set(file=r["output"], author="")
    assert "author" not in ws.info(file=cleared["output"])["metadata"] and ws.info(file=cleared["output"])["metadata"]["title"] == "Informe anual"
    with pytest.raises(KafkaError):
        ws.metadata_set(file=str(src))


# ------------------------------------------------------------------ images -> pdf, pdf -> images
def exif_rotated_jpeg(path: Path) -> Path:
    im = noise_image((100, 300), seed=7)
    exif = Image.Exif()
    exif[0x0112] = 6                                   # rotate 90 degrees to display: 300 x 100 on screen
    im.save(path, "JPEG", exif=exif.tobytes())
    return path


def test_images_to_pdf_page_count_orientation_and_exif(ws, tmp_path):
    folder = tmp_path / "fotos"
    folder.mkdir()
    noise_image((400, 200)).save(folder / "ancha.png")
    noise_image((200, 400), seed=2).save(folder / "alta.jpg")
    exif_rotated_jpeg(folder / "movil.jpg")
    r = ws.from_images(images=[str(folder / "ancha.png"), str(folder / "alta.jpg"), str(folder / "movil.jpg")])
    out = Path(r["output"])
    assert out.name == "ancha_imagenes.pdf" and r["pages"] == 3 and pages_of(out) == 3
    boxes = [(round(float(p.mediabox.width)), round(float(p.mediabox.height))) for p in PdfReader(str(out)).pages]
    assert boxes == [(842, 595), (595, 842), (842, 595)]
    r2 = ws.from_images(images=[str(folder / "alta.jpg")], orientation="landscape", page_size="Letter".lower(), margin_mm=0)
    assert [round(float(x)) for x in PdfReader(r2["output"]).pages[0].mediabox[2:]] == [792, 612]


def test_images_to_pdf_fit_takes_the_image_size_and_flattens_transparency(ws, tmp_path):
    img = tmp_path / "in" / "t.png"
    img.parent.mkdir()
    Image.new("RGBA", (300, 150), (255, 0, 0, 0)).save(img)
    r = ws.from_images(images=[str(img)], page_size="fit", margin_mm=0, output=str(tmp_path / "fit.pdf"))
    box = PdfReader(r["output"]).pages[0].mediabox
    assert (round(float(box.width)), round(float(box.height))) == (144, 72)
    rendered = pdfium.PdfDocument(r["output"])[0].render(scale=1).to_pil().convert("RGB")
    assert rendered.getpixel((70, 30)) == (255, 255, 255)          # fully transparent red became white


def test_images_to_pdf_takes_a_folder_in_natural_order_and_rejects_non_images(ws, tmp_path):
    folder = tmp_path / "escaneos"
    folder.mkdir()
    for name in ("pag10.png", "pag2.png", "pag1.png"):
        noise_image((40, 40)).save(folder / name)
    (folder / "notas.txt").write_text("no soy una imagen")
    r = ws.from_images(images=[str(folder)])
    assert r["inputs"] == ["pag1.png", "pag2.png", "pag10.png"] and r["pages"] == 3
    with pytest.raises(KafkaError) as e:
        ws.from_images(images=[str(folder / "notas.txt")])
    assert e.value.code == "unsupported"
    broken = folder / "rota.png"
    broken.write_bytes(b"no es png")
    with pytest.raises(KafkaError) as e:
        ws.from_images(images=[str(broken)])
    assert "No puedo abrir la imagen" in e.value.message


def test_pdf_to_images(ws, tmp_path):
    src = write_pdf(tmp_path / "in", "libro.pdf", 3)
    r = ws.to_images(file=str(src), dpi=72)
    folder = Path(r["out_dir"])
    assert folder.name == "libro_imagenes" and [o["name"] for o in r["outputs"]] == ["libro_p1.png", "libro_p2.png", "libro_p3.png"]
    assert Image.open(r["outputs"][0]["path"]).size in ((595, 842), (596, 842), (595, 843))
    jpg = ws.to_images(file=str(src), pages="2", format="jpg", dpi=50, out_dir=str(tmp_path / "jpgs"))
    assert [o["name"] for o in jpg["outputs"]] == ["libro_p2.jpg"] and Image.open(jpg["outputs"][0]["path"]).format == "JPEG"
    with pytest.raises(KafkaError):
        ws.to_images(file=str(src), dpi=600, pages="9")


# ------------------------------------------------------------------ office -> pdf
def office_file(folder: Path, name="carta.docx") -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(b"PK\x03\x04 contenido de prueba")
    return path


def test_office_conversion_with_libreoffice(ws, tmp_path):
    src = office_file(tmp_path / "in", "Mi carta (final).docx")

    def handler(cmd, env):
        outdir = Path(cmd[cmd.index("--outdir") + 1])
        (outdir / (Path(cmd[-1]).stem + ".pdf")).write_bytes(make_pdf(["Hola", "Mundo"]))
        return done()
    runner = FakeRun(handler)
    ws.env = Env(runner=runner, which=lambda n: "/usr/bin/soffice" if n == "soffice" else None, windows=False, environ={}, glob=lambda p: [], exists=lambda p: False)
    r = ws.from_office(file=str(src))
    assert r["engine"] == "libreoffice" and r["pages"] == 2 and Path(r["output"]).name == "Mi carta (final).pdf" and Path(r["output"]).parent == src.parent
    cmd = runner.calls[0]["cmd"]
    assert "--headless" in cmd and cmd[cmd.index("--convert-to") + 1] == "pdf" and any(a.startswith("-env:UserInstallation=file:") for a in cmd)
    assert "Mi carta" not in " ".join(cmd)                                  # the real name never reaches the command line
    again = ws.from_office(file=str(src))
    assert Path(again["output"]).name == "Mi carta (final) (2).pdf"


def test_office_conversion_with_word_is_hidden_and_always_quits(ws, tmp_path):
    src = office_file(tmp_path / "in", "informe.docx")
    seen = {}

    def handler(cmd, env):
        if cmd[0] == "reg":
            return done()
        script = base64.b64decode(cmd[cmd.index("-EncodedCommand") + 1]).decode("utf-16-le")
        seen.update(script=script, src=env["KAFKA_SRC"], dst=env["KAFKA_DST"])
        Path(env["KAFKA_DST"]).write_bytes(make_pdf(["Informe"]))
        return done()
    ws.env = Env(runner=FakeRun(handler), which=lambda n: "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe" if n == "powershell" else None,
                 windows=True, environ={}, glob=lambda p: [], exists=lambda p: False)
    r = ws.from_office(file=str(src), engine="auto")
    assert r["engine"] == "word" and r["pages"] == 1
    script = seen["script"]
    assert "Word.Application" in script and "SaveAs2($env:KAFKA_DST, 17)" in script and "Visible = $false" in script
    finally_part = script.split("finally", 1)[1]
    assert "Quit(0)" in finally_part and "ReleaseComObject" in finally_part
    assert "Stop-Process" in finally_part and "/Automation" in script, "a Word left running by COM is ended"
    assert seen["src"].endswith("in.docx") and "informe" not in seen["src"]


def test_office_falls_back_to_libreoffice_when_word_fails(ws, tmp_path):
    src = office_file(tmp_path / "in", "a.rtf")

    def handler(cmd, env):
        if cmd[0] == "reg":
            return done()
        if "-EncodedCommand" in cmd:
            return done(1, "", "Word no responde")
        outdir = Path(cmd[cmd.index("--outdir") + 1])
        (outdir / (Path(cmd[-1]).stem + ".pdf")).write_bytes(make_pdf(["ok"]))
        return done()
    ws.env = Env(runner=FakeRun(handler), which=lambda n: {"powershell": "powershell.exe", "soffice": "soffice.exe"}.get(n), windows=True, environ={},
                 glob=lambda p: [], exists=lambda p: False)
    r = ws.from_office(file=str(src))
    assert r["engine"] == "libreoffice"
    with pytest.raises(KafkaError) as e:
        ws.from_office(file=str(src), engine="word")
    assert "Word" in e.value.message or "no ha podido" in e.value.message


def test_office_reports_clearly_when_nothing_is_installed(ws, tmp_path):
    src = office_file(tmp_path / "in")
    with pytest.raises(KafkaError) as e:
        ws.from_office(file=str(src))
    assert e.value.code == "not_configured" and "ni LibreOffice" in e.value.message and "Word" in e.value.message
    with pytest.raises(KafkaError) as e:
        ws.from_office(file=str(src), engine="libreoffice")
    assert "LibreOffice no está instalado" in e.value.message
    notdoc = tmp_path / "in" / "hoja.xlsx"
    notdoc.write_bytes(b"x")
    with pytest.raises(KafkaError) as e:
        ws.from_office(file=str(notdoc))
    assert e.value.code == "unsupported"


def test_office_failure_message_comes_from_the_program(ws, tmp_path):
    src = office_file(tmp_path / "in")
    ws.env = Env(runner=FakeRun(lambda cmd, env: done(1, "", "Error: source file could not be loaded")), which=lambda n: "soffice" if n == "soffice" else None,
                 windows=False, environ={}, glob=lambda p: [], exists=lambda p: False)
    with pytest.raises(KafkaError) as e:
        ws.from_office(file=str(src))
    assert "could not be loaded" in e.value.message and e.value.status == 500


# ------------------------------------------------------------------ images compress
def big_png(path: Path, size=(300, 300), seed=3) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    noise_image(size, seed).save(path)
    return path


def test_png_ladder_reaches_the_limit_and_leaves_the_original_alone(ws, tmp_path):
    src = big_png(tmp_path / "in" / "ruido.png")
    original = src.read_bytes()
    r = ws.images_compress(sources=[str(src)], limit_kb=40)
    item = r["items"][0]
    assert r["compressed"] == 1 and item["status"] == "ok" and item["size_after"] <= 40 * 1024
    assert item["strategy"].startswith(("quantize-", "resize-")) and Path(item["output"]).name == "ruido_comprimida.png"
    assert src.read_bytes() == original and r["size_before"] == len(original) and r["saved"] > 0
    Image.open(item["output"]).verify()
    twice = ws.images_compress(sources=[str(src)], limit_kb=40)
    assert Path(twice["items"][0]["output"]).name == "ruido_comprimida (2).png"


def test_lossless_first_and_lossless_only_fails_gracefully(ws, tmp_path):
    flat = tmp_path / "in" / "plano.png"
    flat.parent.mkdir()
    Image.new("RGB", (600, 600), (10, 20, 30)).save(flat, compress_level=0)
    r = ws.images_compress(sources=[str(flat)], limit_kb=20, lossless_only=True)
    assert r["items"][0]["strategy"] == "lossless" and r["compressed"] == 1 and r["items"][0]["size_after"] < flat.stat().st_size
    hard = big_png(tmp_path / "in" / "duro.png")
    r = ws.images_compress(sources=[str(hard)], limit_kb=20, lossless_only=True)
    assert r["ok"] is True and r["failed"] == 1 and r["compressed"] == 0 and r["outputs"] == []
    assert "sin perder calidad" in r["items"][0]["reason"] and any("sin perder calidad" in n for n in r["notes"])
    assert not (hard.parent / "duro_comprimida.png").exists()


def test_impossible_limits_are_reported_not_hidden(ws, tmp_path):
    src = big_png(tmp_path / "in" / "x.png", (200, 200))
    r = ws.images_compress(sources=[str(src)], limit_kb=0.01)
    assert r["failed"] == 1 and r["items"][0]["best_size"] and not any(p.name.startswith("x_comprimida") for p in src.parent.iterdir())
    with pytest.raises(KafkaError):
        ws.images_compress(sources=[str(src)])
    with pytest.raises(KafkaError):
        ws.images_compress(sources=[str(src)], limit_mb=1, limit_kb=1)


def test_folder_is_copied_recursively_into_a_sibling_folder(ws, tmp_path):
    root = tmp_path / "fotos"
    big_png(root / "a.png")
    Image.new("RGB", (20, 20), (1, 2, 3)).save(root / "sub" / "b.png") if (root / "sub").mkdir() is None else None
    noise_image((300, 300), 5).save(root / "sub" / "c.jpg", quality=95)
    (root / "notas.txt").write_text("no es imagen")
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    r = ws.images_compress(sources=[str(root)], limit_kb=30)
    out = Path(r["out_dir"])
    assert out == tmp_path / "fotos_comprimidas" and r["total"] == 3 and r["compressed"] == 3
    assert sorted(str(p.relative_to(out)).replace("\\", "/") for p in out.rglob("*") if p.is_file()) == ["a.png", "sub/b.png", "sub/c.jpg"]
    assert all(p.stat().st_size <= 30 * 1024 for p in out.rglob("*") if p.is_file())
    assert {p: p.read_bytes() for p in root.rglob("*") if p.is_file()} == before
    second = ws.images_compress(sources=[str(root)], limit_kb=30, recursive=False)
    assert Path(second["out_dir"]).name == "fotos_comprimidas (2)" and second["total"] == 1
    skipped = ws.images_compress(sources=[str(root)], limit_kb=30, skip_small=True)
    assert skipped["skipped"] == 1 and skipped["compressed"] == 2


def test_an_existing_output_folder_is_resumed_not_overwritten(ws, tmp_path):
    root = tmp_path / "fotos"
    big_png(root / "a.png")
    big_png(root / "b.png", seed=9)
    target = tmp_path / "salida"
    first = ws.images_compress(sources=[str(root)], limit_kb=30, out_dir=str(target))
    assert first["compressed"] == 2
    mark = (target / "a.png").read_bytes()
    second = ws.images_compress(sources=[str(root)], limit_kb=30, out_dir=str(target))
    assert second["skipped"] == 2 and second["compressed"] == 0 and (target / "a.png").read_bytes() == mark


def test_time_limit_reports_what_is_left(ws, tmp_path, monkeypatch):
    root = tmp_path / "fotos"
    for i in range(3):
        big_png(root / f"{i}.png", (60, 60), seed=i)
    ticks = iter([0.0, 0.0] + [999.0] * 20)
    monkeypatch.setattr("kafka_hoard.workshop.service.time.monotonic", lambda: next(ticks))
    r = ws.images_compress(sources=[str(root)], limit_kb=2, time_limit_s=5)
    assert r["partial"] is True and r["pending"] == 2 and r["compressed"] + r["failed"] == 1
    assert any("repite la llamada" in n for n in r["notes"])


def test_jpeg_and_webp_ladders(ws, tmp_path):
    jpg = tmp_path / "in" / "foto.jpg"
    jpg.parent.mkdir()
    noise_image((300, 300), 4).save(jpg, quality=98)
    webp = tmp_path / "in" / "foto.webp"
    noise_image((300, 300), 6).save(webp, lossless=True)
    r = ws.images_compress(sources=[str(jpg), str(webp)], limit_kb=45)
    by = {Path(i["source"]).suffix: i for i in r["items"]}
    assert by[".jpg"]["status"] == "ok" and by[".jpg"]["strategy"].startswith(("quality-", "resize-"))
    assert by[".webp"]["status"] == "ok" and by[".webp"]["size_after"] <= 45 * 1024
    assert Image.open(by[".jpg"]["output"]).format == "JPEG" and Image.open(by[".webp"]["output"]).format == "WEBP"
    keep = ws.images_compress(sources=[str(jpg)], limit_mb=5)
    assert keep["items"][0]["strategy"] in ("optimize", "copia") and keep["items"][0]["size_after"] <= jpg.stat().st_size


def test_exif_is_kept_on_jpeg(ws, tmp_path):
    src = exif_rotated_jpeg(tmp_path / "in" / "movil.jpg") if (tmp_path / "in").mkdir() is None else None
    r = ws.images_compress(sources=[str(src)], limit_kb=3)
    assert r["items"][0]["status"] == "ok"
    assert Image.open(r["items"][0]["output"]).getexif().get(0x0112) == 6


def test_unsupported_and_missing_sources(ws, tmp_path):
    txt = tmp_path / "in" / "a.txt"
    txt.parent.mkdir()
    txt.write_text("x")
    with pytest.raises(KafkaError):
        ws.images_compress(sources=[str(txt)], limit_mb=1)
    empty = tmp_path / "vacia"
    empty.mkdir()
    with pytest.raises(KafkaError) as e:
        ws.images_compress(sources=[str(empty)], limit_mb=1)
    assert e.value.code == "not_found"


# ------------------------------------------------------------------ documents as input, filing the result
def add_pdf_doc(svc, name: str, text: str) -> str:
    out = svc.engine.ingest_bytes(make_pdf(text), name, source="upload")
    return out["documents"][0]["id"]


def test_document_ids_are_inputs_and_outputs_go_to_the_workshop_folder(svc, ws, tmp_path):
    d1 = add_pdf_doc(svc, "multa.pdf", docs.FINE)
    d2 = add_pdf_doc(svc, "poliza.pdf", docs.INSURANCE)
    folder = tmp_path / "Taller"
    tool(svc, "settings_set", values={"workshop.dir": str(folder)})
    r = tool(svc, "pdf_merge", files=[d1, d2])
    out = Path(r["output"])
    assert out.parent == folder and out.name == "multa_unido.pdf" and r["pages"] == 2
    assert "DIRECCIÓN GENERAL" in page_texts(out)[0] and "MAPFRE" in page_texts(out)[1]
    stored = Path(svc.files.path_of(svc.store.document(d1)["file_sha"], "pdf"))
    assert not list(stored.parent.glob("*unido*"))                  # nothing written into the content-addressed store
    info = tool(svc, "pdf_info", file=d1)
    assert info["pages"] == 1 and info["has_text"] is True


def test_default_workshop_folder_is_documents_kafkas_hoard_taller(svc, ws, tmp_path):
    d1 = add_pdf_doc(svc, "multa.pdf", docs.FINE)
    assert ws.default_dir() == tmp_path / "home" / "Documents" / "Kafka's Hoard" / "Taller"
    r = ws.pages(action="extract", file=d1, pages="1")
    assert Path(r["output"]).parent == ws.default_dir() and Path(r["output"]).is_file()


def test_bad_document_ids_are_explained(svc, ws):
    with pytest.raises(KafkaError) as e:
        ws.info(file="d_zzzzzzzzzzz")
    assert e.value.code == "not_found"
    text_doc = tool(svc, "doc_add_text", title="Nota", text=docs.RECEIPT)["documents"][0]["id"]
    with pytest.raises(KafkaError) as e:
        ws.info(file=text_doc)
    assert "necesita .pdf" in e.value.message
    svc.store.update_document(text_doc, file_sha="")
    with pytest.raises(KafkaError) as e:
        ws.info(file=text_doc)
    assert "no tiene fichero guardado" in e.value.message


def test_file_result_files_the_pdf_in_kafka_and_returns_the_new_id(svc, ws, tmp_path):
    src = write_pdf(tmp_path / "in", "factura.pdf", [docs.UTILITY, "segunda página"])
    r = tool(svc, "pdf_pages", action="extract", file=str(src), pages="1", file_result=True)
    assert len(r["doc_ids"]) == 1 and r["filed"][0]["created"] is True
    doc = svc.store.document(r["doc_ids"][0])
    assert doc["file_name"] == "factura_paginas.pdf" and doc["pages"] == 1
    assert tool(svc, "doc_get", doc=doc["id"])["document"]["issuer"]
    # the same content filed again is reported as a duplicate, not filed twice
    again = svc.workshop.pages(action="extract", file=str(src), pages="1")
    from kafka_hoard.agent_tools import _file_outputs
    _file_outputs(svc, again)
    assert again["filed"][0]["created"] is False and again["filed"][0]["doc_id"] == doc["id"] and "Ya estaba archivado" in again["filed"][0]["note"]
    plain = tool(svc, "pdf_pages", action="extract", file=str(src), pages="2")
    assert "doc_ids" not in plain and svc.store.counts()["documents"] == 1


def test_split_and_merge_can_file_every_part(svc, ws, tmp_path):
    src = write_pdf(tmp_path / "in", "dos.pdf", [docs.FINE, docs.INSURANCE])
    r = tool(svc, "pdf_split", file=str(src), mode="pages", file_result=True)
    assert len(r["doc_ids"]) == 2 and svc.store.counts()["documents"] == 2


# ------------------------------------------------------------------ safety of paths
WIN = os.name == "nt"
SYSTEM_FILE = r"C:\Windows\System32\drivers\etc\hosts" if WIN else "/etc/passwd"
SYSTEM_DIR = r"C:\Windows" if WIN else "/etc"


def test_inputs_and_outputs_refuse_system_and_private_places(svc, ws, tmp_path):
    with pytest.raises(KafkaError) as e:
        ws.info(file=SYSTEM_FILE)
    assert e.value.code in ("forbidden", "unsupported")
    cred = tmp_path / "in" / ".env"
    cred.parent.mkdir()
    cred.write_text("X=1")
    with pytest.raises(KafkaError):
        ws.resolve(str(cred))
    src = write_pdf(tmp_path / "in", "a.pdf", 1)
    for bad in (SYSTEM_DIR, str(svc.config.data_dir), str(svc.config.data_dir / "files")):
        with pytest.raises(KafkaError) as e:
            ws.pages(action="extract", file=str(src), pages="1", out_dir=bad)
        assert e.value.code == "forbidden", bad
    with pytest.raises(KafkaError):
        ws.pages(action="extract", file=str(src), pages="1", out_dir="relativa")
    with pytest.raises(KafkaError):
        tool(svc, "settings_set", values={"workshop.dir": SYSTEM_DIR + ("\\kafka" if WIN else "/kafka")})
    with pytest.raises(KafkaError):
        ws.pages(action="extract", file=str(svc.config.data_dir / "kafka.db"), pages="1")


def test_passwords_are_masked_in_every_echo(svc, ws, tmp_path):
    src = write_pdf(tmp_path / "in", "a.pdf", 1)
    r = tool(svc, "pdf_protect", action="protect", file=str(src), password="Contraseña-Larga-1", owner_password="otra-clave-9")
    flat = json.dumps(r, ensure_ascii=False)
    assert "Contraseña-Larga-1" not in flat and "otra-clave-9" not in flat
    assert r["options"]["password"] == "***" and r["options"]["owner_password"] == "***" and r["options"]["current_password"] == ""
    assert redact({"password": "x", "title": "t", "owner_password": ""}) == {"password": "***", "title": "t", "owner_password": ""}


def test_validation_errors_do_not_echo_the_password(svc, ws, tmp_path):
    from pydantic import ValidationError
    with pytest.raises(ValidationError) as e:
        tool(svc, "pdf_protect", action="encriptar", file=str(tmp_path / "a.pdf"), password="topsecret-123")
    assert "topsecret-123" not in str(e.value)


# ------------------------------------------------------------------ REST routes
def upload(client, *files, job=None):
    data = {"job": job} if job else None
    return client.post("/api/workshop/upload", files=[("files", f) for f in files], data=data)


def ui(client, name, **arguments):
    r = client.post("/api/ui/call", json={"name": name, "arguments": arguments})
    return r


def test_upload_run_download_cycle(client, tmp_path):
    client.svc.workshop.env = nothing_installed()
    r = upload(client, ("uno.pdf", make_pdf(["Hola uno", "Hoja dos"]), "application/pdf"), ("dos.pdf", make_pdf(["Hola dos"]), "application/pdf"))
    assert r.status_code == 200
    body = r.json()
    assert jobs.valid_job(body["job"]) and [f["pages"] for f in body["files"]] == [2, 1] and all(f["encrypted"] is False for f in body["files"])
    paths_ = [f["path"] for f in body["files"]]
    root = client.svc.config.data_dir / "workshop" / "in" / body["job"]
    assert all(Path(p).parent == root for p in paths_)
    merged = ui(client, "pdf_merge", files=paths_)
    assert merged.status_code == 200, merged.text
    out = merged.json()["output"]
    assert Path(out).parent == client.svc.config.data_dir / "workshop" / "out" / body["job"] and merged.json()["pages"] == 3
    dl = client.get("/api/workshop/file", params={"path": out})
    assert dl.status_code == 200 and dl.content[:4] == b"%PDF" and "attachment" in dl.headers["content-disposition"]
    assert dl.headers["x-content-type-options"] == "nosniff"
    inline = client.get("/api/workshop/file", params={"path": out, "inline": "1"})
    assert "inline" in inline.headers["content-disposition"]
    more = upload(client, ("tres.pdf", make_pdf(["tres"]), "application/pdf"), job=body["job"])
    assert more.json()["job"] == body["job"]


def test_upload_reports_unsupported_types_and_odd_names(client):
    r = upload(client, ("virus.exe", b"MZ", "application/octet-stream"), ("..\\..\\raro: nombre.pdf", make_pdf(["x"]), "application/pdf")).json()
    assert [x["ok"] for x in r["results"]] == [False, True] and ".exe" in r["results"][0]["error"]
    saved = Path(r["files"][0]["path"])
    assert saved.parent.name == r["job"] and saved.name.endswith(".pdf") and ".." not in saved.name and ":" not in saved.name
    bad = upload(client, ("a.pdf", make_pdf(["x"]), "application/pdf"), job="../../etc").json()
    assert jobs.valid_job(bad["job"])


def test_upload_flags_a_protected_pdf(client, tmp_path):
    ws = client.svc.workshop
    ws.env = nothing_installed()
    plain = write_pdf(tmp_path / "in", "a.pdf", 1)
    locked = Path(ws.protect(action="protect", file=str(plain), password="abc")["output"])
    r = upload(client, ("p.pdf", locked.read_bytes(), "application/pdf")).json()
    assert r["files"][0]["encrypted"] is True and "pages" not in r["files"][0]


def test_download_refuses_anything_the_workshop_did_not_produce(client, tmp_path):
    secret = tmp_path / "otro" / "privado.pdf"
    secret.parent.mkdir()
    secret.write_bytes(make_pdf(["privado"]))
    for bad in (str(secret), str(client.svc.config.data_dir / "kafka.db"), str(client.svc.config.data_dir / "mcp-token"), "/etc/passwd",
                str(client.svc.config.data_dir / "workshop" / ".." / "kafka.db"), "relativo.pdf", ""):
        assert client.get("/api/workshop/file", params={"path": bad}).status_code in (403, 404, 422), bad
    assert client.get("/api/workshop/file", params={"path": str(secret)}).status_code == 403
    assert client.get("/api/workshop/file", params={"path": str(secret)}, headers={"Sec-Fetch-Site": "cross-site"}).status_code == 403
    # a file the workshop wrote in this run is served, even outside <data>/workshop
    client.svc.workshop.env = nothing_installed()
    made = client.svc.workshop.pages(action="extract", file=str(secret), pages="1")["output"]
    assert Path(made).parent == secret.parent
    assert client.get("/api/workshop/file", params={"path": made}).status_code == 200
    assert client.get("/api/workshop/file", params={"path": str(secret)}).status_code == 403


def test_status_route_reports_what_is_installed(client):
    client.svc.workshop.env = gs_env(lambda c, e: done())[0]
    s = client.get("/api/workshop/status").json()
    assert s["ghostscript"] == "/usr/bin/gs" and s["libreoffice"] is None and s["word"] is False and s["pypdf"]
    assert s["dir"].endswith("Taller") and "docx" in s["office_extensions"]


def test_housekeeping_deletes_job_folders_older_than_a_week(svc, clock):
    root = svc.config.data_dir / "workshop"
    old_in, old_out, fresh = root / "in" / "j_aaaaaaaaaaaa", root / "out" / "j_aaaaaaaaaaaa", root / "in" / "j_bbbbbbbbbbbb"
    for folder, age in ((old_in, 8 * 86400), (old_out, 8 * 86400), (fresh, 3600)):
        folder.mkdir(parents=True)
        (folder / "x.pdf").write_bytes(b"%PDF")
        os.utime(folder, (clock() - age, clock() - age))
    other = root / "in" / "no-es-un-trabajo"
    other.mkdir()
    os.utime(other, (clock() - 30 * 86400,) * 2)
    r = tool(svc, "housekeeping_run")
    assert r["workshop_jobs_purged"] == 2
    assert not old_in.exists() and not old_out.exists() and fresh.exists() and other.exists()


# ------------------------------------------------------------------ the tools
WORKSHOP_TOOLS = ["pdf_merge", "pdf_split", "pdf_pages", "pdf_compress", "pdf_protect", "pdf_watermark", "pdf_info", "pdf_metadata_set", "pdf_from_images",
                  "pdf_from_office", "pdf_to_images", "images_compress"]


def test_workshop_tools_follow_the_catalogue_rules():
    for name in WORKSHOP_TOOLS:
        t = TOOLS_BY_NAME[name]
        first = t.description.splitlines()[0]
        assert len(first) <= 110 and ". " in first and t.description.splitlines()[-1].startswith("Sinónimos: "), name
        assert t.annotations["destructiveHint"] is False
        assert t.annotations["readOnlyHint"] is (name == "pdf_info")
        for prop, spec in t.input_model.model_json_schema()["properties"].items():
            assert spec.get("description"), f"{name}.{prop}"
    wanted = ("une estos pdf", "divide el pdf", "quita la página 3", "gira el pdf", "comprime el pdf", "que pese menos de 2 MB", "ponle contraseña",
              "quítale la contraseña", "marca de agua", "pasa estas fotos a pdf", "convierte el word a pdf", "comprime las imágenes",
              "que cada png pese menos de 5 MB", "reduce el tamaño")
    text = " ".join(TOOLS_BY_NAME[n].description for n in WORKSHOP_TOOLS)
    for phrase in wanted:
        assert phrase in text, phrase


def test_tools_through_the_agent_route_and_the_ui_route(client, tmp_path):
    client.svc.workshop.env = nothing_installed()
    src = write_pdf(tmp_path / "in", "agente.pdf", 4)
    call = client.post("/api/agent/call", json={"name": "pdf_pages", "arguments": {"action": "delete", "file": str(src), "pages": "2-3"}}, headers=client.bearer)
    assert call.status_code == 200 and call.json()["pages"] == 2
    bad = client.post("/api/agent/call", json={"name": "pdf_pages", "arguments": {"action": "borrar", "file": str(src)}}, headers=client.bearer)
    assert bad.status_code == 400
    missing = client.post("/api/agent/call", json={"name": "pdf_info", "arguments": {"file": str(tmp_path / "no.pdf")}}, headers=client.bearer)
    assert missing.status_code == 404 and missing.json()["code"] == "not_found"
    listed = {t["name"] for t in client.get("/api/agent/tools").json()["tools"]}
    assert set(WORKSHOP_TOOLS) <= listed and set(WORKSHOP_TOOLS) <= set(client.get("/api/ui/tools").json()["tools"])
    info = ui(client, "pdf_info", file=str(src)).json()
    assert info["pages"] == 4


def test_single_string_arguments_are_accepted_as_lists(svc, ws, tmp_path):
    img = big_png(tmp_path / "in" / "uno.png", (50, 50))
    r = tool(svc, "images_compress", sources=str(img), limit_mb=1)
    assert r["compressed"] == 1
    p = tool(svc, "pdf_from_images", images=str(img))
    assert p["pages"] == 1
