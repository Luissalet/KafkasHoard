"""Synthetic PowerPoint files for the tests: a real OOXML package (presentation order, relationships, notes), no Office needed."""

from __future__ import annotations

import io
import zipfile

NS_P = "http://schemas.openxmlformats.org/presentationml/2006/main"
NS_A = "http://schemas.openxmlformats.org/drawingml/2006/main"
NS_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG = "http://schemas.openxmlformats.org/package/2006/relationships"


def _sp(text: str, kind: str = "") -> str:
    ph = f'<p:ph type="{kind}"/>' if kind else ""
    return (f'<p:sp><p:nvSpPr><p:cNvPr id="1" name="s"/><p:cNvSpPr/><p:nvPr>{ph}</p:nvPr></p:nvSpPr>'
            f'<p:txBody><a:p><a:r><a:t>{text}</a:t></a:r></a:p></p:txBody></p:sp>')


def slide(title: str, lines: list[str] = ()) -> str:
    body = (_sp(title, "title") if title else "") + "".join(_sp(x) for x in lines)
    return f'<p:sld xmlns:p="{NS_P}" xmlns:a="{NS_A}"><p:cSld><p:spTree>{body}</p:spTree></p:cSld></p:sld>'


def make_pptx(slides: list[tuple[str, list[str]]], *, order: list[int] | None = None, notes: dict[int, str] | None = None) -> bytes:
    """``slides`` are the parts slide1..n; ``order`` the part numbers in the order the presentation shows them."""
    order = order or list(range(1, len(slides) + 1))
    notes = notes or {}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>')
        ids = "".join(f'<p:sldId id="{255 + i}" r:id="rId{n}"/>' for i, n in enumerate(order, 1))
        z.writestr("ppt/presentation.xml", f'<p:presentation xmlns:p="{NS_P}" xmlns:r="{NS_R}"><p:sldIdLst>{ids}</p:sldIdLst></p:presentation>')
        rels = "".join(f'<Relationship Id="rId{n}" Type="{NS_R}/slide" Target="slides/slide{n}.xml"/>' for n in range(1, len(slides) + 1))
        z.writestr("ppt/_rels/presentation.xml.rels", f'<Relationships xmlns="{PKG}">{rels}</Relationships>')
        for n, (title, lines) in enumerate(slides, 1):
            z.writestr(f"ppt/slides/slide{n}.xml", slide(title, list(lines)))
            if n in notes:
                z.writestr(f"ppt/slides/_rels/slide{n}.xml.rels",
                           f'<Relationships xmlns="{PKG}"><Relationship Id="rIdN" Type="{NS_R}/notesSlide" Target="../notesSlides/notesSlide{n}.xml"/></Relationships>')
                z.writestr(f"ppt/notesSlides/notesSlide{n}.xml",
                           f'<p:notes xmlns:p="{NS_P}" xmlns:a="{NS_A}"><p:cSld><p:spTree>{_sp(notes[n])}{_sp(str(n), "sldNum")}</p:spTree></p:cSld></p:notes>')
    return buf.getvalue()


# a deck as Kafka sees them: an insurance renewal presented in slides, with an empty slide in the middle
RENEWAL = make_pptx(
    [("Renovación de la póliza de hogar", ["Aseguradora Demo Seguros", "Prima total: 320,00 €"]),
     ("", []),
     ("Coberturas", ["Daños por agua", "Responsabilidad civil"]),
     ("Fechas", ["Efecto: 01/11/2026", "Vencimiento: 31/10/2027"])],
    order=[1, 2, 4, 3], notes={4: "Recordar domiciliar el recibo"})
