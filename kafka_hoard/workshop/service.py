"""The workshop: PDF and image operations on files given as absolute paths or Kafka document ids.

Every operation reads its inputs, writes new files (never over an existing one) and returns a result dict with the output paths,
page counts and sizes before and after. Nothing here touches the database except reading a document's stored original.
Passwords are used in memory only: never logged, never returned, never stored."""

from __future__ import annotations

import os
import re
import tempfile
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .. import paths
from ..errors import KafkaError
from ..files import FileStore
from . import external, imagetools, jobs, pdfops
from .names import human_size, safe_stem, split_output, unique_dir, write_unique
from .proc import Env
from .ranges import describe, parse_groups, parse_ranges

DOC_ID = re.compile(r"^d_[0-9a-z]{6,}$")
IMAGE_INPUT_EXT = {"png", "jpg", "jpeg", "webp", "heic", "heif", "bmp", "tif", "tiff", "gif", "jfif"}
SECRET_KEYS = ("password", "contrasena", "pass")
MB = 1024 * 1024
MAX_REGISTRY = 5000


def redact(options: dict[str, Any]) -> dict[str, Any]:
    """A copy of an arguments dict that is safe to echo: every value whose key mentions a password becomes «***» (or stays empty)."""
    out: dict[str, Any] = {}
    for key, value in options.items():
        if any(s in key.lower() for s in SECRET_KEYS):
            out[key] = "***" if value else ""
        else:
            out[key] = value
    return out


@dataclass
class Src:
    """An input file: where it is, how outputs are named after it and which rule picks the output folder."""
    path: Path
    stem: str
    ext: str
    kind: str                # path | doc | upload
    label: str
    doc_id: str = ""

    @property
    def size(self) -> int:
        return self.path.stat().st_size


def _natural(name: str) -> list[Any]:
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", name)]


class Workshop:
    def __init__(self, config: Any, store: Any, files: FileStore, setting: Callable[[str], str], *, env: Optional[Env] = None,
                 home: Optional[Path] = None):
        self.config = config
        self.store = store
        self.files = files
        self.setting = setting
        self.env = env or Env()
        self.home = Path(home) if home else None
        self.produced: "OrderedDict[str, float]" = OrderedDict()

    # ------------------------------------------------------------------ folders
    @property
    def root(self) -> Path:
        return Path(self.config.data_dir) / "workshop"

    def default_dir(self) -> Path:
        configured = (self.setting("workshop.dir") or "").strip()
        if configured:
            return Path(configured).expanduser()
        return (self.home or Path.home()) / "Documents" / "Kafka's Hoard" / "Taller"

    def _note(self, path: Path) -> None:
        self.produced[str(path.resolve())] = time.time()
        while len(self.produced) > MAX_REGISTRY:
            self.produced.popitem(last=False)

    def is_servable(self, path: str | Path) -> bool:
        """Files the download route may serve: anything under ``<data>/workshop`` or produced by this process."""
        try:
            p = Path(path).resolve()
        except (OSError, ValueError):
            return False
        if not p.is_file():
            return False
        try:
            p.relative_to(self.root.resolve())
            return True
        except ValueError:
            return str(p) in self.produced

    def _check_out(self, folder: Path) -> Path:
        reason = paths.unsafe_output_dir(folder, self.config.data_dir)
        if reason:
            raise KafkaError("forbidden", f"No escribo en «{folder}»: {self._es(reason)}.", "Elige otra carpeta (out_dir) o deja la de por defecto.")
        try:
            Path(folder).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise KafkaError("forbidden", f"No puedo crear la carpeta «{folder}» ({type(exc).__name__}).", "Comprueba que existe la unidad y que tienes permiso.") from exc
        return Path(folder)

    @staticmethod
    def _es(reason: str) -> str:
        table = {"the path must be absolute": "la ruta debe ser absoluta", "system folders are not allowed": "las carpetas del sistema no se pueden usar",
                 "configuration and hidden folders are not allowed": "las carpetas de configuración u ocultas no se pueden usar",
                 "that path is a file, not a folder": "esa ruta es un fichero, no una carpeta", "the path cannot be resolved": "la ruta no se puede resolver",
                 "not a valid path": "la ruta no es válida", "the file does not exist": "el fichero no existe",
                 "that looks like a credentials file": "parece un fichero de credenciales"}
        if reason.startswith("Kafka's own data folder"):
            return "la carpeta de datos de Kafka no se puede usar (salvo la del taller y el buzón)"
        return table.get(reason, reason)

    def _default_out(self, anchor: Src) -> Path:
        if anchor.kind == "upload":
            job = jobs.job_of(self.root, anchor.path)
            if job:
                return jobs.out_dir(self.root, job)
        if anchor.kind == "doc":
            return self.default_dir()
        return anchor.path.parent

    def _target(self, anchor: Src, *, output: str = "", out_dir: str = "", ext: str = "pdf") -> tuple[Path, Optional[str]]:
        if output:
            folder, stem = split_output(output, ext)
            return self._check_out(folder), stem
        folder = Path(out_dir).expanduser() if out_dir else self._default_out(anchor)
        return self._check_out(folder), None

    def _folder_for_many(self, anchor: Src, out_dir: str, name: str) -> Path:
        if out_dir:
            return self._check_out(Path(out_dir).expanduser())
        base = self._check_out(self._default_out(anchor))
        return unique_dir(base, name)

    def _save(self, data: bytes, anchor: Src, *, suffix: str, ext: str = "pdf", output: str = "", out_dir: str = "", stem: str = "") -> Path:
        folder, forced = self._target(anchor, output=output, out_dir=out_dir, ext=ext)
        name = forced if forced is not None else (stem or anchor.stem)
        use_suffix = "" if forced is not None else suffix
        try:
            path = write_unique(folder, name, use_suffix, ext, data)
        except OSError as exc:
            raise KafkaError("forbidden", f"No puedo escribir en «{folder}» ({type(exc).__name__}).", "Comprueba el permiso y el espacio libre.") from exc
        self._note(path)
        return path

    # ------------------------------------------------------------------ inputs
    def resolve(self, ref: str, *, exts: Optional[Iterable[str]] = None, what: str = "") -> Src:
        raw = str(ref or "").strip().strip('"').strip("'").strip()
        if raw.lower().startswith("file:///"):
            raw = raw[8:] if re.match(r"file:///[A-Za-z]:", raw, re.I) else raw[7:]
        if not raw:
            raise KafkaError("invalid", "Falta el fichero.", "Indica una ruta absoluta o el id de un documento (d_…).")
        wanted = {e.lower().lstrip(".") for e in exts} if exts else None
        if DOC_ID.match(raw) and not os.path.exists(raw):
            doc = self.store.find_document(raw)
            if doc is None:
                raise KafkaError("not_found", f"No existe el documento {raw}.", "Lista los documentos con docs_list.")
            if not doc.get("file_sha"):
                raise KafkaError("not_found", f"El documento {raw} no tiene fichero guardado (se archivó como texto).")
            path = self.files.path_of(doc["file_sha"], doc.get("file_ext") or "bin")
            if not path.is_file():
                raise KafkaError("not_found", f"Falta el fichero guardado de {raw}.")
            ext = (doc.get("file_ext") or path.suffix).lower().lstrip(".")
            stem = safe_stem(Path(doc.get("file_name") or "").stem or doc.get("title") or raw)
            src = Src(path, stem, ext, "doc", f"{raw} ({doc.get('title') or stem})", raw)
        else:
            path = Path(raw).expanduser()
            if not path.is_absolute():
                raise KafkaError("invalid", f"La ruta «{raw}» no es absoluta.", "Escribe la ruta completa (C:\\Users\\…\\fichero.pdf) o el id de un documento (d_…).")
            if path.is_dir():
                raise KafkaError("invalid", f"«{path.name}» es una carpeta, no un fichero.")
            if not path.is_file():
                raise KafkaError("not_found", f"No existe el fichero «{path}».")
            reason = paths.unsafe_file(path, self.config.data_dir)
            if reason:
                raise KafkaError("forbidden", f"No puedo leer «{path.name}»: {self._es(reason)}.", "Usa ficheros de tus carpetas personales.")
            path = path.resolve()
            kind = "upload" if jobs.job_of(self.root, path) else "path"
            src = Src(path, safe_stem(path.stem), path.suffix.lower().lstrip("."), kind, path.name)
        if wanted is not None and src.ext not in wanted:
            raise KafkaError("unsupported", f"«{src.label}» es un fichero .{src.ext or '?'} y esta operación necesita {' o '.join('.' + e for e in sorted(wanted))}.",
                             self._wrong_type_hint(src.ext))
        return src

    @staticmethod
    def _wrong_type_hint(ext: str) -> str:
        if ext in IMAGE_INPUT_EXT:
            return "Para pasar imágenes a PDF usa pdf_from_images; para reducirlas, images_compress."
        if ext in external.OFFICE_EXT:
            return "Para pasar un documento de Word a PDF usa pdf_from_office."
        if ext == "pdf":
            return "Es un PDF: usa las herramientas pdf_*."
        return ""

    def _pdf(self, ref: str, password: str = "") -> tuple[Src, Any]:
        src = self.resolve(ref, exts={"pdf"})
        return src, pdfops.open_pdf(src.path, password, src.label)

    def _folder_check(self, folder: Path) -> None:
        try:
            folder.resolve().relative_to(self.root.resolve())
            return
        except ValueError:
            pass
        reason = paths.unsafe_folder(folder, self.config.data_dir)
        if reason:
            raise KafkaError("forbidden", f"No puedo leer la carpeta «{folder}»: {self._es(reason)}.")

    # ------------------------------------------------------------------ results
    def _entry(self, path: Path, pages: Optional[int] = None) -> dict[str, Any]:
        out: dict[str, Any] = {"path": str(path), "name": path.name, "size": path.stat().st_size, "size_text": human_size(path.stat().st_size)}
        if pages is not None:
            out["pages"] = pages
        return out

    def _result(self, op: str, outputs: list[dict[str, Any]], *, size_before: Optional[int] = None, pages: Optional[int] = None,
                notes: Optional[list[str]] = None, **extra: Any) -> dict[str, Any]:
        size_after = sum(o["size"] for o in outputs)
        out: dict[str, Any] = {"ok": True, "operation": op, "outputs": outputs, "output": outputs[0]["path"] if len(outputs) == 1 else None}
        if pages is not None:
            out["pages"] = pages
        if size_before is not None:
            out["size_before"] = size_before
            out["size_before_text"] = human_size(size_before)
        out["size_after"] = size_after
        out["size_after_text"] = human_size(size_after)
        out.update(extra)
        out["notes"] = notes or []
        return out

    def pdf_summary(self, path: Path) -> dict[str, Any]:
        """Page count and whether it asks for a password, for the upload list. Never raises."""
        try:
            pdfops.need_pypdf()
            reader = pdfops.PdfReader(str(path))
            if reader.is_encrypted:
                try:
                    if not int(reader.decrypt("")):
                        return {"encrypted": True}
                except Exception:  # noqa: BLE001
                    return {"encrypted": True}
                return {"encrypted": True, "pages": len(reader.pages)}
            return {"encrypted": False, "pages": len(reader.pages)}
        except Exception:  # noqa: BLE001
            return {"encrypted": False, "damaged": True}

    # ------------------------------------------------------------------ status
    def status(self) -> dict[str, Any]:
        found = external.detect(self.env)
        try:
            import pillow_heif  # type: ignore[import-not-found]  # noqa: F401
            heic = True
        except Exception:  # noqa: BLE001
            heic = False
        return {"dir": str(self.default_dir()), "uploads_dir": str(self.root), "pypdf": pdfops.pypdf_version(), "ghostscript": found["ghostscript"],
                "libreoffice": found["libreoffice"], "word": found["word"], "heic": heic, "windows": self.env.windows,
                "office_extensions": sorted(external.OFFICE_EXT), "image_extensions": sorted(IMAGE_INPUT_EXT)}

    # ================================================================== PDF operations
    def info(self, *, file: str, password: str = "") -> dict[str, Any]:
        src = self.resolve(file, exts={"pdf"})
        size = src.size
        pdfops.need_pypdf()
        try:
            reader = pdfops.PdfReader(str(src.path))
        except Exception as exc:  # noqa: BLE001
            raise KafkaError("invalid", f"«{src.label}» no es un PDF válido o está dañado ({type(exc).__name__}).") from exc
        out: dict[str, Any] = {"ok": True, "operation": "info", "file": str(src.path) if src.kind != "doc" else src.label, "name": src.path.name if src.kind != "doc" else src.stem + ".pdf",
                               "size": size, "size_text": human_size(size), "encrypted": bool(reader.is_encrypted), "locked": False}
        if reader.is_encrypted:
            unlocked = 0
            for pw in ([password] if password else []) + [""]:
                try:
                    unlocked = int(reader.decrypt(pw))
                except Exception:  # noqa: BLE001
                    unlocked = 0
                if unlocked:
                    break
            if not unlocked:
                out["locked"] = True
                out["notes"] = ["El PDF está protegido con contraseña: indícala en password para ver páginas y metadatos."]
                return out
        try:
            out["pages"] = len(reader.pages)
            out["page_sizes"] = pdfops.page_sizes(reader)
            out["metadata"] = pdfops.read_metadata(reader)
            out["version"] = str(getattr(reader, "pdf_header", "") or "").replace("%PDF-", "")
        except Exception as exc:  # noqa: BLE001
            raise KafkaError("invalid", f"«{src.label}» está dañado y no se puede leer ({type(exc).__name__}).") from exc
        probe = pdfops.text_probe(src.path, password)
        out["has_text"] = probe.get("has_text")
        if probe.get("has_text") is False:
            out["scanned"] = True
            out["notes"] = ["No tiene capa de texto (probablemente escaneado): Kafka necesita OCR para leerlo."]
        else:
            out["scanned"] = False
            out["notes"] = []
        return out

    def merge(self, *, files: list[str], ranges: Optional[list[str]] = None, password: str = "", output: str = "", out_dir: str = "") -> dict[str, Any]:
        if len(files) < 2:
            raise KafkaError("invalid", "Indica al menos dos PDF para unir.", "Para quedarte con unas páginas de uno solo usa pdf_pages.")
        ranges = [str(r or "") for r in (ranges or [])]
        if ranges and len(ranges) != len(files):
            raise KafkaError("invalid", f"ranges tiene {len(ranges)} elementos y files {len(files)}: deben coincidir (usa «» para todas las páginas).")
        srcs = [self.resolve(f, exts={"pdf"}) for f in files]
        parts, before = [], 0
        for i, src in enumerate(srcs):
            reader = pdfops.open_pdf(src.path, password, src.label)
            spec = ranges[i].strip() if ranges else ""
            indexes = None
            if spec:
                try:
                    indexes = [p - 1 for p in parse_ranges(spec, len(reader.pages))]
                except KafkaError as exc:
                    raise KafkaError("invalid", f"{src.label}: {exc.message}", exc.hint) from exc
            parts.append((reader, indexes, src.label))
            before += src.size
        data, total = pdfops.merge(parts)
        path = self._save(data, srcs[0], suffix="_unido", output=output, out_dir=out_dir)
        return self._result("merge", [self._entry(path, total)], size_before=before, pages=total, inputs=[s.label for s in srcs])

    def split(self, *, file: str, mode: str = "ranges", ranges: str = "", every: int = 1, password: str = "", out_dir: str = "") -> dict[str, Any]:
        src, reader = self._pdf(file, password)
        total = len(reader.pages)
        if mode == "pages":
            groups = [[i] for i in range(1, total + 1)]
        elif mode == "every":
            n = max(1, int(every))
            groups = [list(range(s, min(s + n, total + 1))) for s in range(1, total + 1, n)]
        else:
            if not str(ranges).strip():
                raise KafkaError("invalid", "Indica los rangos para dividir.", "Por ejemplo 1-3,4-6: un fichero por cada rango.")
            groups = parse_groups(ranges, total)
        folder = self._folder_for_many(src, out_dir, f"{src.stem}_dividido")
        pad = len(str(total))
        outputs, notes = [], []
        for number, group in enumerate(groups, start=1):
            data = pdfops.pages_to_bytes(reader, [p - 1 for p in group])
            path = write_unique(folder, src.stem, "_" + self._split_tag(group, number, pad), "pdf", data)
            self._note(path)
            outputs.append(self._entry(path, len(group)))
        if not outputs:
            raise KafkaError("invalid", "No hay nada que dividir.")
        return self._result("split", outputs, size_before=src.size, pages=total, out_dir=str(folder), notes=notes)

    def pages(self, *, action: str, file: str, pages: str = "", degrees: int = 90, order: str = "", password: str = "", output: str = "",
              out_dir: str = "") -> dict[str, Any]:
        src, reader = self._pdf(file, password)
        total = len(reader.pages)
        notes: list[str] = []
        if action == "extract":
            chosen = parse_ranges(pages, total)
            data = pdfops.pages_to_bytes(reader, [p - 1 for p in chosen])
            suffix, count = "_paginas", len(chosen)
        elif action == "delete":
            gone = set(parse_ranges(pages, total))
            keep = [p for p in range(1, total + 1) if p not in gone]
            if not keep:
                raise KafkaError("invalid", "Así se borrarían todas las páginas: el PDF quedaría vacío.", "Indica solo las páginas que sobran.")
            data = pdfops.pages_to_bytes(reader, [p - 1 for p in keep])
            suffix, count = "_sin_paginas", len(keep)
            notes.append(f"Páginas quitadas: {describe(sorted(gone))}.")
        elif action == "rotate":
            deg = int(degrees) % 360
            if deg not in (90, 180, 270):
                raise KafkaError("invalid", "Los grados deben ser 90, 180 o 270 (o -90).")
            chosen_set = {p - 1 for p in parse_ranges(pages, total, default_all=True)}
            data = pdfops.rotate(reader, chosen_set, deg)
            suffix, count = "_rotado", total
            notes.append(f"Rotadas {deg}° a la derecha las páginas {describe([p + 1 for p in chosen_set])}.")
        elif action == "reorder":
            new_order = self._order(order, total)
            data = pdfops.pages_to_bytes(reader, [p - 1 for p in new_order])
            suffix, count = "_reordenado", total
        else:
            raise KafkaError("invalid", f"Acción desconocida: {action}.", "Usa extract, delete, rotate o reorder.")
        path = self._save(data, src, suffix=suffix, output=output, out_dir=out_dir)
        return self._result("pages", [self._entry(path, count)], size_before=src.size, pages=count, action=action, notes=notes)

    @staticmethod
    def _split_tag(group: list[int], number: int, pad: int) -> str:
        """«p003» for one page, «p01-03» for a run, «p2,4,6» for odd/even groups, «parte_02» when that would be too long."""
        if len(group) == 1:
            return f"p{group[0]:0{pad}d}"
        if group == list(range(group[0], group[-1] + 1)):
            return f"p{group[0]:0{pad}d}-{group[-1]:0{pad}d}"
        label = describe(group)
        return f"p{label}" if len(label) <= 30 else f"parte_{number:02d}"

    @staticmethod
    def _order(order: str, total: int) -> list[int]:
        text = str(order or "").strip()
        if text.lower() in ("reverse", "invertir", "inverso", "al reves", "al revés"):
            return list(range(total, 0, -1))
        chosen = parse_ranges(text, total)
        missing = [p for p in range(1, total + 1) if p not in chosen]
        repeated = sorted({p for p in chosen if chosen.count(p) > 1})
        if missing or repeated:
            what = []
            if missing:
                what.append(f"faltan las páginas {describe(missing)}")
            if repeated:
                what.append(f"se repiten las páginas {describe(repeated)}")
            raise KafkaError("invalid", f"El nuevo orden debe incluir cada página una vez: {' y '.join(what)}.",
                             "Para quedarte con algunas páginas usa action=extract.")
        return chosen

    def compress(self, *, file: str, preset: str = "ebook", target_mb: Optional[float] = None, engine: str = "auto", password: str = "",
                 output: str = "", out_dir: str = "") -> dict[str, Any]:
        src, reader = self._pdf(file, password)
        pages = len(reader.pages)
        original = src.path.read_bytes()
        encrypted = bool(reader.is_encrypted)
        target = int(target_mb * MB) if target_mb else None
        gs = self.env.ghostscript() if engine in ("auto", "ghostscript") else None
        if engine == "ghostscript" and not gs:
            raise KafkaError("not_configured", "Ghostscript no está instalado (no encuentro gswin64c ni gs).", "Instálalo o usa engine=auto para el motor sin Ghostscript.")
        attempts: list[dict[str, Any]] = []
        candidates: list[tuple[int, bytes, str, str]] = []     # size, data, engine, step
        notes: list[str] = []

        def consider(engine_name: str, step: str, data: bytes) -> bool:
            attempts.append({"engine": engine_name, "step": step, "size": len(data)})
            try:
                if pdfops.count_pages_of(data) != pages:
                    notes.append(f"{engine_name} ({step}) cambió el número de páginas: resultado descartado.")
                    return False
            except KafkaError:
                notes.append(f"{engine_name} ({step}) produjo un PDF ilegible: resultado descartado.")
                return False
            candidates.append((len(data), data, engine_name, step))
            return target is not None and len(data) <= target

        reached = False
        start = PRESET_INDEX[preset]
        if gs:
            with tempfile.TemporaryDirectory(prefix="kafka-pdf-") as tmp:
                source_for_gs = src.path
                if encrypted:
                    source_for_gs = Path(tmp) / "plain.pdf"
                    source_for_gs.write_bytes(pdfops.unprotect(reader))
                for step in pdfops.PRESETS[start:]:
                    try:
                        data = external.ghostscript_compress(self.env, gs, source_for_gs, step)
                    except KafkaError as exc:
                        notes.append(exc.message)
                        break
                    if consider("ghostscript", step, data):
                        reached = True
                        break
                    if target is None:
                        break
        if not reached and (not gs or engine == "pypdf" or target is not None or not candidates):
            levels = pdfops.PYPDF_LEVELS[start:]
            for name, quality, dpi in levels:
                data, changed = pdfops.compress_pypdf(reader, quality, dpi)
                if consider("pypdf", name, data):
                    reached = True
                    break
                if target is None:
                    break
        if not encrypted:
            candidates.append((len(original), original, "original", "sin cambios"))
        best_size, best, used_engine, used_step = min(candidates, key=lambda c: c[0])
        if used_engine == "original":
            notes.append("No se ha podido reducir: el PDF ya está optimizado (se guarda una copia igual).")
        if encrypted:
            notes.append("El resultado no lleva contraseña: vuelve a protegerlo con pdf_protect si lo necesitas.")
        if target is not None and best_size > target:
            notes.append(f"No se ha podido bajar de {target_mb:g} MB: lo mínimo conseguido es {human_size(best_size)}.")
        path = self._save(best, src, suffix="_comprimido", output=output, out_dir=out_dir)
        saved = max(0, len(original) - best_size)
        return self._result("compress", [self._entry(path, pages)], size_before=len(original), pages=pages, engine=used_engine, step=used_step,
                            reduction_pct=round(100 * saved / len(original), 1) if original else 0.0,
                            reached_target=(best_size <= target) if target is not None else None, attempts=attempts, notes=notes)

    def protect(self, *, action: str, file: str, password: str, owner_password: str = "", current_password: str = "", allow_print: bool = True,
                allow_copy: bool = True, allow_modify: bool = True, output: str = "", out_dir: str = "") -> dict[str, Any]:
        if not password:
            raise KafkaError("invalid", "Falta la contraseña.")
        if action == "protect":
            src, reader = self._pdf(file, current_password)
            data = pdfops.protect(reader, password, owner_password, allow_print=allow_print, allow_copy=allow_copy, allow_modify=allow_modify)
            path = self._save(data, src, suffix="_protegido", output=output, out_dir=out_dir)
            notes = ["Cifrado AES-256. Sin la contraseña no hay forma de abrirlo: guárdala."]
            if not (allow_print and allow_copy and allow_modify):
                notes.append("Los permisos (imprimir, copiar, modificar) los respetan los lectores de PDF corrientes, pero no son una protección fuerte.")
        elif action == "unprotect":
            src = self.resolve(file, exts={"pdf"})
            pdfops.need_pypdf()
            try:
                reader = pdfops.PdfReader(str(src.path))
            except Exception as exc:  # noqa: BLE001
                raise KafkaError("invalid", f"«{src.label}» no es un PDF válido o está dañado ({type(exc).__name__}).") from exc
            if not reader.is_encrypted:
                raise KafkaError("invalid", f"«{src.label}» no tiene contraseña.")
            try:
                ok = int(reader.decrypt(password))
            except Exception:  # noqa: BLE001
                ok = 0
            if not ok:
                raise KafkaError("invalid", f"La contraseña de «{src.label}» no es correcta.")
            data = pdfops.unprotect(reader)
            path = self._save(data, src, suffix="_sin_clave", output=output, out_dir=out_dir)
            notes = []
        else:
            raise KafkaError("invalid", f"Acción desconocida: {action}.", "Usa protect o unprotect.")
        pages = len(reader.pages)
        return self._result("protect", [self._entry(path, pages)], size_before=src.size, pages=pages, action=action, notes=notes)

    def watermark(self, *, file: str, text: str, opacity: float = 0.3, angle: float = 45.0, font_size: float = 60.0, color: str = "gris", pages: str = "",
                  password: str = "", output: str = "", out_dir: str = "") -> dict[str, Any]:
        src, reader = self._pdf(file, password)
        total = len(reader.pages)
        chosen = {p - 1 for p in parse_ranges(pages, total, default_all=True)}
        data = pdfops.watermark(reader, chosen, text=text, opacity=min(1.0, max(0.02, float(opacity))), angle=float(angle), font_size=float(font_size),
                                colour=pdfops.parse_colour(color))
        path = self._save(data, src, suffix="_marca", output=output, out_dir=out_dir)
        notes = ["El resultado no lleva contraseña."] if reader.is_encrypted else []
        return self._result("watermark", [self._entry(path, total)], size_before=src.size, pages=total, watermarked_pages=describe([p + 1 for p in chosen]), notes=notes)

    def metadata_set(self, *, file: str, title: Optional[str] = None, author: Optional[str] = None, subject: Optional[str] = None,
                     keywords: Optional[str] = None, password: str = "", output: str = "", out_dir: str = "") -> dict[str, Any]:
        values = {"title": title, "author": author, "subject": subject, "keywords": keywords}
        if all(v is None for v in values.values()):
            raise KafkaError("invalid", "Indica al menos un campo: title, author, subject o keywords.", "Una cadena vacía borra el campo.")
        src, reader = self._pdf(file, password)
        data = pdfops.set_metadata(reader, values)
        path = self._save(data, src, suffix="_metadatos", output=output, out_dir=out_dir)
        check = pdfops.open_pdf(path, "", path.name)
        return self._result("metadata_set", [self._entry(path, len(check.pages))], size_before=src.size, pages=len(check.pages), metadata=pdfops.read_metadata(check))

    # ================================================================== images and documents
    def _image_sources(self, images: list[str]) -> list[Src]:
        srcs: list[Src] = []
        for ref in images:
            raw = str(ref or "").strip().strip('"').strip("'")
            candidate = Path(raw).expanduser() if raw and not DOC_ID.match(raw) else None
            if candidate is not None and candidate.is_absolute() and candidate.is_dir():
                self._folder_check(candidate)
                found = sorted((p for p in candidate.iterdir() if p.is_file() and p.suffix.lower().lstrip(".") in IMAGE_INPUT_EXT), key=lambda p: _natural(p.name))
                if not found:
                    raise KafkaError("not_found", f"No hay imágenes en la carpeta «{candidate}».")
                srcs.extend(self.resolve(str(p), exts=IMAGE_INPUT_EXT) for p in found)
            else:
                srcs.append(self.resolve(raw, exts=IMAGE_INPUT_EXT))
        return srcs

    def from_images(self, *, images: list[str], page_size: str = "a4", margin_mm: float = 10.0, orientation: str = "auto", output: str = "",
                    out_dir: str = "") -> dict[str, Any]:
        srcs = self._image_sources(images)
        if not srcs:
            raise KafkaError("invalid", "Indica al menos una imagen.")
        size = page_size.lower()
        if size != "fit" and size not in pdfops.PAGE_SIZES_PT:
            raise KafkaError("invalid", f"Tamaño de página desconocido: {page_size}.", "Usa A4, Letter o fit (ajustar a la imagen).")
        pdfops.need_pypdf()
        stream = ((pdfops.open_image(s.path), s.label) for s in srcs)
        data = pdfops.images_to_pdf(stream, page_size=size, margin_mm=margin_mm, orientation=orientation)
        path = self._save(data, srcs[0], suffix="_imagenes", output=output, out_dir=out_dir)
        return self._result("from_images", [self._entry(path, len(srcs))], size_before=sum(s.size for s in srcs), pages=len(srcs), inputs=[s.label for s in srcs])

    def from_office(self, *, file: str, engine: str = "auto", output: str = "", out_dir: str = "") -> dict[str, Any]:
        src = self.resolve(file, exts=external.OFFICE_EXT)
        data, used = external.office_to_pdf(self.env, src.path, engine)
        try:
            pages: Optional[int] = pdfops.count_pages_of(data)
        except KafkaError:
            pages = None
        path = self._save(data, src, suffix="", output=output, out_dir=out_dir)
        return self._result("from_office", [self._entry(path, pages)], size_before=src.size, pages=pages, engine=used)

    def to_images(self, *, file: str, pages: str = "", format: str = "png", dpi: int = 150, quality: int = 90, password: str = "",
                  out_dir: str = "") -> dict[str, Any]:
        src, reader = self._pdf(file, password)
        total = len(reader.pages)
        chosen = parse_ranges(pages, total, default_all=True, unique=True)
        fmt = "jpg" if format.lower() in ("jpg", "jpeg") else "png"
        dpi = int(dpi)
        widest = max(max(float(reader.pages[p - 1].mediabox.width), float(reader.pages[p - 1].mediabox.height)) for p in chosen)
        if widest * dpi / 72 > 20_000:
            raise KafkaError("invalid", f"A {dpi} dpi las páginas serían de más de 20 000 píxeles: baja los dpi.")
        folder = self._folder_for_many(src, out_dir, f"{src.stem}_imagenes")
        pad = len(str(total))
        outputs: list[dict[str, Any]] = []
        for index, image in pdfops.render_pages(src.path, [p - 1 for p in chosen], dpi=dpi, password=password):
            import io
            buf = io.BytesIO()
            if fmt == "jpg":
                image.convert("RGB").save(buf, "JPEG", quality=int(quality), optimize=True, dpi=(dpi, dpi))
            else:
                image.save(buf, "PNG", optimize=True, dpi=(dpi, dpi))
            path = write_unique(folder, src.stem, f"_p{index + 1:0{pad}d}", fmt, buf.getvalue())
            self._note(path)
            outputs.append(self._entry(path))
        return self._result("to_images", outputs, size_before=src.size, pages=len(outputs), out_dir=str(folder), format=fmt, dpi=dpi)

    def images_compress(self, *, sources: list[str], limit_mb: Optional[float] = None, limit_kb: Optional[float] = None, recursive: bool = True,
                        lossless_only: bool = False, skip_small: bool = False, out_dir: str = "", time_limit_s: float = 0.0) -> dict[str, Any]:
        if limit_mb and limit_kb:
            raise KafkaError("invalid", "Indica el límite en MB o en KB, no en los dos.")
        limit = int((limit_kb * 1024) if limit_kb else (limit_mb or 0) * MB)
        if limit <= 0:
            raise KafkaError("invalid", "Falta el límite de peso.", "Por ejemplo limit_mb=5 o limit_kb=500.")
        plan: list[dict[str, Any]] = []        # {src: Path, label, dest: Path|None (folder mode), anchor: Src|None}
        folders = 0
        refs = [str(s or "").strip().strip('"').strip("'") for s in sources]
        for ref in refs:
            candidate = Path(ref).expanduser() if ref and not DOC_ID.match(ref) else None
            if candidate is not None and candidate.is_absolute() and candidate.is_dir():
                folders += 1
        folder_index = 0
        bases: list[str] = []
        for ref in refs:
            candidate = Path(ref).expanduser() if ref and not DOC_ID.match(ref) else None
            if candidate is not None and candidate.is_absolute() and candidate.is_dir():
                self._folder_check(candidate)
                folder = candidate.resolve()
                if out_dir:
                    base = self._check_out(Path(out_dir).expanduser())
                    if folders > 1:
                        base = self._check_out(base / safe_stem(folder.name, "carpeta"))
                else:
                    base = self._check_out(unique_dir(self._check_out(folder.parent), f"{safe_stem(folder.name, 'imagenes')}_comprimidas"))
                folder_index += 1
                bases.append(str(base))
                for p in imagetools.list_images(folder, recursive, skip_inside=base):
                    plan.append({"src": p, "label": str(p.relative_to(folder)), "dest": base / p.relative_to(folder), "anchor": None})
            else:
                s = self.resolve(ref, exts=IMAGE_INPUT_EXT & set(imagetools.IMAGE_EXT))
                plan.append({"src": s.path, "label": s.label, "dest": None, "anchor": s})
        if not plan:
            raise KafkaError("not_found", "No hay imágenes PNG, JPEG o WEBP que comprimir.")
        started = time.monotonic()
        items: list[dict[str, Any]] = []
        outputs: list[dict[str, Any]] = []
        before = after = 0
        counts = {"ok": 0, "skipped": 0, "failed": 0, "pending": 0}
        stopped = False
        for entry in plan:
            src: Path = entry["src"]
            size = src.stat().st_size
            item: dict[str, Any] = {"source": entry["label"], "size_before": size}
            if stopped or (time_limit_s and time.monotonic() - started > time_limit_s):
                stopped = True
                counts["pending"] += 1
                item.update(status="pending", reason="sin tiempo en esta llamada")
                items.append(item)
                continue
            dest: Optional[Path] = entry["dest"]
            if dest is not None and dest.exists():
                counts["skipped"] += 1
                item.update(status="skipped", reason="ya existe en la carpeta de salida")
                items.append(item)
                continue
            if skip_small and size <= limit:
                counts["skipped"] += 1
                item.update(status="skipped", reason="ya pesa menos del límite")
                items.append(item)
                continue
            try:
                packed = imagetools.compress_image(src, limit, lossless_only)
            except KafkaError as exc:
                counts["failed"] += 1
                item.update(status="failed", reason=exc.message)
                items.append(item)
                continue
            except Exception as exc:  # noqa: BLE001 - one bad file must not stop the batch
                counts["failed"] += 1
                item.update(status="failed", reason=f"{type(exc).__name__}")
                items.append(item)
                continue
            if not packed.ok:
                counts["failed"] += 1
                item.update(status="failed", reason=packed.note or "no cabe en el límite", strategy=packed.strategy, best_size=packed.size or None)
                items.append(item)
                continue
            data, strategy = packed.data, packed.strategy
            if size <= limit and len(data) >= size:
                data, strategy = src.read_bytes(), "copia"
            try:
                if dest is not None:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest, "xb") as handle:
                        handle.write(data)
                    written = dest
                else:
                    anchor: Src = entry["anchor"]
                    written = self._save(data, anchor, suffix="_comprimida", ext=src.suffix.lower().lstrip("."), out_dir=out_dir)
                self._note(written)
            except FileExistsError:
                counts["skipped"] += 1
                item.update(status="skipped", reason="ya existe en la carpeta de salida")
                items.append(item)
                continue
            except OSError as exc:
                counts["failed"] += 1
                item.update(status="failed", reason=f"no se pudo escribir ({type(exc).__name__})")
                items.append(item)
                continue
            counts["ok"] += 1
            before += size
            after += len(data)
            item.update(status="ok", output=str(written), size_after=len(data), strategy=strategy)
            items.append(item)
            outputs.append(self._entry(written))
        notes: list[str] = []
        if counts["pending"]:
            notes.append(f"Quedan {counts['pending']} imágenes sin procesar: repite la llamada con out_dir igual a la carpeta de salida para continuar (las ya hechas se saltan).")
        if counts["failed"]:
            notes.append(f"{counts['failed']} imágenes no han podido bajar del límite" + (" sin perder calidad." if lossless_only else "."))
        result = {"ok": True, "operation": "images_compress", "limit_bytes": limit, "limit_text": human_size(limit), "total": len(plan),
                  "compressed": counts["ok"], "skipped": counts["skipped"], "failed": counts["failed"], "pending": counts["pending"],
                  "partial": bool(counts["pending"]), "size_before": before, "size_before_text": human_size(before), "size_after": after,
                  "size_after_text": human_size(after), "saved": max(0, before - after), "saved_text": human_size(max(0, before - after)),
                  "out_dirs": bases, "outputs": outputs, "output": outputs[0]["path"] if len(outputs) == 1 else None, "items": items, "notes": notes}
        if bases:
            result["out_dir"] = bases[0]
        return result


PRESET_INDEX = {name: i for i, name in enumerate(pdfops.PRESETS)}
