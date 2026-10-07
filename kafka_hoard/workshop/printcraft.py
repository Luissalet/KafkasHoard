"""PrintCraft's headless PDF engine, run in a per-call sandbox.

Kafka keeps each original in its content-addressed store. PrintCraft receives a
temporary copy under ``workshop/`` and writes its result to a new file. A receipt
links every result back to the Kafka document (and, for follow-up edits, to the
previous receipt).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .. import paths
from ..errors import KafkaError
from ..files import sha256_file
from . import external
from .names import safe_stem, write_unique

_MANIFEST_PATH = Path(__file__).with_name("printcraft-tools-0.2.1.json")
_MAX_FILE_BYTES = 200 * 1024 * 1024
_TIMEOUT_S = 160
_DOC_REF_RE = re.compile(r"^(?:d_[0-9a-z]{6,}|pc_[0-9a-f]{12})$")
_OUTPUT_KEYS = {"out", "out_dir", "folder"}
_INPUT_FILE_TOOLS = {
    "doc_open": {"path"}, "page_insert_file": {"path"}, "form_set_image": {"path"},
    "page_add_image": {"path"}, "doc_watermark": {"file"}, "doc_background": {"file"},
    "page_replace": {"path"}, "comment_add": {"path"}, "form_merge_data": {"path", "paths"},
    "action_run": {"paths"}, "ocr_recognize_files": {"paths"}, "doc_create": {"path", "paths"},
    "stamp_custom": {"path"}, "doc_import_data": {"path"},
}
def load_manifest() -> dict[str, Any]:
    try:
        value = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"PrintCraft's pinned tool manifest is unavailable: {type(exc).__name__}") from exc
    tools = value.get("tools") if isinstance(value, dict) else None
    if not isinstance(tools, list) or len(tools) != 123:
        raise RuntimeError("PrintCraft 0.2.1 tool manifest must contain exactly 123 tools")
    names = [str(tool.get("name") or "") for tool in tools if isinstance(tool, dict)]
    if len(names) != 123 or len(set(names)) != 123:
        raise RuntimeError("PrintCraft tool manifest has missing or duplicate names")
    return value


def _is_file_ref(value: Any) -> bool:
    return isinstance(value, str) and (value.startswith(("d_", "pc_")) or Path(value).is_absolute())


class PrintCraft:
    """Kafka's adapter for the PrintCraft CLI and the artifact receipt store."""

    def __init__(self, config: Any, store: Any, files: Any, setting: Callable[[str], str], workshop: Any,
                 *, env: Optional[Any] = None):
        self.config = config
        self.store = store
        self.files = files
        self.setting = setting
        self.workshop = workshop
        self.env = env or external.Env()
        self.manifest = load_manifest()
        self.native_tools = {row["name"]: row for row in self.manifest["tools"]}
        self._live_cache_key: Optional[tuple[str, int, int]] = None
        self._live_cache: list[dict[str, Any]] = []

    @property
    def root(self) -> Path:
        return Path(self.config.data_dir) / "workshop" / "printcraft"

    def executable(self) -> Optional[Path]:
        configured = (self.setting("printcraft.cli") or "").strip() or str(self.env.environ.get("KAFKA_PRINTCRAFT_CLI", "")).strip()
        if configured:
            candidate = Path(configured).expanduser()
            return candidate.resolve() if candidate.is_file() else None
        found = self.env.find(("printcraft-cli", "printcraft-cli.exe"))
        if found:
            return Path(found).resolve()
        app_root = Path(__file__).resolve().parents[2]
        patterns = (
            str(app_root / "native-craft-bundles" / "printcraft-*-windows-x64-portable" / "printcraft-cli.exe"),
            str(app_root / "tools" / "printcraft-*-windows-x64-portable" / "printcraft-cli.exe"),
            str(app_root / "vendor" / "printcraft" / "printcraft-cli.exe"),
        )
        for pattern in patterns:
            matches = sorted(Path(p) for p in __import__("glob").glob(pattern))
            if matches:
                return matches[-1]
        return None

    def status(self, *, discover: bool = False) -> dict[str, Any]:
        exe = self.executable()
        out: dict[str, Any] = {
            "engine": self.manifest["engine"], "schema_release": self.manifest["release"],
            "tool_count": len(self.native_tools), "configured": bool(exe), "available": bool(exe),
            "path": str(exe) if exe else "", "source": "printcraft.cli setting, KAFKA_PRINTCRAFT_CLI, PATH, or portable folder",
        }
        if exe and discover:
            live = self._live_tools(exe)
            out["runtime_tool_count"] = len(live)
            out["runtime_tools"] = [tool["name"] for tool in live]
            by_name = {row["name"]: row for row in live}
            out["schema_matches"] = (
                set(by_name) == set(self.native_tools)
                and all(by_name[name].get("input_schema") == spec.get("input_schema") for name, spec in self.native_tools.items())
            )
        elif not exe:
            out["available"] = False
            out["hint"] = "Set printcraft.cli in Ajustes or KAFKA_PRINTCRAFT_CLI to the portable printcraft-cli executable."
        return out

    def _live_tools(self, exe: Path, *, refresh: bool = False) -> list[dict[str, Any]]:
        try:
            stat = exe.stat()
            cache_key = (str(exe.resolve()), stat.st_mtime_ns, stat.st_size)
        except OSError:
            cache_key = (str(exe), 0, 0)
        if not refresh and self._live_cache_key == cache_key and self._live_cache:
            return self._live_cache
        try:
            done = self.env.runner([str(exe), "tools"], timeout=30)
        except Exception as exc:  # noqa: BLE001 - CLI may be missing, incompatible, or timed out.
            raise KafkaError("unavailable", f"PrintCraft no responde ({type(exc).__name__}).", hint="Revisa printcraft.cli en Ajustes.") from exc
        if getattr(done, "returncode", 1) != 0:
            raise KafkaError("failed", "PrintCraft no pudo enumerar sus herramientas.", hint="Revisa la versión del CLI configurado.")
        try:
            value = json.loads(getattr(done, "stdout", ""))
        except (TypeError, ValueError) as exc:
            raise KafkaError("failed", "PrintCraft devolvió un catálogo que no es JSON.") from exc
        if not isinstance(value, list) or not all(isinstance(row, dict) and row.get("name") for row in value):
            raise KafkaError("failed", "El catálogo de PrintCraft no tiene el formato esperado.")
        self._live_cache_key, self._live_cache = cache_key, value
        return value

    @staticmethod
    def tool_schema(native_name: str, tool: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        tool = tool or next(row for row in load_manifest()["tools"] if row["name"] == native_name)
        schema = json.loads(json.dumps(tool["input_schema"]))
        properties = schema.setdefault("properties", {})
        required = schema.setdefault("required", [])
        if "doc" in properties:
            properties["doc"] = {
                "type": "string", "pattern": _DOC_REF_RE.pattern,
                "description": "Kafka document id (d_…) or a prior PrintCraft artifact id (pc_…).",
            }
        else:
            properties["doc"] = {
                "type": "string", "pattern": _DOC_REF_RE.pattern,
                "description": "Optional Kafka document id (d_…) or prior PrintCraft artifact id (pc_…).",
            }
        if native_name == "doc_open":
            required[:] = [key for key in required if key != "path"]
            properties["path"]["description"] = "Absolute path to a PDF, or use doc for a filed Kafka document."
        schema["additionalProperties"] = False
        return schema

    def _case_path(self, ref: str) -> tuple[Path, Optional[dict[str, Any]], str]:
        if ref.startswith("pc_"):
            receipt = self.store.printcraft_artifact(ref)
            target = Path(receipt["path"]).resolve()
            if not target.is_file():
                raise KafkaError("not_found", "No encuentro el PDF de esa revisión PrintCraft.")
            if target.suffix.lower() != ".pdf":
                raise KafkaError("unsupported", "Esa revisión no es PDF y no se puede abrir como documento.")
            source = self.store.find_document(receipt.get("source_doc_id") or "")
            return target, source, receipt.get("artifact_id") or ""
        doc = self.store.document(ref)
        if doc.get("file_ext", "").lower() != "pdf":
            raise KafkaError("unsupported", "PrintCraft necesita un PDF archivado en Kafka.")
        if not doc.get("file_sha"):
            raise KafkaError("not_found", "El documento no tiene un PDF original disponible.")
        target = self.files.path_of(doc["file_sha"], doc["file_ext"])
        if not target.is_file():
            raise KafkaError("not_found", "No encuentro el PDF original archivado en Kafka.")
        return target, doc, ""

    def _copy_input(self, value: str, work: Path, index: int) -> str:
        if value.startswith(("d_", "pc_")):
            source, _, _ = self._case_path(value)
        else:
            if not Path(value).is_absolute():
                raise KafkaError("invalid", "Las rutas de PrintCraft deben ser absolutas.")
            reason = paths.unsafe_file(value, self.config.data_dir)
            if reason:
                raise KafkaError("forbidden", f"No puedo leer la ruta de PrintCraft: {reason}.")
            source = Path(value).expanduser().resolve()
        if not source.is_file():
            raise KafkaError("not_found", f"No existe el fichero {source.name}.")
        if source.stat().st_size > _MAX_FILE_BYTES:
            raise KafkaError("too_large", f"{source.name} supera los 200 MB.")
        suffix = source.suffix.lower()[:12] or ".bin"
        copied = work / f"input-{index:03d}{suffix}"
        shutil.copyfile(source, copied)
        return str(copied)

    def _sandbox_args(self, native_name: str, args: dict[str, Any], work: Path) -> dict[str, Any]:
        """Copy file inputs into the CLI root and pin all output paths inside it."""
        result = dict(args)
        inputs = _INPUT_FILE_TOOLS.get(native_name, set())
        index = 0
        for key, value in list(result.items()):
            if key in {"doc", "password", "owner_password", "current_password"}:
                continue
            if key in _OUTPUT_KEYS:
                if key == "folder" and native_name not in {"doc_export_images", "doc_export_all_images", "ocr_recognize_files"}:
                    continue
                if key == "out":
                    suffix = Path(str(value or "result.pdf")).suffix or ".pdf"
                    result[key] = str(work / f"result{suffix.lower()}")
                else:
                    result[key] = str(work / "out")
                    Path(result[key]).mkdir(parents=True, exist_ok=True)
                continue
            if key not in inputs and key not in {"paths", "images", "sources", "file"}:
                continue
            if isinstance(value, str):
                if _is_file_ref(value):
                    result[key] = self._copy_input(value, work, index)
                    index += 1
            elif isinstance(value, list) and key in {"paths", "images", "sources"}:
                copied = []
                for item in value:
                    if not isinstance(item, str) or not _is_file_ref(item):
                        copied.append(item)
                        continue
                    copied.append(self._copy_input(item, work, index))
                    index += 1
                result[key] = copied
        # Most tools that accept a path use it as a source. A non-existing
        # explicit path is treated as an output and kept inside the sandbox.
        path = result.get("path")
        if native_name == "doc_save":
            result["path"] = str(work / "result.pdf")
            return result
        if isinstance(path, str) and path and "path" not in inputs and not path.startswith(("d_", "pc_")):
            suffix = Path(path).suffix or ".pdf"
            result["path"] = str(work / (safe_stem(Path(path).stem, "result") + suffix.lower()))
            return result
        if isinstance(path, str) and path and not Path(path).is_file() and not path.startswith(("d_", "pc_")):
            if native_name in {"doc_save", "sign_id_create", "doc_export_office", "doc_export_text", "sign_document", "accessibility_report", "doc_compare_report"}:
                result["path"] = str(work / (safe_stem(Path(path).stem, "result") + (Path(path).suffix or ".pdf")))
        return result

    def _run_script(self, exe: Path, work: Path, steps: Sequence[dict[str, Any]]) -> tuple[list[Any], Any]:
        script = work / "calls.json"
        script.write_text(json.dumps([{"tool": step["tool"], "args": step.get("arguments", {})} for step in steps], ensure_ascii=False), encoding="utf-8")
        try:
            done = self.env.runner([str(exe), "run", "--script", str(script), "--root", str(work)], timeout=_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001
            raise KafkaError("failed", f"PrintCraft no terminó ({type(exc).__name__}).", status=500) from exc
        stdout = str(getattr(done, "stdout", "") or "")
        results: list[Any] = []
        decoder = json.JSONDecoder()
        offset = 0
        while offset < len(stdout):
            while offset < len(stdout) and stdout[offset].isspace():
                offset += 1
            if offset >= len(stdout):
                break
            try:
                item, offset = decoder.raw_decode(stdout, offset)
            except ValueError:
                break
            results.append(item)
        if getattr(done, "returncode", 1) != 0:
            tail = " ".join(str(getattr(done, "stderr", "") or stdout).split())[-500:]
            raise KafkaError("failed", f"PrintCraft falló: {tail or 'sin mensaje'}", status=500)
        if not results:
            tail = " ".join(str(getattr(done, "stderr", "") or stdout).split())[-500:]
            raise KafkaError("failed", f"PrintCraft no devolvió un resultado utilizable: {tail or 'sin mensaje'}", status=500)
        return results, done

    @staticmethod
    def _rewrite_paths(value: Any, mapping: Mapping[str, str]) -> Any:
        if isinstance(value, str):
            for old, new in mapping.items():
                value = value.replace(old, new)
            return value
        if isinstance(value, list):
            return [PrintCraft._rewrite_paths(item, mapping) for item in value]
        if isinstance(value, dict):
            return {key: PrintCraft._rewrite_paths(item, mapping) for key, item in value.items()}
        return value

    def call(self, native_name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        if native_name not in self.native_tools:
            raise KafkaError("not_found", f"No existe la herramienta PrintCraft {native_name}.")
        exe = self.executable()
        if exe is None:
            raise KafkaError("not_configured", "No encuentro printcraft-cli.", hint="Configúralo en Ajustes → Taller de PDF → PrintCraft CLI.")
        # The live CLI is the final authority for version/schema compatibility.
        live_names = {row["name"] for row in self._live_tools(exe)}
        if native_name not in live_names:
            raise KafkaError("unsupported", f"El PrintCraft configurado no ofrece la herramienta {native_name}.",
                             hint=f"El catálogo integrado corresponde a PrintCraft {self.manifest['release']}.")
        return self._run_actions(exe, native_name, dict(arguments), [])

    def workflow(self, document: str, actions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        if not actions or len(actions) > 20:
            raise KafkaError("invalid", "El flujo necesita de 1 a 20 operaciones PrintCraft.")
        exe = self.executable()
        if exe is None:
            raise KafkaError("not_configured", "No encuentro printcraft-cli.", hint="Configúralo en Ajustes → Taller de PDF → PrintCraft CLI.")
        live_names = {row["name"] for row in self._live_tools(exe)}
        checked = []
        for item in actions:
            native_name = str(item.get("tool") or "")
            if native_name not in self.native_tools or native_name not in live_names:
                raise KafkaError("invalid", f"La operación {native_name!r} no está en PrintCraft.")
            spec = self.native_tools[native_name]
            if "doc" not in (spec.get("input_schema", {}).get("properties") or {}) or native_name in {"doc_open", "doc_close", "doc_save"}:
                raise KafkaError("invalid", f"{native_name} no es una operación de edición sobre un documento abierto.")
            args = item.get("arguments") or {}
            if not isinstance(args, Mapping):
                raise KafkaError("invalid", f"Los argumentos de {native_name} deben ser un objeto.")
            checked.append((native_name, dict(args)))
        return self._run_actions(exe, "workflow", {"doc": document}, checked)

    def _run_actions(self, exe: Path, native_name: str, arguments: dict[str, Any], actions: Sequence[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
        ref = str(arguments.pop("doc", "") or "").strip()
        source_doc: Optional[dict[str, Any]] = None
        parent_artifact = ""
        source_path: Optional[Path] = None
        input_path = arguments.get("path") if native_name == "doc_open" and not ref else None
        if input_path:
            if not Path(str(input_path)).is_absolute():
                raise KafkaError("invalid", "Las rutas de PrintCraft deben ser absolutas.")
            unsafe = paths.unsafe_file(str(input_path), self.config.data_dir)
            if unsafe:
                raise KafkaError("forbidden", f"No puedo leer la ruta de PrintCraft: {unsafe}.")
            source_path = Path(str(input_path)).expanduser().resolve()
        if ref:
            source_path, source_doc, parent_artifact = self._case_path(ref)
        self.root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="run-", dir=self.root) as temp:
            work = Path(temp)
            steps: list[dict[str, Any]] = []
            has_doc = bool(source_path and source_path.is_file())
            if has_doc and native_name != "doc_open":
                primary = work / ("source.pdf" if source_path.suffix.lower() == ".pdf" else "source" + source_path.suffix.lower())
                if source_path.stat().st_size > _MAX_FILE_BYTES:
                    raise KafkaError("too_large", "El PDF supera los 200 MB.")
                shutil.copyfile(source_path, primary)
                steps.append({"tool": "doc_open", "arguments": {"path": str(primary)}})
            work_args = self._sandbox_args(native_name, arguments, work) if native_name in self.native_tools else arguments
            if native_name == "doc_open" and ref:
                primary = work / "source.pdf"
                shutil.copyfile(source_path, primary)
                work_args["path"] = str(primary)
                has_doc = True
            if native_name in self.native_tools and has_doc and "doc" in (self.native_tools[native_name].get("input_schema", {}).get("properties") or {}):
                work_args["doc"] = 1
            if native_name in self.native_tools:
                steps.append({"tool": native_name, "arguments": work_args})
            if actions:
                for action_name, action_args in actions:
                    converted = self._sandbox_args(action_name, action_args, work)
                    converted["doc"] = 1
                    steps.append({"tool": action_name, "arguments": converted})
            before = {p.resolve() for p in work.rglob("*") if p.is_file()}
            # Case-bound workflows always edit the document opened as id 1.
            # Save in the same CLI process/script so its in-memory document is still live.
            edits_document = native_name == "doc_create" or (native_name == "doc_open" and not ref)
            if native_name in self.native_tools and native_name != "doc_save":
                edits_document = edits_document or not bool(self.native_tools[native_name].get("read_only"))
            edits_document = edits_document or any(not self.native_tools[name].get("read_only") for name, _ in actions)
            if (has_doc or native_name == "doc_create") and edits_document:
                steps.append({"tool": "doc_save", "arguments": {"doc": 1, "path": str(work / "result.pdf"), "full": True}})
            results, _ = self._run_script(exe, work, steps)
            last_result = results[-1] if results else {}
            # Export tools that write a PDF directly may not leave a dirty open
            # document. Their output stays under --root and is still receipted.
            changed = {p.resolve() for p in work.rglob("*") if p.is_file()} - before
            if (work / "result.pdf").is_file():
                changed.add((work / "result.pdf").resolve())
            artifacts = self._persist_candidates(changed, source_doc, parent_artifact, source_path, native_name)
            path_map = {item.pop("temporary_path"): item["path"] for item in artifacts}
            results = self._rewrite_paths(results, path_map)
            last_result = results[-1] if results else {}
            return {"tool": native_name, "engine": f"PrintCraft {self.manifest['release']}", "source_doc": (source_doc or {}).get("id"),
                    "result": last_result, "artifacts": artifacts, "artifact_ids": [item["artifact_id"] for item in artifacts],
                    "artifact_paths": [item["path"] for item in artifacts], "calls": len(steps),
                    "note": "Edits were saved to a new file. Kafka's archived original was preserved."}

    def _persist_candidates(self, candidates: set[Path], source_doc: Optional[dict[str, Any]], parent: str,
                            source_path: Optional[Path], native_name: str) -> list[dict[str, Any]]:
        out_dir = self.workshop._check_out(self.workshop.default_dir())
        source_sha = sha256_file(source_path) if source_path and source_path.is_file() else ""
        entries = []
        for candidate in sorted(candidates):
            if not candidate.is_file() or candidate.name in {"source.pdf", "calls.json"}:
                continue
            if candidate.stat().st_size > _MAX_FILE_BYTES:
                continue
            data = candidate.read_bytes()
            ext = candidate.suffix.lower().lstrip(".") or "bin"
            name = write_unique(out_dir, safe_stem(candidate.stem, "documento"), "_printcraft", ext, data)
            self.workshop._note(name)
            output_sha = hashlib.sha256(data).hexdigest()
            artifact_id = "pc_" + os.urandom(6).hex()
            row = self.store.create_printcraft_artifact(
                artifact_id=artifact_id, source_doc_id=(source_doc or {}).get("id"), parent_artifact_id=parent,
                path=str(name.resolve()), source_sha=source_sha, output_sha=output_sha,
                engine_release=str(self.manifest["release"]), operation=native_name,
            )
            entries.append({"artifact_id": artifact_id, "source_doc": row.get("source_doc_id"), "parent_artifact": parent or None,
                            "path": row["path"], "source_sha256": source_sha, "output_sha256": output_sha,
                            "bytes": len(data), "extension": ext, "engine": f"PrintCraft {self.manifest['release']}",
                            "operation": native_name, "temporary_path": str(candidate.resolve())})
        return entries

    def artifacts(self, ref: str) -> dict[str, Any]:
        source_doc = ref
        if ref.startswith("pc_"):
            receipt = self.store.printcraft_artifact(ref)
            source_doc = str(receipt.get("source_doc_id") or "")
        rows = self.store.printcraft_artifacts(source_doc)
        return {"source_doc": source_doc or None, "count": len(rows), "artifacts": rows}


def printcraft_catalog_rows() -> list[dict[str, Any]]:
    """The exact pinned 123-tool API surface used by REST, the UI and Kafka's MCP bridge."""
    manifest = load_manifest()
    rows = []
    for native in manifest["tools"]:
        name = native["name"]
        first = f"PrintCraft {name} on a PDF. Operación PDF {name}."
        if len(first) > 110:
            first = f"PrintCraft {name}. Operación PDF {name}."
        detail = str(native.get("description") or "")
        schema = PrintCraft.tool_schema(name, native)
        rows.append({"native_name": name, "name": f"printcraft_{name}", "description": first + ("\n" + detail if detail else ""),
                     "input_schema": schema, "read_only": bool(native.get("read_only"))})
    return rows
