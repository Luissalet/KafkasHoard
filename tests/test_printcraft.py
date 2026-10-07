"""PrintCraft catalogue and real CLI workflow tests (set KAFKA_PRINTCRAFT_CLI to run the integration case)."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
from pypdf import PdfReader

from kafka_hoard.agent_tools import TOOLS_BY_NAME, call_tool
from kafka_hoard.workshop.printcraft import load_manifest, printcraft_catalog_rows


def test_printcraft_catalogue_has_all_123_json_schemas_and_dispatch_entries():
    rows = printcraft_catalog_rows()
    assert len(rows) == 123
    assert len({row["native_name"] for row in rows}) == 123
    for name in ("printcraft_text_paragraphs", "printcraft_text_edit", "printcraft_page_insert_file", "printcraft_page_rotate"):
        assert name in TOOLS_BY_NAME
        assert TOOLS_BY_NAME[name].input_model["type"] == "object"
    assert "printcraft_workflow" in TOOLS_BY_NAME
    assert len(load_manifest()["tools"]) == 123


def test_printcraft_settings_accept_only_an_absolute_cli(svc, tmp_path):
    from kafka_hoard.errors import KafkaError

    with pytest.raises(KafkaError):
        svc.set_settings({"printcraft.cli": "printcraft-cli.exe"})
    fake = tmp_path / "printcraft-cli.exe"
    fake.write_bytes(b"test")
    svc.set_settings({"printcraft.cli": str(fake)})
    assert svc.setting("printcraft.cli") == str(fake)


def test_live_discovery_cache_avoids_restarting_cli_for_each_tool(svc, tmp_path):
    from types import SimpleNamespace

    fake = tmp_path / "printcraft-cli.exe"
    fake.write_bytes(b"cli")
    calls = []
    svc.printcraft.env.runner = lambda argv, timeout: calls.append((argv, timeout)) or SimpleNamespace(returncode=0, stdout='[{"name":"doc_info"}]')
    assert svc.printcraft._live_tools(fake) == [{"name": "doc_info"}]
    assert svc.printcraft._live_tools(fake) == [{"name": "doc_info"}]
    assert len(calls) == 1


def test_real_printcraft_edits_a_filed_pdf_and_returns_a_receipt(svc, tmp_path, monkeypatch):
    """Edits two synthetic PDFs, retains the original hash, checks output with pypdf (not PrintCraft)."""
    exe_value = os.environ.get("KAFKA_PRINTCRAFT_CLI", "")
    if not exe_value or not Path(exe_value).is_file():
        pytest.skip("Set KAFKA_PRINTCRAFT_CLI to the portable PrintCraft executable for the real integration test")
    exe = Path(exe_value).resolve()
    monkeypatch.setenv("KAFKA_PRINTCRAFT_CLI", str(exe))
    svc.printcraft.env.environ = os.environ
    status = svc.printcraft.status(discover=True)
    assert status["runtime_tool_count"] == 123 and status["schema_matches"] is True
    svc.db.set_setting("workshop.dir", str(tmp_path / "outputs"))

    def make_pdf(folder: Path, text: str, filename: str) -> Path:
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / filename
        steps = [
            {"tool": "doc_create", "arguments": {"from": "text", "text": text}},
            {"tool": "doc_save", "arguments": {"doc": 1, "path": str(target), "full": True}},
        ]
        svc.printcraft._run_script(exe, folder, steps)
        assert target.is_file() and target.read_bytes().startswith(b"%PDF-")
        return target

    originals = tmp_path / "synthetic-inputs"
    base = make_pdf(originals, "Original paragraph for editing.", "case-source.pdf")
    inserted = make_pdf(originals / "insert", "Inserted page from another synthetic PDF.", "inserted.pdf")
    original_bytes = base.read_bytes()
    original_sha = hashlib.sha256(original_bytes).hexdigest()
    inserted_sha = hashlib.sha256(inserted.read_bytes()).hexdigest()
    sha, _ = svc.files.put(original_bytes, "pdf")
    source_doc = svc.store.create_document(title="Synthetic PrintCraft case", kind="other", file_sha=sha, file_ext="pdf",
                                           file_name=base.name, mime="application/pdf", size=len(original_bytes), pages=1)

    result = call_tool(svc, "printcraft_workflow", {
        "doc": source_doc["id"],
        "actions": [
            {"tool": "text_paragraphs", "arguments": {"page": 1}},
            {"tool": "text_edit", "arguments": {"page": 1, "paragraph": 1, "text": "Edited paragraph from Kafka workflow."}},
            {"tool": "page_insert_file", "arguments": {"path": str(inserted), "at": 2}},
            {"tool": "page_rotate", "arguments": {"pages": [2], "degrees": 90}},
        ],
    })
    assert result["artifacts"] and result["artifacts"][0]["source_doc"] == source_doc["id"]
    output = Path(result["artifacts"][0]["path"])
    reader = PdfReader(str(output))
    assert len(reader.pages) == 2
    assert "Edited paragraph from Kafka workflow." in reader.pages[0].extract_text()
    assert "Inserted page from another synthetic PDF." in reader.pages[1].extract_text()
    assert int(reader.pages[1].get("/Rotate", 0)) % 360 == 90
    assert hashlib.sha256(base.read_bytes()).hexdigest() == original_sha
    assert hashlib.sha256(inserted.read_bytes()).hexdigest() == inserted_sha
    receipt = svc.store.printcraft_artifact(result["artifacts"][0]["artifact_id"])
    assert receipt["source_sha"] == original_sha
    assert receipt["output_sha"] == hashlib.sha256(output.read_bytes()).hexdigest()
    assert svc.workshop.is_servable(output)
    follow_up = svc.printcraft.workflow(receipt["artifact_id"], [
        {"tool": "page_rotate", "arguments": {"pages": [1], "degrees": 180}},
    ])
    child = svc.store.printcraft_artifact(follow_up["artifacts"][0]["artifact_id"])
    assert child["parent_artifact_id"] == receipt["artifact_id"]
    assert child["source_sha"] == receipt["output_sha"]
