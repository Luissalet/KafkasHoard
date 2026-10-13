from __future__ import annotations

import json
from pathlib import Path

import pytest

from kafka_hoard.agent_tools import TOOLS_BY_NAME, call_tool
from kafka_hoard.workshop.printcraft import load_manifest


def test_all_own_names_and_historical_aliases_share_schemas(svc):
    for row in load_manifest()["tools"]:
        name = row["name"]
        own = TOOLS_BY_NAME["pdf_" + name]
        old = TOOLS_BY_NAME["printcraft_" + name]
        assert own.input_model == old.input_model
        assert own.annotations == old.annotations
    own_catalog = call_tool(svc, "pdf_catalog", {})
    assert own_catalog["engine"] == "Kafka PDF" and own_catalog["count"] == 123
    assert all(row["name"].startswith("pdf_") for row in own_catalog["tools"])


def test_worker_is_selected_even_with_missing_obsolete_paths(svc, tmp_path, monkeypatch):
    missing = str(tmp_path / "absent-printcraft-cli.exe")
    svc.set_settings({"printcraft.cli": missing})
    monkeypatch.setenv("KAFKA_PRINTCRAFT_CLI", missing)
    monkeypatch.setattr(svc.printcraft.env, "find", lambda *args: pytest.fail("External executable search"))
    result = call_tool(svc, "pdf_engine_status", {"discover": True})
    assert result["available"] and result["runtime_tool_count"] == 123
    assert result["schema_matches"] and result["source"] == "incorporated local source"
    assert Path(result["path"]).name.startswith("kafka-pdf-worker")


def test_owned_image_render_has_a_real_png_receipt(svc, tmp_path):
    from PIL import Image

    svc.set_settings({"workshop.dir": str(tmp_path / "outputs")})
    exe = svc.printcraft.executable()
    assert exe is not None
    work = tmp_path / "document"
    work.mkdir()
    source = work / "source.pdf"
    svc.printcraft._run_script(exe, work, [
        {"tool": "doc_create", "arguments": {"from": "text", "text": "CPU rendering proof"}},
        {"tool": "doc_save", "arguments": {"doc": 1, "path": str(source), "full": True}},
    ])
    data = source.read_bytes()
    sha, _ = svc.files.put(data, "pdf")
    document = svc.store.create_document(title="Synthetic render", kind="other", file_sha=sha, file_ext="pdf", file_name=source.name, mime="application/pdf", size=len(data), pages=1)
    result = call_tool(svc, "pdf_page_render", {"doc": document["id"], "page": 1, "dpi": 72})
    assert result["engine"] == "Kafka PDF"
    image = next(item for item in result["artifacts"] if item["extension"] == "png")
    with Image.open(image["path"]) as pixels:
        assert pixels.width > 100 and pixels.height > 100
    assert svc.files.path_of(sha, "pdf").read_bytes() == data


def test_worker_rejects_wrong_batch_without_creating_outputs(svc, tmp_path):
    import subprocess

    request = tmp_path / "request.json"
    request.write_text(json.dumps({"format": "foreign-batch", "operations": []}), encoding="utf-8")
    before = set(tmp_path.iterdir())
    completed = subprocess.run([str(svc.printcraft.executable()), "execute", str(request)], capture_output=True)
    assert completed.returncode != 0
    assert b"Expected kafka-pdf-batch/v1" in completed.stderr
    assert set(tmp_path.iterdir()) == before
