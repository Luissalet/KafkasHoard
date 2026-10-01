"""faustus-plugin.json, the README and the docs stay in sync with the code."""

import json
import re
from pathlib import Path

from kafka_hoard import SERVICE, __version__
from kafka_hoard.agent_tools import TOOLS
from kafka_hoard.config import DEFAULT_PORT

ROOT = Path(__file__).resolve().parent.parent
BANNED = ("chatgpt", "claude", "openai", "anthropic", "lm studio", "odysseus", "gemini", "copilot", "notion", "evernote", "paperless")
SLOGAN_HINTS = ("tagline", "slogan")


def test_manifest_matches_code():
    manifest = json.loads((ROOT / "faustus-plugin.json").read_text(encoding="utf-8"))
    assert manifest["id"] == "kafka" and manifest["name"] == "Kafka's Hoard"
    assert manifest["app"]["health"]["expect"]["service"] == SERVICE == "kafka-hoard"
    assert manifest["defaults"]["APP_URL"].endswith(f":{DEFAULT_PORT}") and DEFAULT_PORT == 5200
    assert manifest["app"]["launch_hint"]["env"]["KAFKA_PORT"] == str(DEFAULT_PORT)


def test_every_tool_is_documented():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    api = (ROOT / "docs" / "API.md").read_text(encoding="utf-8")
    for tool in TOOLS:
        assert f"`{tool.name}`" in readme or tool.name in api, tool.name


def test_first_lines_are_short():
    for tool in TOOLS:
        assert len(tool.description.splitlines()[0]) <= 110, tool.name


def test_no_other_products_or_slogans_in_docs():
    for path in [ROOT / "README.md", ROOT / "README.es.md", ROOT / "AGENTS.md", *sorted((ROOT / "docs").glob("*.md"))]:
        text = path.read_text(encoding="utf-8").lower()
        for word in BANNED + SLOGAN_HINTS:
            assert word not in text, f"{word} in {path.name}"


def test_no_placeholder_lines_in_docs():
    for path in [ROOT / "README.md", ROOT / "README.es.md", ROOT / "AGENTS.md", *sorted((ROOT / "docs").glob("*.md"))]:
        for line in path.read_text(encoding="utf-8").splitlines():
            assert not re.search(r"\b(TODO|TBD|FIXME)\b", line) and not re.search(r"(?i)lorem ipsum", line), f"{path.name}: {line}"


def test_version_matches():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(rf'version = "{re.escape(__version__)}"', pyproject)


def test_the_api_doc_is_up_to_date():
    import subprocess
    import sys
    before = (ROOT / "docs" / "API.md").read_text(encoding="utf-8")
    out = subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_api_doc.py"), "--check"], cwd=ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr
    assert before == (ROOT / "docs" / "API.md").read_text(encoding="utf-8")
