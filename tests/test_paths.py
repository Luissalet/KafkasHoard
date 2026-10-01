"""Which folders and files Kafka may read."""

from pathlib import Path

import pytest

from kafka_hoard import paths


@pytest.fixture
def data(tmp_path):
    d = tmp_path / "data"
    (d / "inbox").mkdir(parents=True)
    (d / "files").mkdir()
    return d


def test_ordinary_folders_are_fine(tmp_path, data):
    f = tmp_path / "papeles"
    f.mkdir()
    assert paths.unsafe_folder(f, data) == ""


def test_roots_profile_system_and_missing_folders_are_refused(tmp_path, data):
    for bad in ("/", Path.home(), "/etc", "/usr/share", tmp_path / "nope", "relative/path"):
        assert paths.unsafe_folder(bad, data), bad


def test_own_data_dir_is_off_limits_except_the_inbox(data):
    assert paths.unsafe_folder(data, data)
    assert paths.unsafe_folder(data / "files", data)
    assert paths.unsafe_folder(data / "inbox", data) == ""


def test_hidden_and_config_folders_are_refused(tmp_path, data):
    for name in (".ssh", ".git", ".config"):
        f = tmp_path / name
        f.mkdir()
        assert paths.unsafe_folder(f, data), name


def test_credential_like_files_are_refused(tmp_path, data):
    for name in (".env", ".env.local", "mcp-token", "id_rsa", "cert.pem", "server.key", "vault.kdbx", "credentials.json", "secrets.txt"):
        f = tmp_path / name
        f.write_text("x", encoding="utf-8")
        assert paths.unsafe_file(f, data), name
    ok = tmp_path / "factura.pdf"
    ok.write_bytes(b"%PDF")
    assert paths.unsafe_file(ok, data) == ""


def test_files_in_the_data_dir_are_off_limits_except_the_inbox(data):
    inside = data / "kafka.db"
    inside.write_bytes(b"x")
    assert paths.unsafe_file(inside, data)
    drop = data / "inbox" / "x.pdf"
    drop.write_bytes(b"%PDF")
    assert paths.unsafe_file(drop, data) == ""


def test_relative_and_missing_files_are_refused(tmp_path, data):
    assert paths.unsafe_file("a.pdf", data) and paths.unsafe_file(tmp_path / "none.pdf", data)


def test_the_system_temp_folder_counts_even_under_appdata(tmp_path, monkeypatch):
    import tempfile as _tempfile

    temp = tmp_path / "AppData" / "Local" / "Temp"
    (temp / "papeles").mkdir(parents=True)
    (tmp_path / "AppData" / "Roaming" / "app").mkdir(parents=True)
    monkeypatch.setattr(_tempfile, "gettempdir", lambda: str(temp))
    assert paths.unsafe_folder(temp / "papeles") == ""
    assert paths.unsafe_folder(tmp_path / "AppData" / "Roaming" / "app") != ""
