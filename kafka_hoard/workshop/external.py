"""Programs outside Python: Ghostscript (PDF compression) and Word / LibreOffice (documents to PDF). Everything runs through
``Env.runner`` with a timeout and no console window; temporary files live in an ASCII-named temp folder, so accents, spaces and
apostrophes in the user's paths never reach the command line."""

from __future__ import annotations

import base64
import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional

from ..errors import KafkaError
from .proc import Env

OFFICE_EXT = {"docx", "doc", "docm", "dotx", "dot", "odt", "rtf"}
GS_TIMEOUT_S = 160  # under the assistant's 180 s call limit (the bridge waits 175 s)
OFFICE_TIMEOUT_S = 160

WORD_SCRIPT = """$ErrorActionPreference = 'Stop'
$word = $null
$doc = $null
# Word started through COM (/Automation) sometimes stays alive after Quit: remember which ones are ours and end them.
function Get-AutomationWord { @(Get-CimInstance Win32_Process -Filter "Name='WINWORD.EXE'" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*/Automation*' } | ForEach-Object { [int]$_.ProcessId }) }
$before = Get-AutomationWord
try {
  $word = New-Object -ComObject Word.Application
  $word.Visible = $false
  $word.DisplayAlerts = 0
  $doc = $word.Documents.Open($env:KAFKA_SRC, $false, $true)
  $doc.SaveAs2($env:KAFKA_DST, 17)
  $doc.Close(0)
  $doc = $null
} finally {
  if ($doc -ne $null) { try { $doc.Close(0) } catch {} }
  if ($word -ne $null) {
    try { $word.Quit(0) } catch {}
    try { [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($word) } catch {}
  }
  $word = $null
  [GC]::Collect(); [GC]::WaitForPendingFinalizers()
  Start-Sleep -Milliseconds 700
  foreach ($id in (Get-AutomationWord)) { if ($before -notcontains $id) { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue } }
}
"""


def _tail(text: Any, limit: int = 300) -> str:
    return " ".join(str(text or "").split())[-limit:]


# ------------------------------------------------------------------ Ghostscript
def ghostscript_compress(env: Env, gs: str, src: Path, preset: str) -> bytes:
    """Run ``gs -sDEVICE=pdfwrite -dPDFSETTINGS=/<preset>`` on a copy of ``src`` and return the new PDF bytes."""
    with tempfile.TemporaryDirectory(prefix="kafka-gs-") as tmp:
        work = Path(tmp)
        source = work / "in.pdf"
        target = work / "out.pdf"
        shutil.copyfile(src, source)
        cmd = [gs, "-q", "-dQUIET", "-dNOPAUSE", "-dBATCH", "-dSAFER", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.5", f"-dPDFSETTINGS=/{preset}",
               "-dDetectDuplicateImages=true", "-dCompressFonts=true", f"-sOutputFile={target}", str(source)]
        try:
            done = env.runner(cmd, timeout=GS_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 - timeout, missing binary
            raise KafkaError("failed", f"Ghostscript no ha terminado ({type(exc).__name__}).", status=500) from exc
        if getattr(done, "returncode", 1) != 0 or not target.is_file():
            raise KafkaError("failed", f"Ghostscript falló: {_tail(getattr(done, 'stderr', '')) or 'sin mensaje'}.", status=500)
        return target.read_bytes()


# ------------------------------------------------------------------ documents -> PDF
def word_available(env: Env) -> bool:
    """Microsoft Word is registered as a COM server (Windows only)."""
    if not env.windows or not env.powershell():
        return False
    try:
        done = env.runner(["reg", "query", r"HKCR\Word.Application\CLSID"], timeout=15)
    except Exception:  # noqa: BLE001
        return False
    return getattr(done, "returncode", 1) == 0


def _word_convert(env: Env, src: Path, work: Path) -> Path:
    shell = env.powershell()
    if not shell:
        raise KafkaError("not_configured", "No encuentro PowerShell para llamar a Word.")
    target = work / "out.pdf"
    encoded = base64.b64encode(WORD_SCRIPT.encode("utf-16-le")).decode("ascii")
    child_env = dict(env.environ)
    child_env["KAFKA_SRC"] = str(src)
    child_env["KAFKA_DST"] = str(target)
    cmd = [shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-EncodedCommand", encoded]
    try:
        done = env.runner(cmd, timeout=OFFICE_TIMEOUT_S, env=child_env)
    except Exception as exc:  # noqa: BLE001
        raise KafkaError("failed", f"Word no ha terminado a tiempo ({type(exc).__name__}).", status=500) from exc
    if getattr(done, "returncode", 1) != 0 or not target.is_file():
        raise KafkaError("failed", f"Word no ha podido convertirlo: {_tail(getattr(done, 'stderr', '') or getattr(done, 'stdout', '')) or 'sin mensaje'}.", status=500)
    return target


def _libreoffice_convert(env: Env, soffice: str, src: Path, work: Path) -> Path:
    outdir = work / "lo-out"
    outdir.mkdir()
    profile = (work / "lo-profile").as_uri()
    cmd = [soffice, f"-env:UserInstallation={profile}", "--headless", "--nologo", "--nolockcheck", "--norestore", "--convert-to", "pdf",
           "--outdir", str(outdir), str(src)]
    try:
        done = env.runner(cmd, timeout=OFFICE_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001
        raise KafkaError("failed", f"LibreOffice no ha terminado a tiempo ({type(exc).__name__}).", status=500) from exc
    produced = outdir / (src.stem + ".pdf")
    if getattr(done, "returncode", 1) != 0 or not produced.is_file():
        raise KafkaError("failed", f"LibreOffice no ha podido convertirlo: {_tail(getattr(done, 'stderr', '') or getattr(done, 'stdout', '')) or 'sin mensaje'}.", status=500)
    return produced


def office_to_pdf(env: Env, src: Path, engine: str = "auto") -> tuple[bytes, str]:
    """Convert a Word/OpenDocument/RTF file to PDF bytes. -> (bytes, engine used: «word» or «libreoffice»).

    ``engine``: auto (Word on Windows when it is installed, else LibreOffice), word, libreoffice."""
    soffice = env.soffice()
    word = word_available(env)
    if engine == "word" and not word:
        raise KafkaError("not_configured", "Microsoft Word no está disponible en este equipo.", "Usa engine=auto o instala Word (solo Windows).")
    if engine == "libreoffice" and not soffice:
        raise KafkaError("not_configured", "LibreOffice no está instalado (no encuentro soffice).", "Instálalo desde libreoffice.org o usa engine=auto.")
    if not word and not soffice:
        raise KafkaError("not_configured", "No hay Microsoft Word ni LibreOffice instalados, así que no puedo convertir documentos a PDF.",
                         "Instala LibreOffice (gratuito) o Microsoft Word y vuelve a intentarlo.")
    order: list[str] = []
    if engine == "word" or (engine == "auto" and word):
        order.append("word")
    if engine == "libreoffice" or (engine == "auto" and soffice):
        order.append("libreoffice")
    errors: list[str] = []
    for which in order:
        with tempfile.TemporaryDirectory(prefix="kafka-office-") as tmp:
            work = Path(tmp)
            copy = work / f"in{src.suffix.lower()}"
            shutil.copyfile(src, copy)
            try:
                produced = _word_convert(env, copy, work) if which == "word" else _libreoffice_convert(env, soffice or "", copy, work)
                return produced.read_bytes(), which
            except KafkaError as exc:
                errors.append(exc.message)
    raise KafkaError("failed", " ".join(errors) or "No se ha podido convertir el documento.", "Comprueba que el fichero abre en Word o LibreOffice.", status=500)


def detect(env: Env) -> dict[str, Any]:
    return {"ghostscript": env.ghostscript(), "libreoffice": env.soffice(), "word": word_available(env)}
