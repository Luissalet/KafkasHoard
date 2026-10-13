"""Build Kafka's owned PDF worker from the local, incorporated source tree."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--toolchain", default="1.95.0")
    args = parser.parse_args()
    source = ROOT / "native" / "pdf"
    subprocess.run(["cargo", f"+{args.toolchain}", "build", "--locked", "--release", "--package", "kafka-pdf-worker"], cwd=source, check=True)
    suffix = ".exe" if os.name == "nt" else ""
    built = source / "target" / "release" / ("kafka-pdf-worker" + suffix)
    destination = ROOT / "native" / "bin" / built.name
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(built, destination)
    print(f"Built owned PDF worker: {destination}")


if __name__ == "__main__":
    main()
