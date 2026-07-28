"""Сценарий: сопоставление PDF со статьями journal XML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ipsas.modules.pdf_matching import PDFMatcher


def execute(zip_path: Path, extract_dir: Path) -> dict[str, Any]:
    return PDFMatcher().process_zip(zip_path, extract_dir)
