"""Сценарий: CSV для загрузки PDF к выпуску."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ipsas.modules.issue_pdf_csv_builder import IssuePdfCsvBuilder


def execute(
    issue_url: str,
    zip_path: Path,
    output_csv_path: Path,
    extract_dir: Path,
) -> dict[str, Any]:
    return IssuePdfCsvBuilder().build_csv(
        issue_url, zip_path, output_csv_path, extract_dir
    )
