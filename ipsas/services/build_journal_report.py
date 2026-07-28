"""Сценарий: скачиваемый HTML-отчёт по journal XML."""

from __future__ import annotations

from pathlib import Path

from ipsas.modules.journal_xml.report.renderer import generate_html_report


def execute(xml_path: Path, output_path: Path) -> Path:
    """Построить HTML-отчёт (тот же конвейер, что analyze → render)."""
    return generate_html_report(xml_path, output_path)
