"""Compatibility shim → ``ipsas.modules.journal_xml.report.renderer``."""

from __future__ import annotations

from pathlib import Path

from ipsas.modules.journal_xml.report.renderer import (
    generate_html_content,
    generate_html_report,
)
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def generate_xml_html_report(xml_path: Path, output_path: Path) -> Path:
    """Обёртка над ``generate_html_report`` для совместимости с web-роутами."""
    if not xml_path.exists():
        raise FileNotFoundError(f"XML файл не найден: {xml_path}")
    logger.info("Генерация HTML-отчёта по XML (web-стиль): %s", xml_path.name)
    return generate_html_report(xml_path, output_path)


__all__ = [
    "generate_xml_html_report",
    "generate_html_report",
    "generate_html_content",
]
