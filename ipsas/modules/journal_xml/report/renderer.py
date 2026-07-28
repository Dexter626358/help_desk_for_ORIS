"""Генерация скачиваемого HTML-отчёта по journal XML (web-стиль)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ipsas.modules.journal_xml.report.standalone import (
    render_web_style_html_report,
    write_web_style_html_report,
)


def generate_html_report(xml_file, output_file: Optional[Path] = None) -> Path:
    """
    Генерирует HTML-отчёт по XML в том же виде, что и web-отчёт валидатора.

    Args:
        xml_file: Путь к XML файлу
        output_file: Путь для сохранения HTML (по умолчанию рядом с XML)

    Returns:
        Path: Путь к созданному HTML файлу
    """
    from ipsas.modules.journal_xml.analyzer import analyze_journal_xml

    xml_path = Path(xml_file)
    if output_file is None:
        output_file = xml_path.parent / f"report_{xml_path.stem}.html"
    else:
        output_file = Path(output_file)

    report = analyze_journal_xml(xml_path)
    result = write_web_style_html_report(
        report,
        output_file,
        filename=xml_path.name,
    )
    logging.info("HTML отчет создан: %s", result)
    return result


def generate_html_content(
    issue_info: Dict[str, Any] | None = None,
    articles_info: List[Dict[str, Any]] | None = None,
    xml_filename: str = "",
    *,
    xml_path: Path | None = None,
    report: Dict[str, Any] | None = None,
) -> str:
    """
    Вернуть HTML-строку отчёта.

    Предпочтительные аргументы: ``xml_path`` или готовый ``report``.
    Старые позиционные аргументы (issue_info, articles_info) больше не используются.
    """
    from ipsas.modules.journal_xml.analyzer import analyze_journal_xml

    if report is None:
        if xml_path is None:
            raise NotImplementedError(
                "generate_html_content: передайте xml_path=... или report=... "
                "(старый формат issue_info/articles_info удалён)."
            )
        report = analyze_journal_xml(Path(xml_path))
        if not xml_filename:
            xml_filename = Path(xml_path).name
    if not xml_filename:
        xml_filename = str(report.get("source_file") or "journal.xml")
    return render_web_style_html_report(report, filename=xml_filename)
