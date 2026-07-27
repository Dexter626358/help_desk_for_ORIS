"""Генерация HTML-отчёта по XML в стиле web-интерфейса."""

from __future__ import annotations

from pathlib import Path

from ipsas.modules.journal_xml_analyzer import analyze_journal_xml
from ipsas.modules.journal_xml_report.web_style_report import write_web_style_html_report
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def generate_xml_html_report(xml_path: Path, output_path: Path) -> Path:
    """
    Сгенерировать HTML-отчёт по XML и сохранить его в `output_path`.

    Отчёт строится из тех же данных (`analyze_journal_xml`) и той же разметки,
    что и web-отчёт в валидаторе.
    """
    if not xml_path.exists():
        raise FileNotFoundError(f"XML файл не найден: {xml_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Генерация HTML-отчёта по XML (web-стиль): %s", xml_path.name)
    report = analyze_journal_xml(xml_path)
    return write_web_style_html_report(
        report,
        output_path,
        filename=xml_path.name,
    )
