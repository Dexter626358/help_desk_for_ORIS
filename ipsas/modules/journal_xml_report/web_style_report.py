"""Standalone HTML-отчёт в стиле web-интерфейса (на базе analyze_journal_xml)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "web" / "templates"


def _jinja_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )


def render_web_style_html_report(
    report: dict[str, Any],
    *,
    filename: str,
) -> str:
    """Собрать self-contained HTML из тех же данных/разметки, что и web-отчёт."""
    template = _jinja_env().get_template("xml_report_download.html")
    return template.render(
        report=report,
        filename=filename,
        download_mode=True,
        xml_filename=None,
    )


def write_web_style_html_report(
    report: dict[str, Any],
    output_path: Path,
    *,
    filename: str,
) -> Path:
    """Сохранить HTML-отчёт на диск."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = render_web_style_html_report(report, filename=filename)
    output_path.write_text(html, encoding="utf-8")
    logger.info("HTML-отчёт (web-стиль) сохранён: %s", output_path.name)
    return output_path
