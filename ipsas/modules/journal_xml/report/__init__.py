"""Рендер HTML-отчёта по готовой модели journal XML."""

from ipsas.modules.journal_xml.report.renderer import generate_html_content, generate_html_report
from ipsas.modules.journal_xml.report.standalone import (
    render_web_style_html_report,
    write_web_style_html_report,
)

__all__ = [
    "generate_html_report",
    "generate_html_content",
    "render_web_style_html_report",
    "write_web_style_html_report",
]
