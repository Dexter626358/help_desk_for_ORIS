"""Пакет анализа и HTML-отчётов по journal XML."""

from ipsas.modules.journal_xml_report.html_report import generate_html_content, generate_html_report
from ipsas.modules.journal_xml_report.parsing import get_articles_info, get_issue_info
from ipsas.modules.journal_xml_report.text_utils import (
    extract_first_last_words,
    format_article_title,
    get_first_last_references,
    safe_strip,
    _format_article_title,
    _safe_strip,
)
from ipsas.modules.journal_xml_report.validation import (
    collect_article_issues,
    validate_author_data,
    validate_keywords_data,
    validate_references_data,
)

__all__ = [
    "safe_strip",
    "format_article_title",
    "extract_first_last_words",
    "get_first_last_references",
    "get_issue_info",
    "get_articles_info",
    "collect_article_issues",
    "validate_keywords_data",
    "validate_references_data",
    "validate_author_data",
    "generate_html_report",
    "generate_html_content",
    "_safe_strip",
    "_format_article_title",
]
