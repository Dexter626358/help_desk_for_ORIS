"""Единый конвейер journal XML: parse → validate → analyze → render."""

from ipsas.modules.journal_xml.analyzer import analyze_journal_xml
from ipsas.modules.journal_xml.parser import get_articles_info, get_issue_info
from ipsas.modules.journal_xml.report.renderer import generate_html_content, generate_html_report
from ipsas.modules.journal_xml.text_utils import (
    extract_first_last_words,
    format_article_title,
    get_first_last_references,
    safe_strip,
    _format_article_title,
    _safe_strip,
)
from ipsas.modules.journal_xml.validators import (
    classify_article_metadata_profile,
    collect_article_issues,
    validate_author_data,
    validate_keywords_data,
    validate_references_data,
)

__all__ = [
    "analyze_journal_xml",
    "safe_strip",
    "format_article_title",
    "extract_first_last_words",
    "get_first_last_references",
    "get_issue_info",
    "get_articles_info",
    "classify_article_metadata_profile",
    "collect_article_issues",
    "validate_keywords_data",
    "validate_references_data",
    "validate_author_data",
    "generate_html_report",
    "generate_html_content",
    "_safe_strip",
    "_format_article_title",
]
