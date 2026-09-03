"""Пакет визуального редактора journal XML."""

from xml_editor.parser import get_issue_summary, list_articles, parse_article
from xml_editor.editor import update_article_from_form
from xml_editor.validator import validate_article, validate_all_articles

__all__ = [
    "get_issue_summary",
    "list_articles",
    "parse_article",
    "update_article_from_form",
    "validate_article",
    "validate_all_articles",
]
