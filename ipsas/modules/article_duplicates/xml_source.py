"""Извлечение эталонных статей из journal XML."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from ipsas.modules.article_duplicates.models import XmlArticle
from ipsas.modules.journal_xml.parser import get_articles_info
from ipsas.modules.journal_xml.text_utils import format_article_title, safe_strip


def _author_display(author: dict[str, Any]) -> str:
    for lang in ("RUS", "ENG"):
        info = author.get(lang)
        if isinstance(info, dict):
            surname = safe_strip(info.get("surname", ""))
            initials = safe_strip(info.get("initials", ""))
            if surname:
                return f"{surname} {initials}".strip()
    for value in author.values():
        if isinstance(value, dict):
            surname = safe_strip(value.get("surname", ""))
            initials = safe_strip(value.get("initials", ""))
            if surname:
                return f"{surname} {initials}".strip()
    return ""


def format_xml_authors(authors: list[dict[str, Any]] | None) -> str:
    if not authors:
        return ""
    parts = [_author_display(a) for a in authors]
    return ", ".join(p for p in parts if p)


def _abstract_text(article: dict[str, Any]) -> str:
    abstracts = article.get("abstracts") or {}
    for lang in ("RUS", "ENG"):
        raw = abstracts.get(lang) or {}
        if isinstance(raw, dict):
            text = safe_strip(raw.get("full_text", ""))
            if text:
                return text
        elif isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _keywords_text(article: dict[str, Any], lang: str) -> str:
    keywords = article.get("keywords") or {}
    raw = keywords.get(lang) or []
    if not isinstance(raw, list):
        return ""
    parts = [safe_strip(item) for item in raw if safe_strip(item)]
    return "; ".join(parts)


def _issue_publication_date(xml_path: Path) -> str:
    """Дата выпуска из dateUni (часто год YYYY)."""
    try:
        root = ET.parse(xml_path).getroot()
    except (ET.ParseError, OSError):
        return ""
    node = root.find(".//dateUni")
    if node is None or node.text is None:
        return ""
    return safe_strip(node.text)


def load_xml_articles(xml_path: Path | str) -> list[XmlArticle]:
    """Загрузить статьи из journal XML как эталоны для поиска дублей."""
    path = Path(xml_path)
    raw_articles = get_articles_info(path)
    issue_date = _issue_publication_date(path)
    result: list[XmlArticle] = []
    for idx, article in enumerate(raw_articles, start=1):
        titles = article.get("titles") or {}
        title_ru = safe_strip(titles.get("RUS", ""))
        title_en = safe_strip(titles.get("ENG", ""))
        result.append(
            XmlArticle(
                index=idx,
                title=format_article_title(titles),
                title_ru=title_ru,
                title_en=title_en,
                authors=format_xml_authors(article.get("authors")),
                abstract=_abstract_text(article),
                pages=safe_strip(article.get("pages", "")),
                keywords=_keywords_text(article, "RUS"),
                keywords_en=_keywords_text(article, "ENG"),
                publication_date=issue_date,
            )
        )
    if not result:
        raise ValueError("В XML нет статей для сравнения")
    return result
