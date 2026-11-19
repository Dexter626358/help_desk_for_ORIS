"""Эталоны статей из опубликованного выпуска OJS (без journal XML)."""

from __future__ import annotations

import logging
import os
from typing import Any

from ipsas.config.settings import get_settings
from ipsas.modules.archive_by_sender.parser import parse_journal_ref
from ipsas.modules.article_duplicates.models import XmlArticle
from ipsas.modules.issue_metadata.orchestrator import IssueMetadataParser
from ipsas.modules.journal_xml.text_utils import safe_strip

logger = logging.getLogger(__name__)


def _format_authors(article: dict[str, Any]) -> str:
    for key in ("authors_ru", "authors_en", "authors"):
        raw = article.get(key)
        if isinstance(raw, list) and raw:
            parts = [safe_strip(a) for a in raw if safe_strip(a)]
            if parts:
                return ", ".join(parts)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _pages(article: dict[str, Any]) -> str:
    pages = safe_strip(article.get("pages", ""))
    if pages:
        return pages
    start = safe_strip(article.get("page_start", ""))
    end = safe_strip(article.get("page_end", ""))
    if start and end:
        return f"{start}-{end}"
    return start or end


def _abstract(article: dict[str, Any]) -> str:
    for key in ("abstract_ru", "page_abstract_ru", "abstract_en", "page_abstract_en"):
        text = safe_strip(article.get(key, ""))
        if text:
            return text
    return ""


def _keywords_join(values: object) -> str:
    if isinstance(values, list):
        parts = [safe_strip(v) for v in values if safe_strip(v)]
        return "; ".join(parts)
    if isinstance(values, str):
        return values.strip()
    return ""


def _keywords(article: dict[str, Any], *, en: bool = False) -> str:
    if en:
        keys = ("page_keywords_en", "keywords_en")
    else:
        keys = ("page_keywords_ru", "keywords_ru")
    for key in keys:
        text = _keywords_join(article.get(key))
        if text:
            return text
    return ""


def _format_iso_date_ru(value: str) -> str:
    """YYYY-MM-DD → DD.MM.YYYY; иначе вернуть как есть."""
    text = safe_strip(value)
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        y, m, d = text[:10].split("-")
        if y.isdigit() and m.isdigit() and d.isdigit():
            return f"{d}.{m}.{y}"
    return text


def _publication_date(article: dict[str, Any], *, fallback: str = "") -> str:
    display = safe_strip(article.get("publication_date_display", ""))
    if display:
        return display
    raw = safe_strip(article.get("publication_date", ""))
    if raw:
        return _format_iso_date_ru(raw)
    return _format_iso_date_ru(fallback)


def _issue_date_fallback(issue_meta: dict[str, Any]) -> str:
    """Дата/идентификатор выпуска, если у статьи нет citation_date."""
    display = safe_strip(issue_meta.get("publication_date_display", ""))
    if display:
        return display
    raw = safe_strip(issue_meta.get("publication_date", ""))
    if raw:
        return _format_iso_date_ru(raw)
    year = safe_strip(issue_meta.get("year", ""))
    volume = safe_strip(issue_meta.get("volume", ""))
    number = safe_strip(
        issue_meta.get("issue") or issue_meta.get("number") or ""
    )
    parts: list[str] = []
    if volume:
        parts.append(f"Том {volume}")
    if number:
        parts.append(f"№ {number}")
    if year:
        parts.append(f"({year})")
    if parts:
        return " ".join(parts)
    title = safe_strip(
        issue_meta.get("title")
        or issue_meta.get("issue_title")
        or issue_meta.get("issue_title_ru")
        or ""
    )
    return title


def article_dict_to_xml_article(
    article: dict[str, Any],
    index: int,
    *,
    fallback_date: str = "",
) -> XmlArticle | None:
    """Преобразовать словарь статьи выпуска в эталон для матчинга."""
    title_ru = safe_strip(article.get("title_ru", ""))
    title_en = safe_strip(article.get("title_en", ""))
    title = title_ru or title_en
    if not title:
        return None
    return XmlArticle(
        index=index,
        title=title,
        title_ru=title_ru,
        title_en=title_en,
        authors=_format_authors(article),
        abstract=_abstract(article),
        pages=_pages(article),
        keywords=_keywords(article, en=False),
        keywords_en=_keywords(article, en=True),
        publication_date=_publication_date(article, fallback=fallback_date),
    )


def load_issue_articles(issue_url: str) -> tuple[list[XmlArticle], str]:
    """
    Статьи опубликованного выпуска → эталоны для поиска дублей.

    Берёт title/authors/abstract со страниц статей (HTML), без journal XML.
    Возвращает (статьи, slug/ISSN журнала из URL).
    """
    raw_url = (issue_url or "").strip()
    if not raw_url:
        raise ValueError("Укажите ссылку на опубликованный выпуск")
    if "/issue/view/" not in raw_url.replace("\\", "/").casefold():
        # допускаем и другие формы, если ISSN извлекается
        logger.info("URL выпуска без /issue/view/: %s", raw_url)

    journal = parse_journal_ref(raw_url)
    settings = get_settings()
    parser = IssueMetadataParser(max_download_size=settings.max_file_size)

    logger.info("Загрузка выпуска для дублей: %s", raw_url)
    issue_root = parser._fetch_html(raw_url, is_issue_page=True)
    issue_meta = parser._parse_issue_page(issue_root, raw_url)
    article_urls = list(issue_meta.get("article_urls") or [])
    if not article_urls:
        raise ValueError(
            "В выпуске не найдены статьи "
            "(проверьте ссылку вида …/ISSN/issue/view/…)"
        )

    try:
        max_articles = int(os.getenv("ISSUE_PARSER_MAX_ARTICLES", "200"))
    except ValueError:
        max_articles = 200
    if max_articles > 0:
        article_urls = article_urls[:max_articles]

    issue_fallback = _issue_date_fallback(issue_meta)
    result: list[XmlArticle] = []
    for idx, article_url in enumerate(article_urls, start=1):
        try:
            article_root = parser._fetch_html(
                str(article_url), is_issue_page=False
            )
            data = parser._parse_article_page(article_root, str(article_url))
            if not isinstance(data, dict):
                continue
            # Подставить дату из meta, если парсер оставил пусто.
            if not _publication_date(data):
                data = dict(data)
                data["publication_date"] = issue_fallback
            item = article_dict_to_xml_article(data, idx, fallback_date=issue_fallback)
            if item is None:
                logger.warning("Статья без названия пропущена: %s", article_url)
                continue
            result.append(item)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Не разобрана статья %s: %s", article_url, exc)

    if not result:
        raise ValueError("Не удалось извлечь статьи из выпуска")

    logger.info(
        "Выпуск %s: эталонов для дублей %s (из %s URL)",
        journal,
        len(result),
        len(article_urls),
    )
    return result, journal
