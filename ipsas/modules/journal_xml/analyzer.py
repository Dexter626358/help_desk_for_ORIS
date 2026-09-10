"""Анализ XML журнала (схема journal) на полноту метаданных."""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

from ipsas.common.xml_secure import parse_xml_file_elementtree
from ipsas.modules.journal_xml.parser import get_articles_info
from ipsas.modules.journal_xml.text_utils import (
    extract_first_last_words,
    format_article_title,
    get_first_last_references,
    safe_strip,
    split_organizations,
)
from ipsas.modules.journal_xml.validators import (
    collect_article_issues,
    validate_author_data,
    validate_keywords_data,
    validate_references_data,
)

logger = logging.getLogger(__name__)

_REF_LANGS = ("RUS", "ENG", "UNK", "ANY")


def _text(el: ET.Element | None) -> str:
    """Полный текст элемента, включая вложенные теги."""
    if el is None:
        return ""
    return " ".join("".join(el.itertext()).split()).strip()


def _norm_lang(raw: str | None) -> str:
    value = (raw or "").strip().upper()
    if value in ("", "UNK"):
        return "UNK"
    if value == "ANY":
        return "ANY"
    return value


def _word_count(text: str) -> int:
    return len(text.split()) if text and text.strip() else 0


def _root_local_name(root: ET.Element) -> str:
    tag = root.tag
    return tag.split("}")[-1] if "}" in tag else tag


def _extract_journal_issue(root: ET.Element) -> dict[str, Any]:
    """Журнал и выпуск: titleid, ISSN, названия RU/EN, том/номер/год."""
    journal_titles: dict[str, str] = {}
    for ji in root.findall("journalInfo"):
        lang = _norm_lang(ji.get("lang"))
        title = _text(ji.find("title"))
        if title:
            journal_titles[lang] = title

    if not journal_titles:
        journal_info = root.find("journalInfo")
        if journal_info is not None:
            title = _text(journal_info.find("title"))
            if title:
                journal_titles[_norm_lang(journal_info.get("lang"))] = title

    issue_el = root.find("issue")
    issue: dict[str, str] = {
        "volume": _text(issue_el.find("volume")) if issue_el is not None else "",
        "number": _text(issue_el.find("number")) if issue_el is not None else "",
        "date_uni": _text(issue_el.find("dateUni")) if issue_el is not None else "",
        "pages": _text(issue_el.find("pages")) if issue_el is not None else "",
    }

    return {
        "titleid": _text(root.find("titleid")),
        "issn": _text(root.find("issn")),
        "eissn": _text(root.find("eissn")),
        "titles": journal_titles,
        "title_ru": journal_titles.get("RUS", ""),
        "title_en": journal_titles.get("ENG", ""),
        "issue": issue,
    }


def _title_of(article: dict[str, Any]) -> str:
    return format_article_title(article.get("titles") or {})


def _abstract_payload(article: dict[str, Any], lang: str) -> dict[str, Any]:
    abstracts = article.get("abstracts") or {}
    raw = abstracts.get(lang) or abstracts.get(lang.lower()) or {}
    if isinstance(raw, dict):
        full = (raw.get("full_text") or "").strip()
        summary = (raw.get("summary") or "").strip()
    else:
        full = str(raw or "").strip()
        summary = ""
    if full and not summary:
        summary = extract_first_last_words(full, 10)
    return {
        "present": bool(full),
        "word_count": _word_count(full),
        "summary": summary,
        "full_text": full,
        "ok": bool(full),
    }


def _unique_affiliations(authors: list[dict[str, Any]], lang: str) -> dict[str, Any]:
    """Уникальные аффилиации авторов (с учётом нескольких org через ';')."""
    unique: set[str] = set()
    for author in authors or []:
        lang_data = author.get(lang) or {}
        orgs = lang_data.get("organizations")
        if isinstance(orgs, list) and orgs:
            values = [safe_strip(o) for o in orgs if safe_strip(o)]
        else:
            values = split_organizations(safe_strip(lang_data.get("orgName", "")))
        for value in values:
            unique.add(value)
    items = sorted(unique)
    return {"count": len(items), "items": items}


def _author_checks(article: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for idx, author in enumerate(article.get("authors") or [], 1):
        rus = author.get("RUS") or {}
        eng = author.get("ENG") or {}
        rus_name = f"{safe_strip(rus.get('surname', ''))} {safe_strip(rus.get('initials', ''))}".strip()
        eng_name = f"{safe_strip(eng.get('surname', ''))} {safe_strip(eng.get('initials', ''))}".strip()
        rus_aff = safe_strip(rus.get("orgName", ""))
        eng_aff = safe_strip(eng.get("orgName", ""))
        status = validate_author_data(
            {
                "RUS": {"name": rus_name, "affiliation": rus_aff},
                "ENG": {"name": eng_name, "affiliation": eng_aff},
            }
        )
        problems: list[str] = []
        if not rus_name:
            problems.append("нет ФИО (RUS)")
        if not eng_name:
            problems.append("нет ФИО (ENG)")
        if rus_name and not rus_aff:
            problems.append("нет аффилиации (RUS)")
        if eng_name and not eng_aff:
            problems.append("нет аффилиации (ENG)")

        result.append(
            {
                "index": idx,
                "name_ru": rus_name or "—",
                "name_en": eng_name or "—",
                "affiliation_ru": rus_aff or "—",
                "affiliation_en": eng_aff or "—",
                "status_ru": status.get("RUS", "error"),
                "status_en": status.get("ENG", "error"),
                "problems": problems,
            }
        )
    return result


def _references_preview(references: dict[str, Any]) -> list[dict[str, Any]]:
    """Первый и последний источник по каждому языку, где есть записи."""
    preview: list[dict[str, Any]] = []
    for lang in _REF_LANGS:
        items = references.get(lang) or []
        if not items:
            continue
        ends = get_first_last_references(items, max_length=None)
        first = ends.get("first") or ""
        last = ends.get("last") or ""
        preview.append(
            {
                "lang": lang,
                "count": len(items),
                "first": first,
                "last": last,
                "same": first == last,
            }
        )
    return preview


def _keywords_preview(items: list[Any] | None) -> dict[str, Any]:
    """Первое и последнее ключевое слово (фраза) для отображения."""
    values = [safe_strip(v) for v in (items or []) if safe_strip(v)]
    if not values:
        return {
            "present": False,
            "count": 0,
            "first": "",
            "last": "",
            "preview": "—",
        }
    first = values[0]
    last = values[-1]
    if len(values) == 1:
        preview = first
    else:
        preview = f"{first} ... {last}"
    return {
        "present": True,
        "count": len(values),
        "first": first,
        "last": last,
        "preview": preview,
    }


def _build_article_report(index: int, article: dict[str, Any]) -> dict[str, Any]:
    titles = article.get("titles") or {}
    title_ru = safe_strip(titles.get("RUS", ""))
    title_en = safe_strip(titles.get("ENG", ""))

    keywords = article.get("keywords") or {}
    kw_status = validate_keywords_data(keywords)
    keywords_ru = _keywords_preview(keywords.get("RUS") or [])
    keywords_en = _keywords_preview(keywords.get("ENG") or [])

    references = article.get("references") or {}
    references_count = article.get("references_count") or {
        lang: len(vals or []) for lang, vals in references.items()
    }
    ref_status = validate_references_data(references)
    refs_preview = _references_preview(references)

    authors = _author_checks(article)
    affiliations_ru = _unique_affiliations(article.get("authors") or [], "RUS")
    affiliations_en = _unique_affiliations(article.get("authors") or [], "ENG")
    issues = list(collect_article_issues(article))

    critical = [t for s, t in issues if s == "critical"]
    secondary = [t for s, t in issues if s == "secondary"]
    if critical:
        severity = "error"
    elif secondary:
        severity = "warning"
    else:
        severity = "ok"

    return {
        "index": index,
        "pages": article.get("pages") or "",
        "art_type": article.get("art_type") or "",
        "title": _title_of(article),
        "title_ru": title_ru or "—",
        "title_en": title_en or "—",
        "has_title_ru": bool(title_ru),
        "has_title_en": bool(title_en),
        "abstract_ru": _abstract_payload(article, "RUS"),
        "abstract_en": _abstract_payload(article, "ENG"),
        "keywords_ru": keywords_ru["count"],
        "keywords_en": keywords_en["count"],
        "keywords_ru_preview": keywords_ru,
        "keywords_en_preview": keywords_en,
        "keywords_status": kw_status,
        "references_rus": references_count.get("RUS", 0),
        "references_eng": references_count.get("ENG", 0),
        "references_unk": references_count.get("UNK", 0),
        "references_any": references_count.get("ANY", 0),
        "references_status": ref_status,
        "references_preview": refs_preview,
        "references_duplicate_text_count": int(
            article.get("references_duplicate_text_count") or 0
        ),
        "references_numbered_count": int(article.get("references_numbered_count") or 0),
        "authors": authors,
        "authors_count": len(authors),
        "unique_affiliations_ru": affiliations_ru["count"],
        "unique_affiliations_en": affiliations_en["count"],
        "unique_affiliations_ru_items": affiliations_ru["items"],
        "unique_affiliations_en_items": affiliations_en["items"],
        "issues": issues,
        "critical_issues": critical,
        "secondary_issues": secondary,
        "severity": severity,
        "ok": severity == "ok",
    }


def analyze_journal_xml(xml_path: Path) -> dict[str, Any]:
    """
    Полный анализ XML журнала (journal schema).

    Returns:
        Словарь с журналом, выпуском, сводкой и отчётами по статьям.
    """
    if not xml_path.exists():
        raise FileNotFoundError(f"XML файл не найден: {xml_path}")

    try:
        tree = parse_xml_file_elementtree(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise ValueError(f"Некорректный XML: {e}") from e

    root_tag = _root_local_name(root)
    if root_tag.lower() not in {"journal", "root"}:
        logger.warning(
            "Ожидался корневой элемент journal, получен %s (%s)",
            root_tag,
            xml_path.name,
        )

    meta = _extract_journal_issue(root)
    articles_raw = get_articles_info(xml_path, root=root)
    articles = [_build_article_report(i, a) for i, a in enumerate(articles_raw, 1)]

    with_errors = sum(1 for a in articles if a["severity"] == "error")
    with_warnings = sum(1 for a in articles if a["severity"] == "warning")
    ok_count = sum(1 for a in articles if a["ok"])
    missing_eng_title = sum(1 for a in articles if not a["has_title_en"])
    missing_eng_abstract = sum(1 for a in articles if not a["abstract_en"]["present"])
    missing_eng_keywords = sum(1 for a in articles if a["keywords_en"] == 0)

    return {
        "source_file": xml_path.name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "root_tag": root_tag,
        "journal": {
            "titleid": meta["titleid"],
            "issn": meta["issn"],
            "eissn": meta["eissn"],
            "title_ru": meta["title_ru"],
            "title_en": meta["title_en"],
            "titles": meta["titles"],
        },
        "issue": meta["issue"],
        "summary": {
            "articles_total": len(articles),
            "articles_ok": ok_count,
            "articles_with_errors": with_errors,
            "articles_with_warnings": with_warnings,
            "missing_eng_title": missing_eng_title,
            "missing_eng_abstract": missing_eng_abstract,
            "missing_eng_keywords": missing_eng_keywords,
        },
        "articles": articles,
    }
