"""Анализ XML журнала (схема journal) на полноту метаданных."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

import report_generator as rg


def _text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _norm_lang(raw: str | None) -> str:
    value = (raw or "").strip().upper()
    if value in ("", "UNK"):
        return "UNK"
    if value == "ANY":
        return "ANY"
    return value


def _word_count(text: str) -> int:
    return len(text.split()) if text and text.strip() else 0


def _extract_journal_issue(xml_path: Path) -> dict[str, Any]:
    """Журнал и выпуск: titleid, ISSN, названия RU/EN, том/номер/год."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    journal_titles: dict[str, str] = {}
    for ji in root.findall("journalInfo"):
        lang = _norm_lang(ji.get("lang"))
        title = _text(ji.find("title"))
        if title:
            journal_titles[lang] = title

    # Fallback на одиночный journalInfo без lang / из старого парсера
    if not journal_titles:
        legacy = rg.get_issue_info(xml_path)
        jt = (legacy.get("journal_title") or "").strip()
        if jt:
            journal_titles[_norm_lang(legacy.get("journal_lang"))] = jt

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
    return rg._format_article_title(article.get("titles") or {})


def _abstract_payload(article: dict[str, Any], lang: str) -> dict[str, Any]:
    abstracts = article.get("abstracts") or {}
    raw = abstracts.get(lang) or abstracts.get(lang.lower()) or {}
    if isinstance(raw, dict):
        full = (raw.get("full_text") or "").strip()
        summary = (raw.get("summary") or "").strip()
    else:
        full = str(raw or "").strip()
        summary = rg.extract_first_last_words(full, 10) if full else ""
    return {
        "present": bool(full),
        "word_count": _word_count(full),
        "summary": summary,
        "ok": bool(full),
    }


def _author_checks(article: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for idx, author in enumerate(article.get("authors") or [], 1):
        rus = author.get("RUS") or {}
        eng = author.get("ENG") or {}
        rus_name = f"{rg._safe_strip(rus.get('surname', ''))} {rg._safe_strip(rus.get('initials', ''))}".strip()
        eng_name = f"{rg._safe_strip(eng.get('surname', ''))} {rg._safe_strip(eng.get('initials', ''))}".strip()
        rus_aff = rg._safe_strip(rus.get("orgName", ""))
        eng_aff = rg._safe_strip(eng.get("orgName", ""))
        status = rg.validate_author_data(
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


def _extra_affiliation_issues(authors: list[dict[str, Any]]) -> list[tuple[str, str]]:
    issues: list[tuple[str, str]] = []
    for a in authors:
        for p in a.get("problems") or []:
            if "аффилиац" in p:
                issues.append(("secondary", f"автор {a['index']}: {p}"))
    return issues


def _references_preview(references: dict[str, Any]) -> list[dict[str, str]]:
    """Первый и последний источник по каждому языку, где есть записи."""
    preview: list[dict[str, str]] = []
    for lang in ("RUS", "ENG", "UNK", "ANY"):
        items = references.get(lang) or []
        if not items:
            continue
        ends = rg.get_first_last_references(items, max_length=None)
        preview.append(
            {
                "lang": lang,
                "count": str(len(items)),
                "first": ends.get("first") or "",
                "last": ends.get("last") or "",
                "same": ends.get("first") == ends.get("last"),
            }
        )
    return preview


def _build_article_report(index: int, article: dict[str, Any]) -> dict[str, Any]:
    titles = article.get("titles") or {}
    title_ru = rg._safe_strip(titles.get("RUS", ""))
    title_en = rg._safe_strip(titles.get("ENG", ""))

    keywords = article.get("keywords") or {}
    keywords_count = article.get("keywords_count") or {
        lang: len(vals or []) for lang, vals in keywords.items()
    }
    kw_status = rg.validate_keywords_data(keywords)

    references = article.get("references") or {}
    references_count = article.get("references_count") or {
        lang: len(vals or []) for lang, vals in references.items()
    }
    ref_status = rg.validate_references_data(references)
    refs_preview = _references_preview(references)

    authors = _author_checks(article)
    base_issues = list(rg.collect_article_issues(article))
    # Дополняем явными проблемами аффилиаций (в сводке collect_article_issues их нет)
    seen = set(base_issues)
    for item in _extra_affiliation_issues(authors):
        if item not in seen:
            base_issues.append(item)
            seen.add(item)

    critical = [t for s, t in base_issues if s == "critical"]
    secondary = [t for s, t in base_issues if s == "secondary"]
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
        "keywords_ru": keywords_count.get("RUS", 0),
        "keywords_en": keywords_count.get("ENG", 0),
        "keywords_status": kw_status,
        "references_rus": references_count.get("RUS", 0),
        "references_eng": references_count.get("ENG", 0),
        "references_unk": references_count.get("UNK", 0),
        "references_any": references_count.get("ANY", 0),
        "references_status": ref_status,
        "references_preview": refs_preview,
        "authors": authors,
        "authors_count": len(authors),
        "issues": base_issues,
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
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise ValueError(f"Некорректный XML: {e}") from e

    root_tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
    if root_tag.lower() not in {"journal", "root"}:
        # root допускается для тестов; production — journal
        pass

    meta = _extract_journal_issue(xml_path)
    articles_raw = rg.get_articles_info(xml_path)
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
