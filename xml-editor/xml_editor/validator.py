"""Простая валидация статей journal XML для UI редактора."""

from __future__ import annotations

import re
from typing import Any

from lxml import etree

from xml_editor.parser import list_articles, parse_article
from xml_editor.utils import doi_ok, email_ok, looks_like_email, orcid_ok


def validate_tree_has_articles(tree: etree._ElementTree) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    from xml_editor.parser import list_article_elements

    if not list_article_elements(tree):
        issues.append({"severity": "error", "text": "В XML не найдено ни одной статьи"})
    return issues


def validate_article_data(article: dict[str, Any]) -> list[dict[str, str]]:
    """Проверки по уже распарсенной статье."""
    issues: list[dict[str, str]] = []

    title_rus = (article.get("title_rus") or "").strip()
    title_eng = (article.get("title_eng") or "").strip()
    if not title_rus and not title_eng:
        issues.append({"severity": "error", "field": "title_rus", "text": "Не заполнено название статьи"})
    elif not title_rus:
        issues.append({"severity": "warning", "field": "title_rus", "text": "Нет названия на русском"})
    elif not title_eng:
        issues.append({"severity": "warning", "field": "title_eng", "text": "Нет названия на английском"})

    authors = article.get("authors") or []
    if not authors:
        issues.append({"severity": "error", "field": "authors", "text": "Нужен хотя бы один автор"})
    else:
        has_named = False
        for i, author in enumerate(authors, start=1):
            for lang in ("RUS", "ENG"):
                block = author.get(lang) or {}
                surname = (block.get("surname") or "").strip()
                if surname:
                    has_named = True
                email = (block.get("email") or "").strip()
                if email and not email_ok(email):
                    issues.append(
                        {
                            "severity": "error",
                            "field": f"author_{i}_{lang}_email",
                            "text": f"Автор {i} ({lang}): некорректный email",
                        }
                    )
                orcid = (block.get("orcid") or "").strip()
                if orcid and not orcid_ok(orcid):
                    issues.append(
                        {
                            "severity": "error",
                            "field": f"author_{i}_{lang}_orcid",
                            "text": f"Автор {i} ({lang}): некорректный ORCID",
                        }
                    )
                org = (block.get("org_name") or "").strip()
                if org and looks_like_email(org):
                    issues.append(
                        {
                            "severity": "warning",
                            "field": f"author_{i}_{lang}_org",
                            "text": f"Автор {i} ({lang}): в orgName похоже попал email",
                        }
                    )
        if not has_named:
            issues.append({"severity": "error", "field": "authors", "text": "У авторов не заполнена фамилия"})

    doi = (article.get("doi") or "").strip()
    if doi and not doi_ok(doi):
        issues.append({"severity": "error", "field": "doi", "text": "DOI имеет недопустимый формат (ожидается 10.…/…)"})

    pf = (article.get("page_first") or "").strip()
    pl = (article.get("page_last") or "").strip()
    if pf.isdigit() and pl.isdigit() and int(pf) > int(pl):
        issues.append(
            {
                "severity": "error",
                "field": "page_first",
                "text": "Первая страница не может быть больше последней",
            }
        )

    for field, label in (
        ("title_rus", "Название RUS"),
        ("title_eng", "Название ENG"),
        ("abstract_rus", "Аннотация RUS"),
        ("abstract_eng", "Аннотация ENG"),
    ):
        val = article.get(field) or ""
        # В уже распарсенном тексте &<> обычно нормальны; ищем «сырые» артефакты вида &something;
        if "&" in val and re.search(r"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", val):
            issues.append(
                {
                    "severity": "warning",
                    "field": field,
                    "text": f"{label}: возможны неэкранированные спецсимволы (&)",
                }
            )

    return issues


def validate_article(tree: etree._ElementTree, article_id: str | int) -> list[dict[str, str]]:
    data = parse_article(tree, article_id)
    return validate_article_data(data)


def validate_all_articles(tree: etree._ElementTree) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = {}
    for item in list_articles(tree):
        aid = item["id"]
        result[aid] = validate_article(tree, aid)
    return result


def summarize_issues(issues: list[dict[str, str]]) -> dict[str, int]:
    errors = sum(1 for i in issues if i.get("severity") == "error")
    warnings = sum(1 for i in issues if i.get("severity") == "warning")
    return {"errors": errors, "warnings": warnings, "total": errors + warnings}
