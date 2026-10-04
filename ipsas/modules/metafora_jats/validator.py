"""Оркестрация проверки JATS XML по требованиям Метафоры."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from lxml.etree import XMLSyntaxError

from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    ValidationIssue,
    ValidationReport,
    issue,
)
from ipsas.modules.metafora_jats.article_types import label_for_article_type
from ipsas.modules.metafora_jats.parser import (
    is_jats_article,
    parse_publication,
    parse_secure_bytes,
)
from ipsas.modules.metafora_jats.rules import (
    check_affiliations,
    check_authors,
    check_doi,
    check_edn,
    check_pagination,
    check_publication_date,
    check_publication_type,
    check_references,
    check_title,
)

# Категории для будущего расширения (v1 использует только METAFORA_REQUIRED).
CATEGORY_METAFORA_REQUIRED = "metafora_required"
CATEGORY_METADATA_QUALITY = "metadata_quality"


def _looks_cyrillic(text: str) -> bool:
    return any("а" <= ch.lower() <= "я" or ch.lower() == "ё" for ch in text)


def _primary_title(pub) -> str:
    """Предпочесть RU-название, иначе кириллическое, иначе первое непустое."""
    titles = [t for t in pub.titles if (t.text or "").strip()]
    if not titles:
        return ""
    for t in titles:
        if (t.lang or "").casefold() in {"ru", "rus", "russian"}:
            return t.text.strip()
    for t in titles:
        if _looks_cyrillic(t.text):
            return t.text.strip()
    return titles[0].text.strip()


def _article_url(pub) -> str:
    """URL статьи из self-uri."""
    url = (pub.article_url or "").strip()
    if url.lower().startswith(("http://", "https://")):
        return url
    return ""


def _syntax_error_report(filename: str, exc: XMLSyntaxError) -> ValidationReport:
    line = getattr(exc, "lineno", None) or 0
    col = getattr(exc, "offset", None)
    if col is None:
        col = 0
    details = f"Строка: {line}\nСтолбец: {col}\nОшибка: {exc.msg or str(exc)}"
    msg = (
        "XML-файл содержит синтаксическую ошибку.\n\n"
        f"Строка: {line}\n"
        f"Столбец: {col}\n"
        f"Ошибка: {exc.msg or str(exc)}"
    )
    iss = issue(
        "PARSE",
        "error",
        msg,
        field="xml",
        details=details,
    )
    return ValidationReport(
        valid_for_metafora=False,
        filename=filename,
        error_count=1,
        issues=[iss],
        parse_ok=False,
        is_jats=False,
    )


def _not_jats_report(filename: str) -> ValidationReport:
    iss = issue(
        "JATS",
        "error",
        "Файл не распознан как JATS XML статьи.",
        field="root",
        xpath="/",
    )
    return ValidationReport(
        valid_for_metafora=False,
        filename=filename,
        error_count=1,
        issues=[iss],
        parse_ok=True,
        is_jats=False,
    )


def validate_jats_bytes(
    xml_bytes: bytes,
    *,
    filename: str = "",
    today: date | None = None,
) -> ValidationReport:
    """Проверить содержимое XML. Без сети, без LLM, без XSD."""
    name = filename or "article.xml"
    try:
        root = parse_secure_bytes(xml_bytes)
    except XMLSyntaxError as exc:
        return _syntax_error_report(name, exc)

    if not is_jats_article(root):
        return _not_jats_report(name)

    pub = parse_publication(root, filename=name)
    issues: list[ValidationIssue] = []
    passed: list[PassedCheck] = []

    for checker in (
        check_publication_type,
        check_title,
        lambda p: check_publication_date(p, today=today),
        check_pagination,
        check_doi,
        check_edn,
        check_authors,
        check_references,
        check_affiliations,
    ):
        iss, ok = checker(pub)
        issues.extend(iss)
        passed.extend(ok)

    # Hook: later JATS XSD/DTD and METADATA_QUALITY rules can be inserted here.

    errors = [i for i in issues if i.severity == "error"]
    warnings = [i for i in issues if i.severity == "warning"]
    infos = [i for i in issues if i.severity == "info"]
    pub_type = (pub.publication_type or "").strip()
    return ValidationReport(
        valid_for_metafora=len(errors) == 0,
        filename=name,
        article_title=_primary_title(pub),
        publication_type=pub_type,
        publication_type_label=label_for_article_type(pub_type),
        article_url=_article_url(pub),
        error_count=len(errors),
        warning_count=len(warnings),
        info_count=len(infos),
        passed_count=len(passed),
        issues=issues,
        passed_checks=passed,
        parse_ok=True,
        is_jats=True,
    )


def validate_jats_file(
    path: str | Path,
    *,
    today: date | None = None,
) -> ValidationReport:
    p = Path(path)
    try:
        data = p.read_bytes()
    except OSError as exc:
        return ValidationReport(
            valid_for_metafora=False,
            filename=p.name,
            error_count=1,
            issues=[
                issue(
                    "IO",
                    "error",
                    f"Не удалось прочитать файл: {exc}",
                    field="file",
                )
            ],
            parse_ok=False,
            is_jats=False,
        )
    return validate_jats_bytes(data, filename=p.name, today=today)
