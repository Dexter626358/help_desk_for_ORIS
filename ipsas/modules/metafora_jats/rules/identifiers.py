"""M005 DOI, M006 EDN."""

from __future__ import annotations

import re

from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)

_DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
)
# 10.<registrant>/<suffix> без пробелов и служебных символов разметки
_DOI_RE = re.compile(r"^10\.\d{4,9}/[^\s<>\"'#]+$", re.I)
_CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
_EDN_RE = re.compile(r"^[A-Za-z0-9]{6}$")


def normalize_doi(value: str) -> str:
    text = (value or "").strip()
    low = text.casefold()
    for prefix in _DOI_PREFIXES:
        if low.startswith(prefix):
            text = text[len(prefix) :]
            break
    # частый артефакт копирования
    return text.strip().rstrip(".")


def doi_has_cyrillic(value: str) -> bool:
    return bool(_CYRILLIC_RE.search(value or ""))


def doi_format_ok(normalized: str) -> bool:
    """Проверка формата DOI после нормализации."""
    if not normalized or any(ch.isspace() for ch in normalized):
        return False
    if doi_has_cyrillic(normalized):
        return False
    if normalized[-1] in ".,;:/":
        return False
    return bool(_DOI_RE.match(normalized))


def check_doi(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    raw = (pub.doi or "").strip()
    declared = bool(pub.doi_declared)

    if not declared and not raw:
        return ([], [PassedCheck(code="M005", label="DOI (не указан — допустимо)")])

    if declared and not raw:
        return (
            [
                issue(
                    "M005",
                    "error",
                    "DOI указан, но значение пустое. "
                    "Пустой DOI нельзя отправить в Метафору.",
                    field="doi",
                    xpath=pub.doi_xpath
                    or '/article/front/article-meta/article-id[@pub-id-type="doi"]',
                )
            ],
            [],
        )

    normalized = normalize_doi(raw)
    if doi_has_cyrillic(raw) or doi_has_cyrillic(normalized):
        return (
            [
                issue(
                    "M005",
                    "error",
                    "В DOI не должно быть кириллицы.",
                    field="doi",
                    value=raw,
                    xpath=pub.doi_xpath,
                )
            ],
            [],
        )
    if not doi_format_ok(normalized):
        return (
            [
                issue(
                    "M005",
                    "error",
                    "DOI имеет недопустимый формат. "
                    "Ожидается вид 10.xxxx/suffix без пробелов "
                    "(иначе Метафора не примет запись).",
                    field="doi",
                    value=raw,
                    xpath=pub.doi_xpath,
                )
            ],
            [],
        )
    return ([], [PassedCheck(code="M005", label=f"DOI: {normalized}")])


def check_edn(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    raw = (pub.edn or "").strip()
    if not raw:
        return ([], [PassedCheck(code="M006", label="EDN (не указан — допустимо)")])

    if not _EDN_RE.match(raw):
        return (
            [
                issue(
                    "M006",
                    "error",
                    "EDN имеет недопустимый формат.",
                    field="edn",
                    value=raw,
                    xpath=pub.edn_xpath,
                    details="Ожидается ровно 6 латинских букв или цифр.",
                )
            ],
            [],
        )
    return ([], [PassedCheck(code="M006", label=f"EDN: {raw}")])
