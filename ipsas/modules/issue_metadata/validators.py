"""Валидация выделенных метаданных выпуска и статей.

Цель: отделить проверки (business rules) от загрузки/парсинга.
Пока используем простые строки проблем/замечаний, чтобы сохранить контракт с UI.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional


_ISSN_PATTERN = re.compile(r"^\d{4}-?\d{3}[\dXx]$")  # XXXX-XXXX или XXXX-XXXx
_DOI_PATTERN = re.compile(r"^10\.\d{4,9}/.+")  # 10.XXXX/suffix
_EDN_PATTERN = re.compile(r"^[A-Za-z0-9]{6}$")  # 6 латинских символов
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
_YEAR_PATTERN = re.compile(r"^\d{4}$")


def validate_issn(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _ISSN_PATTERN.match(s):
        return f"ISSN не соответствует формату XXXX-XXXX: «{s[:20]}{'…' if len(s) > 20 else ''}»"
    return None


def validate_doi(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip().lower()
    if not _DOI_PATTERN.match(s):
        return f"DOI не соответствует формату 10.XXXX/...: «{s[:30]}{'…' if len(s) > 30 else ''}»"
    if len(s) < 15:
        return "DOI подозрительно короткий"
    return None


def validate_edn(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _EDN_PATTERN.match(s):
        return f"EDN должен быть 6 латинских символов: «{s[:15]}{'…' if len(s) > 15 else ''}»"
    return None


def validate_date(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _DATE_PATTERN.match(s):
        return f"Дата не в формате ГГГГ-ММ-ДД: «{s[:20]}»"
    parts = s.split("-")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return f"Некорректная дата: «{s}»"
    return None


def validate_year(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if not _YEAR_PATTERN.match(s):
        return f"Год не в формате ГГГГ: «{s[:15]}»"
    y = int(s)
    if not (1900 <= y <= 2100):
        return f"Год вне допустимого диапазона: «{s}»"
    return None


def validate_volume_issue(value: Optional[object], name: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list) and value:
        value = value[0]
    s = str(value).strip()
    if not s:
        return None
    if not re.match(r"^\d{1,5}$", s):
        return f"{name} должен быть числом: «{s[:15]}»"
    n = int(s)
    if n < 1 or n > 99999:
        return f"{name} вне допустимого диапазона (1–99999): «{s}»"
    return None


def validate_journal_title(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if len(s) < 2:
        return "Название журнала слишком короткое"
    if len(s) > 500:
        return "Название журнала слишком длинное"
    return None


def validate_author_name(name: str) -> Optional[str]:
    if not name or not name.strip():
        return "Пустое имя автора"
    s = name.strip()
    if len(s) < 3:
        return f"Слишком короткое имя автора: «{s}»"
    if len(s) > 150:
        return f"Слишком длинное имя автора: «{s[:30]}…»"
    if " " not in s and "." not in s:
        return f"Имя автора должно содержать пробел или инициалы: «{s[:30]}»"
    return None


def validate_affiliation(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if len(s) < 2:
        return "Название организации слишком короткое"
    if len(s) > 1000:
        return "Название организации слишком длинное"
    return None


def build_article_problems(article: Dict[str, object]) -> List[str]:
    problems: List[str] = []
    title_ru = article.get("title_ru")
    title_en = article.get("title_en")
    abstract_ru = article.get("abstract_ru")
    abstract_en = article.get("abstract_en")
    abstract_ru_stats = article.get("abstract_ru_stats") or {}
    abstract_en_stats = article.get("abstract_en_stats") or {}
    keywords_ru_count = article.get("keywords_ru_count", 0) or 0
    keywords_en_count = article.get("keywords_en_count", 0) or 0
    references_count = article.get("references_count", 0) or 0
    identifiers = article.get("identifiers") or {}
    affiliations = article.get("affiliations") or []

    if not (title_ru or "").strip() and not (title_en or "").strip():
        problems.append("Отсутствует название статьи")
    elif not (title_ru or "").strip():
        problems.append("Отсутствует название статьи (RU)")
    elif not (title_en or "").strip():
        problems.append("Отсутствует название статьи (EN)")

    if not (abstract_ru or "").strip():
        problems.append("Отсутствует аннотация (RU)")
    if not (abstract_en or "").strip():
        problems.append("Отсутствует аннотация (EN)")

    abstract_ru_s = (abstract_ru or "").strip()
    abstract_en_s = (abstract_en or "").strip()
    if abstract_ru_s:
        lat = len(re.findall(r"[A-Za-z]", abstract_ru_s))
        cyr = len(re.findall(r"[А-Яа-яЁё]", abstract_ru_s))
        total_alpha = lat + cyr
        if total_alpha > 0 and lat >= cyr:
            problems.append("Аннотация (RU) целиком или преимущественно на латинице")
    if abstract_en_s:
        cyr = len(re.findall(r"[А-Яа-яЁё]", abstract_en_s))
        lat = len(re.findall(r"[A-Za-z]", abstract_en_s))
        total_alpha = lat + cyr
        if total_alpha > 0 and cyr >= lat:
            problems.append("Аннотация (EN) целиком или преимущественно на кириллице")

    len_ru = abstract_ru_stats.get("length")
    len_en = abstract_en_stats.get("length")
    min_abstract_words = 50
    if len_ru is not None and len_ru < min_abstract_words:
        problems.append(
            f"Слишком короткая аннотация (RU): {len_ru} слов (рекомендуется не менее {min_abstract_words})"
        )
    if len_en is not None and len_en < min_abstract_words:
        problems.append(
            f"Слишком короткая аннотация (EN): {len_en} слов (рекомендуется не менее {min_abstract_words})"
        )
    if len_ru is not None and len_en is not None and (len_ru > 0 or len_en > 0):
        shorter, longer = min(len_ru, len_en), max(len_ru, len_en)
        if longer > 0 and shorter < 0.5 * longer:
            problems.append(f"Длина аннотаций должна быть сопоставимой: RU — {len_ru} слов, EN — {len_en} слов")

    if keywords_ru_count == 0:
        problems.append("Не найдены ключевые слова на русском")
    if keywords_en_count == 0:
        problems.append("Не найдены ключевые слова на английском")
    if keywords_ru_count != keywords_en_count and (keywords_ru_count > 0 or keywords_en_count > 0):
        problems.append(f"Количество ключевых слов должно совпадать: RU — {keywords_ru_count}, EN — {keywords_en_count}")

    if references_count == 0:
        problems.append("Отсутствует список литературы")

    if isinstance(identifiers, dict):
        if not identifiers.get("doi"):
            problems.append("Не найден DOI статьи")
        else:
            err = validate_doi(identifiers.get("doi"))
            if err:
                problems.append(err)
        edn = identifiers.get("edn")
        if edn:
            err = validate_edn(edn)
            if err:
                problems.append(err)

    pub_date = article.get("publication_date")
    if pub_date:
        err = validate_date(pub_date if isinstance(pub_date, str) else str(pub_date))
        if err:
            problems.append(err)

    authors_count = article.get("authors_count") or 0
    authors_ru = article.get("authors_ru") or []
    authors_en_list = article.get("authors_en") or []
    if not authors_ru and not article.get("authors_en") and not article.get("authors"):
        problems.append("Отсутствуют авторы")
    elif authors_count == 0 and (authors_ru or authors_en_list):
        problems.append("Количество авторов не согласовано с списком")
    for name in (authors_ru or []) + (authors_en_list or []):
        err = validate_author_name(str(name))
        if err:
            problems.append(err)
            break

    if not affiliations and not (article.get("organizations") or []):
        problems.append("Отсутствуют организации (аффилиации)")
    for aff in list(affiliations or article.get("organizations") or [])[:5]:
        err = validate_affiliation(str(aff))
        if err:
            problems.append(err)
            break

    title_ru_s = (title_ru or "").strip()
    title_en_s = (title_en or "").strip()
    if title_ru_s and len(title_ru_s) < 5:
        problems.append("Название статьи (RU) слишком короткое")
    if title_en_s and len(title_en_s) < 5:
        problems.append("Название статьи (EN) слишком короткое")

    return problems


def issue_warn(
    warnings: List[Dict[str, object]],
    text: str,
    severity: str = "warning",
    field: Optional[str] = None,
) -> None:
    w: Dict[str, object] = {"text": text, "severity": severity}
    if field:
        w["field"] = field
    warnings.append(w)


def build_issue_warnings(issue_metadata: Dict[str, object]) -> List[Dict[str, object]]:
    warnings: List[Dict[str, object]] = []
    if not issue_metadata.get("journal_title"):
        issue_warn(warnings, "Не найдено название журнала", "error", "journal_title")
    else:
        err = validate_journal_title(issue_metadata.get("journal_title") if isinstance(issue_metadata.get("journal_title"), str) else str(issue_metadata.get("journal_title")))
        if err:
            issue_warn(warnings, err, "warning", "journal_title")
    if not issue_metadata.get("issue_title"):
        issue_warn(warnings, "Не найден заголовок выпуска", "warning", "issue_title")
    urls = issue_metadata.get("article_urls") or []
    if not urls:
        issue_warn(warnings, "Не найден список статей в выпуске", "error", "article_count")
    if not issue_metadata.get("volume") and not issue_metadata.get("issue_serial"):
        issue_warn(warnings, "Не определен том выпуска", "warning", "volume")
    else:
        err = validate_volume_issue(issue_metadata.get("volume"), "Том")
        if err:
            issue_warn(warnings, err, "warning", "volume")
    if not issue_metadata.get("issue"):
        issue_warn(warnings, "Не определен номер выпуска", "warning", "issue")
    else:
        err = validate_volume_issue(issue_metadata.get("issue"), "Номер выпуска")
        if err:
            issue_warn(warnings, err, "warning", "issue")
    if not issue_metadata.get("year"):
        issue_warn(warnings, "Не определен год выпуска", "warning", "year")
    else:
        err = validate_year(issue_metadata.get("year"))
        if err:
            issue_warn(warnings, err, "warning", "year")
    article_count = issue_metadata.get("article_count")
    if article_count is not None and urls and article_count != len(urls):
        issue_warn(
            warnings,
            f"Количество статей не совпадает: указано {article_count}, ссылок в выпуске: {len(urls)}",
            "warning",
            "article_count",
        )
    if urls and article_count == 0:
        issue_warn(warnings, "Количество статей указано как 0 при наличии ссылок на статьи", "warning", "article_count")
    return warnings

