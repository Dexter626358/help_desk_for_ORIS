"""Валидация метаданных статей journal XML."""

from __future__ import annotations

import re
from typing import Any, Dict, List

from ipsas.modules.journal_xml.text_utils import safe_strip

# Нумерация в начале источника: "1. Text", "12) Text"
_REF_NUMBERING_RE = re.compile(r"^\s*\d+[.)]\s*")


def _count_numbered_references(references_data: Dict[str, Any]) -> int:
    """Сколько источников начинаются с явной нумерации."""
    count = 0
    for lang in ("RUS", "ENG", "UNK", "ANY"):
        for text in references_data.get(lang) or []:
            if _REF_NUMBERING_RE.match(safe_strip(text) or ""):
                count += 1
    return count


def collect_article_issues(article: Dict[str, Any]) -> List[tuple[str, str]]:
    """
    Собирает краткий список проблем по статье для сводки в начале отчёта.
    Возвращает пустой список, если проблем не найдено.
    """
    # (severity, text), где severity: "critical" | "secondary"
    issues: List[tuple[str, str]] = []

    titles = article.get("titles", {}) or {}
    if not safe_strip(titles.get("RUS", "")):
        issues.append(("critical", "нет названия (RUS)"))
    if not safe_strip(titles.get("ENG", "")):
        issues.append(("critical", "нет названия (ENG)"))

    abstracts = article.get("abstracts", {}) or {}
    rus_abstract = abstracts.get("RUS", {})
    eng_abstract = abstracts.get("ENG", {})

    rus_full = rus_abstract.get("full_text", "") if isinstance(rus_abstract, dict) else rus_abstract
    eng_full = eng_abstract.get("full_text", "") if isinstance(eng_abstract, dict) else eng_abstract

    rus_full_s = safe_strip(rus_full)
    eng_full_s = safe_strip(eng_full)

    if not rus_full_s:
        issues.append(("critical", "аннотация RUS: отсутствует"))

    if not eng_full_s:
        issues.append(("critical", "аннотация ENG: отсутствует"))

    keywords_data = article.get("keywords", {}) or {}
    kw_status = validate_keywords_data(keywords_data)
    rus_kw_missing = "❌" in kw_status.get("RUS", "")
    eng_kw_missing = "❌" in kw_status.get("ENG", "")
    if rus_kw_missing and eng_kw_missing:
        issues.append(("critical", "нет ключевых слов"))
    elif rus_kw_missing:
        issues.append(("secondary", "нет ключевых слов (RUS)"))
    elif eng_kw_missing:
        issues.append(("secondary", "нет ключевых слов (ENG)"))
    if "⚠️" in kw_status.get("comparison", ""):
        issues.append(("secondary", kw_status["comparison"].replace("⚠️ ", "")))

    references_data = article.get("references", {}) or {}
    total_refs = sum(
        len(references_data.get(lang) or [])
        for lang in ("RUS", "ENG", "UNK", "ANY")
    )
    if total_refs == 0:
        issues.append(("critical", "нет источников"))
    else:
        numbered = int(article.get("references_numbered_count") or 0)
        if numbered <= 0:
            numbered = _count_numbered_references(references_data)
        if numbered:
            issues.append(
                (
                    "secondary",
                    f"список литературы содержит нумерацию ({numbered} из {total_refs})",
                )
            )
        duplicated = int(article.get("references_duplicate_text_count") or 0)
        if duplicated:
            issues.append(
                (
                    "secondary",
                    f"дублирование текста источников "
                    f"(текст в <reference> и в <refInfo>/<text>, {duplicated} из {total_refs})",
                )
            )

    # Сигнализируем, если хотя бы один автор без данных на каком-то языке
    authors = article.get("authors", []) or []
    if not authors:
        issues.append(("critical", "нет авторов"))
    else:
        for idx, author in enumerate(authors, 1):
            rus = author.get("RUS", {}) or {}
            eng = author.get("ENG", {}) or {}
            rus_name = f"{safe_strip(rus.get('surname', ''))} {safe_strip(rus.get('initials', ''))}".strip()
            eng_name = f"{safe_strip(eng.get('surname', ''))} {safe_strip(eng.get('initials', ''))}".strip()

            author_validation = validate_author_data(
                {
                    "RUS": {"name": rus_name, "affiliation": safe_strip(rus.get("orgName", ""))},
                    "ENG": {"name": eng_name, "affiliation": safe_strip(eng.get("orgName", ""))},
                }
            )
            rus_aff = safe_strip(rus.get("orgName", ""))
            eng_aff = safe_strip(eng.get("orgName", ""))
            if author_validation.get("RUS") == "error":
                issues.append(("critical", f"автор {idx}: нет данных (RUS)"))
            elif rus_name and not rus_aff:
                issues.append(("secondary", f"автор {idx}: нет аффилиации (RUS)"))
            if author_validation.get("ENG") == "error":
                issues.append(("critical", f"автор {idx}: нет данных (ENG)"))
            elif eng_name and not eng_aff:
                issues.append(("secondary", f"автор {idx}: нет аффилиации (ENG)"))

    # Дедуплицируем, сохраняя порядок
    seen: set[tuple[str, str]] = set()
    deduped: List[tuple[str, str]] = []
    for item in issues:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped

def validate_keywords_data(keywords_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Валидирует данные ключевых слов и возвращает статус валидации
    
    Args:
        keywords_data: Словарь с ключевыми словами по языкам
        
    Returns:
        Dict[str, str]: Статус валидации для каждого языка
    """
    validation_status = {}
    
    rus_keywords = keywords_data.get('RUS', [])
    eng_keywords = keywords_data.get('ENG', [])
    
    # Проверяем русские ключевые слова
    if not rus_keywords:
        validation_status['RUS'] = "❌ Отсутствуют"
    else:
        validation_status['RUS'] = f"✅ {len(rus_keywords)} слов"
    
    # Проверяем английские ключевые слова
    if not eng_keywords:
        validation_status['ENG'] = "❌ Отсутствуют"
    else:
        validation_status['ENG'] = f"✅ {len(eng_keywords)} слов"
    
    # Сравниваем количество
    if rus_keywords and eng_keywords:
        rus_count = len(rus_keywords)
        eng_count = len(eng_keywords)
        if rus_count != eng_count:
            validation_status['comparison'] = f"⚠️ Ключевые слова — разное количество: RUS={rus_count}, ENG={eng_count}"
        else:
            validation_status['comparison'] = f"✅ Ключевые слова — одинаковое количество: {rus_count} слов"
    elif rus_keywords or eng_keywords:
        validation_status['comparison'] = "⚠️ Ключевые слова — только на одном языке"
    else:
        validation_status['comparison'] = "❌ Ключевые слова — отсутствуют на обоих языках"
    
    return validation_status

def validate_references_data(references_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Валидирует источники литературы.

    RUS / ENG / UNK / ANY — допустимые языковые метки.
    Критично только полное отсутствие источников.
    """
    validation_status: Dict[str, str] = {}

    rus_references = references_data.get("RUS", []) or []
    eng_references = references_data.get("ENG", []) or []
    unk_references = references_data.get("UNK", []) or []
    any_references = references_data.get("ANY", []) or []
    total = (
        len(rus_references)
        + len(eng_references)
        + len(unk_references)
        + len(any_references)
    )

    validation_status["RUS"] = (
        f"✅ {len(rus_references)} источников" if rus_references else "—"
    )
    validation_status["ENG"] = (
        f"✅ {len(eng_references)} источников" if eng_references else "—"
    )
    validation_status["ANY"] = (
        f"✅ {len(any_references)} источников" if any_references else "—"
    )
    validation_status["UNK"] = (
        f"✅ {len(unk_references)} источников" if unk_references else "—"
    )
    validation_status["total"] = str(total)

    if total == 0:
        validation_status["comparison"] = "❌ Источники отсутствуют"
    else:
        parts = [
            f"{label}={count}"
            for label, count in (
                ("RUS", len(rus_references)),
                ("ENG", len(eng_references)),
                ("UNK", len(unk_references)),
                ("ANY", len(any_references)),
            )
            if count
        ]
        validation_status["comparison"] = f"✅ Источники есть ({', '.join(parts)})"

    return validation_status

def validate_author_data(author_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Валидирует данные автора и возвращает статус валидации
    
    Args:
        author_data: Данные автора с языковыми версиями
        
    Returns:
        Dict[str, str]: Статус валидации для каждого языка
    """
    validation_status = {}
    
    rus_data = author_data.get('RUS', {})
    eng_data = author_data.get('ENG', {})
    
    # Проверяем наличие данных
    rus_has_name = bool(rus_data.get('name', '').strip())
    rus_has_affiliation = bool(rus_data.get('affiliation', '').strip())
    eng_has_name = bool(eng_data.get('name', '').strip())
    eng_has_affiliation = bool(eng_data.get('affiliation', '').strip())
    
    # Определяем статус валидации
    if not rus_has_name and not eng_has_name:
        validation_status['RUS'] = 'error'  # 🔴 Нет данных вообще
        validation_status['ENG'] = 'error'
    elif rus_has_name and not eng_has_name:
        validation_status['RUS'] = 'warning'  # 🟠 Есть только русские данные
        validation_status['ENG'] = 'error'
    elif not rus_has_name and eng_has_name:
        validation_status['RUS'] = 'error'
        validation_status['ENG'] = 'warning'  # 🟠 Есть только английские данные
    else:
        # Есть данные на обоих языках
        if not rus_has_affiliation and not eng_has_affiliation:
            validation_status['RUS'] = 'warning'  # 🟠 Нет аффилиации
            validation_status['ENG'] = 'warning'
        elif rus_has_affiliation and eng_has_affiliation:
            validation_status['RUS'] = 'success'  # 🟢 Все данные есть
            validation_status['ENG'] = 'success'
        else:
            validation_status['RUS'] = 'warning'  # 🟠 Неполные данные
            validation_status['ENG'] = 'warning'
    
    return validation_status

def validate_organization_data(org_data: Dict[str, str]) -> str:
    """
    Валидирует данные организации
    
    Args:
        org_data: Данные организации с русским и английским названиями
        
    Returns:
        str: Статус валидации ('success', 'warning', 'error')
    """
    rus_name = org_data.get('RUS', '').strip()
    eng_name = org_data.get('ENG', '').strip()
    
    if not rus_name and not eng_name:
        return 'error'  # 🔴 Нет данных вообще
    elif rus_name and not eng_name:
        return 'warning'  # 🟠 Есть только русское название
    elif not rus_name and eng_name:
        return 'warning'  # 🟠 Есть только английское название
    else:
        return 'success'  # 🟢 Есть оба названия

