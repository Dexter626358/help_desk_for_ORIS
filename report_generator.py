#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Генератор HTML отчетов для проверки сгенерированного XML
"""

import xml.etree.ElementTree as ET
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

# Windows консоль часто использует cp1251/cp866 и падает на эмодзи/символах.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def _safe_strip(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _format_article_title(titles: Dict[str, str]) -> str:
    """
    Возвращает человеко-читаемое название статьи для краткой сводки.
    Приоритет: RUS -> ENG -> "(без названия)".
    """
    rus = _safe_strip(titles.get("RUS", ""))
    if rus:
        return rus
    eng = _safe_strip(titles.get("ENG", ""))
    if eng:
        return eng
    return "(без названия)"


def collect_article_issues(article: Dict[str, Any]) -> List[tuple[str, str]]:
    """
    Собирает краткий список проблем по статье для сводки в начале отчёта.
    Возвращает пустой список, если проблем не найдено.
    """
    # (severity, text), где severity: "critical" | "secondary"
    issues: List[tuple[str, str]] = []

    titles = article.get("titles", {}) or {}
    if not _safe_strip(titles.get("RUS", "")):
        issues.append(("critical", "нет названия (RUS)"))
    if not _safe_strip(titles.get("ENG", "")):
        issues.append(("critical", "нет названия (ENG)"))

    abstracts = article.get("abstracts", {}) or {}
    rus_abstract = abstracts.get("RUS", {})
    eng_abstract = abstracts.get("ENG", {})

    rus_full = rus_abstract.get("full_text", "") if isinstance(rus_abstract, dict) else rus_abstract
    eng_full = eng_abstract.get("full_text", "") if isinstance(eng_abstract, dict) else eng_abstract

    rus_full_s = _safe_strip(rus_full)
    eng_full_s = _safe_strip(eng_full)

    if not rus_full_s:
        issues.append(("critical", "аннотация RUS: отсутствует"))
    else:
        rus_len_check = annotation_length_check(rus_full_s)
        if rus_len_check:
            severity = "critical" if "❌" in rus_len_check else "secondary"
            issues.append(("secondary" if severity == "secondary" else "critical", f"аннотация RUS: {rus_len_check.replace('❌ ', '').replace('⚠️ ', '')}"))

    if not eng_full_s:
        issues.append(("critical", "аннотация ENG: отсутствует"))
    else:
        eng_len_check = annotation_length_check(eng_full_s)
        if eng_len_check:
            severity = "critical" if "❌" in eng_len_check else "secondary"
            issues.append(("secondary" if severity == "secondary" else "critical", f"аннотация ENG: {eng_len_check.replace('❌ ', '').replace('⚠️ ', '')}"))

    keywords_data = article.get("keywords", {}) or {}
    kw_status = validate_keywords_data(keywords_data)
    if "❌" in kw_status.get("RUS", ""):
        issues.append(("critical", "нет ключевых слов (RUS)"))
    if "❌" in kw_status.get("ENG", ""):
        issues.append(("critical", "нет ключевых слов (ENG)"))
    if "⚠️" in kw_status.get("comparison", ""):
        issues.append(("secondary", kw_status["comparison"].replace("⚠️ ", "")))

    references_data = article.get("references", {}) or {}
    ref_status = validate_references_data(references_data)
    if "❌" in ref_status.get("RUS", ""):
        issues.append(("critical", "нет источников (RUS)"))
    if "❌" in ref_status.get("ENG", ""):
        issues.append(("critical", "нет источников (ENG)"))
    if "⚠️" in ref_status.get("comparison", ""):
        issues.append(("secondary", ref_status["comparison"].replace("⚠️ ", "")))

    # Сигнализируем, если хотя бы один автор без данных на каком-то языке
    authors = article.get("authors", []) or []
    if not authors:
        issues.append(("critical", "нет авторов"))
    else:
        for idx, author in enumerate(authors, 1):
            rus = author.get("RUS", {}) or {}
            eng = author.get("ENG", {}) or {}
            rus_name = f"{_safe_strip(rus.get('surname', ''))} {_safe_strip(rus.get('initials', ''))}".strip()
            eng_name = f"{_safe_strip(eng.get('surname', ''))} {_safe_strip(eng.get('initials', ''))}".strip()

            author_validation = validate_author_data(
                {
                    "RUS": {"name": rus_name, "affiliation": _safe_strip(rus.get("orgName", ""))},
                    "ENG": {"name": eng_name, "affiliation": _safe_strip(eng.get("orgName", ""))},
                }
            )
            if author_validation.get("RUS") == "error":
                issues.append(("critical", f"автор {idx}: нет данных (RUS)"))
            if author_validation.get("ENG") == "error":
                issues.append(("critical", f"автор {idx}: нет данных (ENG)"))

    # Дедуплицируем, сохраняя порядок
    seen: set[tuple[str, str]] = set()
    deduped: List[tuple[str, str]] = []
    for item in issues:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def extract_first_last_words(text: str, word_count: int = 10) -> str:
    """
    Извлекает первые и последние N слов из текста
    
    Args:
        text: Исходный текст
        word_count: Количество слов с начала и с конца
        
    Returns:
        str: Строка вида "первые_слова ... последние_слова"
    """
    if not text or not text.strip():
        return ""
    
    words = text.strip().split()
    
    if len(words) <= word_count * 2:
        return text.strip()
    
    first_words = " ".join(words[:word_count])
    last_words = " ".join(words[-word_count:])
    
    return f"{first_words} ... {last_words}"


def get_issue_info(xml_file: Path) -> Dict[str, Any]:
    """
    Извлекает информацию о выпуске из XML файла
    
    Args:
        xml_file: Путь к XML файлу
        
    Returns:
        Dict: Информация о выпуске
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        issue_info = {}
        
        # Основная информация о журнале
        issue_info['titleid'] = root.find('titleid').text if root.find('titleid') is not None else ""
        issue_info['issn'] = root.find('issn').text if root.find('issn') is not None else ""
        issue_info['eissn'] = root.find('eissn').text if root.find('eissn') is not None else ""
        
        # Информация о журнале
        journal_info = root.find('journalInfo')
        if journal_info is not None:
            issue_info['journal_title'] = journal_info.find('title').text if journal_info.find('title') is not None else ""
            issue_info['journal_lang'] = journal_info.get('lang', '')
        
        # Информация о выпуске
        issue = root.find('issue')
        if issue is not None:
            issue_info['volume'] = issue.find('volume').text if issue.find('volume') is not None else ""
            issue_info['number'] = issue.find('number').text if issue.find('number') is not None else ""
            issue_info['date_uni'] = issue.find('dateUni').text if issue.find('dateUni') is not None else ""
            issue_info['pages'] = issue.find('pages').text if issue.find('pages') is not None else ""
        
        return issue_info
        
    except Exception as e:
        logging.error(f"Ошибка при извлечении информации о выпуске: {e}")
        return {}


def split_organizations(org_text: str) -> List[str]:
    """
    Разделяет строку с организациями на отдельные организации
    
    Args:
        org_text: Строка с организациями (может содержать несколько, разделенных точкой с запятой)
        
    Returns:
        List[str]: Список отдельных организаций
    """
    if not org_text or not org_text.strip():
        return []
    
    # Разделяем по точке с запятой и очищаем от лишних пробелов
    orgs = [org.strip() for org in org_text.split(';') if org.strip()]
    return orgs


def annotation_length_check(text: str) -> str:
    """
    Проверяет длину аннотации и возвращает соответствующую пометку
    
    Args:
        text: Текст аннотации
        
    Returns:
        str: Пометка о длине аннотации или пустая строка
    """
    if not text or not text.strip():
        return "❌ Отсутствует"
    
    n_words = len(text.split())
    if n_words < 70:
        return f"❌ Слишком короткая аннотация ({n_words} слов)"
    elif n_words > 250:
        return f"⚠️ Аннотация слишком длинная ({n_words} слов)"
    else:
        return ""  # Нормальная длина


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
    Валидирует данные источников и возвращает статус валидации
    
    Args:
        references_data: Словарь с источниками по языкам
        
    Returns:
        Dict[str, str]: Статус валидации для каждого языка
    """
    validation_status = {}
    
    rus_references = references_data.get('RUS', [])
    eng_references = references_data.get('ENG', [])
    
    # Проверяем русские источники
    if not rus_references:
        validation_status['RUS'] = "❌ Отсутствуют"
    else:
        validation_status['RUS'] = f"✅ {len(rus_references)} источников"
    
    # Проверяем английские источники
    if not eng_references:
        validation_status['ENG'] = "❌ Отсутствуют"
    else:
        validation_status['ENG'] = f"✅ {len(eng_references)} источников"
    
    # Сравниваем количество
    if rus_references and eng_references:
        rus_count = len(rus_references)
        eng_count = len(eng_references)
        if rus_count != eng_count:
            validation_status['comparison'] = f"⚠️ Разное количество: RUS={rus_count}, ENG={eng_count}"
        else:
            validation_status['comparison'] = f"✅ Одинаковое количество: {rus_count} источников"
    elif rus_references or eng_references:
        validation_status['comparison'] = "⚠️ Источники только на одном языке"
    else:
        validation_status['comparison'] = "❌ Источники отсутствуют на обоих языках"
    
    return validation_status


def get_first_last_references(references_list: List[str], max_length: int = None) -> Dict[str, str]:
    """
    Извлекает первый и последний источник из списка
    
    Args:
        references_list: Список источников
        max_length: Максимальная длина для отображения (None = без ограничений)
        
    Returns:
        Dict[str, str]: Первый и последний источник
    """
    if not references_list:
        return {'first': '', 'last': ''}
    
    first_ref = references_list[0]
    last_ref = references_list[-1]
    
    # Обрезаем длинные источники только если указан max_length
    if max_length is not None:
        if len(first_ref) > max_length:
            first_ref = first_ref[:max_length] + "..."
        if len(last_ref) > max_length:
            last_ref = last_ref[:max_length] + "..."
    
    return {'first': first_ref, 'last': last_ref}


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


def parse_page_number(pages_str: str) -> int:
    """
    Парсит номер страницы из строки с диапазоном страниц
    
    Args:
        pages_str: Строка с номерами страниц (например, "4-9", "15", "10-15")
        
    Returns:
        int: Номер первой страницы для сортировки
    """
    if not pages_str or not pages_str.strip():
        return 0
    
    # Убираем лишние пробелы
    pages_str = pages_str.strip()
    
    # Если есть диапазон (например, "4-9"), берем первую страницу
    if '-' in pages_str:
        try:
            first_page = int(pages_str.split('-')[0].strip())
            return first_page
        except (ValueError, IndexError):
            return 0
    
    # Если это просто число
    try:
        return int(pages_str)
    except ValueError:
        return 0


def sort_articles_by_pages(articles_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Сортирует статьи по номерам страниц
    
    Args:
        articles_info: Список информации о статьях
        
    Returns:
        List[Dict]: Отсортированный список статей
    """
    def get_sort_key(article):
        pages = article.get('pages', '')
        return parse_page_number(pages)
    
    return sorted(articles_info, key=get_sort_key)


def compare_author_fields(rus_data: Dict[str, str], eng_data: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Сравнивает поля автора между русской и английской версиями
    
    Args:
        rus_data: Русские данные автора
        eng_data: Английские данные автора
        
    Returns:
        List[Dict]: Список сравнений полей
    """
    comparisons = []
    
    # Сравниваем фамилию
    rus_surname = rus_data.get('surname', '').strip()
    eng_surname = eng_data.get('surname', '').strip()
    surname_match = compare_text_fields(rus_surname, eng_surname)
    comparisons.append({
        'field': 'Фамилия',
        'rus': rus_surname,
        'eng': eng_surname,
        'match': surname_match['status'],
        'details': surname_match['details']
    })
    
    # Сравниваем инициалы
    rus_initials = rus_data.get('initials', '').strip()
    eng_initials = eng_data.get('initials', '').strip()
    initials_match = compare_text_fields(rus_initials, eng_initials)
    comparisons.append({
        'field': 'Инициалы',
        'rus': rus_initials,
        'eng': eng_initials,
        'match': initials_match['status'],
        'details': initials_match['details']
    })
    
    # Сравниваем организацию
    rus_org = rus_data.get('orgName', '').strip()
    eng_org = eng_data.get('orgName', '').strip()
    org_match = compare_organization_fields(rus_org, eng_org)
    
    # Разделяем организации для отображения
    rus_orgs = rus_data.get('organizations', [])
    eng_orgs = eng_data.get('organizations', [])
    
    # Формируем HTML для отображения организаций
    rus_orgs_html = ""
    eng_orgs_html = ""
    
    if rus_orgs:
        for org in rus_orgs:
            rus_orgs_html += f"<div>{org}</div>"
    else:
        rus_orgs_html = rus_org
    
    if eng_orgs:
        for org in eng_orgs:
            eng_orgs_html += f"<div>{org}</div>"
    else:
        eng_orgs_html = eng_org
    
    comparisons.append({
        'field': 'Организация',
        'rus': rus_orgs_html,
        'eng': eng_orgs_html,
        'match': org_match['status'],
        'details': org_match['details']
    })
    
    # Сравниваем адрес
    rus_address = rus_data.get('address', '').strip()
    eng_address = eng_data.get('address', '').strip()
    address_match = compare_text_fields(rus_address, eng_address)
    
    # Разделяем адреса для отображения
    rus_addresses = split_organizations(rus_address) if rus_address else []
    eng_addresses = split_organizations(eng_address) if eng_address else []
    
    # Формируем HTML для отображения адресов
    rus_addresses_html = ""
    eng_addresses_html = ""
    
    if rus_addresses:
        for addr in rus_addresses:
            rus_addresses_html += f"<div>{addr}</div>"
    else:
        rus_addresses_html = rus_address
    
    if eng_addresses:
        for addr in eng_addresses:
            eng_addresses_html += f"<div>{addr}</div>"
    else:
        eng_addresses_html = eng_address
    
    comparisons.append({
        'field': 'Адрес',
        'rus': rus_addresses_html,
        'eng': eng_addresses_html,
        'match': address_match['status'],
        'details': address_match['details']
    })
    
    # Сравниваем email
    rus_email = rus_data.get('email', '').strip()
    eng_email = eng_data.get('email', '').strip()
    email_match = compare_text_fields(rus_email, eng_email)
    comparisons.append({
        'field': 'Email',
        'rus': rus_email,
        'eng': eng_email,
        'match': email_match['status'],
        'details': email_match['details']
    })
    
    return comparisons


def compare_text_fields(rus_text: str, eng_text: str) -> Dict[str, Any]:
    """
    Сравнивает текстовые поля между русской и английской версиями
    
    Args:
        rus_text: Русский текст
        eng_text: Английский текст
        
    Returns:
        Dict: Результат сравнения
    """
    if not rus_text and not eng_text:
        return {'status': 'error', 'details': 'Нет данных'}
    elif not rus_text:
        return {'status': 'warning', 'details': 'Нет русского текста'}
    elif not eng_text:
        return {'status': 'warning', 'details': 'Нет английского текста'}
    elif rus_text == eng_text:
        return {'status': 'success', 'details': 'Точное совпадение'}
    else:
        # Простая проверка на транслитерацию
        if is_likely_transliteration(rus_text, eng_text):
            return {'status': 'success', 'details': 'Транслитерация'}
        else:
            return {'status': 'warning', 'details': 'Разные значения'}


def compare_organization_fields(rus_org: str, eng_org: str) -> Dict[str, Any]:
    """
    Сравнивает поля организаций с учетом специфики
    
    Args:
        rus_org: Русское название организации
        eng_org: Английское название организации
        
    Returns:
        Dict: Результат сравнения
    """
    if not rus_org and not eng_org:
        return {'status': 'error', 'details': 'Нет данных'}
    elif not rus_org:
        return {'status': 'warning', 'details': 'Нет русского названия'}
    elif not eng_org:
        return {'status': 'warning', 'details': 'Нет английского названия'}
    elif rus_org == eng_org:
        return {'status': 'success', 'details': 'Точное совпадение'}
    else:
        # Проверяем на наличие аббревиатур в русском тексте
        if has_abbreviations(rus_org) and not has_abbreviations(eng_org):
            return {'status': 'warning', 'details': 'Аббревиатуры не переведены'}
        elif is_likely_translation(rus_org, eng_org):
            return {'status': 'success', 'details': 'Перевод'}
        else:
            return {'status': 'warning', 'details': 'Разные значения'}


def is_likely_transliteration(rus_text: str, eng_text: str) -> bool:
    """
    Проверяет, является ли английский текст транслитерацией русского
    
    Args:
        rus_text: Русский текст
        eng_text: Английский текст
        
    Returns:
        bool: True если похоже на транслитерацию
    """
    # Простая проверка: если длины примерно одинаковые и есть общие символы
    if abs(len(rus_text) - len(eng_text)) > 2:
        return False
    
    # Проверяем наличие общих символов (цифры, знаки препинания)
    common_chars = set(rus_text.lower()) & set(eng_text.lower())
    return len(common_chars) > 0


def has_abbreviations(text: str) -> bool:
    """
    Проверяет наличие аббревиатур в тексте
    
    Args:
        text: Текст для проверки
        
    Returns:
        bool: True если есть аббревиатуры
    """
    # Ищем паттерны аббревиатур (заглавные буквы, точки)
    import re
    abbreviation_pattern = r'\b[А-ЯЁ]{2,}\b|\b[A-Z]{2,}\b'
    return bool(re.search(abbreviation_pattern, text))


def is_likely_translation(rus_text: str, eng_text: str) -> bool:
    """
    Проверяет, является ли английский текст переводом русского
    
    Args:
        rus_text: Русский текст
        eng_text: Английский текст
        
    Returns:
        bool: True если похоже на перевод
    """
    # Простая эвристика: если английский текст длиннее русского
    # (переводы обычно длиннее оригинала)
    return len(eng_text) > len(rus_text) * 0.8


def extract_organizations_from_article(article: ET.Element) -> List[Dict[str, str]]:
    """
    Извлекает уникальные организации из статьи
    
    Args:
        article: XML элемент статьи
        
    Returns:
        List[Dict]: Список уникальных организаций с русскими и английскими названиями
    """
    organizations = []
    org_dict = {}  # Для группировки по русскому названию
    
    authors = article.find('authors')
    if authors is not None:
        for author in authors.findall('author'):
            for individ_info in author.findall('individInfo'):
                lang = individ_info.get('lang', '')
                org_name = individ_info.find('orgName')
                
                if org_name is not None and org_name.text:
                    org_text = org_name.text.strip()
                    
                    # Разделяем организации по точке с запятой
                    org_list = split_organizations(org_text)
                    
                    for single_org in org_list:
                        if lang == 'RUS':
                            # Если русская организация еще не встречалась
                            if single_org not in org_dict:
                                org_dict[single_org] = {'RUS': single_org, 'ENG': ''}
                        elif lang == 'ENG':
                            # Ищем соответствующую русскую организацию
                            found_rus_org = False
                            for rus_org in org_dict:
                                if single_org not in [org_dict[rus_org]['ENG'] for rus_org in org_dict]:
                                    # Проверяем, есть ли уже английская версия для этой русской организации
                                    if not org_dict[rus_org]['ENG']:
                                        org_dict[rus_org]['ENG'] = single_org
                                        found_rus_org = True
                                        break
                            
                            # Если не нашли соответствующую русскую организацию, создаем новую
                            if not found_rus_org:
                                org_dict[single_org] = {'RUS': '', 'ENG': single_org}
    
    # Преобразуем словарь в список
    for org_data in org_dict.values():
        if org_data['RUS'] or org_data['ENG']:  # Добавляем только если есть хотя бы одно название
            organizations.append(org_data)
    
    return organizations


def get_articles_info(xml_file: Path) -> List[Dict[str, Any]]:
    """
    Извлекает информацию о статьях из XML файла
    
    Args:
        xml_file: Путь к XML файлу
        
    Returns:
        List[Dict]: Список информации о статьях
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Подготовка: быстрый доступ к extraction_info из JSON (если есть)
        jsons_dir = xml_file.parent / "jsons"
        extraction_by_pdf: Dict[str, Dict[str, Any]] = {}
        if jsons_dir.exists():
            try:
                for jf in jsons_dir.glob("*.json"):
                    try:
                        with open(jf, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        if not isinstance(data, dict):
                            continue
                        file_meta = data.get("file_metadata", {}) or {}
                        pdf_name = str(file_meta.get("name") or "").strip()
                        if not pdf_name:
                            continue
                        ex = data.get("extraction_info")
                        if isinstance(ex, dict):
                            extraction_by_pdf[pdf_name] = ex
                    except Exception:
                        continue
            except Exception:
                extraction_by_pdf = {}
        
        articles_info = []
        articles = root.findall('.//article')
        
        for article in articles:
            article_data = {}
            
            # Основная информация
            article_data['pages'] = article.find('pages').text if article.find('pages') is not None else ""
            article_data['art_type'] = article.find('artType').text if article.find('artType') is not None else ""

            # Определяем PDF fullText из XML, чтобы подтянуть extraction_info
            pdf_fulltext_name = ""
            files_elem = article.find("files")
            if files_elem is not None:
                for f_el in files_elem.findall("file"):
                    if f_el.get("desc") == "fullText" and f_el.text:
                        pdf_fulltext_name = f_el.text.strip()
                        break

            ex = extraction_by_pdf.get(pdf_fulltext_name) if pdf_fulltext_name else None
            if isinstance(ex, dict):
                article_data["references_start_page"] = ex.get("references_start_page")
            
            # Названия статей
            art_titles = article.find('artTitles')
            if art_titles is not None:
                titles = {}
                for title_elem in art_titles.findall('artTitle'):
                    lang = title_elem.get('lang', '')
                    title_text = title_elem.text if title_elem.text else ""
                    titles[lang] = title_text
                article_data['titles'] = titles
            
            # Извлекаем организации
            article_data['organizations'] = extract_organizations_from_article(article)
            
            # Авторы
            authors = article.find('authors')
            if authors is not None:
                authors_list = []
                for author in authors.findall('author'):
                    author_data = {}
                    
                    # Информация об авторе
                    for individ_info in author.findall('individInfo'):
                        lang = individ_info.get('lang', '')
                        author_lang_data = {}
                        
                        # ФИО
                        surname = individ_info.find('surname')
                        initials = individ_info.find('initials')
                        if surname is not None:
                            author_lang_data['surname'] = surname.text or ''
                        if initials is not None:
                            author_lang_data['initials'] = initials.text or ''
                        
                        # Аффилиация
                        org_name = individ_info.find('orgName')
                        if org_name is not None:
                            org_text = org_name.text or ""
                            # Разделяем организации по точке с запятой
                            org_list = split_organizations(org_text)
                            author_lang_data['orgName'] = org_text  # Сохраняем оригинальный текст
                            author_lang_data['organizations'] = org_list  # Добавляем список организаций
                        
                        # Адрес
                        address = individ_info.find('address')
                        if address is not None:
                            author_lang_data['address'] = address.text or ""
                        
                        # Email
                        email = individ_info.find('email')
                        if email is not None:
                            author_lang_data['email'] = email.text or ""
                        
                        author_data[lang] = author_lang_data
                    
                    authors_list.append(author_data)
                
                article_data['authors'] = authors_list
            
            # Аннотации
            abstracts = article.find('abstracts')
            if abstracts is not None:
                abstracts_data = {}
                for abstract_elem in abstracts.findall('abstract'):
                    lang = abstract_elem.get('lang', '')
                    abstract_text = abstract_elem.text if abstract_elem.text else ""
                    # Сохраняем полную аннотацию для проверки длины и выжимку для отображения
                    abstracts_data[lang] = {
                        'full_text': abstract_text,
                        'summary': extract_first_last_words(abstract_text, 10)
                    }
                article_data['abstracts'] = abstracts_data
            
            # Ключевые слова
            keywords = article.find('keywords')
            if keywords is not None:
                keywords_data = {}
                keywords_count = {}
                for kwd_group in keywords.findall('kwdGroup'):
                    lang = kwd_group.get('lang', '')
                    keyword_list = []
                    for kw in kwd_group.findall('keyword'):
                        if kw.text:
                            keyword_list.append(kw.text)
                    keywords_data[lang] = keyword_list
                    keywords_count[lang] = len(keyword_list)
                article_data['keywords'] = keywords_data
                article_data['keywords_count'] = keywords_count
            
            # Источники
            references = article.find('references')
            if references is not None:
                refs_data = {}
                refs_count = {}
                for ref_elem in references.findall('reference'):
                    # Ищем refInfo с атрибутом lang
                    ref_info = ref_elem.find('refInfo')
                    if ref_info is not None:
                        lang = ref_info.get('lang', '')
                        if lang not in refs_data:
                            refs_data[lang] = []
                            refs_count[lang] = 0
                        
                        # Извлекаем текст источника из элемента text внутри refInfo
                        text_elem = ref_info.find('text')
                        if text_elem is not None and text_elem.text:
                            ref_text = text_elem.text.strip()
                            if ref_text:
                                refs_data[lang].append(ref_text)
                                refs_count[lang] += 1
                
                article_data['references'] = refs_data
                article_data['references_count'] = refs_count
            
            articles_info.append(article_data)
        
        # Сортируем статьи по номерам страниц
        sorted_articles = sort_articles_by_pages(articles_info)
        
        return sorted_articles
        
    except Exception as e:
        logging.error(f"Ошибка при извлечении информации о статьях: {e}")
        return []


def generate_html_report(xml_file, output_file: Optional[Path] = None) -> Path:
    """
    Генерирует HTML отчет для проверки XML файла
    
    Args:
        xml_file: Путь к XML файлу
        output_file: Путь для сохранения HTML отчета (по умолчанию рядом с XML)
        
    Returns:
        Path: Путь к созданному HTML файлу
    """
    # Преобразуем xml_file в Path если это строка
    if isinstance(xml_file, str):
        xml_file = Path(xml_file)
    
    if output_file is None:
        # Создаем имя файла с префиксом "report_"
        report_name = f"report_{xml_file.stem}.html"
        output_file = xml_file.parent / report_name
    
    # Извлекаем данные
    issue_info = get_issue_info(xml_file)
    articles_info = get_articles_info(xml_file)
    
    # Генерируем HTML
    html_content = generate_html_content(issue_info, articles_info, xml_file.name)
    
    # Сохраняем файл
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logging.info(f"HTML отчет создан: {output_file}")
    return output_file


def generate_html_content(issue_info: Dict[str, Any], 
                         articles_info: List[Dict[str, Any]], 
                         xml_filename: str) -> str:
    """
    Генерирует HTML содержимое отчета
    
    Args:
        issue_info: Информация о выпуске
        articles_info: Информация о статьях
        xml_filename: Имя XML файла
        
    Returns:
        str: HTML содержимое
    """
    current_time = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    html = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Отчет по XML файлу: {xml_filename}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 3px solid #007acc;
        }}
        .header h1 {{
            color: #007acc;
            margin: 0;
            font-size: 2.2em;
        }}
        .header .subtitle {{
            color: #666;
            margin-top: 10px;
            font-size: 1.1em;
        }}
        .issue-info {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            border-left: 4px solid #007acc;
        }}
        .issues-summary {{
            background: #fff8f8;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            border-left: 4px solid #dc3545;
        }}
        .issues-summary h2 {{
            color: #dc3545;
            margin-top: 0;
            font-size: 1.5em;
        }}
        .issues-summary ul {{
            margin: 10px 0 0 0;
            padding-left: 20px;
        }}
        .issues-summary li {{
            margin-bottom: 10px;
        }}
        .issues-summary .article-link {{
            text-decoration: none;
            color: #007acc;
            font-weight: bold;
        }}
        .issues-summary .article-link:hover {{
            text-decoration: underline;
        }}
        .issues-summary .issues-list {{
            margin-top: 4px;
            color: #333;
        }}
        .issues-summary .issue-tag {{
            display: inline-block;
            background: #ffe5e7;
            border: 1px solid #f5c2c7;
            color: #842029;
            padding: 2px 8px;
            border-radius: 999px;
            font-size: 0.85em;
            margin: 0 6px 6px 0;
        }}
        .issues-summary .issue-tag.critical {{
            background: #ffe5e7;
            border: 1px solid #f5c2c7;
            color: #842029;
        }}
        .issues-summary .issue-tag.secondary {{
            background: #fff3cd;
            border: 1px solid #ffecb5;
            color: #664d03;
        }}
        .issue-info h2 {{
            color: #007acc;
            margin-top: 0;
            font-size: 1.5em;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .info-item {{
            background: white;
            padding: 12px;
            border-radius: 5px;
            border: 1px solid #ddd;
        }}
        .info-label {{
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }}
        .info-value {{
            color: #666;
        }}
        .articles-section {{
            margin-top: 30px;
        }}
        .articles-section h2 {{
            color: #007acc;
            font-size: 1.8em;
            margin-bottom: 20px;
        }}
        .article {{
            background: #f8f9fa;
            margin-bottom: 25px;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #28a745;
        }}
        .article-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #ddd;
        }}
        .article-number {{
            background: #007acc;
            color: white;
            padding: 5px 12px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 0.9em;
        }}
        .article-pages {{
            color: #666;
            font-weight: bold;
        }}
        .article-ref-start {{
            color: #666;
            font-size: 0.9em;
            margin-top: 4px;
        }}
        .article-titles {{
            margin-bottom: 15px;
        }}
        .title-block {{
            margin-bottom: 10px;
        }}
        .title-lang {{
            font-weight: bold;
            color: #007acc;
            margin-bottom: 5px;
        }}
        .title-text {{
            font-size: 1.1em;
            line-height: 1.4;
        }}
        .authors-section {{
            margin-bottom: 15px;
        }}
        .authors-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
        }}
        .author {{
            background: white;
            padding: 10px;
            margin-bottom: 8px;
            border-radius: 5px;
            border: 1px solid #ddd;
        }}
        .author-name {{
            font-weight: bold;
            color: #333;
        }}
        .author-affiliation {{
            color: #666;
            font-size: 0.9em;
            margin-top: 3px;
        }}
        .author-compact {{
            background: white;
            padding: 8px;
            margin-bottom: 6px;
            border-radius: 5px;
            border: 1px solid #ddd;
        }}
        .author-names {{
            font-weight: bold;
            color: #333;
            margin-bottom: 4px;
        }}
        .author-affiliations {{
            color: #666;
            font-size: 0.85em;
        }}
        .author-names .lang-badge,
        .author-affiliations .lang-badge {{
            margin-right: 5px;
            margin-left: 8px;
        }}
        .author-names .lang-badge:first-child,
        .author-affiliations .lang-badge:first-child {{
            margin-left: 0;
        }}
        .abstract-section {{
            margin-bottom: 15px;
        }}
        .abstract-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 8px;
        }}
        .abstract-text {{
            background: white;
            padding: 10px;
            border-radius: 5px;
            border: 1px solid #ddd;
            font-style: italic;
            color: #555;
        }}
        .stats-section {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-top: 15px;
        }}
        .stat-item {{
            background: white;
            padding: 10px;
            border-radius: 5px;
            border: 1px solid #ddd;
            text-align: center;
        }}
        .stat-number {{
            font-size: 1.5em;
            font-weight: bold;
            color: #007acc;
        }}
        .stat-label {{
            font-size: 0.9em;
            color: #666;
            margin-top: 3px;
        }}
        .stat-status {{
            font-size: 0.8em;
            margin-top: 3px;
            font-weight: bold;
        }}
        .reference-examples {{
            margin-top: 8px;
            font-size: 0.75em;
            text-align: left;
        }}
        .reference-item {{
            margin-bottom: 4px;
            padding: 3px;
            background: #f8f9fa;
            border-radius: 3px;
            border-left: 3px solid #007acc;
        }}
        .keywords-comparison {{
            grid-column: 1 / -1;
        }}
        .keywords-row {{
            display: flex;
            gap: 20px;
            margin-bottom: 8px;
        }}
        .keyword-lang {{
            flex: 1;
            text-align: center;
        }}
        .comparison-row {{
            text-align: center;
            margin-top: 5px;
        }}
        .references-comparison {{
            grid-column: 1 / -1;
        }}
        .references-row {{
            display: flex;
            gap: 20px;
            margin-bottom: 8px;
        }}
        .reference-lang {{
            flex: 1;
            text-align: center;
        }}
        .full-width {{
            grid-column: 1 / -1;
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #666;
            font-size: 0.9em;
        }}
        .lang-badge {{
            display: inline-block;
            background: #007acc;
            color: white;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            margin-right: 8px;
        }}
        .organizations-section {{
            margin-bottom: 15px;
        }}
        .organizations-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
        }}
        .organization {{
            background: white;
            padding: 12px;
            margin-bottom: 8px;
            border-radius: 5px;
            border: 1px solid #ddd;
            border-left: 4px solid #28a745;
        }}
        .organization-name {{
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }}
        .organization-translation {{
            color: #666;
            font-size: 0.9em;
            font-style: italic;
        }}
        .organizations-list {{
            counter-reset: org-counter;
            list-style: none;
            padding: 0;
            margin: 0;
        }}
        .organization-item {{
            counter-increment: org-counter;
            background: white;
            padding: 12px 12px 12px 45px;
            margin-bottom: 8px;
            border-radius: 5px;
            border: 1px solid #ddd;
            border-left: 4px solid #28a745;
            position: relative;
        }}
        .organization-item::before {{
            content: counter(org-counter);
            position: absolute;
            left: 12px;
            top: 50%;
            transform: translateY(-50%);
            background: #28a745;
            color: white;
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 0.9em;
        }}
        .organization-names {{
            font-weight: bold;
            color: #333;
            margin-bottom: 4px;
        }}
        .organization-translations {{
            color: #666;
            font-size: 0.85em;
            font-style: italic;
        }}
        .organization-name-line {{
            margin-bottom: 3px;
        }}
        .organization-name-line:last-child {{
            margin-bottom: 0;
        }}
        .organization-compact {{
            background: white;
            padding: 10px;
            margin-bottom: 6px;
            border-radius: 5px;
            border: 1px solid #ddd;
            border-left: 3px solid #28a745;
        }}
        /* Стили для валидации */
        .validation-success {{
            border-left-color: #28a745 !important;
            background-color: #f8fff8 !important;
        }}
        .validation-warning {{
            border-left-color: #ffc107 !important;
            background-color: #fffdf0 !important;
        }}
        .validation-error {{
            border-left-color: #dc3545 !important;
            background-color: #fff5f5 !important;
        }}
        .validation-indicator {{
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
            vertical-align: middle;
        }}
        .validation-indicator.success {{
            background-color: #28a745;
        }}
        .validation-indicator.warning {{
            background-color: #ffc107;
        }}
        .validation-indicator.error {{
            background-color: #dc3545;
        }}
        .validation-status {{
            font-size: 0.8em;
            font-weight: bold;
            margin-left: 8px;
        }}
        .validation-status.success {{
            color: #28a745;
        }}
        .validation-status.warning {{
            color: #ffc107;
        }}
        .validation-status.error {{
            color: #dc3545;
        }}
        /* Стили для таблицы сравнения */
        .comparison-section {{
            margin-bottom: 20px;
        }}
        .comparison-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
        }}
        .comparison-table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 15px;
            background: white;
            border-radius: 5px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .comparison-table th {{
            background: #f8f9fa;
            color: #333;
            font-weight: bold;
            padding: 12px 8px;
            text-align: left;
            border-bottom: 2px solid #dee2e6;
        }}
        .comparison-table td {{
            padding: 10px 8px;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }}
        .comparison-table tr:last-child td {{
            border-bottom: none;
        }}
        .comparison-field {{
            font-weight: bold;
            color: #495057;
            min-width: 100px;
        }}
        .comparison-rus {{
            color: #333;
            max-width: 200px;
            word-wrap: break-word;
        }}
        .comparison-eng {{
            color: #333;
            max-width: 200px;
            word-wrap: break-word;
        }}
        .comparison-match {{
            text-align: center;
            min-width: 80px;
        }}
        .match-success {{
            color: #28a745;
            font-weight: bold;
        }}
        .match-warning {{
            color: #ffc107;
            font-weight: bold;
        }}
        .match-error {{
            color: #dc3545;
            font-weight: bold;
        }}
        .match-details {{
            font-size: 0.8em;
            color: #6c757d;
            font-style: italic;
        }}
        /* Стили для списка авторов */
        .authors-list-section {{
            margin-bottom: 20px;
        }}
        .authors-list-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        .authors-list {{
            background: white;
            border-radius: 5px;
            padding: 15px;
            border: 1px solid #ddd;
            border-left: 4px solid #007acc;
        }}
        .authors-count {{
            background: #f8f9fa;
            padding: 8px 12px;
            border-radius: 4px;
            margin-bottom: 10px;
            font-weight: bold;
            color: #007acc;
            border: 1px solid #dee2e6;
        }}
        .author-item {{
            padding: 8px 0;
            border-bottom: 1px solid #f0f0f0;
            display: flex;
            align-items: center;
        }}
        .author-item:last-child {{
            border-bottom: none;
        }}
        .author-number {{
            background: #007acc;
            color: white;
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8em;
            font-weight: bold;
            margin-right: 12px;
            flex-shrink: 0;
        }}
        .author-names {{
            flex: 1;
        }}
        .author-name-line {{
            margin-bottom: 2px;
        }}
        .author-name-line:last-child {{
            margin-bottom: 0;
        }}
        /* Упрощённый блок авторов */
        .authors-simple-section {{
            margin-bottom: 15px;
        }}
        .authors-simple-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
        }}
        .authors-simple-table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 6px;
            overflow: hidden;
            border: 1px solid #ddd;
        }}
        .authors-simple-table th {{
            background: #f8f9fa;
            color: #333;
            font-weight: bold;
            padding: 10px 8px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }}
        .authors-simple-table td {{
            padding: 10px 8px;
            border-bottom: 1px solid #f0f0f0;
            vertical-align: top;
            color: #333;
        }}
        .authors-simple-table tr:last-child td {{
            border-bottom: none;
        }}
        .authors-simple-meta {{
            color: #666;
            font-size: 0.9em;
            margin-top: 4px;
        }}
        .authors-simple-org {{
            margin-top: 4px;
            color: #333;
        }}
        .authors-simple-email-ok {{
            color: #28a745;
            font-weight: bold;
        }}
        .authors-simple-email-missing {{
            color: #dc3545;
            font-weight: bold;
        }}
        /* Чек-лист по статье */
        .article-checklist {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 8px 14px;
            background: white;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 12px;
            margin: 12px 0 16px 0;
        }}
        .article-checklist .ok {{
            color: #198754;
            font-weight: bold;
        }}
        .article-checklist .warning {{
            color: #fd7e14;
            font-weight: bold;
        }}
        .article-checklist .error {{
            color: #dc3545;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Отчет по XML файлу</h1>
            <div class="subtitle">
                Файл: <strong>{xml_filename}</strong><br>
                Создан: {current_time}
            </div>
        </div>
        
        <div class="issue-info">
            <h2>📖 Информация о выпуске</h2>
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">ID журнала</div>
                    <div class="info-value">{issue_info.get('titleid', 'Не указан')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">ISSN</div>
                    <div class="info-value">{issue_info.get('issn', 'Не указан')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">e-ISSN</div>
                    <div class="info-value">{issue_info.get('eissn', 'Не указан')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Название журнала</div>
                    <div class="info-value">{issue_info.get('journal_title', 'Не указано')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Том</div>
                    <div class="info-value">{issue_info.get('volume', 'Не указан')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Номер</div>
                    <div class="info-value">{issue_info.get('number', 'Не указан')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Дата выпуска</div>
                    <div class="info-value">{issue_info.get('date_uni', 'Не указана')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Страницы выпуска</div>
                    <div class="info-value">{issue_info.get('pages', 'Не указаны')}</div>
                </div>
            </div>
        </div>
        
"""
    # Сводка по проблемным статьям (после информации о выпуске)
    articles_with_issues: List[Dict[str, Any]] = []
    for idx, article in enumerate(articles_info, 1):
        issues = collect_article_issues(article)
        if issues:
            articles_with_issues.append(
                {
                    "index": idx,
                    "pages": _safe_strip(article.get("pages", "")),
                    "title": _format_article_title(article.get("titles", {}) or {}),
                    "issues": issues,
                }
            )

    if articles_with_issues:
        html += f"""
        <div class="issues-summary">
            <h2>🚨 Статьи с ошибками ({len(articles_with_issues)} шт.)</h2>
            <ul>
"""
        for item in articles_with_issues:
            pages_part = f" — стр. {item['pages']}" if item["pages"] else ""
            html += f"""
                <li>
                    <a class="article-link" href="#article-{item['index']}">Статья {item['index']}{pages_part}</a><br>
                    <div class="issues-list">
                        <div><strong>{item['title']}</strong></div>
"""
            for severity, issue in item["issues"]:
                severity_class = "critical" if severity == "critical" else "secondary"
                html += f"""                        <span class="issue-tag {severity_class}">{issue}</span>"""
            html += """
                    </div>
                </li>
"""
        html += """
            </ul>
        </div>
"""

    html += f"""
        <div class="articles-section">
            <h2>📚 Статьи ({len(articles_info)} шт.)</h2>
"""
    
    # Добавляем информацию о статьях
    for i, article in enumerate(articles_info, 1):
        html += f"""
            <div class="article" id="article-{i}">
                <div class="article-header">
                    <div class="article-number">Статья {i}</div>
                    <div style="text-align: right;">
                        <div class="article-pages">Стр. {article.get('pages', 'Не указаны')}</div>
                        <div class="article-ref-start">Литература: стр. {article.get('references_start_page', '—')}</div>
                    </div>
                </div>
                
                <div class="article-titles">
"""
        
        # Названия статей
        titles = article.get('titles', {})
        
        # Показываем русское название
        rus_title = titles.get('RUS', '')
        if rus_title:
            html += f"""
                    <div class="title-block">
                        <div class="title-lang">🇷🇺 RUS</div>
                        <div class="title-text">{rus_title}</div>
                    </div>
"""
        else:
            html += f"""
                    <div class="title-block">
                        <div class="title-lang">🇷🇺 RUS</div>
                        <div class="title-text" style="color: #dc3545; font-style: italic;">❌ Отсутствует</div>
                    </div>
"""
        
        # Показываем английское название
        eng_title = titles.get('ENG', '')
        if eng_title:
            html += f"""
                    <div class="title-block">
                        <div class="title-lang">🇬🇧 ENG</div>
                        <div class="title-text">{eng_title}</div>
                    </div>
"""
        else:
            html += f"""
                    <div class="title-block">
                        <div class="title-lang">🇬🇧 ENG</div>
                        <div class="title-text" style="color: #dc3545; font-style: italic;">❌ Отсутствует</div>
                    </div>
"""
        
        html += """
                </div>
                
"""
        # Чек-лист статьи (быстрая диагностика)
        titles = article.get("titles", {}) or {}
        rus_title_present = bool(_safe_strip(titles.get("RUS", "")))
        eng_title_present = bool(_safe_strip(titles.get("ENG", "")))

        abstracts = article.get("abstracts", {}) or {}
        rus_abs = abstracts.get("RUS", {})
        eng_abs = abstracts.get("ENG", {})
        rus_abs_full = rus_abs.get("full_text", "") if isinstance(rus_abs, dict) else rus_abs
        eng_abs_full = eng_abs.get("full_text", "") if isinstance(eng_abs, dict) else eng_abs
        rus_abs_full = _safe_strip(rus_abs_full)
        eng_abs_full = _safe_strip(eng_abs_full)

        # Длина аннотации: '' / '❌ ...' / '⚠️ ...'
        rus_abs_len_status = annotation_length_check(rus_abs_full) if rus_abs_full else "❌ Отсутствует"
        eng_abs_len_status = annotation_length_check(eng_abs_full) if eng_abs_full else "❌ Отсутствует"

        keywords_data = article.get("keywords", {}) or {}
        kw_status = validate_keywords_data(keywords_data)
        refs_data = article.get("references", {}) or {}
        ref_status = validate_references_data(refs_data)

        def _as_check_item(label: str, status_text: str) -> str:
            # status_text: '' (ok) | startswith('❌') | startswith('⚠️')
            if not status_text:
                return f'<div class="ok">✔ {label}</div>'
            if status_text.startswith("⚠️") or status_text.startswith("⚠"):
                return f'<div class="warning">⚠ {label}</div>'
            return f'<div class="error">❌ {label}</div>'

        # Название: требуем хотя бы одно, но подсвечиваем если нет обеих версий
        if rus_title_present and eng_title_present:
            title_status = ""
            title_label = "Название"
        elif rus_title_present or eng_title_present:
            title_status = "⚠️ Только один язык"
            title_label = "Название (не на всех языках)"
        else:
            title_status = "❌ Отсутствует"
            title_label = "Название"

        # Аннотации: отдельно по языкам
        rus_abs_status = "" if rus_abs_len_status == "" else rus_abs_len_status
        eng_abs_status = "" if eng_abs_len_status == "" else eng_abs_len_status

        # Ключевые слова: error если нет на любом языке, warning если только на одном языке или разное количество
        kw_overall_status = ""
        if "❌" in kw_status.get("RUS", "") or "❌" in kw_status.get("ENG", ""):
            kw_overall_status = "❌"
        elif "⚠️" in kw_status.get("comparison", ""):
            kw_overall_status = "⚠️"

        # Источники: error если нет на любом языке, warning если только на одном языке или разное количество
        ref_overall_status = ""
        if "❌" in ref_status.get("RUS", "") or "❌" in ref_status.get("ENG", ""):
            ref_overall_status = "❌"
        elif "⚠️" in ref_status.get("comparison", ""):
            ref_overall_status = "⚠️"

        html += """
                <div class="article-checklist">
"""
        html += _as_check_item(title_label, title_status)
        html += _as_check_item("Аннотация (RUS)", rus_abs_status)
        html += _as_check_item("Аннотация (ENG)", eng_abs_status)
        html += _as_check_item("Ключевые слова", kw_overall_status)
        # Источники: показываем более точно, если отсутствует один язык
        if "❌" in ref_status.get("RUS", "") and "❌" in ref_status.get("ENG", ""):
            html += _as_check_item("Источники", "❌")
        elif "❌" in ref_status.get("RUS", ""):
            html += _as_check_item("Источники (RUS)", "❌")
        elif "❌" in ref_status.get("ENG", ""):
            html += _as_check_item("Источники (ENG)", "❌")
        else:
            html += _as_check_item("Источники", ref_overall_status)

        html += """
                </div>
                
                <div class="authors-simple-section">
                    <div class="authors-simple-title">👥 Авторы</div>
"""

        authors = article.get("authors", []) or []
        html += f"""
                    <div class="authors-count">Всего авторов: {len(authors)}</div>
"""
        if not authors:
            html += """
                    <div class="abstract-text" style="color: #dc3545; font-style: italic;">❌ Авторы не указаны</div>
"""
        else:
            html += """
                    <table class="authors-simple-table">
                        <thead>
                            <tr>
                                <th style="width: 60px;">№</th>
                                <th>ФИО (RUS / ENG)</th>
                                <th>Аффилиации / организации (RUS / ENG)</th>
                                <th style="width: 220px;">Адрес</th>
                                <th style="width: 120px;">E-mail</th>
                            </tr>
                        </thead>
                        <tbody>
"""
            for author_idx, author in enumerate(authors, 1):
                rus_data = author.get("RUS", {}) or {}
                eng_data = author.get("ENG", {}) or {}

                rus_name = f"{_safe_strip(rus_data.get('surname', ''))} {_safe_strip(rus_data.get('initials', ''))}".strip()
                eng_name = f"{_safe_strip(eng_data.get('surname', ''))} {_safe_strip(eng_data.get('initials', ''))}".strip()

                rus_orgs = rus_data.get("organizations", []) or []
                eng_orgs = eng_data.get("organizations", []) or []
                rus_org_text = _safe_strip(rus_data.get("orgName", ""))
                eng_org_text = _safe_strip(eng_data.get("orgName", ""))

                rus_org_render = "<br>".join(_safe_strip(o) for o in rus_orgs if _safe_strip(o)) or (rus_org_text or "-")
                eng_org_render = "<br>".join(_safe_strip(o) for o in eng_orgs if _safe_strip(o)) or (eng_org_text or "-")

                rus_address = _safe_strip(rus_data.get("address", ""))
                eng_address = _safe_strip(eng_data.get("address", ""))
                address_render = ""
                if rus_address:
                    address_render += f"<div><span class='lang-badge'>🇷🇺</span> {rus_address}</div>"
                if eng_address:
                    address_render += f"<div><span class='lang-badge'>🇬🇧</span> {eng_address}</div>"
                if not address_render:
                    address_render = "<span style='color:#999; font-style: italic;'>—</span>"

                rus_email = _safe_strip(rus_data.get("email", ""))
                eng_email = _safe_strip(eng_data.get("email", ""))
                email_value = rus_email or eng_email
                if email_value:
                    email_render = f"<span class='authors-simple-email-ok'>✅</span><div class='authors-simple-meta'>{email_value}</div>"
                else:
                    email_render = "<span class='authors-simple-email-missing'>❌</span>"

                html += f"""
                            <tr>
                                <td>{author_idx}</td>
                                <td>
                                    <div><span class="lang-badge">🇷🇺</span> {rus_name or 'Не указано'}</div>
                                    <div><span class="lang-badge">🇬🇧</span> {eng_name or 'Not specified'}</div>
                                </td>
                                <td>
                                    <div class="authors-simple-org"><span class="lang-badge">🇷🇺</span> {rus_org_render}</div>
                                    <div class="authors-simple-org"><span class="lang-badge">🇬🇧</span> {eng_org_render}</div>
                                </td>
                                <td>{address_render}</td>
                                <td>{email_render}</td>
                            </tr>
"""
            html += """
                        </tbody>
                    </table>
"""

        html += """
                
                <div class="abstract-section">
                    <div class="abstract-title">📝 Аннотация (первые и последние 10 слов)</div>
"""
        
        # Аннотации
        abstracts = article.get('abstracts', {})
        
        # Показываем русскую аннотацию
        rus_abstract_data = abstracts.get('RUS', {})
        if isinstance(rus_abstract_data, dict):
            rus_full_text = rus_abstract_data.get('full_text', '')
            rus_summary = rus_abstract_data.get('summary', '')
        else:
            # Обратная совместимость со старым форматом
            rus_full_text = rus_abstract_data
            rus_summary = rus_abstract_data
        
        rus_length_check = annotation_length_check(rus_full_text)
        
        html += f"""
                    <div class="title-lang">🇷🇺 RUS</div>"""
        
        if rus_full_text:
            # Показываем выжимку аннотации
            html += f"""
                    <div class="abstract-text">{rus_summary}</div>"""
            
            # Если есть проблемы с длиной, показываем пометку
            if rus_length_check:
                style = "color: #dc3545; font-style: italic;" if "❌" in rus_length_check else "color: #fd7e14; font-style: italic;"
                html += f"""
                    <div class="abstract-text" style="{style}">{rus_length_check}</div>"""
        else:
            # Аннотация отсутствует
            html += f"""
                    <div class="abstract-text" style="color: #dc3545; font-style: italic;">❌ Отсутствует</div>"""
        
        # Показываем английскую аннотацию
        eng_abstract_data = abstracts.get('ENG', {})
        if isinstance(eng_abstract_data, dict):
            eng_full_text = eng_abstract_data.get('full_text', '')
            eng_summary = eng_abstract_data.get('summary', '')
        else:
            # Обратная совместимость со старым форматом
            eng_full_text = eng_abstract_data
            eng_summary = eng_abstract_data
        
        eng_length_check = annotation_length_check(eng_full_text)
        
        html += f"""
                    <div class="title-lang">🇬🇧 ENG</div>"""
        
        if eng_full_text:
            # Показываем выжимку аннотации
            html += f"""
                    <div class="abstract-text">{eng_summary}</div>"""
            
            # Если есть проблемы с длиной, показываем пометку
            if eng_length_check:
                style = "color: #dc3545; font-style: italic;" if "❌" in eng_length_check else "color: #fd7e14; font-style: italic;"
                html += f"""
                    <div class="abstract-text" style="{style}">{eng_length_check}</div>"""
        else:
            # Аннотация отсутствует
            html += f"""
                    <div class="abstract-text" style="color: #dc3545; font-style: italic;">❌ Отсутствует</div>"""
        
        html += """
                </div>
                
                <div class="stats-section">
"""
        
        # Статистика по ключевым словам с валидацией (на одной строке)
        keywords_data = article.get('keywords', {})
        keywords_validation = validate_keywords_data(keywords_data)
        
        rus_keywords = keywords_data.get('RUS', [])
        eng_keywords = keywords_data.get('ENG', [])
        rus_status = keywords_validation.get('RUS', '❌ Отсутствуют')
        eng_status = keywords_validation.get('ENG', '❌ Отсутствуют')
        comparison_status = keywords_validation.get('comparison', '')
        
        rus_style = "color: #dc3545;" if "❌" in rus_status else "color: #28a745;"
        eng_style = "color: #dc3545;" if "❌" in eng_status else "color: #28a745;"
        comparison_style = "color: #dc3545;" if "❌" in comparison_status else "color: #fd7e14;" if "⚠️" in comparison_status else "color: #28a745;"
        
        html += f"""
                    <div class="stat-item keywords-comparison">
                        <div class="keywords-row">
                            <div class="keyword-lang">
                                <div class="stat-number" style="{rus_style}">{len(rus_keywords)}</div>
                                <div class="stat-label">🇷🇺 Ключевые слова</div>
                                <div class="stat-status" style="{rus_style}">{rus_status}</div>
                            </div>
                            <div class="keyword-lang">
                                <div class="stat-number" style="{eng_style}">{len(eng_keywords)}</div>
                                <div class="stat-label">🇬🇧 Ключевые слова</div>
                                <div class="stat-status" style="{eng_style}">{eng_status}</div>
                            </div>
                        </div>
                        <div class="comparison-row">
                            <div class="stat-status" style="{comparison_style}">{comparison_status}</div>
                        </div>
                    </div>
"""
        
        # Статистика по источникам с валидацией (отдельные строки)
        references_data = article.get('references', {})
        references_validation = validate_references_data(references_data)
        
        rus_references = references_data.get('RUS', [])
        eng_references = references_data.get('ENG', [])
        rus_status = references_validation.get('RUS', '❌ Отсутствуют')
        eng_status = references_validation.get('ENG', '❌ Отсутствуют')
        comparison_status = references_validation.get('comparison', '')
        
        rus_style = "color: #dc3545;" if "❌" in rus_status else "color: #28a745;"
        eng_style = "color: #dc3545;" if "❌" in eng_status else "color: #28a745;"
        comparison_style = "color: #dc3545;" if "❌" in comparison_status else "color: #fd7e14;" if "⚠️" in comparison_status else "color: #28a745;"
        
        # Получаем первый и последний источник (полностью, без сокращений)
        rus_first_last = get_first_last_references(rus_references, max_length=None)
        eng_first_last = get_first_last_references(eng_references, max_length=None)
        
        # Русские источники - отдельная строка на всю ширину
        html += f"""
                    <div class="stat-item full-width">
                        <div class="stat-number" style="{rus_style}">{len(rus_references)}</div>
                        <div class="stat-label">🇷🇺 Источники</div>
                        <div class="stat-status" style="{rus_style}">{rus_status}</div>"""
        
        if rus_references:
            html += f"""
                        <div class="reference-examples">
                            <div class="reference-item">
                                <strong>Первый:</strong> {rus_first_last['first']}
                            </div>
                            <div class="reference-item">
                                <strong>Последний:</strong> {rus_first_last['last']}
                            </div>
                        </div>"""
        
        html += """
                    </div>
"""
        
        # Английские источники - отдельная строка на всю ширину
        html += f"""
                    <div class="stat-item full-width">
                        <div class="stat-number" style="{eng_style}">{len(eng_references)}</div>
                        <div class="stat-label">🇬🇧 Источники</div>
                        <div class="stat-status" style="{eng_style}">{eng_status}</div>"""
        
        if eng_references:
            html += f"""
                        <div class="reference-examples">
                            <div class="reference-item">
                                <strong>First:</strong> {eng_first_last['first']}
                            </div>
                            <div class="reference-item">
                                <strong>Last:</strong> {eng_first_last['last']}
                            </div>
                        </div>"""
        
        html += """
                    </div>
"""
        
        # Сравнение - отдельная строка на всю ширину
        if comparison_status:
            html += f"""
                    <div class="stat-item full-width">
                        <div class="stat-label">Сравнение источников</div>
                        <div class="stat-status" style="{comparison_style}">{comparison_status}</div>
                    </div>
"""
        
        html += """
                </div>
            </div>
"""
    
    html += f"""
        </div>
        
        <div class="footer">
            <p>Отчет сгенерирован автоматически системой PDF GPT Parser</p>
            <p>Время создания: {current_time}</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html


def main():
    """
    Основная функция для генерации HTML отчета
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Использование: python report_generator.py <путь_к_xml_файлу> [путь_к_html_файлу]")
        return 1
    
    xml_file = Path(sys.argv[1])
    if not xml_file.exists():
        print(f"Ошибка: XML файл не найден: {xml_file}")
        return 1
    
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    
    try:
        html_file = generate_html_report(xml_file, output_file)
        print(f"✅ HTML отчет успешно создан: {html_file}")
        return 0
    except Exception as e:
        print(f"❌ Ошибка при создании отчета: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
