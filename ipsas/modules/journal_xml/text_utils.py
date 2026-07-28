"""Текстовые утилиты и сравнения полей XML-отчёта."""

from __future__ import annotations

from typing import Any, Dict, List

def safe_strip(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def format_article_title(titles: Dict[str, str]) -> str:
    """
    Возвращает человеко-читаемое название статьи для краткой сводки.
    Приоритет: RUS -> ENG -> "(без названия)".
    """
    rus = safe_strip(titles.get("RUS", ""))
    if rus:
        return rus
    eng = safe_strip(titles.get("ENG", ""))
    if eng:
        return eng
    return "(без названия)"

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


# Обратная совместимость
_safe_strip = safe_strip
_format_article_title = format_article_title
