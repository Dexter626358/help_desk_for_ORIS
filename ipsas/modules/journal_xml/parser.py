"""Парсинг journal XML: журнал, выпуск, статьи."""

from __future__ import annotations

import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional

from ipsas.common.xml_secure import parse_xml_file_elementtree
from ipsas.modules.journal_xml.text_utils import (
    extract_first_last_words,
    sort_articles_by_pages,
    split_organizations,
)


def _local_tag(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _find_child(parent: ET.Element, name: str) -> Optional[ET.Element]:
    """Найти дочерний элемент по имени без учёта регистра."""
    target = name.lower()
    for child in list(parent):
        if _local_tag(child.tag).lower() == target:
            return child
    return None


def _find_children(parent: ET.Element, name: str) -> List[ET.Element]:
    target = name.lower()
    return [c for c in list(parent) if _local_tag(c.tag).lower() == target]


def _element_text(elem: Optional[ET.Element]) -> str:
    """Полный текст элемента, включая содержимое вложенных тегов."""
    if elem is None:
        return ""
    text = "".join(elem.itertext())
    return " ".join(text.split()).strip()


def get_issue_info(xml_file: Path) -> Dict[str, Any]:
    """
    Извлекает информацию о выпуске из XML файла
    
    Args:
        xml_file: Путь к XML файлу
        
    Returns:
        Dict: Информация о выпуске
    """
    try:
        tree = parse_xml_file_elementtree(xml_file)
        root = tree.getroot()
        
        issue_info = {}
        
        # Основная информация о журнале
        issue_info['titleid'] = root.find('titleid').text if root.find('titleid') is not None else ""
        issue_info['issn'] = root.find('issn').text if root.find('issn') is not None else ""
        issue_info['eissn'] = root.find('eissn').text if root.find('eissn') is not None else ""
        
        # Информация о журнале
        journal_info = root.find('journalInfo')
        if journal_info is not None:
            issue_info['journal_title'] = _element_text(journal_info.find('title'))
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

def get_articles_info(
    xml_file: Path | str,
    root: Optional[ET.Element] = None,
) -> List[Dict[str, Any]]:
    """
    Извлекает информацию о статьях из XML файла.

    Args:
        xml_file: Путь к XML файлу (нужен для jsons/ рядом с файлом).
        root: Уже распарсенный корень (чтобы не читать файл повторно).
    """
    try:
        xml_path = Path(xml_file)
        if root is None:
            tree = parse_xml_file_elementtree(xml_path)
            root = tree.getroot()

        def _norm_lang(raw: str | None) -> str:
            """
            Нормализует язык для группировок в отчёте.
            - пустое значение -> UNK
            - UNK остаётся UNK
            - ANY остаётся ANY (универсальный язык)
            """
            value = (raw or "").strip().upper()
            if value == "":
                return "UNK"
            if value == "UNK":
                return "UNK"
            if value == "ANY":
                return "ANY"
            return value

        def _meta_lang(raw: str | None) -> str:
            """Язык метаданных статьи (RUS/ENG): только upper, пустое не трогаем."""
            return (raw or "").strip().upper()

        # Подготовка: быстрый доступ к extraction_info из JSON (если есть)
        jsons_dir = xml_path.parent / "jsons"
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
                    lang = _meta_lang(title_elem.get('lang', ''))
                    title_text = _element_text(title_elem)
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
                        lang = _meta_lang(individ_info.get('lang', ''))
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
                    lang = _meta_lang(abstract_elem.get('lang', ''))
                    abstract_text = _element_text(abstract_elem)
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
                    lang = _norm_lang(kwd_group.get('lang', ''))
                    keyword_list = []
                    for kw in kwd_group.findall('keyword'):
                        kw_text = _element_text(kw)
                        if kw_text:
                            keyword_list.append(kw_text)
                    keywords_data[lang] = keyword_list
                    keywords_count[lang] = len(keyword_list)
                article_data['keywords'] = keywords_data
                article_data['keywords_count'] = keywords_count
            
            # Источники
            references = article.find('references')
            if references is not None:
                refs_data: Dict[str, List[str]] = {}
                refs_count: Dict[str, int] = {}
                duplicate_text_count = 0
                numbered_count = 0
                for ref_elem in _find_children(references, "reference"):
                    # Текст напрямую в <reference> (часто дубль + нумерация)
                    direct_text = " ".join((ref_elem.text or "").split()).strip()

                    ref_info = _find_child(ref_elem, "refInfo")
                    text_elem = _find_child(ref_info, "text") if ref_info is not None else None
                    inner_text = _element_text(text_elem)

                    if direct_text and inner_text:
                        duplicate_text_count += 1

                    # Нумерация может быть во внешнем тексте или внутри refInfo/text
                    for candidate in (direct_text, inner_text):
                        if candidate and re.match(r"^\s*\d+[.)]\s*", candidate):
                            numbered_count += 1
                            break

                    lang = _norm_lang(ref_info.get("lang", "") if ref_info is not None else "")
                    ref_text = inner_text or direct_text
                    if not ref_text:
                        continue
                    if lang not in refs_data:
                        refs_data[lang] = []
                        refs_count[lang] = 0
                    refs_data[lang].append(ref_text)
                    refs_count[lang] += 1

                article_data["references"] = refs_data
                article_data["references_count"] = refs_count
                article_data["references_duplicate_text_count"] = duplicate_text_count
                article_data["references_numbered_count"] = numbered_count
            
            articles_info.append(article_data)
        
        # Сортируем статьи по номерам страниц
        sorted_articles = sort_articles_by_pages(articles_info)
        
        return sorted_articles
        
    except Exception as e:
        logging.error(f"Ошибка при извлечении информации о статьях: {e}")
        return []

