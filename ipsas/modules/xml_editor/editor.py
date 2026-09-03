"""Точечное изменение journal XML-дерева без пересборки документа."""

from __future__ import annotations

from typing import Any, Optional

from lxml import etree

from ipsas.modules.xml_editor.parser import get_article_element, list_article_elements
from ipsas.modules.xml_editor.utils import (
    XML_PATHS,
    element_text,
    ensure_lang_child,
    find_one,
    get_or_create_child,
    join_pages,
    set_element_text,
    split_keywords,
)


def _set_simple_child(parent: etree._Element, tag: str, value: str) -> None:
    value = (value or "").strip()
    existing = None
    for child in parent:
        local = child.tag.split("}")[-1] if isinstance(child.tag, str) else ""
        if local == tag:
            existing = child
            break
    if not value:
        # Не создаём пустой тег; существующий очищаем текстом
        if existing is not None:
            set_element_text(existing, "")
        return
    if existing is None:
        existing = etree.SubElement(parent, tag)
    set_element_text(existing, value)


def _set_title(article: etree._Element, lang: str, value: str) -> None:
    value = (value or "").strip()
    el = find_one(article, XML_PATHS["art_title"], lang=lang)
    if not value:
        if el is not None:
            set_element_text(el, "")
        return
    if el is None:
        el = ensure_lang_child(article, "artTitles", "artTitle", lang)
    set_element_text(el, value)


def _set_abstract(article: etree._Element, lang: str, value: str) -> None:
    value = (value or "").strip()
    el = find_one(article, XML_PATHS["abstract"], lang=lang)
    if not value:
        if el is not None:
            set_element_text(el, "")
        return
    if el is None:
        el = ensure_lang_child(article, "abstracts", "abstract", lang)
    set_element_text(el, value)


def _set_doi(article: etree._Element, value: str) -> None:
    value = (value or "").strip()
    doi = find_one(article, XML_PATHS["doi"])
    doi_codes = find_one(article, XML_PATHS["doi_codes"])
    if doi is not None:
        set_element_text(doi, value)
        return
    if doi_codes is not None:
        set_element_text(doi_codes, value)
        return
    if value:
        _set_simple_child(article, "doi", value)


def _set_udk(article: etree._Element, value: str) -> None:
    value = (value or "").strip()
    udk = find_one(article, XML_PATHS["udk"])
    if udk is not None:
        set_element_text(udk, value)
        return
    udk_direct = find_one(article, XML_PATHS["udk_direct"])
    if udk_direct is not None:
        set_element_text(udk_direct, value)
        return
    if not value:
        return
    codes = get_or_create_child(article, "codes")
    el = etree.SubElement(codes, "udk")
    el.text = value


def _set_section(article: etree._Element, value: str) -> None:
    value = (value or "").strip()
    section = find_one(article, XML_PATHS["section"])
    if section is not None:
        set_element_text(section, value)
        return
    rubric = find_one(article, XML_PATHS["rubric"])
    if rubric is not None:
        set_element_text(rubric, value)
        return
    if value:
        _set_simple_child(article, "section", value)


def _set_keywords(article: etree._Element, lang: str, raw: str) -> None:
    items = split_keywords(raw)
    group = find_one(article, XML_PATHS["kwd_group"], lang=lang)
    if not items:
        if group is not None:
            for child in list(group):
                group.remove(child)
        return
    if group is None:
        group = ensure_lang_child(article, "keywords", "kwdGroup", lang)
    for child in list(group):
        group.remove(child)
    for kw in items:
        el = etree.SubElement(group, "keyword")
        el.text = kw


def _ensure_individ(author: etree._Element, lang: str) -> etree._Element:
    for child in author.xpath("./individInfo"):
        if (child.get("lang") or "").upper() == lang.upper():
            return child
    el = etree.SubElement(author, "individInfo")
    el.set("lang", lang.upper())
    return el


def _set_individ_field(individ: etree._Element, tag: str, value: str) -> None:
    value = (value or "").strip()
    existing = find_one(individ, f"./{tag}")
    if not value:
        if existing is not None:
            set_element_text(existing, "")
        return
    if existing is None:
        existing = etree.SubElement(individ, tag)
    set_element_text(existing, value)


def _apply_author_lang(author: etree._Element, lang: str, data: dict[str, Any]) -> None:
    # Создаём individInfo только если есть хоть одно значение
    has_any = any(
        str(data.get(k) or "").strip()
        for k in (
            "surname",
            "initials",
            "first_name",
            "middle_name",
            "org_name",
            "address",
            "email",
        )
    )
    individ = find_one(author, XML_PATHS["individ_info"], lang=lang)
    if not has_any:
        if individ is not None:
            for tag in (
                "surname",
                "initials",
                "firstName",
                "middleName",
                "orgName",
                "address",
                "email",
            ):
                _set_individ_field(individ, tag, "")
        return
    if individ is None:
        individ = _ensure_individ(author, lang)
    _set_individ_field(individ, "surname", str(data.get("surname") or ""))
    _set_individ_field(individ, "initials", str(data.get("initials") or ""))
    _set_individ_field(individ, "firstName", str(data.get("first_name") or ""))
    _set_individ_field(individ, "middleName", str(data.get("middle_name") or ""))
    _set_individ_field(individ, "orgName", str(data.get("org_name") or ""))
    _set_individ_field(individ, "address", str(data.get("address") or ""))
    _set_individ_field(individ, "email", str(data.get("email") or ""))


def _set_author_orcid(author: etree._Element, value: str) -> None:
    """Пишет ORCID в authorCodes/orcid по схеме journal3.xsd."""
    value = (value or "").strip()
    codes = find_one(author, XML_PATHS["author_codes"])
    orcid_el = find_one(author, XML_PATHS["author_orcid"])
    if not value:
        if orcid_el is not None:
            set_element_text(orcid_el, "")
        return
    if codes is None:
        codes = etree.SubElement(author, "authorCodes")
    if orcid_el is None:
        orcid_el = etree.SubElement(codes, "orcid")
    set_element_text(orcid_el, value)


def update_article_from_form(
    tree: etree._ElementTree,
    article_id: str | int,
    form: dict[str, Any],
) -> None:
    """Применить данные формы к статье. form — нормализованный dict."""
    article = get_article_element(tree, article_id)

    _set_title(article, "RUS", str(form.get("title_rus") or ""))
    _set_title(article, "ENG", str(form.get("title_eng") or ""))
    _set_doi(article, str(form.get("doi") or ""))
    _set_simple_child(article, "lang", str(form.get("lang") or ""))
    _set_simple_child(article, "artType", str(form.get("art_type") or ""))
    _set_section(article, str(form.get("section") or ""))
    _set_udk(article, str(form.get("udk") or ""))

    pages = join_pages(str(form.get("page_first") or ""), str(form.get("page_last") or ""))
    if pages or find_one(article, XML_PATHS["pages"]) is not None:
        _set_simple_child(article, "pages", pages)

    _set_abstract(article, "RUS", str(form.get("abstract_rus") or ""))
    _set_abstract(article, "ENG", str(form.get("abstract_eng") or ""))
    _set_keywords(article, "RUS", str(form.get("keywords_rus") or ""))
    _set_keywords(article, "ENG", str(form.get("keywords_eng") or ""))

    authors_data = form.get("authors") or []
    authors_el = find_one(article, XML_PATHS["authors"])
    if authors_el is None and authors_data:
        authors_el = etree.SubElement(article, "authors")
    if authors_el is not None:
        existing = list(authors_el.xpath("./author"))
        for i, author_data in enumerate(authors_data):
            if i < len(existing):
                author_el = existing[i]
            else:
                author_el = etree.SubElement(authors_el, "author")
            _apply_author_lang(author_el, "RUS", author_data.get("RUS") or {})
            _apply_author_lang(author_el, "ENG", author_data.get("ENG") or {})
            _set_author_orcid(author_el, str(author_data.get("orcid") or ""))

    _update_references(article, form.get("references") or [])


def _update_references(article: etree._Element, refs: list[dict[str, Any]]) -> None:
    refs_el = find_one(article, XML_PATHS["references"])
    if refs_el is None:
        if not any(str(r.get("text") or "").strip() for r in refs):
            return
        refs_el = etree.SubElement(article, "references")

    existing = list(refs_el.xpath("./reference"))
    # Синхронизируем количество: обновляем, добавляем, удаляем хвост
    for i, ref_data in enumerate(refs):
        text = str(ref_data.get("text") or "").strip()
        lang = (str(ref_data.get("lang") or "UNK")).upper() or "UNK"
        if i < len(existing):
            ref_el = existing[i]
        else:
            ref_el = etree.SubElement(refs_el, "reference")
        ref_info = None
        for child in ref_el:
            local = child.tag.split("}")[-1] if isinstance(child.tag, str) else ""
            if local.lower() == "refinfo":
                ref_info = child
                break
        if ref_info is None:
            # Если был только прямой текст — сохраняем совместимость: создаём refInfo
            if text or list(ref_el) == []:
                ref_info = etree.SubElement(ref_el, "refInfo")
                ref_info.set("lang", lang)
            else:
                continue
        else:
            if not ref_info.get("lang"):
                ref_info.set("lang", lang)
        text_el = None
        for child in ref_info:
            local = child.tag.split("}")[-1] if isinstance(child.tag, str) else ""
            if local.lower() == "text":
                text_el = child
                break
        if text_el is None:
            text_el = etree.SubElement(ref_info, "text")
        set_element_text(text_el, text)
        # Очищаем дублирующий прямой text у reference, если есть refInfo/text
        if ref_el.text and str(ref_el.text).strip():
            ref_el.text = None

    # Удалить лишние reference с конца, если форма короче
    for ref_el in existing[len(refs) :]:
        refs_el.remove(ref_el)


def add_author(tree: etree._ElementTree, article_id: str | int) -> int:
    article = get_article_element(tree, article_id)
    authors_el = find_one(article, XML_PATHS["authors"])
    if authors_el is None:
        authors_el = etree.SubElement(article, "authors")
    author = etree.SubElement(authors_el, "author")
    for lang in ("RUS", "ENG"):
        individ = etree.SubElement(author, "individInfo")
        individ.set("lang", lang)
        etree.SubElement(individ, "surname")
        etree.SubElement(individ, "initials")
    return len(authors_el.xpath("./author")) - 1


def delete_author(tree: etree._ElementTree, article_id: str | int, author_id: str | int) -> None:
    article = get_article_element(tree, article_id)
    authors_el = find_one(article, XML_PATHS["authors"])
    if authors_el is None:
        raise IndexError("Авторы не найдены")
    authors = list(authors_el.xpath("./author"))
    idx = int(author_id)
    if idx < 0 or idx >= len(authors):
        raise IndexError("Автор не найден")
    authors_el.remove(authors[idx])


def move_author(
    tree: etree._ElementTree,
    article_id: str | int,
    author_id: str | int,
    direction: str,
) -> None:
    article = get_article_element(tree, article_id)
    authors_el = find_one(article, XML_PATHS["authors"])
    if authors_el is None:
        raise IndexError("Авторы не найдены")
    authors = list(authors_el.xpath("./author"))
    idx = int(author_id)
    if idx < 0 or idx >= len(authors):
        raise IndexError("Автор не найден")
    if direction == "up" and idx > 0:
        authors_el.remove(authors[idx])
        authors_el.insert(idx - 1, authors[idx])
    elif direction == "down" and idx < len(authors) - 1:
        authors_el.remove(authors[idx])
        authors_el.insert(idx + 1, authors[idx])


def restore_article_from_original(
    edited_tree: etree._ElementTree,
    original_tree: etree._ElementTree,
    article_id: str | int,
) -> None:
    """Заменить статью в edited копией из original (глубокая копия элемента)."""
    edited_articles = list_article_elements(edited_tree)
    original_articles = list_article_elements(original_tree)
    idx = int(article_id)
    if idx < 0 or idx >= len(edited_articles) or idx >= len(original_articles):
        raise IndexError("Статья не найдена")
    old = edited_articles[idx]
    parent = old.getparent()
    if parent is None:
        raise RuntimeError("У статьи нет родителя")
    new = etree.fromstring(etree.tostring(original_articles[idx]))
    parent.replace(old, new)


def restore_all_from_original(session_edited_path, session_original_path) -> etree._ElementTree:
    from ipsas.modules.xml_editor.utils import parse_xml_file, save_tree

    tree = parse_xml_file(session_original_path)
    save_tree(tree, session_edited_path)
    return tree
