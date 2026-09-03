"""Парсер journal XML → структуры для UI."""

from __future__ import annotations

from typing import Any, Optional

from lxml import etree

from xml_editor.utils import (
    XML_PATHS,
    element_text,
    find_one,
    join_keywords,
    parse_pages,
)


def _lang_of(elem: etree._Element) -> str:
    return (elem.get("lang") or "").strip().upper()


def get_issue_summary(tree: etree._ElementTree) -> dict[str, Any]:
    root = tree.getroot()
    issue = find_one(root, XML_PATHS["issue"])
    journal_ru = find_one(root, XML_PATHS["journal_title_rus"])
    journal_en = find_one(root, XML_PATHS["journal_title_eng"])
    volume = find_one(root, XML_PATHS["volume"])
    number = find_one(root, XML_PATHS["number"])
    date_uni = find_one(root, XML_PATHS["date_uni"])
    issn = find_one(root, XML_PATHS["issn"])
    articles = list_article_elements(tree)
    return {
        "journal_title_ru": element_text(journal_ru),
        "journal_title_en": element_text(journal_en),
        "volume": element_text(volume),
        "number": element_text(number),
        "year": element_text(date_uni),
        "issn": element_text(issn),
        "article_count": len(articles),
        "issue_label": _issue_label(
            element_text(volume),
            element_text(number),
            element_text(date_uni),
        ),
    }


def _issue_label(volume: str, number: str, year: str) -> str:
    parts: list[str] = []
    if volume:
        parts.append(f"Т. {volume}")
    if number:
        parts.append(f"№ {number}")
    if year:
        parts.append(f"({year})")
    return " ".join(parts)


def list_article_elements(tree: etree._ElementTree) -> list[etree._Element]:
    root = tree.getroot()
    articles = root.xpath(XML_PATHS["articles"])
    if articles:
        return list(articles)
    # запасной вариант: любые article
    return list(root.xpath(".//article"))


def get_article_element(tree: etree._ElementTree, article_id: str | int) -> etree._Element:
    articles = list_article_elements(tree)
    idx = int(article_id)
    if idx < 0 or idx >= len(articles):
        raise IndexError("Статья не найдена")
    return articles[idx]


def _get_doi(article: etree._Element) -> str:
    doi = find_one(article, XML_PATHS["doi"])
    if doi is not None and element_text(doi):
        return element_text(doi)
    doi2 = find_one(article, XML_PATHS["doi_codes"])
    return element_text(doi2)


def _get_udk(article: etree._Element) -> str:
    udk = find_one(article, XML_PATHS["udk"])
    if udk is not None and element_text(udk):
        return element_text(udk)
    return element_text(find_one(article, XML_PATHS["udk_direct"]))


def _get_section(article: etree._Element) -> str:
    section = find_one(article, XML_PATHS["section"])
    if section is not None and element_text(section):
        return element_text(section)
    return element_text(find_one(article, XML_PATHS["rubric"]))


def _title(article: etree._Element, lang: str) -> str:
    el = find_one(article, XML_PATHS["art_title"], lang=lang)
    return element_text(el)


def _abstract(article: etree._Element, lang: str) -> str:
    el = find_one(article, XML_PATHS["abstract"], lang=lang)
    return element_text(el)


def _keywords(article: etree._Element, lang: str) -> str:
    group = find_one(article, XML_PATHS["kwd_group"], lang=lang)
    if group is None:
        return ""
    items = [element_text(kw) for kw in group.xpath(XML_PATHS["keyword"])]
    return join_keywords([x for x in items if x])


def _author_lang_block(individ: etree._Element) -> dict[str, str]:
    return {
        "surname": element_text(find_one(individ, XML_PATHS["surname"])),
        "initials": element_text(find_one(individ, XML_PATHS["initials"])),
        "first_name": element_text(find_one(individ, XML_PATHS["first_name"])),
        "middle_name": element_text(find_one(individ, XML_PATHS["middle_name"])),
        "org_name": element_text(find_one(individ, XML_PATHS["org_name"])),
        "address": element_text(find_one(individ, XML_PATHS["address"])),
        "email": element_text(find_one(individ, XML_PATHS["email"])),
        "orcid": element_text(find_one(individ, XML_PATHS["orcid"])),
    }


def _empty_author_lang() -> dict[str, str]:
    return {
        "surname": "",
        "initials": "",
        "first_name": "",
        "middle_name": "",
        "org_name": "",
        "address": "",
        "email": "",
        "orcid": "",
    }


def parse_authors(article: etree._Element) -> list[dict[str, Any]]:
    authors_el = find_one(article, XML_PATHS["authors"])
    if authors_el is None:
        return []
    result: list[dict[str, Any]] = []
    for idx, author in enumerate(authors_el.xpath("./author")):
        block: dict[str, Any] = {"id": str(idx), "RUS": _empty_author_lang(), "ENG": _empty_author_lang()}
        for individ in author.xpath("./individInfo"):
            lang = _lang_of(individ) or "RUS"
            if lang not in ("RUS", "ENG"):
                lang = "RUS"
            block[lang] = _author_lang_block(individ)
        result.append(block)
    return result


def parse_references(article: etree._Element) -> list[dict[str, str]]:
    refs_el = find_one(article, XML_PATHS["references"])
    if refs_el is None:
        return []
    items: list[dict[str, str]] = []
    for idx, ref in enumerate(refs_el.xpath("./reference")):
        ref_info = None
        for child in ref:
            local = child.tag.split("}")[-1] if isinstance(child.tag, str) else ""
            if local.lower() == "refinfo":
                ref_info = child
                break
        text_el = None
        lang = ""
        if ref_info is not None:
            lang = _lang_of(ref_info)
            for child in ref_info:
                local = child.tag.split("}")[-1] if isinstance(child.tag, str) else ""
                if local.lower() == "text":
                    text_el = child
                    break
        text = element_text(text_el) if text_el is not None else element_text(ref)
        items.append({"id": str(idx), "lang": lang or "UNK", "text": text})
    return items


def article_list_item(article: etree._Element, article_id: int) -> dict[str, Any]:
    title = _title(article, "RUS") or _title(article, "ENG") or f"Статья {article_id + 1}"
    authors = parse_authors(article)
    author_label = ""
    if authors:
        rus = authors[0].get("RUS") or {}
        eng = authors[0].get("ENG") or {}
        surname = rus.get("surname") or eng.get("surname") or ""
        initials = rus.get("initials") or eng.get("initials") or ""
        author_label = f"{surname} {initials}".strip()
        if len(authors) > 1:
            author_label = f"{author_label} и др." if author_label else f"{len(authors)} авт."
    pages = element_text(find_one(article, XML_PATHS["pages"]))
    return {
        "id": str(article_id),
        "index": article_id + 1,
        "title": title,
        "authors_label": author_label,
        "pages": pages,
    }


def list_articles(tree: etree._ElementTree) -> list[dict[str, Any]]:
    return [article_list_item(a, i) for i, a in enumerate(list_article_elements(tree))]


def parse_article(tree: etree._ElementTree, article_id: str | int) -> dict[str, Any]:
    article = get_article_element(tree, article_id)
    pages_raw = element_text(find_one(article, XML_PATHS["pages"]))
    first, last = parse_pages(pages_raw)
    xml_fragment = etree.tostring(article, encoding="unicode", pretty_print=True)
    return {
        "id": str(article_id),
        "title_rus": _title(article, "RUS"),
        "title_eng": _title(article, "ENG"),
        "doi": _get_doi(article),
        "lang": element_text(find_one(article, XML_PATHS["lang"])),
        "art_type": element_text(find_one(article, XML_PATHS["art_type"])),
        "section": _get_section(article),
        "udk": _get_udk(article),
        "page_first": first,
        "page_last": last,
        "pages_raw": pages_raw,
        "authors": parse_authors(article),
        "abstract_rus": _abstract(article, "RUS"),
        "abstract_eng": _abstract(article, "ENG"),
        "keywords_rus": _keywords(article, "RUS"),
        "keywords_eng": _keywords(article, "ENG"),
        "references": parse_references(article),
        "xml_fragment": xml_fragment,
    }
