"""HTML-парсеры для страницы выпуска и статьи.

Выделены из `IssueMetadataParser`, чтобы отделить извлечение данных из HTML от orchestration/HTTP/JATS.
Функции возвращают dict-структуры (обратная совместимость с текущими шаблонами).
"""

from __future__ import annotations

import re
from typing import Callable, Dict, List, Optional, Tuple

from lxml import html


def _has_cyrillic(text: Optional[str]) -> bool:
    return bool(text and re.search(r"[А-Яа-яЁё]", text))


def parse_issue_identifiers(text: Optional[str]) -> Dict[str, Optional[str]]:
    """Извлечь том, номер выпуска, сквозной номер и год из заголовка OJS."""
    empty: Dict[str, Optional[str]] = {
        "volume": None,
        "issue": None,
        "issue_serial": None,
        "year": None,
    }
    if not text or not text.strip():
        return empty
    t = text.strip()

    match = re.search(r"Vol\s*(\d+)[,\s]+No\s*(\d+)\s*\((\d{4})\)", t, re.IGNORECASE)
    if match:
        return {"volume": match.group(1), "issue": match.group(2), "issue_serial": None, "year": match.group(3)}

    match = re.search(r"Том\s*(\d+).+?№\s*(\d+).+?(\d{4})", t, re.IGNORECASE)
    if match:
        return {"volume": match.group(1), "issue": match.group(2), "issue_serial": None, "year": match.group(3)}

    # RCSI/OJS: «№ 1 (35) (2026)» / «No 1 (35) (2026)» — номер в году, сквозной номер, год (не том).
    match = re.search(
        r"(?:№|No\.?)\s*(\d+)\s*\(\s*(\d+)\s*\)\s*\(\s*(\d{4})\s*\)",
        t,
        re.IGNORECASE,
    )
    if match:
        return {
            "volume": None,
            "issue": match.group(1),
            "issue_serial": match.group(2),
            "year": match.group(3),
        }

    match = re.search(r"(?:№|No\.?)\s*(\d+)\s*\(\s*(\d{4})\s*\)", t, re.IGNORECASE)
    if match:
        return {"volume": None, "issue": match.group(1), "issue_serial": None, "year": match.group(2)}

    return empty


def extract_journal_title(root: html.HtmlElement) -> Optional[str]:
    """Название журнала со страницы выпуска."""

    def meta_content(name: str, attr: str = "name") -> Optional[str]:
        values = root.xpath(f"//meta[@{attr}='{name}']/@content")
        return values[0].strip() if values else None

    journal_title, journal_title_ru = _journal_titles_by_lang(root)
    title = journal_title or journal_title_ru or meta_content("citation_journal_title") or meta_content("og:site_name", "property")
    if title:
        return title
    title_tag = root.xpath("string(//title)")
    if title_tag and " - " in title_tag:
        return title_tag.split(" - ", 1)[1].strip()
    return None


def _journal_titles_by_lang(root: html.HtmlElement) -> Tuple[Optional[str], Optional[str]]:
    journal_title: Optional[str] = None
    journal_title_ru: Optional[str] = None
    for node in root.xpath("//meta[@name='citation_journal_title']"):
        content = (node.get("content") or "").strip()
        if not content:
            continue
        lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or node.get("lang") or "").strip().lower()
        if lang.startswith("ru"):
            journal_title_ru = content
        elif lang.startswith("en") or not lang:
            journal_title = journal_title or content
    if not journal_title and journal_title_ru:
        journal_title = journal_title_ru
    return journal_title, journal_title_ru


def extract_article_links(root: html.HtmlElement, issue_url: str) -> List[str]:
    links = root.xpath("//a[contains(@href, '/article/view/')]/@href")
    normalized: List[str] = []
    for link in links:
        if not link:
            continue
        link = link.strip()
        if link.startswith("/"):
            base = re.match(r"^(https?://[^/]+)", issue_url)
            if base:
                link = base.group(1) + link
        if re.search(r"/article/view/\d+/\d+", link):
            continue
        match = re.search(r"^(https?://[^?#]+/article/view/\d+)", link)
        if match:
            link = match.group(1)
        if link not in normalized:
            normalized.append(link)
    return normalized


def parse_issue_page(root: html.HtmlElement, issue_url: str) -> Dict[str, object]:
    def text_from_xpath(xpath: str) -> Optional[str]:
        values = [v.strip() for v in root.xpath(xpath) if isinstance(v, str) and v.strip()]
        return values[0] if values else None

    def meta_content(name: str, attr: str = "name") -> Optional[str]:
        values = root.xpath(f"//meta[@{attr}='{name}']/@content")
        return values[0].strip() if values else None

    def journal_titles_by_lang() -> Tuple[Optional[str], Optional[str]]:
        return _journal_titles_by_lang(root)

    journal_title, journal_title_ru_from_meta = journal_titles_by_lang()
    journal_title = journal_title or meta_content("citation_journal_title") or meta_content("og:site_name", "property")

    title_tag = (root.xpath("string(//title)") or "").strip()
    issue_title_from_tag: Optional[str] = None
    journal_from_tag: Optional[str] = None
    if title_tag and " - " in title_tag:
        issue_title_from_tag, journal_from_tag = [p.strip() for p in title_tag.split(" - ", 1)]

    if journal_from_tag:
        if _has_cyrillic(journal_from_tag):
            if not journal_title_ru_from_meta:
                journal_title_ru_from_meta = journal_from_tag
            if not journal_title or _has_cyrillic(journal_title):
                journal_title = journal_from_tag
        elif not journal_title:
            journal_title = journal_from_tag

    page_lang = (root.get("lang") or root.get("{http://www.w3.org/XML/1998/namespace}lang") or "").strip().lower()
    og_locale = (meta_content("og:locale", "property") or "").strip().lower()
    is_ru_page = page_lang.startswith("ru") or og_locale.startswith("ru")
    if is_ru_page and not journal_title_ru_from_meta and journal_title and _has_cyrillic(journal_title):
        journal_title_ru_from_meta = journal_title

    issn = None
    issn_online = None

    header_issn = root.xpath("//*[@id='headerIssn']")
    if header_issn:
        block_text = (header_issn[0].text_content() or "")
        m_print = re.search(
            r"ISSN\s+(\d{4}-\d{3}[\dXx])\s*\(\s*(?:Print|Печатный)\s*\)",
            block_text,
            re.IGNORECASE,
        )
        if m_print:
            issn = m_print.group(1)
        m_online = re.search(
            r"ISSN\s+(\d{4}-\d{3}[\dXx])\s*\(\s*(?:Online|Онлайн)\s*\)",
            block_text,
            re.IGNORECASE,
        )
        if m_online:
            issn_online = m_online.group(1)
    if not issn:
        issn = meta_content("citation_issn")
    if not issn_online:
        issn_online = meta_content("citation_issn_online") or meta_content("citation_eissn") or None
    if not issn and not issn_online:
        page_text = root.text_content()
        match = re.search(r"ISSN[:\s]+(\d{4}-\d{3}[\dXx])", page_text)
        if match:
            issn = match.group(1)

    issue_title = (
        text_from_xpath("//h1/text()")
        or issue_title_from_tag
        or meta_content("og:title", "property")
        or text_from_xpath("//title/text()")
    )

    ids = parse_issue_identifiers(issue_title or issue_title_from_tag)
    volume = ids["volume"]
    issue = ids["issue"]
    issue_serial = ids["issue_serial"]
    year = ids["year"]

    article_urls = extract_article_links(root, issue_url)

    cover_full_url = None
    cover_thumb_url = None
    # OJS2: блок превью обложки
    fancy = root.xpath("//div[contains(@class,'preview') and contains(@class,'fancybox')]//a/@href")
    if fancy:
        cover_full_url = fancy[0].strip() if fancy[0] else None
    thumb = root.xpath("//div[contains(@class,'preview') and contains(@class,'fancybox')]//img/@src")
    if thumb:
        cover_thumb_url = thumb[0].strip() if thumb[0] else None
    # Fallback: og:image
    if not cover_full_url:
        cover_full_url = meta_content("og:image", "property")
    # OJS3 / альтернативная разметка обложки
    if not cover_full_url:
        alt_cover = root.xpath(
            "//img[contains(@class,'cover') or contains(@class,'issueCover')]/@src"
            " | //a[contains(@class,'cover')]/@href"
        )
        if alt_cover and alt_cover[0]:
            cover_full_url = alt_cover[0].strip()

    # Полный файл выпуска (PDF) — может быть несколько, на RU/EN.
    issue_galleys: List[Dict[str, Optional[str]]] = []
    galley_labels = root.xpath(
        "//div[contains(@class,'galleyLabel')][.//img[contains(@src,'pdf.png') or contains(@alt,'PDF')]]"
    )
    for label in galley_labels:
        hrefs = label.xpath("./ancestor::a[1]/@href")
        href = hrefs[0].strip() if hrefs and hrefs[0] else None
        lang_texts = [t.strip() for t in label.xpath(".//span[contains(@class,'galleyLanguageLabel')]/text()") if t and t.strip()]
        lang_raw = lang_texts[0] if lang_texts else None
        if lang_raw:
            lang_raw = lang_raw.strip().strip("()").strip()
        if href and href.startswith("/"):
            base = re.match(r"^(https?://[^/]+)", issue_url)
            if base:
                href = base.group(1) + href
        if href:
            issue_galleys.append({"url": href, "lang": lang_raw})
    if not issue_galleys:
        for a in root.xpath("//a[.//img[contains(@src,'pdf') or contains(@alt,'PDF')]]/@href"):
            href = (a or "").strip()
            if not href or "/article/" in href:
                continue
            if href.startswith("/"):
                base = re.match(r"^(https?://[^/]+)", issue_url)
                if base:
                    href = base.group(1) + href
            issue_galleys.append({"url": href, "lang": None})

    # PDF-файлы по статьям
    # Привязываем по article_id из URL вида /article/view/<id>/...
    article_pdf_files: Dict[str, List[Dict[str, Optional[object]]]] = {}
    pdf_links = root.xpath(
        "//div[contains(@class,'issueArticlesFiles')]//a[.//img[contains(@src,'pdf.png') or contains(@alt,'PDF')]]"
    )
    for a in pdf_links:
        hrefs = a.xpath("./@href")
        href = hrefs[0].strip() if hrefs and hrefs[0] else None
        if not href:
            continue
        if href.startswith("/"):
            base = re.match(r"^(https?://[^/]+)", issue_url)
            if base:
                href = base.group(1) + href
        m = re.search(r"/article/view/(\d+)", href)
        if not m:
            continue
        article_id = m.group(1)
        lang_texts = [t.strip() for t in a.xpath(".//span[contains(@class,'issueArticlesLabel')]/text()") if t and t.strip()]
        lang_raw = lang_texts[0] if lang_texts else None
        if lang_raw:
            lang_raw = lang_raw.strip().strip("()").strip()
        is_locked = bool(a.xpath(".//img[contains(@class,'issueArticlesAccessLogo') or contains(@alt,'Доступ закрыт')]"))
        article_pdf_files.setdefault(article_id, []).append(
            {"url": href, "lang": lang_raw, "locked": is_locked}
        )

    return {
        "issue_url": issue_url,
        "journal_title": journal_title,
        "journal_title_ru": journal_title_ru_from_meta,
        "issue_title": issue_title,
        "issn": issn,
        "eissn": issn_online,
        "volume": volume,
        "issue": issue,
        "issue_serial": issue_serial,
        "year": year,
        "article_count": len(article_urls),
        "article_urls": article_urls,
        "cover_url": cover_full_url,
        "cover_thumb_url": cover_thumb_url,
        "issue_galleys": issue_galleys,
        "article_pdf_files": article_pdf_files,
    }


def parse_article_page(
    root: html.HtmlElement,
    article_url: str,
    *,
    detect_lang: Callable[[Optional[str]], Optional[str]],
) -> Dict[str, object]:
    # Полная логика вынесена из старого метода `_parse_article_page` (без расчёта `problems/errors`).

    def meta_values(name: str) -> List[str]:
        return [v.strip() for v in root.xpath(f"//meta[@name='{name}']/@content") if v.strip()]

    def text_list(xpath: str) -> List[str]:
        return [v.strip() for v in root.xpath(xpath) if isinstance(v, str) and v.strip()]

    def normalize_spaces(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def words(text: str) -> List[str]:
        return re.findall(r"[A-Za-zА-Яа-я0-9]+", text)

    def abstract_stats(text: Optional[str]) -> Dict[str, Optional[object]]:
        if not text:
            return {"length": None, "first_10": None, "last_10": None}
        tokens = words(text)
        first = " ".join(tokens[:10]) if tokens else None
        last = " ".join(tokens[-10:]) if tokens else None
        return {"length": len(tokens), "first_10": first, "last_10": last}

    def collect_section_text(title: str) -> Optional[str]:
        if title.lower() == "аннотация":
            abstract_blocks = root.xpath("//div[@id='articleAbstract']")
            if abstract_blocks:
                block = abstract_blocks[0]
                texts = []
                for node in block:
                    if getattr(node, "tag", None) in {"h2", "h3"}:
                        continue
                    texts.extend(node.xpath(".//text()"))
                text = normalize_spaces(" ".join([t for t in texts if t.strip()]))
                if text and detect_lang(text) == "ru":
                    return text

        headings = root.xpath(f"//h2[normalize-space(text())='{title}']")
        if not headings:
            abstract_blocks = root.xpath("//div[@id='articleAbstract']")
            if abstract_blocks:
                block = abstract_blocks[0]
                label_nodes = block.xpath(".//*[self::h2 or self::h3]/text()")
                label = label_nodes[0].strip() if label_nodes else None
                if label and label.lower() == title.lower():
                    texts = []
                    for node in block:
                        if getattr(node, "tag", None) in {"h2", "h3"}:
                            continue
                        texts.extend(node.xpath(".//text()"))
                    text = normalize_spaces(" ".join([t for t in texts if t.strip()]))
                    return text or None
            return None
        section = headings[0].getparent()
        if section is None:
            return None
        texts = []
        for node in section:
            if node is headings[0]:
                continue
            texts.extend(node.xpath(".//text()"))
        text = normalize_spaces(" ".join([t for t in texts if t.strip()]))
        return text or None

    def collect_keywords(title: str) -> List[str]:
        if title.lower() == "ключевые слова":
            keyword_blocks = root.xpath("//div[@id='articleSubject' or @id='articleKeywords']")
            if keyword_blocks:
                block = keyword_blocks[0]
                link_texts = [t.strip() for t in block.xpath(".//a/text()") if t.strip()]
                ru_links = [t for t in link_texts if detect_lang(t) == "ru"]
                if link_texts:
                    return ru_links or link_texts
                texts = []
                for node in block:
                    if getattr(node, "tag", None) in {"h2", "h3"}:
                        continue
                    texts.extend(node.xpath(".//text()"))
                text = normalize_spaces(" ".join([t for t in texts if t.strip()]))
                if text:
                    parts = [p.strip() for p in re.split(r"[,;]", text) if p.strip()]
                    ru_parts = [p for p in parts if detect_lang(p) == "ru"]
                    return ru_parts or parts

        headings = root.xpath(f"//h2[normalize-space(text())='{title}']")
        if not headings:
            keyword_blocks = root.xpath("//div[@id='articleKeywords' or @id='articleSubject']")
            if keyword_blocks:
                block = keyword_blocks[0]
                label_nodes = block.xpath(".//*[self::h2 or self::h3]/text()")
                label = label_nodes[0].strip() if label_nodes else None
                if label and label.lower() == title.lower():
                    link_texts = [t.strip() for t in block.xpath(".//a/text()") if t.strip()]
                    if link_texts:
                        return link_texts
                    texts = []
                    for node in block:
                        if getattr(node, "tag", None) in {"h2", "h3"}:
                            continue
                        texts.extend(node.xpath(".//text()"))
                    text = normalize_spaces(" ".join([t for t in texts if t.strip()]))
                    if not text:
                        return []
                    parts = [p.strip() for p in re.split(r"[,;]", text) if p.strip()]
                    return parts
            return []
        section = headings[0].getparent()
        if section is None:
            return []
        items = [normalize_spaces(" ".join(node.xpath(".//text()"))) for node in section.xpath(".//li")]
        items = [item for item in items if item]
        if items:
            return items
        text = normalize_spaces(" ".join(section.xpath(".//text()")))
        text = text.replace(title, "").strip()
        if not text:
            return []
        parts = [p.strip() for p in re.split(r"[,;]", text) if p.strip()]
        return parts

    def unique(values: List[str]) -> List[str]:
        seen = set()
        result: List[str] = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                result.append(value)
        return result

    def collect_meta_lang_values(meta_name: str) -> Dict[str, List[str]]:
        nodes = root.xpath(f"//meta[@name='{meta_name}']")
        values = {"ru": [], "en": [], "other": []}
        for node in nodes:
            value = (node.get("content") or "").strip()
            if not value:
                continue
            lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or node.get("lang") or "").lower()
            if lang.startswith("ru"):
                values["ru"].append(value)
            elif lang.startswith("en"):
                values["en"].append(value)
            else:
                values["other"].append(value)
        for key in values:
            values[key] = unique(values[key])
        return values

    def collect_author_section_names() -> List[str]:
        headings = root.xpath(
            "//h2[normalize-space(text())='About the authors' or normalize-space(text())='Сведения об авторах']"
        )
        if not headings:
            return []
        section = headings[0].getparent()
        if section is None:
            return []
        names = []
        for node in section.xpath(".//h3"):
            text = normalize_spaces(" ".join(node.xpath(".//text()")))
            if text:
                names.append(text)
        return unique(names)

    def normalize_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        if "/" in value:
            parts = value.split("/")
            if len(parts) == 3:
                return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
        return value

    def format_date_ru(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        parts = value.split("-")
        if len(parts) != 3:
            return value
        year, month, day = parts
        day_int = int(day) if day.isdigit() else 0
        month_int = int(month) if month.isdigit() else 0
        if not (1 <= day_int <= 31 and 1 <= month_int <= 12):
            return value
        return f"{day_int:02d}.{month_int:02d}.{year}"

    def collect_references() -> List[str]:
        headings = root.xpath(
            "//h2[normalize-space(text())='References' or normalize-space(text())='Литература' or normalize-space(text())='Список литературы']"
        )
        items = []
        if headings:
            section = headings[0].getparent()
            if section is not None:
                items = section.xpath(".//li")
        if not items:
            items = root.xpath("//*[contains(@class,'references')]//li")
        references: List[str] = []
        for item in items:
            text = normalize_spaces(" ".join(item.xpath(".//text()")))
            if text:
                references.append(text)
        return references

    title_candidates = text_list("//h1/text()") + meta_values("citation_title") + meta_values("DC.Title")
    title_candidates = [normalize_spaces(t) for t in title_candidates if t]
    title_ru = None
    title_en = None
    for title in title_candidates:
        lang = detect_lang(title)
        if lang == "ru" and not title_ru:
            title_ru = title
        if lang == "en" and not title_en:
            title_en = title

    abstract_en = collect_section_text("Abstract")
    abstract_ru = collect_section_text("Аннотация") or collect_section_text("Реферат")
    if not abstract_ru and not abstract_en:
        fallback = collect_section_text("Summary")
        if fallback:
            if detect_lang(fallback) == "ru":
                abstract_ru = fallback
            else:
                abstract_en = fallback

    keywords_en = collect_keywords("Keywords")
    keywords_ru = collect_keywords("Ключевые слова")
    if keywords_en and not keywords_ru:
        if any(re.search(r"[А-Яа-яЁё]", kw) for kw in keywords_en):
            keywords_ru = keywords_en
            keywords_en = []
    if keywords_ru and not keywords_en:
        if not any(re.search(r"[А-Яа-яЁё]", kw) for kw in keywords_ru):
            keywords_en = keywords_ru
            keywords_ru = []

    abstract_en_stats = abstract_stats(abstract_en)
    abstract_ru_stats = abstract_stats(abstract_ru)

    doi = meta_values("citation_doi") or meta_values("DC.Identifier.DOI")
    issn_values = meta_values("citation_issn")
    pdf_values = meta_values("citation_pdf_url")
    internal_values = meta_values("DC.Identifier")
    identifiers = {"doi": doi[0] if doi else None, "edn": None, "pdf_url": pdf_values[0] if pdf_values else None}

    edn_values = [v for v in internal_values if re.match(r"^[A-Za-z0-9]{6}$", v)]
    if edn_values:
        identifiers["edn"] = edn_values[0]

    pub_dates = collect_meta_lang_values("citation_publication_date")
    publication_date = None
    if pub_dates["ru"]:
        publication_date = pub_dates["ru"][0]
    elif pub_dates["en"]:
        publication_date = pub_dates["en"][0]
    elif pub_dates["other"]:
        publication_date = pub_dates["other"][0]
    publication_date = normalize_date(publication_date)
    publication_date_display = format_date_ru(publication_date)

    authors_ru_meta = collect_meta_lang_values("citation_author")
    authors_ru = authors_ru_meta["ru"]
    authors_en = authors_ru_meta["en"]
    authors = authors_ru or authors_en or authors_ru_meta["other"]
    authors = unique([normalize_spaces(a) for a in authors if a])

    if not authors:
        authors = collect_author_section_names()
    authors_count = len(authors)

    affiliations_meta = collect_meta_lang_values("citation_author_institution")
    affiliations = affiliations_meta["ru"] or affiliations_meta["en"] or affiliations_meta["other"]

    references = collect_references()
    references_count = len(references)
    reference_first = references[0] if references else None
    reference_last = references[-1] if references else None

    return {
        "url": article_url,
        "issn": issn_values[0] if issn_values else None,
        "authors": authors,
        "authors_ru": authors_ru,
        "authors_en": authors_en,
        "authors_count": authors_count,
        "affiliations": affiliations,
        "publication_date": publication_date,
        "publication_date_display": publication_date_display,
        "title_ru": title_ru,
        "title_en": title_en,
        "article_type": None,
        "identifiers": identifiers,
        "abstract_ru": abstract_ru,
        "abstract_en": abstract_en,
        "abstract_ru_stats": abstract_ru_stats,
        "abstract_en_stats": abstract_en_stats,
        "keywords_ru": keywords_ru,
        "keywords_en": keywords_en,
        "keywords_ru_count": len(keywords_ru),
        "keywords_en_count": len(keywords_en),
        "references_count": references_count,
        "reference_first": reference_first,
        "reference_last": reference_last,
    }

