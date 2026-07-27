"""HTML-парсеры для страницы выпуска и статьи.

Выделены из `IssueMetadataParser`, чтобы отделить извлечение данных из HTML от orchestration/HTTP/JATS.
Функции возвращают dict-структуры (обратная совместимость с текущими шаблонами).
"""

from __future__ import annotations

import re
from typing import Callable, Dict, List, Optional, Tuple

from lxml import html

from ipsas.modules.issue_metadata.validators import looks_like_edn


def _has_cyrillic(text: Optional[str]) -> bool:
    return bool(text and re.search(r"[А-Яа-яЁё]", text))


def parse_issue_identifiers(text: Optional[str]) -> Dict[str, Optional[str]]:
    """Извлечь том, номер выпуска, сквозной номер и год из заголовка OJS."""
    empty: Dict[str, Optional[str]] = {
        "volume": None,
        "issue": None,
        "issue_serial": None,
        "year": None,
        "uses_volume": None,
    }
    if not text or not text.strip():
        return empty
    t = text.strip()

    match = re.search(r"Vol\s*(\d+)[,\s]+No\s*(\d+)\s*\((\d{4})\)", t, re.IGNORECASE)
    if match:
        return {
            "volume": match.group(1),
            "issue": match.group(2),
            "issue_serial": None,
            "year": match.group(3),
            "uses_volume": "1",
        }

    match = re.search(r"Том\s*(\d+).+?№\s*(\d+).+?(\d{4})", t, re.IGNORECASE)
    if match:
        return {
            "volume": match.group(1),
            "issue": match.group(2),
            "issue_serial": None,
            "year": match.group(3),
            "uses_volume": "1",
        }

    # «Том N» упомянут, но номер/год могли не распарситься полностью
    if re.search(r"(?:^|[^\w])(?:том|vol\.?|volume)\s*\d+", t, re.IGNORECASE):
        # попробуем вытащить хотя бы том
        vm = re.search(r"(?:том|vol\.?|volume)\s*(\d+)", t, re.IGNORECASE)
        im = re.search(r"(?:№|No\.?)\s*(\d+)", t, re.IGNORECASE)
        ym = re.search(r"(19|20)\d{2}", t)
        return {
            "volume": vm.group(1) if vm else None,
            "issue": im.group(1) if im else None,
            "issue_serial": None,
            "year": ym.group(0) if ym else None,
            "uses_volume": "1",
        }

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
            "uses_volume": "0",
        }

    match = re.search(r"(?:№|No\.?)\s*(\d+)\s*\(\s*(\d{4})\s*\)", t, re.IGNORECASE)
    if match:
        return {
            "volume": None,
            "issue": match.group(1),
            "issue_serial": None,
            "year": match.group(2),
            "uses_volume": "0",
        }

    # Только «№ N» / «No N» без года — модель без тома
    match = re.search(r"(?:№|No\.?)\s*(\d+)\b", t, re.IGNORECASE)
    if match and not re.search(r"(?:том|vol\.?|volume)\b", t, re.IGNORECASE):
        ym = re.search(r"(19|20)\d{2}", t)
        return {
            "volume": None,
            "issue": match.group(1),
            "issue_serial": None,
            "year": ym.group(0) if ym else None,
            "uses_volume": "0",
        }

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


def _absolutize_url(href: Optional[str], issue_url: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if not href:
        return None
    if href.startswith("/"):
        base = re.match(r"^(https?://[^/]+)", issue_url)
        if base:
            return base.group(1) + href
    return href


def _galley_lang_from_label(label: html.HtmlElement) -> Optional[str]:
    lang_texts = [
        t.strip()
        for t in label.xpath(".//span[contains(@class,'galleyLanguageLabel')]/text()")
        if t and t.strip()
    ]
    if not lang_texts:
        return None
    return lang_texts[0].strip().strip("()").strip() or None


def _href_for_galley_label(label: html.HtmlElement) -> Optional[str]:
    """
    Найти URL PDF у galleyLabel.

    В разных темах OJS ссылка может быть:
    - предком <a> вокруг label;
    - соседним <a>;
    - <a> внутри общего родителя (типичный контейнер галереи выпуска).
    """
    # 1) Сам label обёрнут в <a>
    hrefs = label.xpath("./ancestor::a[@href][1]/@href")
    if hrefs and hrefs[0]:
        return str(hrefs[0]).strip()

    # 2) Ссылка-потомок (редко, но встречается)
    hrefs = label.xpath(".//a[@href]/@href")
    if hrefs and hrefs[0]:
        return str(hrefs[0]).strip()

    # 3) Соседние ссылки
    for axis in ("./preceding-sibling::a[@href][1]/@href", "./following-sibling::a[@href][1]/@href"):
        hrefs = label.xpath(axis)
        if hrefs and hrefs[0]:
            return str(hrefs[0]).strip()

    # 4) Ближайший контейнер галереи / родителя со ссылкой на issue PDF
    containers = label.xpath(
        "./ancestor::*[contains(@class,'galley') or contains(@class,'galleys') "
        "or contains(@class,'issueGalleys') or contains(@class,'obj_galley') "
        "or self::li or self::div][1]"
    )
    for container in containers:
        hrefs = container.xpath(".//a[@href]/@href")
        for raw in hrefs:
            href = (raw or "").strip()
            if href and "/article/" not in href:
                return href
        # если только article-ссылки — не берём
        if hrefs and hrefs[0]:
            return str(hrefs[0]).strip()

    # 5) Любой предок со ссылкой не на article
    hrefs = label.xpath("./ancestor::*//a[@href]/@href")
    for raw in hrefs:
        href = (raw or "").strip()
        if href and "/article/" not in href:
            return href
    return None


def _is_issue_pdf_href(href: str) -> bool:
    """Отличить PDF выпуска от PDF статьи."""
    h = href.lower()
    if "/article/" in h:
        return False
    # Типичные пути OJS для файла выпуска
    if any(token in h for token in ("/issue/", "/download/", "/view/", ".pdf")):
        return True
    return "/galley" in h or "issue" in h


_FULL_ISSUE_HEADING_RE = re.compile(
    r"^\s*(весь\s+выпуск|full\s+issue|complete\s+issue|entire\s+issue)\s*$",
    re.IGNORECASE,
)


def _is_full_issue_heading_text(text: Optional[str]) -> bool:
    if not text:
        return False
    normalized = re.sub(r"\s+", " ", text).strip()
    return bool(_FULL_ISSUE_HEADING_RE.match(normalized))


def _pdf_galley_label_xpath() -> str:
    return (
        ".//div[contains(@class,'galleyLabel')][.//img["
        "contains(translate(@src,'PDF','pdf'),'pdf') or "
        "contains(translate(@alt,'PDF','pdf'),'pdf')"
        "]]"
    )


def _full_issue_pdf_labels(root: html.HtmlElement) -> List[html.HtmlElement]:
    """
    PDF-метки в блоке «Весь выпуск» / Full Issue.

    При закрытом доступе у метки часто нет <a href>, но сам блок всё равно
    означает, что файл выпуска прикреплён.
    """
    headings = root.xpath("//h1 | //h2 | //h3 | //h4 | //*[contains(@class,'title')]")
    labels: List[html.HtmlElement] = []
    seen: set[int] = set()
    for heading in headings:
        if not _is_full_issue_heading_text(heading.text_content()):
            continue
        # Содержимое секции: следующие сиблинги до следующего заголовка того же уровня.
        tag = (heading.tag or "").lower()
        stop_tags = {"h1", "h2", "h3", "h4"}
        for sibling in heading.itersiblings(preceding=False):
            sib_tag = (sibling.tag or "").lower()
            if sib_tag in stop_tags and sib_tag <= tag:
                break
            if _is_full_issue_heading_text(sibling.text_content() if sib_tag in stop_tags else None):
                break
            for label in sibling.xpath(_pdf_galley_label_xpath()):
                key = id(label)
                if key not in seen:
                    seen.add(key)
                    labels.append(label)
        # Иногда метки лежат в общем родителе рядом с заголовком
        parent = heading.getparent()
        if parent is not None:
            for label in parent.xpath(_pdf_galley_label_xpath()):
                key = id(label)
                if key not in seen:
                    seen.add(key)
                    labels.append(label)
    return labels


def _append_issue_galley(
    issue_galleys: List[Dict[str, Optional[str]]],
    seen_keys: set[str],
    *,
    url: Optional[str],
    lang: Optional[str],
    access_restricted: bool = False,
) -> None:
    key = (url or "").strip() or f"restricted:{lang or ''}:{len(issue_galleys)}"
    if key in seen_keys:
        return
    seen_keys.add(key)
    issue_galleys.append(
        {
            "url": url,
            "lang": lang,
            "access_restricted": access_restricted or not bool(url),
        }
    )


def _article_id_from_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"/article/view/(\d+)", href)
    return m.group(1) if m else None


def _article_id_near_element(el: html.HtmlElement) -> Optional[str]:
    """Найти article_id по ближайшей ссылке на статью (TOC / summary)."""
    # Сначала в предках / том же блоке статьи
    for ancestor in el.xpath("./ancestor-or-self::*[position()<=8]"):
        for href in ancestor.xpath(".//a/@href"):
            article_id = _article_id_from_href(str(href))
            if article_id:
                return article_id
    # Соседние блоки в строке TOC
    parent = el.getparent()
    if parent is not None:
        for href in parent.xpath("./preceding-sibling::*//a/@href | ./following-sibling::*//a/@href"):
            article_id = _article_id_from_href(str(href))
            if article_id:
                return article_id
    return None


def _is_access_locked(el: html.HtmlElement) -> bool:
    return bool(
        el.xpath(
            ".//img[contains(@class,'issueArticlesAccessLogo') "
            "or contains(@alt,'Доступ закрыт') "
            "or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'restricted') "
            "or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'closed access')]"
        )
    )


def _article_pdf_lang_from_node(el: html.HtmlElement) -> Optional[str]:
    lang_texts = [
        t.strip()
        for t in el.xpath(
            ".//span[contains(@class,'issueArticlesLabel') or contains(@class,'galleyLanguageLabel')]/text()"
        )
        if t and t.strip()
    ]
    if not lang_texts:
        return None
    return lang_texts[0].strip().strip("()").strip() or None


def _append_article_pdf(
    article_pdf_files: Dict[str, List[Dict[str, Optional[object]]]],
    *,
    article_id: str,
    url: Optional[str],
    lang: Optional[str],
    locked: bool,
) -> None:
    bucket = article_pdf_files.setdefault(article_id, [])
    key = ((url or "").strip(), (lang or "").strip().lower())
    for existing in bucket:
        existing_key = (
            str(existing.get("url") or "").strip(),
            str(existing.get("lang") or "").strip().lower(),
        )
        if existing_key == key:
            if locked and not existing.get("locked"):
                existing["locked"] = True
            return
    bucket.append({"url": url, "lang": lang, "locked": locked})


def _collect_article_pdfs_from_issue_page(
    root: html.HtmlElement,
    issue_url: str,
) -> Dict[str, List[Dict[str, Optional[object]]]]:
    """
    PDF статей на TOC выпуска (issueArticlesFiles).

    При закрытом доступе часто есть иконка PDF и «Доступ закрыт», но без рабочей
    публичной ссылки или без обёртки <a> — файл всё равно считаем прикреплённым.
    """
    article_pdf_files: Dict[str, List[Dict[str, Optional[object]]]] = {}

    # 1) Классика: <a> с pdf.png внутри issueArticlesFiles
    pdf_links = root.xpath(
        "//div[contains(@class,'issueArticlesFiles')]//a["
        ".//img[contains(translate(@src,'PDF','pdf'),'pdf') "
        "or contains(translate(@alt,'PDF','pdf'),'pdf')]]"
    )
    for a in pdf_links:
        hrefs = a.xpath("./@href")
        href = _absolutize_url(hrefs[0] if hrefs else None, issue_url)
        article_id = _article_id_from_href(href) or _article_id_near_element(a)
        if not article_id:
            continue
        _append_article_pdf(
            article_pdf_files,
            article_id=article_id,
            url=href,
            lang=_article_pdf_lang_from_node(a),
            locked=_is_access_locked(a) or not bool(href),
        )

    # 2) Блоки без <a> (закрытый доступ): issueArticlesFilesBlock / сам issueArticlesFiles
    blocks = root.xpath(
        "//div[contains(@class,'issueArticlesFilesBlock')]"
        "[.//img[contains(translate(@src,'PDF','pdf'),'pdf') "
        "or contains(translate(@alt,'PDF','pdf'),'pdf')]]"
    )
    if not blocks:
        blocks = root.xpath(
            "//div[contains(@class,'issueArticlesFiles')]"
            "[.//img[contains(translate(@src,'PDF','pdf'),'pdf') "
            "or contains(translate(@alt,'PDF','pdf'),'pdf')]]"
        )
    for block in blocks:
        # Если внутри уже есть разобранные <a> — не дублируем «голые» метки без id
        has_anchor_pdf = bool(
            block.xpath(
                ".//a[.//img[contains(translate(@src,'PDF','pdf'),'pdf') "
                "or contains(translate(@alt,'PDF','pdf'),'pdf')]]"
            )
        )
        article_id = _article_id_near_element(block)
        if not article_id:
            continue
        if has_anchor_pdf:
            # Уже собрали через ветку <a>; если замок только на блоке — пометим
            if _is_access_locked(block):
                for item in article_pdf_files.get(article_id, []):
                    item["locked"] = True
            continue
        pdf_imgs = block.xpath(
            ".//img[contains(translate(@src,'PDF','pdf'),'pdf') "
            "or contains(translate(@alt,'PDF','pdf'),'pdf')]"
        )
        if not pdf_imgs:
            continue
        _append_article_pdf(
            article_pdf_files,
            article_id=article_id,
            url=None,
            lang=_article_pdf_lang_from_node(block),
            locked=True,
        )

    return article_pdf_files


def _collect_article_pdfs_from_article_page(
    root: html.HtmlElement,
    article_url: str,
) -> List[Dict[str, Optional[object]]]:
    """PDF на странице статьи: meta + galleyLabel / obj_galley_link, в т.ч. без href."""
    pdfs: List[Dict[str, Optional[object]]] = []
    seen: set[tuple[str, str]] = set()

    def add(url: Optional[str], lang: Optional[str], locked: bool) -> None:
        key = ((url or "").strip(), (lang or "").strip().lower())
        if key in seen and key != ("", ""):
            return
        # Для нескольких restricted без url допускаем разные lang
        if not url:
            key = ("", (lang or "").strip().lower() or f"#{len(pdfs)}")
            if key in seen:
                return
        seen.add(key)
        pdfs.append({"url": url, "lang": lang, "locked": locked or not bool(url)})

    for href in root.xpath("//meta[@name='citation_pdf_url']/@content"):
        url = _absolutize_url(str(href), article_url)
        if url:
            add(url, None, False)

    for label in root.xpath(
        "//div[contains(@class,'galleyLabel')][.//img["
        "contains(translate(@src,'PDF','pdf'),'pdf') or "
        "contains(translate(@alt,'PDF','pdf'),'pdf')]]"
    ):
        href = _absolutize_url(_href_for_galley_label(label), article_url)
        # На странице статьи ссылки обычно /article/...
        if href and "/issue/" in href.lower() and "/article/" not in href.lower():
            continue
        add(href, _galley_lang_from_label(label), _is_access_locked(label) or not bool(href))

    for a in root.xpath(
        "//a[contains(@class,'obj_galley_link') or contains(@class,'galley-link')]"
        "[contains(translate(@class,'PDF','pdf'),'pdf') "
        "or .//img[contains(translate(@src,'PDF','pdf'),'pdf') "
        "or contains(translate(@alt,'PDF','pdf'),'pdf')] "
        "or contains(translate(@href,'PDF','pdf'),'pdf')]"
    ):
        hrefs = a.xpath("./@href")
        href = _absolutize_url(hrefs[0] if hrefs else None, article_url)
        lang = _article_pdf_lang_from_node(a)
        add(href, lang, _is_access_locked(a) or not bool(href))

    return pdfs


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

    def _extract_typed_issns(text: str) -> tuple[Optional[str], Optional[str]]:
        print_m = re.search(
            r"ISSN\s+(\d{4}-\d{3}[\dXx])\s*\(\s*(?:Print|Printed|Печатн\w*|print)\s*\)",
            text,
            re.IGNORECASE,
        )
        online_m = re.search(
            r"ISSN\s+(\d{4}-\d{3}[\dXx])\s*\(\s*(?:Online|Electronic|Онлайн|online|eISSN|e-ISSN)\s*\)",
            text,
            re.IGNORECASE,
        )
        # «ISSN 0869-5733 (Print) ISSN 3034-5391 (Online)» без id=headerIssn
        return (
            print_m.group(1) if print_m else None,
            online_m.group(1) if online_m else None,
        )

    header_issn = root.xpath("//*[@id='headerIssn']")
    if header_issn:
        block_text = (header_issn[0].text_content() or "")
        issn, issn_online = _extract_typed_issns(block_text)

    if not issn or not issn_online:
        # Запасной разбор по шапке/странице: оба типа в одной строке
        page_chunks = []
        for node in root.xpath(
            "//*[contains(@id,'Issn') or contains(@class,'issn') or contains(@class,'Issn')]"
        )[:10]:
            page_chunks.append(node.text_content() or "")
        page_chunks.append(root.text_content() or "")
        for chunk in page_chunks:
            p, o = _extract_typed_issns(chunk)
            if p and not issn:
                issn = p
            if o and not issn_online:
                issn_online = o
            if issn and issn_online:
                break

    if not issn:
        issn = meta_content("citation_issn")
    if not issn_online:
        issn_online = meta_content("citation_issn_online") or meta_content("citation_eissn") or None
    # Не подставлять печатный ISSN в электронный «по умолчанию»
    if issn and issn_online and issn.strip().lower() == issn_online.strip().lower():
        # Скорее всего оба взяты из одного нетипизированного источника — сбрасываем eISSN,
        # если не подтверждён отдельной Online-меткой на странице.
        header_text = ""
        if header_issn:
            header_text = header_issn[0].text_content() or ""
        confirmed_online = bool(
            re.search(
                r"ISSN\s+" + re.escape(issn_online) + r"\s*\(\s*(?:Online|Electronic|Онлайн)",
                header_text + "\n" + (root.text_content() or ""),
                re.IGNORECASE,
            )
        )
        confirmed_print = bool(
            re.search(
                r"ISSN\s+" + re.escape(issn) + r"\s*\(\s*(?:Print|Печатн)",
                header_text + "\n" + (root.text_content() or ""),
                re.IGNORECASE,
            )
        )
        if not (confirmed_online and confirmed_print):
            issn_online = None

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
    uses_volume_raw = ids.get("uses_volume")
    uses_volume: Optional[bool]
    if uses_volume_raw == "1":
        uses_volume = True
    elif uses_volume_raw == "0":
        uses_volume = False
    else:
        uses_volume = None

    # Язык страницы выпуска
    issue_language: Optional[str] = None
    if page_lang:
        issue_language = page_lang[:2]
    elif og_locale:
        issue_language = og_locale.split("_")[0][:2] if og_locale else None

    # Дата публикации выпуска (если есть в meta / тексте)
    issue_publication_date = (
        meta_content("citation_publication_date")
        or meta_content("DC.Date")
        or meta_content("citation_date")
    )
    if not issue_publication_date:
        pub_nodes = root.xpath(
            "//*[contains(@class,'published') or contains(@class,'datePublished') "
            "or contains(@class,'issueDescription')]/text()"
        )
        for raw in pub_nodes:
            m_date = re.search(r"(20\d{2}|19\d{2})[-./](\d{1,2})[-./](\d{1,2})", (raw or "").strip())
            if m_date:
                issue_publication_date = f"{m_date.group(1)}-{int(m_date.group(2)):02d}-{int(m_date.group(3)):02d}"
                break

    # Ожидается ли сквозной номер: «№ N (serial) (year)»
    expects_issue_serial = bool(
        re.search(
            r"(?:№|No\.?)\s*\d+\s*\(\s*\d+\s*\)\s*\(\s*\d{4}\s*\)",
            (issue_title or issue_title_from_tag or ""),
            re.IGNORECASE,
        )
    )

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
    # При закрытом доступе ссылки может не быть: ориентируемся на блок «Весь выпуск».
    issue_galleys: List[Dict[str, Optional[str]]] = []
    seen_keys: set[str] = set()

    for label in _full_issue_pdf_labels(root):
        href = _absolutize_url(_href_for_galley_label(label), issue_url)
        if href and not _is_issue_pdf_href(href):
            # В секции выпуска попала ссылка на статью — пропускаем
            continue
        _append_issue_galley(
            issue_galleys,
            seen_keys,
            url=href,
            lang=_galley_lang_from_label(label),
            access_restricted=not bool(href),
        )

    galley_labels = root.xpath(
        "//div[contains(@class,'galleyLabel')][.//img["
        "contains(translate(@src,'PDF','pdf'),'pdf') or "
        "contains(translate(@alt,'PDF','pdf'),'pdf')"
        "]]"
    )
    for label in galley_labels:
        href = _absolutize_url(_href_for_galley_label(label), issue_url)
        if not href or not _is_issue_pdf_href(href):
            continue
        _append_issue_galley(
            issue_galleys,
            seen_keys,
            url=href,
            lang=_galley_lang_from_label(label),
        )

    # OJS3 / альтернативные классы ссылок на PDF выпуска
    if not issue_galleys:
        for a in root.xpath(
            "//a[contains(@class,'obj_galley_link') or contains(@class,'galley-link') "
            "or contains(@class,'pdf')]"
            "[contains(translate(@href,'PDF','pdf'),'pdf') "
            "or .//img[contains(translate(@src,'PDF','pdf'),'pdf') "
            "or contains(translate(@alt,'PDF','pdf'),'pdf')]]"
        ):
            hrefs = a.xpath("./@href")
            href = _absolutize_url(hrefs[0] if hrefs else None, issue_url)
            if not href or not _is_issue_pdf_href(href):
                continue
            lang = None
            lang_nodes = a.xpath(".//span[contains(@class,'galleyLanguageLabel')]/text()")
            if lang_nodes and lang_nodes[0]:
                lang = str(lang_nodes[0]).strip().strip("()").strip() or None
            _append_issue_galley(issue_galleys, seen_keys, url=href, lang=lang)

    if not issue_galleys:
        for a in root.xpath(
            "//a[.//img[contains(translate(@src,'PDF','pdf'),'pdf') "
            "or contains(translate(@alt,'PDF','pdf'),'pdf')]]/@href"
        ):
            href = _absolutize_url(a, issue_url)
            if not href or not _is_issue_pdf_href(href):
                continue
            _append_issue_galley(issue_galleys, seen_keys, url=href, lang=None)

    # PDF-файлы по статьям (TOC выпуска), в т.ч. при закрытом доступе без <a>.
    article_pdf_files = _collect_article_pdfs_from_issue_page(root, issue_url)

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
        "uses_volume": uses_volume,
        "expects_issue_serial": expects_issue_serial,
        "issue_language": issue_language,
        "publication_date": issue_publication_date,
        "article_count": len(article_urls),
        "article_urls": article_urls,
        "cover_url": cover_full_url,
        "cover_thumb_url": cover_thumb_url,
        "issue_galleys": issue_galleys,
        "article_pdf_files": article_pdf_files,
    }


def _normalize_spaces_local(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def collect_article_pages_from_html(root: html.HtmlElement) -> Dict[str, object]:
    """
    Страницы с HTML-страницы статьи и citation meta.
    Возвращает pages_sources-фрагмент: html, ojs_meta, biblio.
    """
    result: Dict[str, object] = {
        "html": None,
        "ojs_meta": None,
        "biblio": None,
        "page_start": None,
        "page_end": None,
        "pages": None,
    }

    def meta_values(name: str) -> List[str]:
        return [v.strip() for v in root.xpath(f"//meta[@name='{name}']/@content") if v and v.strip()]

    first = meta_values("citation_firstpage") or meta_values("citation_first_page")
    last = meta_values("citation_lastpage") or meta_values("citation_last_page")
    if first:
        start = first[0]
        end = last[0] if last else start
        result["ojs_meta"] = f"{start}-{end}"
        try:
            result["page_start"] = int(start)
            result["page_end"] = int(end)
            result["pages"] = f"{int(start)}–{int(end)}"
        except ValueError:
            result["pages"] = f"{start}-{end}"

    # Блок Pages / Страницы на странице
    for label in ("Pages", "Page", "Страницы", "Стр.", "С."):
        headings = root.xpath(
            f".//*[self::h2 or self::h3 or self::span or self::div or self::th or self::dt]"
            f"[contains(normalize-space(.), '{label}')]"
        )
        for h in headings[:6]:
            text = _normalize_spaces_local(" ".join(h.xpath(".//text()")))
            # «Pages: 18-28» в одном узле
            m = re.search(
                r"(?i)(?:pages?|страницы|стр\.?|с\.)\s*[:.]?\s*(\d{1,5}\s*[-–—]\s*\d{1,5}|\d{1,5}|[eE]\d{3,})",
                text,
            )
            if m:
                result["html"] = m.group(1).replace(" ", "")
                break
            # значение в соседнем узле
            parent = h.getparent()
            if parent is None:
                continue
            for sib in list(parent) + list(h):
                if sib is h:
                    continue
                sib_text = _normalize_spaces_local(" ".join(sib.xpath(".//text()")))
                if not sib_text or len(sib_text) > 40:
                    continue
                if re.fullmatch(r"\d{1,5}\s*[-–—]\s*\d{1,5}", sib_text) or re.fullmatch(
                    r"\d{1,5}|[eE]\d{3,}", sib_text
                ):
                    result["html"] = sib_text.replace(" ", "")
                    break
            if result["html"]:
                break
        if result["html"]:
            break

    # class="pages" / item pages
    if not result["html"]:
        for node in root.xpath(
            ".//*[contains(@class,'pages') or contains(@class,'page-range') "
            "or @id='articlePages' or contains(@class,'csl-pages')]"
        )[:8]:
            text = _normalize_spaces_local(" ".join(node.xpath(".//text()")))
            text = re.sub(r"(?i)^(?:pages?|страницы|стр\.?)\s*[:.]?\s*", "", text).strip()
            m = re.search(r"(\d{1,5}\s*[-–—]\s*\d{1,5})", text)
            if m:
                result["html"] = m.group(1).replace(" ", "")
                break

    # Библиографическая строка: «С. 18–28» / «pp. 18-28»
    if not result["biblio"]:
        for node in root.xpath(
            ".//*[contains(@class,'csl-entry') or contains(@class,'citation') "
            "or contains(@class,'bibliographic') or contains(@class,'how-to-cite')]"
        )[:10]:
            text = _normalize_spaces_local(" ".join(node.xpath(".//text()")))
            m = re.search(
                r"(?i)(?:(?:с|стр|pp?|pages?)\.?\s*)(\d{1,5}\s*[-–—]\s*\d{1,5})",
                text,
            )
            if m:
                result["biblio"] = m.group(1).replace(" ", "")
                break

    # Если html нашли — заполним page_start/end для удобства
    if result["html"]:
        from ipsas.modules.issue_metadata.validators import parse_pages_value, format_pages_display

        parsed = parse_pages_value(str(result["html"]))
        if parsed:
            result["page_start"] = parsed.get("start")
            result["page_end"] = parsed.get("end")
            result["pages"] = format_pages_display(parsed)

    return result


def collect_page_identifiers(root: html.HtmlElement, article_url: str) -> Dict[str, Optional[str]]:
    """Разделить внутренний ID и EDN на публичной странице."""
    from ipsas.modules.issue_metadata.validators import looks_like_edn

    identifiers: Dict[str, Optional[str]] = {
        "edn": None,
        "internal_id": None,
        "edn_source": None,
        "internal_id_source": None,
    }
    m_art = re.search(r"/article/view/(\d+)", article_url or "")
    if m_art:
        identifiers["internal_id"] = m_art.group(1)
        identifiers["internal_id_source"] = "url"

    def meta_values(name: str) -> List[str]:
        return [v.strip() for v in root.xpath(f"//meta[@name='{name}']/@content") if v and v.strip()]

    # Явный EDN в meta
    for name in ("citation_edn", "EDN", "edn", "DC.Identifier.EDN"):
        for v in meta_values(name):
            if looks_like_edn(v):
                identifiers["edn"] = v
                identifiers["edn_source"] = f"meta:{name}"
                break
        if identifiers["edn"]:
            break

    # Текст вида «EDN: ABCDEF» / «ID: 257724»
    body_text_nodes = root.xpath(
        ".//*[contains(translate(normalize-space(.),'ednid','EDNID'),'EDN') "
        "or contains(translate(normalize-space(.),'id','ID'),'ID')]"
    )[:40]
    for node in body_text_nodes:
        text = _normalize_spaces_local(" ".join(node.xpath(".//text()")))
        m_edn = re.search(r"(?i)\bEDN\s*[:№#]?\s*([A-Za-z]{6})\b", text)
        if m_edn and looks_like_edn(m_edn.group(1)) and not identifiers["edn"]:
            identifiers["edn"] = m_edn.group(1).upper() if m_edn.group(1).islower() else m_edn.group(1)
            # keep original case from match
            identifiers["edn"] = m_edn.group(1)
            identifiers["edn_source"] = "html_label"
        m_id = re.search(r"(?i)\b(?:ID|ИД)\s*[:№#]?\s*(\d{3,})\b", text)
        if m_id:
            # не перезаписываем URL id без нужды, но фиксируем совпадение
            if not identifiers["internal_id"]:
                identifiers["internal_id"] = m_id.group(1)
                identifiers["internal_id_source"] = "html_label"
            elif identifiers["internal_id"] == m_id.group(1):
                identifiers["internal_id_source"] = identifiers.get("internal_id_source") or "html_label"

    # DC.Identifier: только если это EDN (6 букв), иначе игнор / internal
    for v in meta_values("DC.Identifier"):
        if not v:
            continue
        if identifiers.get("internal_id") and v == identifiers["internal_id"]:
            continue
        if looks_like_edn(v) and not identifiers["edn"]:
            identifiers["edn"] = v
            identifiers["edn_source"] = "meta:DC.Identifier"
        elif v.isdigit() and not identifiers["internal_id"]:
            identifiers["internal_id"] = v
            identifiers["internal_id_source"] = "meta:DC.Identifier"

    return identifiers


def collect_page_affiliations(root: html.HtmlElement) -> Dict[str, object]:
    """
    Аффилиации с публичной страницы статьи (секция Affiliations / Аффилиации).

    Важно: отдельно фиксируем «номер есть — названия нет» (типичный баг отображения).
    """
    items: List[Dict[str, object]] = []
    author_aff_refs: List[List[int]] = []

    for author_node in root.xpath(
        ".//*[contains(@class,'authors') or contains(@class,'author')]"
        "//*[contains(@class,'affiliation') or self::sup or contains(@class,'author-affiliation')]"
    )[:40]:
        raw = _normalize_spaces_local(" ".join(author_node.xpath(".//text()")))
        nums = [int(x) for x in re.findall(r"\d+", raw)]
        if nums:
            author_aff_refs.append(nums)

    headings = root.xpath(
        ".//h2["
        "contains(translate(normalize-space(.),"
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',"
        "'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя'),'affiliation') "
        "or contains(translate(normalize-space(.),"
        "'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        "'абвгдеёжзийклмнопрстуфхцчшщъыьэюяabcdefghijklmnopqrstuvwxyz'),'аффилиац') "
        "or contains(translate(normalize-space(.),"
        "'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        "'абвгдеёжзийклмнопрстуфхцчшщъыьэюяabcdefghijklmnopqrstuvwxyz'),'организац')"
        "]"
    )
    blocks: List[html.HtmlElement] = []
    for h in headings:
        parent = h.getparent()
        if parent is not None:
            blocks.append(parent)
    blocks.extend(
        root.xpath(
            ".//*[contains(@class,'affiliations') or @id='authorAffiliations' "
            "or contains(@class,'item affiliations')]"
        )
    )

    seen_idx: set[int] = set()
    for block in blocks:
        for node in block.xpath(".//*[self::div or self::li or self::p or self::span]"):
            full = _normalize_spaces_local(" ".join(node.xpath(".//text()")))
            if not full or len(full) > 800:
                continue
            # Пропускаем заголовок секции
            if re.fullmatch(r"(?i)affiliations?|аффилиации|организации|organizations?", full):
                continue
            m = re.match(r"^(\d+)\s*[.)]?\s*(.*)$", full)
            if not m:
                continue
            idx = int(m.group(1))
            name = _normalize_spaces_local(m.group(2))
            if not name or re.fullmatch(r"\d+[.)]?", name):
                name = ""
            if idx in seen_idx:
                if name:
                    for it in items:
                        if it.get("index") == idx and not it.get("name"):
                            it["name"] = name
                            it["displayed"] = True
                continue
            seen_idx.add(idx)
            ror = None
            for href in node.xpath(".//a[contains(@href,'ror.org')]/@href"):
                ror = str(href).strip()
                break
            city = None
            country = None
            tail = re.search(r",\s*([^,]+),\s*([^,]+)$", name)
            if tail and len(tail.group(1)) < 40 and len(tail.group(2)) < 40:
                city = tail.group(1).strip()
                country = tail.group(2).strip()
            items.append(
                {
                    "index": idx,
                    "name": name,
                    "displayed": bool(name),
                    "city": city,
                    "country": country,
                    "ror": ror,
                }
            )

        if not items:
            bare = _normalize_spaces_local(" ".join(block.xpath(".//text()")))
            bare = re.sub(
                r"(?i)affiliations?|аффилиации|организации|organizations?",
                "",
                bare,
            ).strip()
            if bare and re.fullmatch(r"[\d\s,;./]+", bare):
                for n in re.findall(r"\d+", bare):
                    idx = int(n)
                    if idx in seen_idx:
                        continue
                    seen_idx.add(idx)
                    items.append(
                        {
                            "index": idx,
                            "name": "",
                            "displayed": False,
                            "city": None,
                            "country": None,
                            "ror": None,
                        }
                    )

    uniq_refs: List[List[int]] = []
    for refs in author_aff_refs:
        if refs not in uniq_refs:
            uniq_refs.append(refs)

    return {
        "page_affiliations": items,
        "author_affiliation_refs": uniq_refs,
        "affiliations_section_present": bool(headings or blocks),
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

    def unique(values: List[str]) -> List[str]:
        seen = set()
        result: List[str] = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                result.append(value)
        return result

    def _keyword_items_from_block(block: html.HtmlElement) -> List[str]:
        link_texts = [t.strip() for t in block.xpath(".//a/text()") if t and t.strip()]
        if link_texts:
            return unique(link_texts)
        texts: List[str] = []
        for node in block:
            if getattr(node, "tag", None) in {"h2", "h3"}:
                continue
            texts.extend(node.xpath(".//text()"))
        text = normalize_spaces(" ".join([t for t in texts if t and t.strip()]))
        # Убрать заголовок из текста
        text = re.sub(
            r"(?i)^(keywords?|ключевые\s+слова|index\s+terms?)\s*[:.]?\s*",
            "",
            text,
        ).strip()
        if not text:
            return []
        return unique([p.strip() for p in re.split(r"[,;]", text) if p.strip()])

    def _heading_lang(label: str) -> Optional[str]:
        low = label.strip().lower()
        if low in {"keywords", "keyword", "index terms", "subjects", "subject"}:
            return "en"
        if "ключ" in low or low in {"предметный указатель"}:
            return "ru"
        return None

    def collect_page_keywords() -> Dict[str, List[str]]:
        """Собрать RU/EN ключевые слова со страницы и из meta, без смешивания языков."""
        ru: List[str] = []
        en: List[str] = []

        blocks = root.xpath(
            "//div[@id='articleSubject' or @id='articleKeywords' "
            "or contains(@class,'keywords') or contains(@class,'article-keywords')]"
        )
        seen_blocks: set[int] = set()
        for block in blocks:
            bid = id(block)
            if bid in seen_blocks:
                continue
            seen_blocks.add(bid)
            label_nodes = block.xpath(".//*[self::h2 or self::h3 or self::span[contains(@class,'label')]]")
            label = ""
            if label_nodes:
                label = normalize_spaces(" ".join(label_nodes[0].xpath(".//text()")))
            items = _keyword_items_from_block(block)
            if not items:
                continue
            h_lang = _heading_lang(label) if label else None
            # Язык содержимого: если все пункты одного скрипта — он приоритетнее «ложного» заголовка
            # (частый OJS: <h2>Keywords</h2> + русские термины).
            cyr_n = sum(1 for it in items if detect_lang(it) == "ru")
            lat_n = sum(1 for it in items if detect_lang(it) == "en")
            content_lang = None
            if cyr_n and not lat_n:
                content_lang = "ru"
            elif lat_n and not cyr_n:
                content_lang = "en"
            target = content_lang or h_lang
            if target == "ru":
                ru.extend(items)
            elif target == "en":
                en.extend(items)
            else:
                for it in items:
                    lang = detect_lang(it)
                    if lang == "ru":
                        ru.append(it)
                    elif lang == "en":
                        en.append(it)

        # Заголовки вне известных id (если блоки выше пусты)
        if not ru and not en:
            for title, lang in (("Ключевые слова", "ru"), ("Keywords", "en")):
                headings = root.xpath(
                    f"//h2[contains(normalize-space(.), '{title}')] | "
                    f"//h3[contains(normalize-space(.), '{title}')]"
                )
                for h in headings:
                    section = h.getparent()
                    if section is None:
                        continue
                    items = _keyword_items_from_block(section)
                    if not items:
                        continue
                    cyr_n = sum(1 for it in items if detect_lang(it) == "ru")
                    lat_n = sum(1 for it in items if detect_lang(it) == "en")
                    if cyr_n and not lat_n:
                        ru.extend(items)
                    elif lat_n and not cyr_n:
                        en.extend(items)
                    elif lang == "ru":
                        ru.extend(items)
                    else:
                        en.extend(items)

        # meta: citation_keywords / keywords с xml:lang / lang
        for meta_name in ("citation_keywords", "keywords", "DC.Subject"):
            for node in root.xpath(f"//meta[@name='{meta_name}']"):
                value = (node.get("content") or "").strip()
                if not value:
                    continue
                lang_attr = (
                    node.get("{http://www.w3.org/XML/1998/namespace}lang")
                    or node.get("lang")
                    or ""
                ).strip().lower()
                parts = [p.strip() for p in re.split(r"[,;]", value) if p.strip()]
                if not parts:
                    parts = [value]
                if lang_attr.startswith("ru"):
                    ru.extend(parts)
                elif lang_attr.startswith("en"):
                    en.extend(parts)
                else:
                    for p in parts:
                        pl = detect_lang(p)
                        if pl == "ru":
                            ru.append(p)
                        elif pl == "en":
                            en.append(p)

        return {"ru": unique(ru), "en": unique(en)}

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

    def collect_references() -> Dict[str, object]:
        """
        Собрать библиографию с HTML.

        mode:
          - single_list — один блок (Литература/References), язык записей классифицируется внутри;
          - parallel_blocks — два самостоятельных блока (Литература + References).
        """
        heading_nodes = root.xpath(
            "//h2["
            "normalize-space(text())='References' or normalize-space(text())='Reference' "
            "or normalize-space(text())='Литература' or normalize-space(text())='Список литературы' "
            "or normalize-space(text())='Библиографический список'"
            "]"
        )
        blocks: List[Dict[str, object]] = []
        for h in heading_nodes:
            title = normalize_spaces(" ".join(h.xpath(".//text()")))
            section = h.getparent()
            items_nodes = section.xpath(".//li") if section is not None else []
            items = []
            for item in items_nodes:
                text = normalize_spaces(" ".join(item.xpath(".//text()")))
                if text:
                    items.append(text)
            if items:
                lang_hint = "en" if re.search(r"(?i)reference", title or "") else "ru"
                if re.search(r"(?i)литератур|библиографи", title or ""):
                    lang_hint = "ru"
                blocks.append({"title": title, "lang_hint": lang_hint, "items": items, "count": len(items)})

        if not blocks:
            items_nodes = root.xpath("//*[contains(@class,'references')]//li")
            items = []
            for item in items_nodes:
                text = normalize_spaces(" ".join(item.xpath(".//text()")))
                if text:
                    items.append(text)
            if items:
                blocks.append(
                    {"title": "References", "lang_hint": "unk", "items": items, "count": len(items)}
                )

        mode = "single_list"
        if len(blocks) >= 2:
            # Два самостоятельных блока похожего размера
            counts = sorted(int(b["count"]) for b in blocks[:2])
            titles = " ".join(str(b.get("title") or "") for b in blocks[:2]).lower()
            has_ru = bool(re.search(r"литератур|библиографи", titles))
            has_en = bool(re.search(r"reference", titles))
            if has_ru and has_en and counts[0] > 0:
                mode = "parallel_blocks"

        # Для single_list берём самый длинный блок как основной список
        primary_items: List[str] = []
        if mode == "parallel_blocks":
            primary_items = list(blocks[0]["items"])  # type: ignore[index]
        elif blocks:
            primary = max(blocks, key=lambda b: int(b.get("count") or 0))
            primary_items = list(primary.get("items") or [])  # type: ignore[arg-type]

        ru_n = en_n = unk_n = 0
        for text in primary_items:
            lat = len(re.findall(r"[A-Za-z]", text))
            cyr = len(re.findall(r"[А-Яа-яЁё]", text))
            if cyr > lat and cyr > 0:
                ru_n += 1
            elif lat > cyr and lat > 0:
                en_n += 1
            else:
                unk_n += 1

        return {
            "mode": mode,
            "blocks": blocks,
            "items": primary_items,
            "count": len(primary_items) if mode == "single_list" else sum(int(b["count"]) for b in blocks),
            "parallel_ru_count": int(blocks[0]["count"]) if mode == "parallel_blocks" and blocks else None,
            "parallel_en_count": int(blocks[1]["count"]) if mode == "parallel_blocks" and len(blocks) > 1 else None,
            "ru_count": ru_n,
            "en_count": en_n,
            "unk_count": unk_n,
            "first": primary_items[0] if primary_items else None,
            "last": primary_items[-1] if primary_items else None,
        }

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

    kw_page = collect_page_keywords()
    keywords_ru = kw_page["ru"]
    keywords_en = kw_page["en"]

    abstract_en_stats = abstract_stats(abstract_en)
    abstract_ru_stats = abstract_stats(abstract_ru)

    doi = meta_values("citation_doi") or meta_values("DC.Identifier.DOI")
    issn_values = meta_values("citation_issn")
    pdf_values = meta_values("citation_pdf_url")
    id_parts = collect_page_identifiers(root, article_url)
    identifiers: Dict[str, Optional[str]] = {
        "doi": doi[0] if doi else None,
        "edn": id_parts.get("edn"),
        "pdf_url": pdf_values[0] if pdf_values else None,
        "internal_id": id_parts.get("internal_id"),
    }
    # Не переносить internal_id в edn
    if identifiers.get("edn") and identifiers.get("internal_id"):
        if str(identifiers["edn"]) == str(identifiers["internal_id"]):
            identifiers["edn"] = None
    if identifiers.get("edn") and not looks_like_edn(str(identifiers["edn"])):
        identifiers["edn"] = None

    pages_info = collect_article_pages_from_html(root)
    pages_sources: Dict[str, object] = {
        "html": pages_info.get("html"),
        "ojs_meta": pages_info.get("ojs_meta"),
        "biblio": pages_info.get("biblio"),
        "jats": None,
        "doi": None,
    }

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

    orcids = unique(
        [
            normalize_spaces(v)
            for v in (
                meta_values("citation_author_orcid")
                + meta_values("DC.Identifier.ORCID")
                + meta_values("author_orcid")
            )
            if v
        ]
    )
    emails = unique(
        [
            normalize_spaces(v)
            for v in (meta_values("citation_author_email") + meta_values("author_email"))
            if v
        ]
    )
    # mailto: на странице статьи
    for a in root.xpath(".//a[starts-with(translate(@href,'MAILTO','mailto'),'mailto:')]"):
        href = (a.get("href") or "").strip()
        if href.lower().startswith("mailto:"):
            addr = href.split(":", 1)[1].split("?", 1)[0].strip()
            if addr:
                emails.append(addr)
    emails = unique([e for e in emails if e])

    has_corresponding = False
    for node in root.xpath(
        ".//*[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'corresponding')]"
    )[:8]:
        has_corresponding = True
        break
    if not has_corresponding and (
        meta_values("citation_author_corresponding") or meta_values("corresponding_author")
    ):
        has_corresponding = True

    affiliations_meta = collect_meta_lang_values("citation_author_institution")
    affiliations_ru = affiliations_meta["ru"]
    affiliations_en = affiliations_meta["en"]
    affiliations = affiliations_ru or affiliations_en or affiliations_meta["other"]

    page_aff = collect_page_affiliations(root)
    page_affiliations = page_aff.get("page_affiliations") or []
    # Если meta пуста, но на странице есть названия — используем их как affiliations
    if not affiliations:
        affiliations = [
            str(it.get("name"))
            for it in page_affiliations
            if isinstance(it, dict) and it.get("name")
        ]

    references_info = collect_references()
    references = list(references_info.get("items") or [])
    references_count = int(references_info.get("count") or len(references))
    reference_first = references_info.get("first")
    reference_last = references_info.get("last")

    pdf_files = _collect_article_pdfs_from_article_page(root, article_url)

    return {
        "url": article_url,
        "issn": issn_values[0] if issn_values else None,
        "authors": authors,
        "authors_ru": authors_ru,
        "authors_en": authors_en,
        "authors_count": authors_count,
        "orcids": orcids,
        "emails": emails,
        "has_corresponding_author": has_corresponding if (orcids or emails or has_corresponding) else None,
        "affiliations": affiliations,
        "affiliations_ru": affiliations_ru,
        "affiliations_en": affiliations_en,
        "page_affiliations": page_affiliations,
        "author_affiliation_refs": page_aff.get("author_affiliation_refs") or [],
        "affiliations_section_present": bool(page_aff.get("affiliations_section_present")),
        "publication_date": publication_date,
        "publication_date_display": publication_date_display,
        "title_ru": title_ru,
        "title_en": title_en,
        "article_type": None,
        "identifiers": identifiers,
        "edn_source": id_parts.get("edn_source"),
        "internal_id_source": id_parts.get("internal_id_source"),
        "pages_sources": pages_sources,
        "page_start": pages_info.get("page_start"),
        "page_end": pages_info.get("page_end"),
        "pages": pages_info.get("pages"),
        "abstract_ru": abstract_ru,
        "abstract_en": abstract_en,
        "abstract_ru_stats": abstract_ru_stats,
        "abstract_en_stats": abstract_en_stats,
        "page_abstract_ru": abstract_ru,
        "page_abstract_en": abstract_en,
        "keywords_ru": keywords_ru,
        "keywords_en": keywords_en,
        "keywords_ru_count": len(keywords_ru),
        "keywords_en_count": len(keywords_en),
        "page_keywords_ru": list(keywords_ru),
        "page_keywords_en": list(keywords_en),
        "references_count": references_count,
        "references": references,
        "references_mode": references_info.get("mode") or "single_list",
        "references_blocks": references_info.get("blocks") or [],
        "references_ru_count": int(references_info.get("ru_count") or 0),
        "references_en_count": int(references_info.get("en_count") or 0),
        "references_unk_count": int(references_info.get("unk_count") or 0),
        "references_parallel_ru_count": references_info.get("parallel_ru_count"),
        "references_parallel_en_count": references_info.get("parallel_en_count"),
        "reference_first": reference_first,
        "reference_last": reference_last,
        "pdf_files": pdf_files,
        "pdf_files_count": len(pdf_files),
    }

