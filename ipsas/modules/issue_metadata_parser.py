"""\
Модуль для парсинга метаданных выпуска из загруженного файла (XML или ZIP с XML).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import os
import zipfile
import urllib.request
import urllib.error
import urllib.parse
import http.cookiejar
import re
import time

from lxml import etree
from lxml import html

from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DownloadResult:
    path: Path
    size_bytes: int
    content_type: Optional[str] = None


class IssueMetadataParser:
    """Парсер метаданных выпуска по ссылке на загруженный файл."""

    def __init__(self, max_download_size: Optional[int] = None):
        if max_download_size is None:
            env_limit = os.getenv("MAX_ISSUE_DOWNLOAD_SIZE")
            max_download_size = int(env_limit) if env_limit else 0
        self.max_download_size = max_download_size

    @staticmethod
    def _detect_lang(text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        if re.search(r"[А-Яа-яЁё]", text):
            return "ru"
        return "en"

    @staticmethod
    def _abstract_stats(text: Optional[str]) -> Dict[str, Optional[object]]:
        if not text:
            return {"length": None, "first_10": None, "last_10": None}
        tokens = re.findall(r"[A-Za-zА-Яа-я0-9]+", text)
        first = " ".join(tokens[:10]) if tokens else None
        last = " ".join(tokens[-10:]) if tokens else None
        return {"length": len(tokens), "first_10": first, "last_10": last}

    def parse_issue_url(self, issue_url: str) -> Dict[str, object]:
        """Парсинг страницы выпуска и статей по URL."""
        if not issue_url:
            raise ValueError("Не указана ссылка на выпуск")

        issue_root = self._fetch_html(issue_url)
        issue_metadata = self._parse_issue_page(issue_root, issue_url)
        article_urls = issue_metadata.get("article_urls", [])

        articles: List[Dict[str, object]] = []
        for article_url in article_urls:
            try:
                article_root = self._fetch_html(article_url)
                article_data = self._parse_article_page(article_root, article_url)
                # Аннотации и ключевые слова берём только из JATS XML.
                article_data["abstract_ru"] = None
                article_data["abstract_en"] = None
                article_data["abstract_ru_stats"] = {"length": None, "first_10": None, "last_10": None}
                article_data["abstract_en_stats"] = {"length": None, "first_10": None, "last_10": None}
                article_data["keywords_ru"] = []
                article_data["keywords_en"] = []
                article_data["keywords_ru_count"] = 0
                article_data["keywords_en_count"] = 0
                xml_url = self._build_xml_url(article_url)
                if xml_url:
                    try:
                        xml_data = self._fetch_xml(xml_url)
                        xml_parsed = self._parse_jats_xml(xml_data)
                        if xml_parsed.get("abstract_ru"):
                            article_data["abstract_ru"] = xml_parsed["abstract_ru"]
                            article_data["abstract_ru_stats"] = self._abstract_stats(xml_parsed["abstract_ru"])
                        if xml_parsed.get("abstract_en"):
                            article_data["abstract_en"] = xml_parsed["abstract_en"]
                            article_data["abstract_en_stats"] = self._abstract_stats(xml_parsed["abstract_en"])
                        if xml_parsed.get("keywords_ru"):
                            article_data["keywords_ru"] = xml_parsed["keywords_ru"]
                            article_data["keywords_ru_count"] = len(xml_parsed["keywords_ru"])
                        if xml_parsed.get("keywords_en"):
                            article_data["keywords_en"] = xml_parsed["keywords_en"]
                            article_data["keywords_en_count"] = len(xml_parsed["keywords_en"])
                        if xml_parsed.get("identifiers"):
                            for key, val in xml_parsed["identifiers"].items():
                                if val is not None:
                                    article_data["identifiers"][key] = val
                    except Exception as exc:
                        logger.warning("Не удалось получить JATS XML для статьи %s: %s", article_url, exc)

                article_data["problems"] = self._build_article_problems(article_data)
                articles.append(article_data)
            except Exception as exc:
                logger.warning("Ошибка парсинга статьи %s: %s", article_url, exc)
                articles.append({
                    "url": article_url,
                    "errors": [str(exc)],
                    "issn": None,
                    "authors": [],
                    "authors_ru": [],
                    "authors_en": [],
                    "authors_count": 0,
                    "affiliations": [],
                    "publication_date": None,
                    "publication_date_display": None,
                    "title_ru": None,
                    "title_en": None,
                    "identifiers": {
                        "doi": None,
                        "edn": None,
                        "pdf_url": None,
                        "internal_id": None,
                    },
                    "abstract_ru_stats": {"length": None, "first_10": None, "last_10": None},
                    "abstract_en_stats": {"length": None, "first_10": None, "last_10": None},
                    "keywords_ru": [],
                    "keywords_en": [],
                    "keywords_ru_count": 0,
                    "keywords_en_count": 0,
                    "references_count": 0,
                    "reference_first": None,
                    "reference_last": None,
                    "problems": [],
                })
            time.sleep(0.2)

        for article in articles:
            issn = article.get("issn")
            if issn:
                if not issue_metadata.get("issn") or issue_metadata.get("issn") != issn:
                    issue_metadata["issn"] = issn
                break

        issue_warnings = self._build_issue_warnings(issue_metadata, articles)
        issue_metadata["warnings"] = issue_warnings

        return {
            "issue": issue_metadata,
            "articles": articles,
        }

    def download(self, url: str, dest_path: Path) -> DownloadResult:
        """Скачать файл по URL с ограничением по размеру (если задано)."""
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "IPSAS-Issue-Metadata-Parser/1.0"}
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                content_type = response.headers.get("Content-Type")
                total = 0
                dest_path.parent.mkdir(parents=True, exist_ok=True)

                with open(dest_path, "wb") as f:
                    while True:
                        chunk = response.read(1024 * 64)
                        if not chunk:
                            break
                        total += len(chunk)
                        if self.max_download_size and total > self.max_download_size:
                            raise ValueError("Превышен допустимый размер загружаемого файла")
                        f.write(chunk)

                return DownloadResult(path=dest_path, size_bytes=total, content_type=content_type)
        except urllib.error.HTTPError as e:
            raise ValueError(f"HTTP ошибка при загрузке: {e.code}") from e
        except urllib.error.URLError as e:
            raise ValueError(f"Ошибка при загрузке: {e.reason}") from e

    def parse_issue_metadata(self, file_path: Path) -> Dict[str, object]:
        """Определить формат и извлечь метаданные выпуска."""
        if not file_path.exists():
            raise ValueError("Файл не найден")

        if zipfile.is_zipfile(file_path):
            xml_bytes, xml_name = self._extract_xml_from_zip(file_path)
            metadata = self._parse_xml_bytes(xml_bytes)
            metadata["source_xml"] = xml_name
            return metadata

        if file_path.suffix.lower() != ".xml":
            raise ValueError("Поддерживаются только XML или ZIP с XML")

        xml_bytes = file_path.read_bytes()
        metadata = self._parse_xml_bytes(xml_bytes)
        metadata["source_xml"] = file_path.name
        return metadata

    def _fetch_html(self, url: str) -> html.HtmlElement:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "IPSAS-Issue-Metadata-Parser/1.0"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                data = response.read()
        except urllib.error.HTTPError as e:
            raise ValueError(f"HTTP ошибка при загрузке: {e.code}") from e
        except urllib.error.URLError as e:
            raise ValueError(f"Ошибка при загрузке: {e.reason}") from e

        return html.fromstring(data)

    def _with_locale(self, url: str, locale: str) -> str:
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        query["locale"] = [locale]
        new_query = urllib.parse.urlencode(query, doseq=True)
        return urllib.parse.urlunparse(parsed._replace(query=new_query))

    def _build_setlocale_url(self, url: str, locale: str) -> Optional[str]:
        parsed = urllib.parse.urlparse(url)
        path_parts = parsed.path.strip("/").split("/")
        if not path_parts:
            return None
        journal_slug = path_parts[0]
        source = urllib.parse.quote(parsed.path, safe="")
        base = f"{parsed.scheme}://{parsed.netloc}"
        return f"{base}/{journal_slug}/user/setLocale/{locale}?source={source}"

    def _fetch_html_with_locale(self, url: str, locale: str) -> html.HtmlElement:
        # Method 1: try ?locale= first
        try:
            locale_url = self._with_locale(url, locale)
            return self._fetch_html(locale_url)
        except Exception:
            pass

        # Method 2: setLocale endpoint with cookies + redirects
        setlocale_url = self._build_setlocale_url(url, locale)
        if not setlocale_url:
            return self._fetch_html(url)
        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cookie_jar),
            urllib.request.HTTPRedirectHandler()
        )
        opener.addheaders = [("User-Agent", "IPSAS-Issue-Metadata-Parser/1.0")]
        try:
            opener.open(setlocale_url, timeout=30)
            with opener.open(url, timeout=30) as response:
                data = response.read()
            return html.fromstring(data)
        except urllib.error.HTTPError as e:
            raise ValueError(f"HTTP ошибка при загрузке: {e.code}") from e
        except urllib.error.URLError as e:
            raise ValueError(f"Ошибка при загрузке: {e.reason}") from e

    def _fetch_xml(self, url: str) -> bytes:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "IPSAS-Issue-Metadata-Parser/1.0"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as e:
            raise ValueError(f"HTTP ошибка при загрузке XML: {e.code}") from e
        except urllib.error.URLError as e:
            raise ValueError(f"Ошибка при загрузке XML: {e.reason}") from e

    def _build_xml_url(self, article_url: str) -> Optional[str]:
        match = re.search(r"/article/view/(\d+)", article_url)
        if not match:
            return None
        article_id = match.group(1)
        parsed = urllib.parse.urlparse(article_url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:
            return None
        journal_slug = parts[0]
        base = f"{parsed.scheme}://{parsed.netloc}"
        return f"{base}/{journal_slug}/article/xml/{article_id}"

    def _parse_jats_xml(self, xml_bytes: bytes) -> Dict[str, object]:
        parser = etree.XMLParser(recover=True, huge_tree=True)
        try:
            root = etree.fromstring(xml_bytes, parser=parser)
        except etree.XMLSyntaxError as e:
            raise ValueError(f"Ошибка парсинга JATS XML: {e}") from e

        def detect_lang(text: Optional[str]) -> Optional[str]:
            return self._detect_lang(text)

        # XML namespace для атрибута xml:lang (JATS и др. используют namespace)
        NS_XML = "http://www.w3.org/XML/1998/namespace"

        def get_lang_attr(node: etree._Element) -> str:
            xml_lang = node.get("{http://www.w3.org/XML/1998/namespace}lang")
            lang = xml_lang or node.get("lang") or ""
            return lang.strip().lower()

        def normalize_lang(lang: str) -> str:
            if lang in {"ru", "rus", "russian"}:
                return "ru"
            if lang in {"en", "eng", "english"}:
                return "en"
            return lang

        def extract_text(node: etree._Element) -> Optional[str]:
            para_texts = [t.strip() for t in node.xpath(".//*[local-name()='p']//text()") if t and t.strip()]
            if para_texts:
                text = " ".join(para_texts)
            else:
                raw_texts = []
                for child in node:
                    if etree.QName(child).localname in {"title", "label"}:
                        continue
                    raw_texts.extend([t.strip() for t in child.xpath(".//text()") if t and t.strip()])
                text = " ".join(raw_texts)
            cleaned = re.sub(r"\s+", " ", text).strip()
            return cleaned or None

        def collect_abstract(lang: str) -> Optional[str]:
            # 0) Сначала ищем по XPath с учётом namespace: abstract/trans-abstract с xml:lang или lang.
            # В JATS элементы в namespace, атрибут xml:lang тоже в namespace — без local-name() и
            # привязки префикса xml XPath ничего не находит.
            nodes_by_lang = root.xpath(
                f".//*[(local-name()='abstract' or local-name()='trans-abstract') and "
                f"(starts-with(@xml:lang, '{lang}') or starts-with(@lang, '{lang}'))]",
                namespaces={"xml": NS_XML},
            )
            if nodes_by_lang:
                for node in nodes_by_lang:
                    text = extract_text(node)
                    if text:
                        detected = detect_lang(text)
                        if lang == "ru" and detected != "ru":
                            continue
                        if lang == "en" and detected != "en":
                            continue
                        return text

            # Все abstract/trans-abstract без привязки к namespace (local-name() уже от namespace не зависит)
            all_abstracts: List[etree._Element] = root.xpath(
                ".//*[local-name()='abstract' or local-name()='trans-abstract']"
            )
            if not all_abstracts:
                return None

            # 1) strict by lang attr (по уже полученному атрибуту узла)
            for node in all_abstracts:
                node_lang = normalize_lang(get_lang_attr(node))
                if node_lang == lang:
                    text = extract_text(node)
                    if text:
                        detected = detect_lang(text)
                        if lang == "ru" and detected != "ru":
                            continue
                        if lang == "en" and detected != "en":
                            continue
                        return text

            # 2) fallback by content language detection
            for node in all_abstracts:
                text = extract_text(node)
                if text and detect_lang(text) == lang:
                    return text

            return None

        def collect_keywords(lang: str) -> List[str]:
            kwd_groups: List[etree._Element] = root.xpath(".//*[local-name()='kwd-group']")
            # 1) strict by lang attr
            for group in kwd_groups:
                if normalize_lang(get_lang_attr(group)) != lang:
                    continue
                keywords = []
                for node in group.xpath(".//*[local-name()='kwd']"):
                    text = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
                    if text:
                        keywords.append(text)
                if keywords:
                    return keywords

            # 2) fallback by content language detection
            for group in kwd_groups:
                keywords = []
                for node in group.xpath(".//*[local-name()='kwd']"):
                    text = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
                    if text:
                        keywords.append(text)
                if not keywords:
                    continue
                sample = " ".join(keywords[:3])
                if detect_lang(sample) == lang:
                    return keywords
            return []

        # Идентификаторы статьи: DOI и EDN (из codes/ или article-id)
        identifiers: Dict[str, Optional[str]] = {"doi": None, "edn": None}
        codes_elems = root.xpath(".//*[local-name()='codes']")
        codes = codes_elems[0] if codes_elems else None
        if codes is not None:
            doi_el = codes.xpath(".//*[local-name()='doi']")
            if doi_el and doi_el[0].text and doi_el[0].text.strip():
                identifiers["doi"] = doi_el[0].text.strip()
            edn_el = codes.xpath(".//*[local-name()='edn']")
            if edn_el and edn_el[0].text and edn_el[0].text.strip():
                identifiers["edn"] = edn_el[0].text.strip()
        if not identifiers["doi"]:
            article_id_doi = root.xpath(".//*[local-name()='article-id'][@pub-id-type='doi']")
            if article_id_doi and article_id_doi[0].text and article_id_doi[0].text.strip():
                identifiers["doi"] = article_id_doi[0].text.strip()

        return {
            "abstract_ru": collect_abstract("ru"),
            "abstract_en": collect_abstract("en"),
            "keywords_ru": collect_keywords("ru"),
            "keywords_en": collect_keywords("en"),
            "identifiers": identifiers,
        }

    def _extract_xml_from_zip(self, zip_path: Path) -> Tuple[bytes, str]:
        with zipfile.ZipFile(zip_path, "r") as zf:
            xml_members = [m for m in zf.infolist() if not m.is_dir() and m.filename.lower().endswith(".xml")]
            if not xml_members:
                raise ValueError("В архиве не найден XML файл")
            xml_member = xml_members[0]
            if len(xml_members) > 1:
                logger.warning("В архиве найдено несколько XML файлов, используется первый: %s", xml_member.filename)
            with zf.open(xml_member, "r") as xml_file:
                return xml_file.read(), xml_member.filename

    def _parse_issue_page(self, root: html.HtmlElement, issue_url: str) -> Dict[str, object]:
        def text_from_xpath(xpath: str) -> Optional[str]:
            values = [v.strip() for v in root.xpath(xpath) if isinstance(v, str) and v.strip()]
            return values[0] if values else None

        def meta_content(name: str, attr: str = "name") -> Optional[str]:
            values = root.xpath(f"//meta[@{attr}='{name}']/@content")
            return values[0].strip() if values else None

        journal_title = meta_content("citation_journal_title") or meta_content("og:site_name", "property")
        issn = meta_content("citation_issn")
        if not issn:
            page_text = root.text_content()
            match = re.search(r"ISSN[:\s]+(\d{4}-\d{3}[\dXx])", page_text)
            if match:
                issn = match.group(1)
        # EDN относится к статьям, а не к выпуску — не извлекаем на уровне выпуска
        issue_title = text_from_xpath("//h1/text()") or meta_content("og:title", "property")

        volume = None
        issue = None
        year = None
        if issue_title:
            match = re.search(r"Vol\s*(\d+)[,\s]+No\s*(\d+)\s*\((\d{4})\)", issue_title, re.IGNORECASE)
            if match:
                volume, issue, year = match.group(1), match.group(2), match.group(3)
            else:
                match = re.search(r"Том\s*(\d+).+?№\s*(\d+).+?(\d{4})", issue_title, re.IGNORECASE)
                if match:
                    volume, issue, year = match.group(1), match.group(2), match.group(3)

        article_urls = self._extract_article_links(root, issue_url)

        return {
            "issue_url": issue_url,
            "journal_title": journal_title,
            "issue_title": issue_title,
            "issn": issn,
            "volume": volume,
            "issue": issue,
            "year": year,
            "article_count": len(article_urls),
            "article_urls": article_urls,
        }

    def _extract_article_links(self, root: html.HtmlElement, issue_url: str) -> List[str]:
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

    def _parse_article_page(self, root: html.HtmlElement, article_url: str) -> Dict[str, object]:
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

        def detect_lang(text: Optional[str]) -> Optional[str]:
            return self._detect_lang(text)

        def collect_section_text(title: str) -> Optional[str]:
            # OJS block with id=articleAbstract (prefer for RU)
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
                # fallback for OJS style block with id=articleAbstract
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
                texts.extend(node.xpath('.//text()'))
            text = normalize_spaces(" ".join([t for t in texts if t.strip()]))
            return text or None

        def collect_keywords(title: str) -> List[str]:
            # OJS block with id=articleSubject (prefer for RU keywords)
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
                # fallback for OJS style block with id=articleKeywords or articleSubject
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
            items = [normalize_spaces(" ".join(node.xpath('.//text()'))) for node in section.xpath('.//li')]
            items = [item for item in items if item]
            if items:
                return items
            text = normalize_spaces(" ".join(section.xpath('.//text()')))
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
            headings = root.xpath("//h2[normalize-space(text())='About the authors' or normalize-space(text())='Сведения об авторах']")
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
            months = {
                "01": "января",
                "02": "февраля",
                "03": "марта",
                "04": "апреля",
                "05": "мая",
                "06": "июня",
                "07": "июля",
                "08": "августа",
                "09": "сентября",
                "10": "октября",
                "11": "ноября",
                "12": "декабря",
            }
            month_name = months.get(month.zfill(2))
            if not month_name:
                return value
            return f"{int(day)} {month_name} {year}"

        def collect_references() -> List[str]:
            headings = root.xpath("//h2[normalize-space(text())='References' or normalize-space(text())='Литература' or normalize-space(text())='Список литературы']")
            items = []
            if headings:
                section = headings[0].getparent()
                if section is not None:
                    items = section.xpath('.//li')
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
        identifiers = {
            "doi": doi[0] if doi else None,
            "edn": None,  # EDN статьи (при наличии — из JATS XML)
            "pdf_url": pdf_values[0] if pdf_values else None,
            "internal_id": internal_values[0] if internal_values else None,
        }

        publication_candidates = (
            meta_values("citation_date")
            or meta_values("citation_publication_date")
            or meta_values("DC.Date.issued")
            or meta_values("DC.Date")
        )
        publication_date = normalize_date(publication_candidates[0] if publication_candidates else None)
        publication_date_display = format_date_ru(publication_date)

        author_meta = collect_meta_lang_values("citation_author")
        authors_ru = author_meta["ru"]
        authors_en = author_meta["en"]
        if not authors_ru and not authors_en:
            authors_en = collect_author_section_names()
        authors = authors_ru or authors_en
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
            "problems": self._build_article_problems({
                "title_ru": title_ru,
                "title_en": title_en,
                "abstract_ru": abstract_ru,
                "abstract_en": abstract_en,
                "abstract_ru_stats": abstract_ru_stats,
                "abstract_en_stats": abstract_en_stats,
                "keywords_ru_count": len(keywords_ru),
                "keywords_en_count": len(keywords_en),
                "references_count": references_count,
                "identifiers": identifiers,
            }),
        }

    def _build_article_problems(self, article: Dict[str, object]) -> List[str]:
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

        if not title_ru and not title_en:
            problems.append("Не найдено название статьи")
        if not abstract_ru:
            problems.append("Не найдена аннотация на русском")
        if not abstract_en:
            problems.append("Не найдена аннотация на английском")
        if abstract_ru_stats.get("length") is not None and abstract_ru_stats.get("length") < 50:
            problems.append("Слишком короткая аннотация на русском")
        if abstract_en_stats.get("length") is not None and abstract_en_stats.get("length") < 50:
            problems.append("Слишком короткая аннотация на английском")
        if keywords_ru_count == 0:
            problems.append("Не найдены ключевые слова на русском")
        if keywords_en_count == 0:
            problems.append("Не найдены ключевые слова на английском")
        if references_count == 0:
            problems.append("Не найден список литературы")
        if not identifiers.get("doi"):
            problems.append("Не найден DOI статьи")

        return problems

    def _build_issue_warnings(self, issue_metadata: Dict[str, object], articles: List[Dict[str, object]]) -> List[str]:
        warnings: List[str] = []
        if not issue_metadata.get("journal_title"):
            warnings.append("Не найдено название журнала")
        if not issue_metadata.get("issue_title"):
            warnings.append("Не найден заголовок выпуска")
        if not issue_metadata.get("article_urls"):
            warnings.append("Не найден список статей в выпуске")
        if not issue_metadata.get("volume"):
            warnings.append("Не определен том выпуска")
        if not issue_metadata.get("issue"):
            warnings.append("Не определен номер выпуска")
        if not issue_metadata.get("year"):
            warnings.append("Не определен год выпуска")

        doi_missing = sum(1 for article in articles if not article.get("identifiers", {}).get("doi"))
        if doi_missing:
            warnings.append(f"Статей без DOI: {doi_missing}")

        return warnings

    def _parse_xml_bytes(self, xml_bytes: bytes) -> Dict[str, object]:
        parser = etree.XMLParser(recover=True, huge_tree=True)
        try:
            root = etree.fromstring(xml_bytes, parser=parser)
        except etree.XMLSyntaxError as e:
            raise ValueError(f"Ошибка парсинга XML: {e}") from e

        def texts(xpath: str) -> List[str]:
            values = root.xpath(xpath)
            cleaned: List[str] = []
            for value in values:
                if isinstance(value, str):
                    text = value.strip()
                else:
                    text = (value.text or "").strip()
                if text:
                    cleaned.append(text)
            return cleaned

        def first_text(xpath: str) -> Optional[str]:
            values = texts(xpath)
            return values[0] if values else None

        def unique_values(values: List[str]) -> List[str]:
            result: List[str] = []
            for item in values:
                if item not in result:
                    result.append(item)
            return result

        journal_title = first_text("//*[local-name()='journal-title']/text()")
        journal_abbrev = first_text("//*[local-name()='journal-abbrev']/text()")
        publisher = first_text("//*[local-name()='publisher-name']/text()")
        issue_title = first_text("//*[local-name()='issue-title']/text()")

        issn_values = unique_values(texts("//*[local-name()='issn']/text()"))
        issn_print = None
        issn_online = None
        if issn_values:
            for node in root.xpath("//*[local-name()='issn']"):
                value = (node.text or "").strip()
                if not value:
                    continue
                pub_type = (node.attrib.get("pub-type") or "").lower()
                if pub_type in {"ppub", "print"}:
                    issn_print = value
                elif pub_type in {"epub", "online"}:
                    issn_online = value
        if not issn_print and issn_values:
            issn_print = issn_values[0]

        volume_values = unique_values(texts("//*[local-name()='front']//*[local-name()='volume']/text()"))
        if not volume_values:
            volume_values = unique_values(texts("//*[local-name()='volume']/text()"))

        issue_values = unique_values(texts("//*[local-name()='front']//*[local-name()='issue']/text()"))
        if not issue_values:
            issue_values = unique_values(texts("//*[local-name()='issue']/text()"))

        pub_dates = root.xpath("//*[local-name()='pub-date']")
        pub_date_value = None
        pub_year = None
        if pub_dates:
            def date_score(node) -> int:
                pub_type = (node.attrib.get("pub-type") or "").lower()
                if pub_type == "ppub":
                    return 0
                if pub_type == "epub":
                    return 1
                return 2

            def node_text(node, name: str) -> Optional[str]:
                values = node.xpath(f".//*[local-name()='{name}']/text()")
                return values[0].strip() if values else None

            pub_dates_sorted = sorted(pub_dates, key=date_score)
            for node in pub_dates_sorted:
                year = node_text(node, "year")
                month = node_text(node, "month")
                day = node_text(node, "day")

                if year:
                    pub_year = year
                    if month and day:
                        pub_date_value = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif month:
                        pub_date_value = f"{year}-{month.zfill(2)}"
                    else:
                        pub_date_value = year
                    break

        article_count = len(root.xpath("//*[local-name()='article-meta']"))

        def collapse(values: List[str]) -> Optional[object]:
            if not values:
                return None
            if len(values) == 1:
                return values[0]
            return values

        metadata = {
            "journal_title": journal_title,
            "journal_abbrev": journal_abbrev,
            "publisher": publisher,
            "issn": issn_print,
            "eissn": issn_online,
            "volume": collapse(volume_values),
            "issue": collapse(issue_values),
            "issue_title": issue_title,
            "publication_date": pub_date_value,
            "year": pub_year,
            "article_count": article_count,
        }

        warnings: List[str] = []
        if not journal_title:
            warnings.append("Не найдено название журнала")
        if not volume_values:
            warnings.append("Не найден том выпуска")
        if not issue_values:
            warnings.append("Не найден номер выпуска")
        if len(volume_values) > 1:
            warnings.append("Найдено несколько значений тома")
        if len(issue_values) > 1:
            warnings.append("Найдено несколько значений номера выпуска")

        metadata["warnings"] = warnings
        return metadata
