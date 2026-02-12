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


# ===========================
# Формальные проверки выделенных данных
# ===========================

_ISSN_PATTERN = re.compile(r"^\d{4}-?\d{3}[\dXx]$")  # XXXX-XXXX или XXXX-XXXx
_DOI_PATTERN = re.compile(r"^10\.\d{4,9}/.+")  # 10.XXXX/suffix
_EDN_PATTERN = re.compile(r"^[A-Za-z0-9]{6}$")  # 6 латинских символов
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
_YEAR_PATTERN = re.compile(r"^\d{4}$")

# Известные русские названия журналов (если на странице нет мета с lang=ru)
JOURNAL_TITLE_RU_BY_EN: Dict[str, str] = {
    "Inland Water Biology": "Биология внутренних вод",
}


def _validate_issn(value: Optional[str]) -> Optional[str]:
    """Проверка формата ISSN. Возвращает сообщение об ошибке или None."""
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _ISSN_PATTERN.match(s):
        return f"ISSN не соответствует формату XXXX-XXXX: «{s[:20]}{'…' if len(s) > 20 else ''}»"
    return None


def _validate_doi(value: Optional[str]) -> Optional[str]:
    """Проверка формата DOI. Возвращает сообщение об ошибке или None."""
    if not value or not value.strip():
        return None
    s = value.strip().lower()
    if not _DOI_PATTERN.match(s):
        return f"DOI не соответствует формату 10.XXXX/...: «{s[:30]}{'…' if len(s) > 30 else ''}»"
    if len(s) < 15:
        return "DOI подозрительно короткий"
    return None


def _validate_edn(value: Optional[str]) -> Optional[str]:
    """Проверка формата EDN (6 латинских символов). Возвращает сообщение об ошибке или None."""
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _EDN_PATTERN.match(s):
        return f"EDN должен быть 6 латинских символов: «{s[:15]}{'…' if len(s) > 15 else ''}»"
    return None


def _validate_date(value: Optional[str]) -> Optional[str]:
    """Проверка формата даты YYYY-MM-DD. Возвращает сообщение об ошибке или None."""
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


def _validate_year(value: Optional[object]) -> Optional[str]:
    """Проверка формата года (4 цифры). Возвращает сообщение об ошибке или None."""
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


def _validate_volume_issue(value: Optional[object], name: str) -> Optional[str]:
    """Проверка тома/номера выпуска: непустое, разумное число. Возвращает сообщение об ошибке или None."""
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


def _validate_journal_title(value: Optional[str]) -> Optional[str]:
    """Проверка названия журнала: непустое, разумная длина. Возвращает сообщение об ошибке или None."""
    if not value or not value.strip():
        return None
    s = value.strip()
    if len(s) < 2:
        return "Название журнала слишком короткое"
    if len(s) > 500:
        return "Название журнала слишком длинное"
    return None


def _validate_author_name(name: str) -> Optional[str]:
    """Проверка формата имени автора (Фамилия И. О. или Surname I. O.). Возвращает сообщение об ошибке или None."""
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


def _validate_affiliation(value: Optional[str]) -> Optional[str]:
    """Проверка организации: непустое, разумная длина. Возвращает сообщение об ошибке или None."""
    if not value or not value.strip():
        return None
    s = value.strip()
    if len(s) < 2:
        return "Название организации слишком короткое"
    if len(s) > 1000:
        return "Название организации слишком длинное"
    return None


def _transliterate_ru_to_en(text: str) -> str:
    """Транслитерация русских букв в латиницу (ГОСТ 7.79-2000) для дублирования авторов на английском."""
    if not text:
        return text
    # Многобуквенные соответствия (сначала их, чтобы не разбить на однобуквенные)
    multi = [
        ("щ", "shch"), ("ш", "sh"), ("ч", "ch"), ("ж", "zh"), ("ю", "yu"), ("я", "ya"),
        ("Щ", "Shch"), ("Ш", "Sh"), ("Ч", "Ch"), ("Ж", "Zh"), ("Ю", "Yu"), ("Я", "Ya"),
        ("х", "kh"), ("ц", "ts"), ("Х", "Kh"), ("Ц", "Ts"), ("ё", "e"), ("Ё", "E"),
    ]
    res = text
    for ru, en in multi:
        res = res.replace(ru, en)
    # Остальные буквы 1:1
    single = str.maketrans(
        "абвгдезийклмнопрстуфыьэАБВГДЕЗИЙКЛМНОПРСТУФЫЬЭ",
        "abvgdezijklmnoprstufy'eABVGDEZIJKLMNOPRSTUFY'E"
    )
    return res.translate(single)


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
        # Название журнала на русском: с той же страницы (meta lang=ru), с locale=ru или из словаря
        if not issue_metadata.get("journal_title_ru"):
            try:
                ru_root = self._fetch_html_with_locale(issue_url, "ru")
                ru_title = ru_root.xpath("//meta[@name='citation_journal_title']/@content")
                if ru_title and ru_title[0].strip():
                    issue_metadata["journal_title_ru"] = ru_title[0].strip()
            except Exception:
                pass
        if not issue_metadata.get("journal_title_ru") and issue_metadata.get("journal_title"):
            fallback = JOURNAL_TITLE_RU_BY_EN.get(issue_metadata["journal_title"].strip())
            if fallback:
                issue_metadata["journal_title_ru"] = fallback
        if not issue_metadata.get("journal_title_ru"):
            issue_metadata["journal_title_ru"] = None
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
                        if xml_parsed.get("article_type") is not None:
                            article_data["article_type"] = xml_parsed["article_type"]
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
                    "article_type": None,
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
        def pub_id_type(node: etree._Element) -> Optional[str]:
            """Значение атрибута pub-id-type (с учётом namespace)."""
            v = node.get("pub-id-type")
            if v:
                return v.strip().lower()
            for key, val in (node.attrib or {}).items():
                if key.split("}")[-1] == "pub-id-type" and val:
                    return val.strip().lower()
            return None

        for node in root.xpath(".//*[local-name()='article-id']"):
            text = (node.text or "").strip()
            if not text:
                continue
            pt = pub_id_type(node)
            if pt == "doi" and not identifiers["doi"]:
                identifiers["doi"] = text
            elif pt == "edn" and not identifiers["edn"]:
                identifiers["edn"] = text

        # Тип статьи: subj-group subj-group-type="article-type" / subject (напр. Research Article)
        article_type: Optional[str] = None
        for node in root.xpath(".//*[local-name()='subj-group']"):
            if (node.get("subj-group-type") or "").strip().lower() == "article-type":
                subj = node.xpath(".//*[local-name()='subject']/text()")
                if subj and (subj[0] or "").strip():
                    article_type = (subj[0] or "").strip()
                    break

        return {
            "abstract_ru": collect_abstract("ru"),
            "abstract_en": collect_abstract("en"),
            "keywords_ru": collect_keywords("ru"),
            "keywords_en": collect_keywords("en"),
            "identifiers": identifiers,
            "article_type": article_type,
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

        def journal_titles_by_lang() -> Tuple[Optional[str], Optional[str]]:
            """(journal_title, journal_title_ru) из мета citation_journal_title с учётом lang."""
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
            return (journal_title, journal_title_ru)

        journal_title, journal_title_ru_from_meta = journal_titles_by_lang()
        journal_title = journal_title or meta_content("citation_journal_title") or meta_content("og:site_name", "property")
        issn = None
        issn_online = None
        # Сначала блок #headerIssn — однозначно печатный/онлайн по подписи (Print/Печатный, Online/Онлайн)
        header_issn = root.xpath("//*[@id='headerIssn']")
        if header_issn:
            block_text = (header_issn[0].text_content() or "")
            m_print = re.search(
                r"ISSN\s+(\d{4}-\d{3}[\dXx])\s*\(\s*(?:Print|Печатный)\s*\)", block_text, re.IGNORECASE
            )
            if m_print:
                issn = m_print.group(1)
            m_online = re.search(
                r"ISSN\s+(\d{4}-\d{3}[\dXx])\s*\(\s*(?:Online|Онлайн)\s*\)", block_text, re.IGNORECASE
            )
            if m_online:
                issn_online = m_online.group(1)
        if not issn:
            issn = meta_content("citation_issn")
        if not issn_online:
            issn_online = meta_content("citation_issn_online") or meta_content("citation_eissn") or None
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
            "journal_title_ru": journal_title_ru_from_meta,  # может быть дополнено в parse_issue_url (locale=ru или словарь)
            "issue_title": issue_title,
            "issn": issn,
            "eissn": issn_online,
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
            """Формат даты для отображения в статье: DD.MM.YYYY."""
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
        # Англ. имена только из мета citation_author с xml:lang="en"; иначе в шаблоне показываем «Нет данных»
        authors = authors_ru or authors_en
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
        affiliations = article.get("affiliations") or []

        # Отсутствие названий
        if not (title_ru or "").strip() and not (title_en or "").strip():
            problems.append("Отсутствует название статьи")
        elif not (title_ru or "").strip():
            problems.append("Отсутствует название статьи (RU)")
        elif not (title_en or "").strip():
            problems.append("Отсутствует название статьи (EN)")
        # Отсутствие аннотаций
        if not (abstract_ru or "").strip():
            problems.append("Отсутствует аннотация (RU)")
        if not (abstract_en or "").strip():
            problems.append("Отсутствует аннотация (EN)")
        # Язык аннотаций: RU — не должна быть целиком/преимущественно на латинице; EN — на кириллице
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
            problems.append(f"Слишком короткая аннотация (RU): {len_ru} слов (рекомендуется не менее {min_abstract_words})")
        if len_en is not None and len_en < min_abstract_words:
            problems.append(f"Слишком короткая аннотация (EN): {len_en} слов (рекомендуется не менее {min_abstract_words})")
        if len_ru is not None and len_en is not None and (len_ru > 0 or len_en > 0):
            shorter, longer = min(len_ru, len_en), max(len_ru, len_en)
            if longer > 0 and shorter < 0.5 * longer:
                problems.append(
                    f"Длина аннотаций должна быть сопоставимой: RU — {len_ru} слов, EN — {len_en} слов"
                )
        if keywords_ru_count == 0:
            problems.append("Не найдены ключевые слова на русском")
        if keywords_en_count == 0:
            problems.append("Не найдены ключевые слова на английском")
        if keywords_ru_count != keywords_en_count and (keywords_ru_count > 0 or keywords_en_count > 0):
            problems.append(
                f"Количество ключевых слов должно совпадать: RU — {keywords_ru_count}, EN — {keywords_en_count}"
            )
        # Отсутствие списка литературы
        if references_count == 0:
            problems.append("Отсутствует список литературы")
        if not identifiers.get("doi"):
            problems.append("Не найден DOI статьи")
        else:
            err = _validate_doi(identifiers.get("doi"))
            if err:
                problems.append(err)
        edn = identifiers.get("edn")
        if edn:
            err = _validate_edn(edn)
            if err:
                problems.append(err)
        pub_date = article.get("publication_date")
        if pub_date:
            err = _validate_date(pub_date)
            if err:
                problems.append(err)
        authors_count = article.get("authors_count") or 0
        authors_ru = article.get("authors_ru") or []
        authors_en_list = article.get("authors_en") or []
        # Отсутствие авторов
        if not authors_ru and not article.get("authors_en") and not article.get("authors"):
            problems.append("Отсутствуют авторы")
        elif authors_count == 0 and (authors_ru or authors_en_list):
            problems.append("Количество авторов не согласовано с списком")
        for name in (authors_ru or []) + (authors_en_list or []):
            err = _validate_author_name(name)
            if err:
                problems.append(err)
                break

        # Отсутствие аффилиаций
        if not affiliations:
            problems.append("Отсутствуют организации (аффилиации)")
        for aff in affiliations[:5]:
            err = _validate_affiliation(aff)
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

    def _issue_warn(
        self, warnings: List[Dict[str, object]], text: str, severity: str = "warning", field: Optional[str] = None
    ) -> None:
        """Добавить замечание по выпуску (error — критично, warning — обратить внимание)."""
        w: Dict[str, object] = {"text": text, "severity": severity}
        if field:
            w["field"] = field
        warnings.append(w)

    def _build_issue_warnings(self, issue_metadata: Dict[str, object], articles: List[Dict[str, object]]) -> List[Dict[str, object]]:
        """Формальные проверки выпуска. Возвращает список {text, severity, field?} для подсветки в интерфейсе."""
        warnings: List[Dict[str, object]] = []
        if not issue_metadata.get("journal_title"):
            self._issue_warn(warnings, "Не найдено название журнала", "error", "journal_title")
        else:
            err = _validate_journal_title(issue_metadata.get("journal_title"))
            if err:
                self._issue_warn(warnings, err, "warning", "journal_title")
        if not issue_metadata.get("issue_title"):
            self._issue_warn(warnings, "Не найден заголовок выпуска", "warning", "issue_title")
        urls = issue_metadata.get("article_urls") or []
        if not urls:
            self._issue_warn(warnings, "Не найден список статей в выпуске", "error", "article_count")
        if not issue_metadata.get("volume"):
            self._issue_warn(warnings, "Не определен том выпуска", "warning", "volume")
        else:
            err = _validate_volume_issue(issue_metadata.get("volume"), "Том")
            if err:
                self._issue_warn(warnings, err, "warning", "volume")
        if not issue_metadata.get("issue"):
            self._issue_warn(warnings, "Не определен номер выпуска", "warning", "issue")
        else:
            err = _validate_volume_issue(issue_metadata.get("issue"), "Номер выпуска")
            if err:
                self._issue_warn(warnings, err, "warning", "issue")
        if not issue_metadata.get("year"):
            self._issue_warn(warnings, "Не определен год выпуска", "warning", "year")
        else:
            err = _validate_year(issue_metadata.get("year"))
            if err:
                self._issue_warn(warnings, err, "warning", "year")
        article_count = issue_metadata.get("article_count")
        if article_count is not None and urls and article_count != len(urls):
            self._issue_warn(
                warnings,
                f"Количество статей не совпадает: указано {article_count}, ссылок в выпуске: {len(urls)}",
                "warning",
                "article_count",
            )
        if urls and article_count == 0:
            self._issue_warn(warnings, "Количество статей указано как 0 при наличии ссылок на статьи", "warning", "article_count")

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
        journal_title_ru = None
        for node in root.xpath("//*[local-name()='journal-title']"):
            lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or "").strip().lower()
            if lang.startswith("ru"):
                t = (node.text or "").strip()
                if t:
                    journal_title_ru = t
                    break
        if not journal_title_ru:
            # JATS: trans-title-group xml:lang="ru" / trans-title (напр. «Биология внутренних вод»)
            for node in root.xpath("//*[local-name()='trans-title-group']"):
                lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or "").strip().lower()
                if lang.startswith("ru"):
                    trans = node.xpath(".//*[local-name()='trans-title']/text()")
                    if trans and (trans[0] or "").strip():
                        journal_title_ru = (trans[0] or "").strip()
                        break
        if not journal_title_ru and journal_title:
            journal_title_ru = journal_title
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
            "journal_title_ru": journal_title_ru,
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

        warnings: List[Dict[str, object]] = []
        if not journal_title:
            warnings.append({"text": "Не найдено название журнала", "severity": "error", "field": "journal_title"})
        else:
            err = _validate_journal_title(journal_title)
            if err:
                warnings.append({"text": err, "severity": "warning", "field": "journal_title"})
        if not volume_values:
            warnings.append({"text": "Не найден том выпуска", "severity": "warning", "field": "volume"})
        else:
            err = _validate_volume_issue(volume_values[0], "Том")
            if err:
                warnings.append({"text": err, "severity": "warning", "field": "volume"})
        if not issue_values:
            warnings.append({"text": "Не найден номер выпуска", "severity": "warning", "field": "issue"})
        else:
            err = _validate_volume_issue(issue_values[0], "Номер выпуска")
            if err:
                warnings.append({"text": err, "severity": "warning", "field": "issue"})
        if len(volume_values) > 1:
            warnings.append({"text": "Найдено несколько значений тома", "severity": "warning", "field": "volume"})
        if len(issue_values) > 1:
            warnings.append({"text": "Найдено несколько значений номера выпуска", "severity": "warning", "field": "issue"})
        err = _validate_year(pub_year)
        if err:
            warnings.append({"text": err, "severity": "warning", "field": "year"})
        if article_count == 0:
            warnings.append({
                "text": "В XML не найдено статей (article-meta)",
                "severity": "warning",
                "field": "article_count",
            })

        metadata["warnings"] = warnings
        return metadata
