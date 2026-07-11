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
import socket

from lxml import etree
from lxml import html

from ipsas.utils.logger import get_logger
from ipsas.modules.issue_metadata.http_client import HttpClient, DEFAULT_HEADERS
from ipsas.modules.issue_metadata.models import IssueParseResult
from ipsas.modules.issue_metadata import validators as issue_validators
from ipsas.modules.issue_metadata import parsers as issue_parsers

logger = get_logger(__name__)


# Известные русские названия журналов (если на странице нет мета с lang=ru)
JOURNAL_TITLE_RU_BY_EN: Dict[str, str] = {
    "Inland Water Biology": "Биология внутренних вод",
}


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
        self.http = HttpClient.from_env(max_bytes=int(self.max_download_size or 0))

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

        started = time.monotonic()
        logger.info("Issue parse started: %s", issue_url)

        t0 = time.monotonic()
        issue_root = self._fetch_html(issue_url, is_issue_page=True)
        logger.info("Issue page fetched in %.2fs: %s", time.monotonic() - t0, issue_url)

        t0 = time.monotonic()
        issue_metadata = self._parse_issue_page(issue_root, issue_url)
        logger.info("Issue page parsed in %.2fs: %s", time.monotonic() - t0, issue_url)
        self._enrich_issue_journal_titles(issue_metadata, issue_url)
        article_urls = issue_metadata.get("article_urls", [])
        article_pdf_files = issue_metadata.get("article_pdf_files", {}) if isinstance(issue_metadata, dict) else {}

        try:
            max_articles = int(os.getenv("ISSUE_PARSER_MAX_ARTICLES", "200"))
        except ValueError:
            max_articles = 200
        if max_articles > 0 and len(article_urls) > max_articles:
            warnings = issue_metadata.get("warnings")
            if not isinstance(warnings, list):
                warnings = []
            warnings.append({
                "text": f"В выпуске найдено {len(article_urls)} статей, но будет обработано только {max_articles} (лимит ISSUE_PARSER_MAX_ARTICLES).",
                "severity": "warning",
                "field": "article_urls",
            })
            issue_metadata["warnings"] = warnings
            article_urls = list(article_urls)[:max_articles]

        logger.info("Found %d article urls on issue page", len(article_urls))

        disable_xml = (os.getenv("ISSUE_PARSER_DISABLE_XML", "").strip().lower() in {"1", "true", "yes", "y"})
        try:
            xml_failure_limit = int(os.getenv("ISSUE_PARSER_XML_FAILURE_LIMIT", "3"))
        except ValueError:
            xml_failure_limit = 3
        xml_consecutive_failures = 0

        articles: List[Dict[str, object]] = []
        for article_url in article_urls:
            try:
                t_article = time.monotonic()
                logger.info("Fetching article HTML: %s", article_url)
                article_root = self._fetch_html(article_url, is_issue_page=False)
                logger.info("Article HTML fetched in %.2fs: %s", time.monotonic() - t_article, article_url)
                article_data = self._parse_article_page(article_root, article_url)
                # PDF-файлы статьи, извлечённые с issue page (если есть)
                m_id = re.search(r"/article/view/(\d+)", article_url)
                if m_id and isinstance(article_pdf_files, dict):
                    pdfs = article_pdf_files.get(m_id.group(1))
                    if isinstance(pdfs, list):
                        article_data["pdf_files"] = pdfs
                        article_data["pdf_files_count"] = len(pdfs)

                # Нормализация организаций в список + подсчёт.
                organizations: List[str] = []
                aff = article_data.get("affiliations")
                if isinstance(aff, str):
                    parts = [p.strip() for p in aff.split(";") if p and p.strip()]
                    organizations.extend(parts)
                elif isinstance(aff, list):
                    for item in aff:
                        if isinstance(item, str):
                            parts = [p.strip() for p in item.split(";") if p and p.strip()]
                            organizations.extend(parts)
                # unique preserve order
                org_seen = set()
                org_unique: List[str] = []
                for org in organizations:
                    if org not in org_seen:
                        org_seen.add(org)
                        org_unique.append(org)
                article_data["organizations"] = org_unique
                article_data["organizations_count"] = len(org_unique)

                # Кол-во авторов: если есть список authors_ru/authors_en/authors — считаем по нему.
                authors_total = 0
                if isinstance(article_data.get("authors_ru"), list) and article_data["authors_ru"]:
                    authors_total = len(article_data["authors_ru"])
                elif isinstance(article_data.get("authors_en"), list) and article_data["authors_en"]:
                    authors_total = len(article_data["authors_en"])
                elif isinstance(article_data.get("authors"), list) and article_data["authors"]:
                    authors_total = len(article_data["authors"])
                elif isinstance(article_data.get("authors_count"), int):
                    authors_total = int(article_data.get("authors_count") or 0)
                article_data["authors_count"] = authors_total
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
                    if disable_xml:
                        logger.info("XML fetching disabled by env (ISSUE_PARSER_DISABLE_XML=1)")
                    elif xml_failure_limit > 0 and xml_consecutive_failures >= xml_failure_limit:
                        logger.warning(
                            "XML fetching disabled after %d consecutive failures (limit=%d)",
                            xml_consecutive_failures,
                            xml_failure_limit,
                        )
                    else:
                        try:
                            t_xml = time.monotonic()
                            logger.info("Fetching article XML: %s", xml_url)
                            xml_data = self._fetch_xml(xml_url)
                            logger.info("Article XML fetched in %.2fs: %s", time.monotonic() - t_xml, xml_url)
                            t_parse = time.monotonic()
                            logger.info("Parsing JATS XML: %s", xml_url)
                            xml_parsed = self._parse_jats_xml(xml_data)
                            logger.info("JATS XML parsed in %.2fs: %s", time.monotonic() - t_parse, xml_url)
                            xml_consecutive_failures = 0
                            # Прокидываем метаданные журнала из JATS (самый надёжный источник для OJS2),
                            # если HTML-страница их не дала или дала некорректно (RU попало в EN).
                            jmeta = xml_parsed.get("journal_meta")
                            if isinstance(jmeta, dict):
                                j_ru = jmeta.get("journal_title_ru")
                                j_en = jmeta.get("journal_title")
                                if not issue_metadata.get("journal_title_ru") and isinstance(j_ru, str) and j_ru.strip():
                                    issue_metadata["journal_title_ru"] = j_ru.strip()
                                if (
                                    (not issue_metadata.get("journal_title"))
                                    or (issue_metadata.get("journal_title_ru") and issue_metadata.get("journal_title") == issue_metadata.get("journal_title_ru"))
                                ):
                                    if isinstance(j_en, str) and j_en.strip():
                                        issue_metadata["journal_title"] = j_en.strip()

                                if not issue_metadata.get("issn") and isinstance(jmeta.get("issn"), str) and jmeta.get("issn").strip():
                                    issue_metadata["issn"] = jmeta["issn"].strip()
                                if not issue_metadata.get("eissn") and isinstance(jmeta.get("eissn"), str) and jmeta.get("eissn").strip():
                                    issue_metadata["eissn"] = jmeta["eissn"].strip()
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
                            pub_date = xml_parsed.get("publication_date")
                            if isinstance(pub_date, str) and pub_date.strip() and not article_data.get("publication_date"):
                                article_data["publication_date"] = pub_date.strip()
                                m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", pub_date.strip())
                                if m:
                                    article_data["publication_date_display"] = f"{m.group(3)}.{m.group(2)}.{m.group(1)}"
                            refs_lang = xml_parsed.get("references_by_lang")
                            ru = en = unk = None
                            if isinstance(refs_lang, dict):
                                ru = refs_lang.get("ru") if isinstance(refs_lang.get("ru"), dict) else None
                                en = refs_lang.get("en") if isinstance(refs_lang.get("en"), dict) else None
                                unk = refs_lang.get("unk") if isinstance(refs_lang.get("unk"), dict) else None
                            if isinstance(ru, dict):
                                article_data["references_ru_count"] = int(ru.get("count") or 0)
                                article_data["reference_ru_first"] = ru.get("first")
                                article_data["reference_ru_last"] = ru.get("last")
                            if isinstance(en, dict):
                                article_data["references_en_count"] = int(en.get("count") or 0)
                                article_data["reference_en_first"] = en.get("first")
                                article_data["reference_en_last"] = en.get("last")
                            if isinstance(unk, dict):
                                article_data["references_unk_count"] = int(unk.get("count") or 0)
                                article_data["reference_unk_first"] = unk.get("first")
                                article_data["reference_unk_last"] = unk.get("last")
                            xml_total = (
                                int(article_data.get("references_ru_count") or 0)
                                + int(article_data.get("references_en_count") or 0)
                                + int(article_data.get("references_unk_count") or 0)
                            )
                            if xml_total:
                                article_data["references_count"] = xml_total
                        except ValueError as exc:
                            xml_consecutive_failures += 1
                            logger.warning(
                                "Не удалось получить JATS XML для статьи %s (%d/%d): %s",
                                article_url,
                                xml_consecutive_failures,
                                xml_failure_limit,
                                exc,
                            )

                article_data["problems"] = self._build_article_problems(article_data)
                articles.append(article_data)
                logger.debug("Article parsed in %.2fs: %s", time.monotonic() - t_article, article_url)
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
                    "organizations": [],
                    "organizations_count": 0,
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
                    "references_ru_count": 0,
                    "references_en_count": 0,
                    "references_unk_count": 0,
                    "reference_first": None,
                    "reference_last": None,
                    "reference_ru_first": None,
                    "reference_ru_last": None,
                    "reference_en_first": None,
                    "reference_en_last": None,
                    "reference_unk_first": None,
                    "reference_unk_last": None,
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

        # Преобразуем в модели (type safety), затем обратно в dict для обратной совместимости с web/UI.
        result_model = IssueParseResult.from_mapping({"issue": issue_metadata, "articles": articles})
        result = result_model.to_dict()
        logger.info("Issue parse finished in %.2fs: %s", time.monotonic() - started, issue_url)
        return result

    def _enrich_issue_journal_titles(self, issue_metadata: Dict[str, object], issue_url: str) -> None:
        """Подтянуть RU/EN названия журнала с локализованных страниц выпуска."""
        jt = str(issue_metadata.get("journal_title") or "").strip()
        jtr = str(issue_metadata.get("journal_title_ru") or "").strip()

        if jt and self._detect_lang(jt) == "ru" and not jtr:
            issue_metadata["journal_title_ru"] = jt

        if not jtr:
            try:
                ru_root = self._fetch_html_with_locale(issue_url, "ru")
                ru_title = issue_parsers.extract_journal_title(ru_root)
                if ru_title and self._detect_lang(ru_title) == "ru":
                    issue_metadata["journal_title_ru"] = ru_title
            except ValueError as exc:
                logger.info("Не удалось получить RU-локаль выпуска %s: %s", issue_url, exc)

        jtr = str(issue_metadata.get("journal_title_ru") or "").strip()
        jt = str(issue_metadata.get("journal_title") or "").strip()
        need_en = not jt or self._detect_lang(jt) == "ru"
        if need_en:
            try:
                en_root = self._fetch_html_with_locale(issue_url, "en")
                en_title = issue_parsers.extract_journal_title(en_root)
                if en_title and self._detect_lang(en_title) == "en":
                    issue_metadata["journal_title"] = en_title
            except ValueError as exc:
                logger.info("Не удалось получить EN-локаль выпуска %s: %s", issue_url, exc)

        jt = str(issue_metadata.get("journal_title") or "").strip()
        jtr = str(issue_metadata.get("journal_title_ru") or "").strip()
        if not jtr and jt:
            fallback = JOURNAL_TITLE_RU_BY_EN.get(jt)
            if fallback:
                issue_metadata["journal_title_ru"] = fallback

        if not issue_metadata.get("year"):
            title_tag = str(issue_metadata.get("issue_title") or "")
            ids = issue_parsers.parse_issue_identifiers(title_tag)
            for key in ("volume", "issue", "issue_serial", "year"):
                if ids.get(key) and not issue_metadata.get(key):
                    issue_metadata[key] = ids[key]

    def download(self, url: str, dest_path: Path) -> DownloadResult:
        """Скачать файл по URL с ограничением по размеру (если задано)."""
        req = self.http.make_request(url)

        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(float(self.http.timeout_s))
        try:
            with urllib.request.urlopen(req, timeout=self.http.timeout_s) as response:
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
        except (TimeoutError, socket.timeout) as e:
            raise ValueError("Таймаут при загрузке файла. Попробуйте повторить позже.") from e
        finally:
            socket.setdefaulttimeout(old_timeout)

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

        if self.max_download_size:
            size = file_path.stat().st_size
            if size > self.max_download_size:
                raise ValueError("Превышен допустимый размер XML файла")

        xml_bytes = file_path.read_bytes()
        metadata = self._parse_xml_bytes(xml_bytes)
        metadata["source_xml"] = file_path.name
        return metadata

    def _fetch_html(self, url: str, *, is_issue_page: bool = False) -> html.HtmlElement:
        data, content_type = self.http.fetch_bytes_with_content_type(url, is_issue_page=is_issue_page)
        if content_type and ("html" not in content_type.lower()):
            logger.warning("Неожиданный Content-Type при загрузке %s: %s", url, content_type)
        if not data:
            raise ValueError("Пустой ответ при загрузке страницы")
        try:
            return html.fromstring(data)
        except Exception as exc:
            raise ValueError(f"Не удалось распарсить HTML страницу: {exc}") from exc

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
        try:
            locale_timeout_s = int(os.getenv("ISSUE_PARSER_LOCALE_TIMEOUT", "10"))
        except ValueError:
            locale_timeout_s = 10

        # Method 1: try ?locale= first
        try:
            t0 = time.monotonic()
            locale_url = self._with_locale(url, locale)
            logger.info("Trying locale via query param: %s", locale_url)
            return self._fetch_html(locale_url)
        except ValueError:
            logger.info("Locale via query param failed (%.2fs): %s", time.monotonic() - t0, locale_url)
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
        opener.addheaders = list(DEFAULT_HEADERS.items())
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(float(locale_timeout_s))
        try:
            logger.info("Trying locale switch (setLocale): %s", setlocale_url)
            t0 = time.monotonic()
            try:
                # Ошибка смены локали некритична: просто продолжаем загрузку страницы.
                opener.open(setlocale_url)
            except Exception as exc:
                logger.info("setLocale request failed (non-critical): %s", exc)

            logger.info("Locale switch finished in %.2fs, fetching page: %s", time.monotonic() - t0, url)
            with opener.open(url) as response:
                data = self.http.read_response_limited(
                    response,
                    limit_bytes=int(self.http.max_bytes or 0),
                    read_timeout_s=float(locale_timeout_s),
                )
            return html.fromstring(data)
        except urllib.error.HTTPError as e:
            raise ValueError(f"HTTP ошибка при загрузке: {e.code}") from e
        except urllib.error.URLError as e:
            raise ValueError(f"Ошибка при загрузке: {e.reason}") from e
        except (TimeoutError, socket.timeout) as e:
            raise ValueError("Таймаут при загрузке страницы. Попробуйте повторить позже.") from e
        finally:
            socket.setdefaulttimeout(old_timeout)

    def _fetch_xml(self, url: str) -> bytes:
        # В этой ветке сообщения должны быть про XML, поэтому ловим и переформулируем.
        try:
            return self.http.fetch_bytes(url, is_issue_page=False)
        except ValueError as e:
            msg = str(e)
            if msg.startswith("HTTP ошибка при загрузке"):
                raise ValueError(msg.replace("HTTP ошибка при загрузке", "HTTP ошибка при загрузке XML")) from e
            if msg.startswith("Ошибка при загрузке"):
                raise ValueError(msg.replace("Ошибка при загрузке", "Ошибка при загрузке XML")) from e
            if "Таймаут" in msg:
                raise ValueError("Таймаут при загрузке XML. Попробуйте повторить позже.") from e
            raise

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
        # Сначала строгий парсинг (чтобы не получить silent corruption), затем recover с явным предупреждением.
        parser = etree.XMLParser(recover=False, huge_tree=True)
        recovered = False
        try:
            root = etree.fromstring(xml_bytes, parser=parser)
        except etree.XMLSyntaxError as e:
            logger.warning("JATS XML синтаксически некорректен, включаем recover=True: %s", e)
            recovered = True
            parser_recover = etree.XMLParser(recover=True, huge_tree=True)
            try:
                root = etree.fromstring(xml_bytes, parser=parser_recover)
            except etree.XMLSyntaxError as e2:
                raise ValueError(f"Ошибка парсинга JATS XML: {e2}") from e2

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

        def collect_references_by_lang() -> Dict[str, Dict[str, Optional[object]]]:
            # JATS: ref-list/ref. В OJS/JATS часто бывает citation-alternatives с mixed-citation xml:lang=ru/en
            # в одном ref. В этом случае язык нужно брать из атрибута, а не определять по тексту.
            ru_refs: List[str] = []
            en_refs: List[str] = []
            unk_refs: List[str] = []

            for ref_node in root.xpath(".//*[local-name()='ref-list']//*[local-name()='ref']"):
                mixed_nodes: List[etree._Element] = ref_node.xpath(".//*[local-name()='mixed-citation' or local-name()='element-citation']")
                took_any = False
                for node in mixed_nodes:
                    lang = normalize_lang(get_lang_attr(node))
                    txt = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
                    txt = re.sub(r"\s+", " ", txt).strip()
                    if not txt:
                        continue
                    if lang in {"ru", "en"}:
                        (ru_refs if lang == "ru" else en_refs).append(txt)
                    else:
                        detected = detect_lang(txt)
                        if detected == "ru":
                            ru_refs.append(txt)
                        elif detected == "en":
                            en_refs.append(txt)
                        else:
                            unk_refs.append(txt)
                    took_any = True

                if took_any:
                    continue

                txt = " ".join(t.strip() for t in ref_node.xpath(".//text()") if t and t.strip())
                txt = re.sub(r"\s+", " ", txt).strip()
                if not txt:
                    continue
                lang = detect_lang(txt)
                if lang == "ru":
                    ru_refs.append(txt)
                elif lang == "en":
                    en_refs.append(txt)
                else:
                    unk_refs.append(txt)

            def stats(items: List[str]) -> Dict[str, Optional[object]]:
                return {
                    "count": len(items),
                    "first": items[0] if items else None,
                    "last": items[-1] if items else None,
                }

            return {"ru": stats(ru_refs), "en": stats(en_refs), "unk": stats(unk_refs)}

        def extract_journal_meta() -> Dict[str, Optional[str]]:
            # JATS: journal-title-group/journal-title (EN) + trans-title-group xml:lang="ru"/trans-title (RU)
            journal_title_en = None
            for node in root.xpath(".//*[local-name()='journal-title']"):
                lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or node.get("lang") or "").strip().lower()
                txt = (node.text or "").strip()
                if not txt:
                    continue
                if lang.startswith("en") or not lang:
                    journal_title_en = journal_title_en or txt
            journal_title_ru = None
            for node in root.xpath(".//*[local-name()='trans-title-group']"):
                lang = (node.get("{http://www.w3.org/XML/1998/namespace}lang") or node.get("lang") or "").strip().lower()
                if not lang.startswith("ru"):
                    continue
                trans = node.xpath(".//*[local-name()='trans-title']/text()")
                if trans and (trans[0] or "").strip():
                    journal_title_ru = (trans[0] or "").strip()
                    break
            # ISSN: встречаются и pub-type (JATS), и publication-format (некоторые выгрузки)
            issn_print = None
            issn_online = None
            for node in root.xpath(".//*[local-name()='issn']"):
                value = (node.text or "").strip()
                if not value:
                    continue
                pub_type = (node.attrib.get("pub-type") or "").strip().lower()
                pub_format = (node.attrib.get("publication-format") or node.attrib.get("publication_format") or "").strip().lower()
                if pub_type in {"ppub", "print"} or pub_format in {"print"}:
                    issn_print = issn_print or value
                elif pub_type in {"epub", "online"} or pub_format in {"electronic", "online"}:
                    issn_online = issn_online or value
                else:
                    issn_print = issn_print or value
            return {
                "journal_title": journal_title_en,
                "journal_title_ru": journal_title_ru,
                "issn": issn_print,
                "eissn": issn_online,
            }

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

        def extract_publication_date() -> Optional[str]:
            nodes = root.xpath(".//*[local-name()='pub-date']")
            ordered: List[etree._Element] = []
            for pt in ("epub", "ppub"):
                for node in nodes:
                    if (node.get("pub-type") or "").strip().lower() == pt:
                        ordered.append(node)
            for node in nodes:
                if node not in ordered:
                    ordered.append(node)
            for node in ordered:
                def part(name: str) -> Optional[str]:
                    vals = node.xpath(f".//*[local-name()='{name}']/text()")
                    return vals[0].strip() if vals and (vals[0] or "").strip() else None

                year = part("year")
                if not year or not re.match(r"^\d{4}$", year):
                    continue
                month = part("month") or "1"
                day = part("day") or "1"
                try:
                    mi, di = int(month), int(day)
                    if 1 <= mi <= 12 and 1 <= di <= 31:
                        return f"{year}-{mi:02d}-{di:02d}"
                except ValueError:
                    return f"{year}-01-01"
            return None

        data = {
            "abstract_ru": collect_abstract("ru"),
            "abstract_en": collect_abstract("en"),
            "keywords_ru": collect_keywords("ru"),
            "keywords_en": collect_keywords("en"),
            "identifiers": identifiers,
            "article_type": article_type,
            "journal_meta": extract_journal_meta(),
            "references_by_lang": collect_references_by_lang(),
            "publication_date": extract_publication_date(),
        }
        if recovered:
            warnings = data.get("warnings")
            if not isinstance(warnings, list):
                warnings = []
            warnings.append({
                "text": "JATS XML содержит синтаксические ошибки: данные извлечены в режиме восстановления (recover=True) и могут быть неполными/искажёнными.",
                "severity": "warning",
                "field": "jats_xml",
            })
            data["warnings"] = warnings
        return data

    def _extract_xml_from_zip(self, zip_path: Path) -> Tuple[bytes, str]:
        with zipfile.ZipFile(zip_path, "r") as zf:
            xml_members = [m for m in zf.infolist() if not m.is_dir() and m.filename.lower().endswith(".xml")]
            if not xml_members:
                raise ValueError("В архиве не найден XML файл")
            xml_member = xml_members[0]
            if len(xml_members) > 1:
                logger.warning("В архиве найдено несколько XML файлов, используется первый: %s", xml_member.filename)
            if self.max_download_size and xml_member.file_size > self.max_download_size:
                raise ValueError("Превышен допустимый размер XML файла в архиве")
            with zf.open(xml_member, "r") as xml_file:
                return xml_file.read(), xml_member.filename

    def _parse_issue_page(self, root: html.HtmlElement, issue_url: str) -> Dict[str, object]:
        return issue_parsers.parse_issue_page(root, issue_url)

    def _extract_article_links(self, root: html.HtmlElement, issue_url: str) -> List[str]:
        return issue_parsers.extract_article_links(root, issue_url)

    def _parse_article_page(self, root: html.HtmlElement, article_url: str) -> Dict[str, object]:
        article_data = issue_parsers.parse_article_page(root, article_url, detect_lang=self._detect_lang)
        article_data["problems"] = self._build_article_problems(article_data)
        return article_data

    def _build_article_problems(self, article: Dict[str, object]) -> List[str]:
        return issue_validators.build_article_problems(article)

    def _issue_warn(
        self, warnings: List[Dict[str, object]], text: str, severity: str = "warning", field: Optional[str] = None
    ) -> None:
        """Backward-compatible wrapper."""
        issue_validators.issue_warn(warnings, text=text, severity=severity, field=field)

    def _build_issue_warnings(self, issue_metadata: Dict[str, object], articles: List[Dict[str, object]]) -> List[Dict[str, object]]:
        # `articles` пока не используется в правилах выпуска; оставляем параметр для обратной совместимости.
        _ = articles
        return issue_validators.build_issue_warnings(issue_metadata)

    def _parse_xml_bytes(self, xml_bytes: bytes) -> Dict[str, object]:
        # Сначала строгий парсинг, затем recover с явным warning (иначе recover=True может silently портить структуру).
        recovered = False
        try:
            parser_strict = etree.XMLParser(recover=False, huge_tree=True)
            root = etree.fromstring(xml_bytes, parser=parser_strict)
        except etree.XMLSyntaxError as e:
            logger.warning("XML синтаксически некорректен, включаем recover=True: %s", e)
            recovered = True
            parser_recover = etree.XMLParser(recover=True, huge_tree=True)
            try:
                root = etree.fromstring(xml_bytes, parser=parser_recover)
            except etree.XMLSyntaxError as e2:
                raise ValueError(f"Ошибка парсинга XML: {e2}") from e2

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
        if recovered:
            warnings.append({
                "text": "XML содержит синтаксические ошибки: метаданные извлечены в режиме восстановления (recover=True) и могут быть неполными/искажёнными.",
                "severity": "warning",
                "field": "xml",
            })
        if not journal_title:
            warnings.append({"text": "Не найдено название журнала", "severity": "error", "field": "journal_title"})
        else:
            err = issue_validators.validate_journal_title(journal_title)
            if err:
                warnings.append({"text": err, "severity": "warning", "field": "journal_title"})
        if not volume_values:
            warnings.append({"text": "Не найден том выпуска", "severity": "warning", "field": "volume"})
        else:
            err = issue_validators.validate_volume_issue(volume_values[0], "Том")
            if err:
                warnings.append({"text": err, "severity": "warning", "field": "volume"})
        if not issue_values:
            warnings.append({"text": "Не найден номер выпуска", "severity": "warning", "field": "issue"})
        else:
            err = issue_validators.validate_volume_issue(issue_values[0], "Номер выпуска")
            if err:
                warnings.append({"text": err, "severity": "warning", "field": "issue"})
        if len(volume_values) > 1:
            warnings.append({"text": "Найдено несколько значений тома", "severity": "warning", "field": "volume"})
        if len(issue_values) > 1:
            warnings.append({"text": "Найдено несколько значений номера выпуска", "severity": "warning", "field": "issue"})
        err = issue_validators.validate_year(pub_year)
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
