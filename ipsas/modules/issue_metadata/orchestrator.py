"""\
Модуль для парсинга метаданных выпуска из загруженного файла (XML или ZIP с XML).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
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
from ipsas.modules.issue_metadata.jats import parse_jats_xml
from ipsas.modules.issue_metadata.merge import merge_jats_into_article, abstract_stats
from ipsas.modules.issue_metadata.lang import detect_lang

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
        return detect_lang(text)

    @staticmethod
    def _is_system_xml_failure(exc: BaseException) -> bool:
        """404 отдельной статьи не должен отключать JATS для остальных."""
        msg = str(exc)
        if re.search(r"\b404\b", msg):
            return False
        if "Таймаут" in msg or "timeout" in msg.lower():
            return True
        if re.search(r"\b(500|502|503|504)\b", msg):
            return True
        if "Ошибка при загрузке" in msg or "URLError" in msg or "сеть" in msg.lower():
            return True
        return False

    @staticmethod
    def _merge_pdf_files(
        from_issue: List[Dict[str, object]],
        from_article: List[Dict[str, object]],
    ) -> List[Dict[str, object]]:
        seen: set[str] = set()
        merged: List[Dict[str, object]] = []
        for pdf in [*from_issue, *from_article]:
            if not isinstance(pdf, dict):
                continue
            url = str(pdf.get("url") or "").strip()
            key = url or f"locked:{pdf.get('lang')}:{pdf.get('label')}"
            if key in seen:
                continue
            seen.add(key)
            merged.append(pdf)
        return merged

    @staticmethod
    def _dedupe_warnings(warnings: List[Dict[str, object]]) -> List[Dict[str, object]]:
        seen: set[str] = set()
        out: List[Dict[str, object]] = []
        for w in warnings:
            if not isinstance(w, dict):
                continue
            key = f"{w.get('severity')}|{w.get('field')}|{w.get('text')}"
            if key in seen:
                continue
            seen.add(key)
            out.append(w)
        return out

    @staticmethod
    def _abstract_stats(text: Optional[str]) -> Dict[str, Optional[object]]:
        return abstract_stats(text)

    def parse_issue_url(
        self,
        issue_url: str,
        *,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, object]:
        """Парсинг страницы выпуска и статей по URL.

        on_progress: необязательный колбэк этапов UI — ``fetch`` | ``articles`` | ``report``.
        """
        if not issue_url:
            raise ValueError("Не указана ссылка на выпуск")

        def _progress(stage: str) -> None:
            if on_progress is not None:
                on_progress(stage)

        started = time.monotonic()
        logger.info("Issue parse started: %s", issue_url)

        _progress("fetch")
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

        _progress("articles")
        articles: List[Dict[str, object]] = []
        for article_url in article_urls:
            try:
                t_article = time.monotonic()
                logger.info("Fetching article HTML: %s", article_url)
                article_root = self._fetch_html(article_url, is_issue_page=False)
                logger.info("Article HTML fetched in %.2fs: %s", time.monotonic() - t_article, article_url)
                article_data = self._parse_article_page(article_root, article_url)
                # PDF: сначала с TOC выпуска, иначе/дополнительно со страницы статьи
                # (при закрытом доступе на TOC может не быть публичной ссылки).
                m_id = re.search(r"/article/view/(\d+)", article_url)
                pdfs_from_issue: List[Dict[str, object]] = []
                if m_id and isinstance(article_pdf_files, dict):
                    raw = article_pdf_files.get(m_id.group(1))
                    if isinstance(raw, list):
                        pdfs_from_issue = [p for p in raw if isinstance(p, dict)]
                pdfs_from_article = article_data.get("pdf_files")
                if not isinstance(pdfs_from_article, list):
                    pdfs_from_article = []
                article_data["pdf_files"] = self._merge_pdf_files(pdfs_from_issue, pdfs_from_article)
                article_data["pdf_files_count"] = len(article_data["pdf_files"])

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
                for page_aff in article_data.get("page_affiliations") or []:
                    if isinstance(page_aff, dict) and page_aff.get("name"):
                        organizations.append(str(page_aff["name"]).strip())
                # unique preserve order
                org_seen = set()
                org_unique: List[str] = []
                for org in organizations:
                    if org and org not in org_seen:
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

                # HTML → page_*: что отображается на публичной странице.
                # Канонические данные статьи после merge — из JATS XML.
                page_abstract_ru = article_data.get("page_abstract_ru") or article_data.get("abstract_ru")
                page_abstract_en = article_data.get("page_abstract_en") or article_data.get("abstract_en")
                page_keywords_ru = list(article_data.get("page_keywords_ru") or article_data.get("keywords_ru") or [])
                page_keywords_en = list(article_data.get("page_keywords_en") or article_data.get("keywords_en") or [])
                article_data["page_abstract_ru"] = page_abstract_ru
                article_data["page_abstract_en"] = page_abstract_en
                article_data["page_keywords_ru"] = page_keywords_ru
                article_data["page_keywords_en"] = page_keywords_en
                # Временно заполняем из HTML; _merge_jats_into_article перезапишет из XML
                article_data["abstract_ru"] = page_abstract_ru
                article_data["abstract_en"] = page_abstract_en
                article_data["abstract_ru_stats"] = self._abstract_stats(
                    page_abstract_ru if isinstance(page_abstract_ru, str) else None
                )
                article_data["abstract_en_stats"] = self._abstract_stats(
                    page_abstract_en if isinstance(page_abstract_en, str) else None
                )
                article_data["keywords_ru"] = page_keywords_ru
                article_data["keywords_en"] = page_keywords_en
                article_data["keywords_ru_count"] = len(page_keywords_ru)
                article_data["keywords_en_count"] = len(page_keywords_en)
                xml_url = self._build_xml_url(article_url)
                if xml_url:
                    if disable_xml:
                        logger.info("XML fetching disabled by env (ISSUE_PARSER_DISABLE_XML=1)")
                        article_data["jats_status"] = "disabled"
                    elif xml_failure_limit > 0 and xml_consecutive_failures >= xml_failure_limit:
                        logger.warning(
                            "XML fetching disabled after %d consecutive system failures (limit=%d)",
                            xml_consecutive_failures,
                            xml_failure_limit,
                        )
                        article_data["jats_status"] = "not_checked"
                        article_data["jats_skip_reason"] = (
                            "Проверка JATS временно отключена после сетевых ошибок"
                        )
                        src_warns = article_data.setdefault("source_warnings", [])
                        if isinstance(src_warns, list):
                            src_warns.append({
                                "text": article_data["jats_skip_reason"],
                                "severity": "warning",
                                "field": "jats_xml",
                            })
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
                            article_data["jats_status"] = "ok"
                            self._merge_jats_into_article(article_data, issue_metadata, xml_parsed)
                        except ValueError as exc:
                            if self._is_system_xml_failure(exc):
                                xml_consecutive_failures += 1
                            article_data["jats_status"] = "error"
                            article_data["jats_error"] = str(exc)
                            logger.warning(
                                "Не удалось получить JATS XML для статьи %s (system_fail=%s, %d/%d): %s",
                                article_url,
                                self._is_system_xml_failure(exc),
                                xml_consecutive_failures,
                                xml_failure_limit,
                                exc,
                            )

                # DOI resolve (этапы 3–4) — опционально, по умолчанию включено
                # По умолчанию не ходим в doi.org: незарегистрированный DOI даёт 404 и не помогает правке.
                # Включить: ISSUE_PARSER_CHECK_DOI=1
                check_doi = os.getenv("ISSUE_PARSER_CHECK_DOI", "0").strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "y",
                    "on",
                }
                if check_doi:
                    try:
                        issue_validators.build_doi_check(article_data, resolve=True, timeout_s=10.0)
                    except Exception as exc:  # noqa: BLE001
                        logger.info("DOI resolve skipped for %s: %s", article_url, exc)
                self._apply_article_validation(article_data)
                articles.append(article_data)
                logger.debug("Article parsed in %.2fs: %s", time.monotonic() - t_article, article_url)
            except Exception as exc:
                logger.warning("Ошибка парсинга статьи %s: %s", article_url, exc)
                error_text = f"Не удалось проанализировать статью: {exc}"
                articles.append({
                    "url": article_url,
                    "errors": [error_text],
                    "problems": [error_text],
                    "issues": [{
                        "text": error_text,
                        "severity": "error",
                        "field": "article",
                    }],
                    "parse_status": "failed",
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
                })
            time.sleep(0.2)

        # ISSN выпуска берём со страницы выпуска (Print/Online).
        # Не перезаписываем их ISSN статьи: citation_issn статьи часто один и может
        # совпасть с eISSN, из‑за чего ложно срабатывает «печатный = электронный».
        if not issue_metadata.get("issn"):
            for article in articles:
                issn = article.get("issn")
                if issn and str(issn).strip():
                    candidate = str(issn).strip()
                    eissn = str(issue_metadata.get("eissn") or "").strip()
                    if eissn and candidate.lower() == eissn.lower():
                        continue
                    issue_metadata["issn"] = candidate
                    break
        else:
            # Расхождение ISSN выпуска и статьи — только предупреждение, без перезаписи
            issue_issn = str(issue_metadata.get("issn") or "").strip()
            for article in articles:
                article_issn = str(article.get("issn") or "").strip()
                if not article_issn or article_issn.lower() == issue_issn.lower():
                    continue
                eissn = str(issue_metadata.get("eissn") or "").strip()
                if eissn and article_issn.lower() == eissn.lower():
                    # Статья часто указывает только eISSN — это не конфликт print ISSN
                    continue
                existing = issue_metadata.get("warnings")
                if not isinstance(existing, list):
                    existing = []
                existing.append({
                    "text": (
                        "ISSN выпуска и ISSN статьи различаются: "
                        f"выпуск — {issue_issn}, статья — {article_issn}"
                    ),
                    "severity": "warning",
                    "field": "issn",
                })
                issue_metadata["warnings"] = existing
                break

        _progress("report")
        existing_warnings = issue_metadata.get("warnings")
        if not isinstance(existing_warnings, list):
            existing_warnings = []
        generated_warnings = self._build_issue_warnings(issue_metadata, articles)
        issue_metadata["warnings"] = self._dedupe_warnings(
            list(existing_warnings) + list(generated_warnings)
        )

        # Преобразуем в модели (type safety), затем обратно в dict для обратной совместимости с web/UI.
        result_model = IssueParseResult.from_mapping({"issue": issue_metadata, "articles": articles})
        result = result_model.to_dict()
        # Таблицы авторов/источников для UI — после roundtrip модели
        from ipsas.modules.issue_metadata.report_display import enrich_article_report_display

        for art in result.get("articles") or []:
            if isinstance(art, dict):
                enrich_article_report_display(art)
        logger.info("Issue parse finished in %.2fs: %s", time.monotonic() - started, issue_url)
        return result

    def _merge_jats_into_article(
        self,
        article_data: Dict[str, object],
        issue_metadata: Dict[str, object],
        xml_parsed: Dict[str, object],
    ) -> None:
        merge_jats_into_article(article_data, issue_metadata, xml_parsed)

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

        def _locale_matches(root: html.HtmlElement, expected: str) -> bool:
            title = issue_parsers.extract_journal_title(root)
            if not title:
                langs = root.xpath("/*/@lang | //html/@lang")
                if langs:
                    lang = str(langs[0]).strip().lower()
                    if expected == "ru" and lang.startswith("ru"):
                        return True
                    if expected == "en" and lang.startswith("en"):
                        return True
                page_title = root.xpath("//title/text()")
                title = (page_title[0] or "").strip() if page_title else ""
            detected = self._detect_lang(str(title) if title else None)
            if expected == "ru":
                return detected == "ru"
            if expected == "en":
                return detected == "en"
            return True

        # Method 1: try ?locale= first
        locale_url = self._with_locale(url, locale)
        try:
            t0 = time.monotonic()
            logger.info("Trying locale via query param: %s", locale_url)
            root = self._fetch_html(locale_url)
            if _locale_matches(root, locale):
                return root
            logger.info(
                "Locale via query param returned page but language mismatch (%.2fs): %s",
                time.monotonic() - t0,
                locale_url,
            )
        except ValueError:
            logger.info("Locale via query param failed: %s", locale_url)

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
        try:
            logger.info("Trying locale switch (setLocale): %s", setlocale_url)
            t0 = time.monotonic()
            try:
                opener.open(setlocale_url, timeout=locale_timeout_s)
            except Exception as exc:
                logger.info("setLocale request failed (non-critical): %s", exc)

            logger.info("Locale switch finished in %.2fs, fetching page: %s", time.monotonic() - t0, url)
            with opener.open(url, timeout=locale_timeout_s) as response:
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
        return parse_jats_xml(xml_bytes, detect_lang_fn=self._detect_lang)

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
        return article_data

    def _build_article_problems(self, article: Dict[str, object]) -> List[str]:
        return issue_validators.build_article_problems(article)

    def _apply_article_validation(self, article: Dict[str, object]) -> None:
        issue_validators.apply_article_validation(article)

    def _issue_warn(
        self, warnings: List[Dict[str, object]], text: str, severity: str = "warning", field: Optional[str] = None
    ) -> None:
        """Backward-compatible wrapper."""
        issue_validators.issue_warn(warnings, text=text, severity=severity, field=field)

    def _build_issue_warnings(self, issue_metadata: Dict[str, object], articles: List[Dict[str, object]]) -> List[Dict[str, object]]:
        return issue_validators.build_issue_warnings(issue_metadata, articles)

    def _parse_xml_bytes(self, xml_bytes: bytes) -> Dict[str, object]:
        # Сначала строгий парсинг, затем recover с явным warning (иначе recover=True может silently портить структуру).
        recovered = False
        try:
            parser_strict = etree.XMLParser(
                recover=False,
                huge_tree=False,
                resolve_entities=False,
                no_network=True,
                load_dtd=False,
            )
            root = etree.fromstring(xml_bytes, parser=parser_strict)
        except etree.XMLSyntaxError as e:
            logger.warning("XML синтаксически некорректен, включаем recover=True: %s", e)
            recovered = True
            parser_recover = etree.XMLParser(
                recover=True,
                huge_tree=False,
                resolve_entities=False,
                no_network=True,
                load_dtd=False,
            )
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
        if not journal_title_ru and journal_title and self._detect_lang(journal_title) == "ru":
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
        # Второй отличный ISSN без типа — считаем электронным (частый случай в JATS)
        if not issn_online and len(issn_values) >= 2:
            for value in issn_values:
                if value and value != issn_print:
                    issn_online = value
                    break

        volume_values = unique_values(
            texts("//*[local-name()='article-meta']/*[local-name()='volume']/text()")
        )
        if not volume_values:
            volume_values = unique_values(
                texts("//*[local-name()='front']//*[local-name()='article-meta']/*[local-name()='volume']/text()")
            )

        issue_values = unique_values(
            texts("//*[local-name()='article-meta']/*[local-name()='issue']/text()")
        )
        if not issue_values:
            issue_values = unique_values(
                texts("//*[local-name()='front']//*[local-name()='article-meta']/*[local-name()='issue']/text()")
            )

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
        from ipsas.modules.issue_metadata.findings import warning_dict

        if recovered:
            warnings.append(warning_dict(
                "XML содержит синтаксические ошибки: метаданные извлечены в режиме восстановления (recover=True) и могут быть неполными/искажёнными.",
                severity="warning",
                field="xml",
                code="xml.recovered",
            ))
        if not journal_title:
            warnings.append(warning_dict(
                "Не найдено название журнала",
                severity="error",
                field="journal_title",
                code="journal.title.missing",
            ))
        else:
            err = issue_validators.validate_journal_title(journal_title)
            if err:
                warnings.append(warning_dict(err, severity="warning", field="journal_title"))
        if not volume_values:
            warnings.append(warning_dict(
                "Не найден том выпуска", severity="warning", field="volume"
            ))
        else:
            err = issue_validators.validate_volume_issue(volume_values[0], "Том")
            if err:
                warnings.append(warning_dict(err, severity="warning", field="volume"))
        if not issue_values:
            warnings.append(warning_dict(
                "Не найден номер выпуска", severity="warning", field="issue"
            ))
        else:
            err = issue_validators.validate_volume_issue(issue_values[0], "Номер выпуска")
            if err:
                warnings.append(warning_dict(err, severity="warning", field="issue"))
        if len(volume_values) > 1:
            warnings.append(warning_dict(
                "Найдено несколько значений тома", severity="warning", field="volume"
            ))
        if len(issue_values) > 1:
            warnings.append(warning_dict(
                "Найдено несколько значений номера выпуска",
                severity="warning",
                field="issue",
            ))
        err = issue_validators.validate_year(pub_year)
        if err:
            warnings.append(warning_dict(err, severity="warning", field="year"))
        if article_count == 0:
            warnings.append(warning_dict(
                "В XML не найдено статей (article-meta)",
                severity="warning",
                field="article_count",
                code="articles.empty",
            ))

        metadata["warnings"] = warnings
        return metadata
