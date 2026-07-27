"""\
Модуль для парсинга метаданных выпуска из загруженного файла (XML или ZIP с XML).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
        """Грубая эвристика языка: ru / en / None (не текст).

        Не считает «английским» строки без букв (числа, DOI, формулы).
        Транслитерацию отделяет `looks_like_transliteration` в validators.
        """
        if not text or not str(text).strip():
            return None
        s = str(text)
        cyrillic = len(re.findall(r"[А-Яа-яЁё]", s))
        latin = len(re.findall(r"[A-Za-z]", s))
        total = cyrillic + latin
        if total == 0:
            return None
        if cyrillic / total >= 0.8:
            return "ru"
        if latin / total >= 0.8:
            return "en"
        if cyrillic > latin:
            return "ru"
        if latin > cyrillic:
            return "en"
        return None

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
        logger.info("Issue parse finished in %.2fs: %s", time.monotonic() - started, issue_url)
        return result

    def _merge_jats_into_article(
        self,
        article_data: Dict[str, object],
        issue_metadata: Dict[str, object],
        xml_parsed: Dict[str, object],
    ) -> None:
        """Слить JATS в статью: канонические данные статьи — из XML; HTML — для page_* (отображение)."""
        xml_warnings = xml_parsed.get("warnings")
        if isinstance(xml_warnings, list):
            src_warns = article_data.setdefault("source_warnings", [])
            if isinstance(src_warns, list):
                for warning in xml_warnings:
                    if isinstance(warning, dict):
                        src_warns.append(warning)

        jmeta = xml_parsed.get("journal_meta")
        if isinstance(jmeta, dict):
            j_ru = jmeta.get("journal_title_ru")
            j_en = jmeta.get("journal_title")
            if not issue_metadata.get("journal_title_ru") and isinstance(j_ru, str) and j_ru.strip():
                issue_metadata["journal_title_ru"] = j_ru.strip()
            if (
                (not issue_metadata.get("journal_title"))
                or (
                    issue_metadata.get("journal_title_ru")
                    and issue_metadata.get("journal_title") == issue_metadata.get("journal_title_ru")
                )
            ):
                if isinstance(j_en, str) and j_en.strip():
                    issue_metadata["journal_title"] = j_en.strip()

            if not issue_metadata.get("issn") and isinstance(jmeta.get("issn"), str) and jmeta.get("issn").strip():
                cand = jmeta["issn"].strip()
                if cand.lower() != str(issue_metadata.get("eissn") or "").strip().lower():
                    issue_metadata["issn"] = cand
            if not issue_metadata.get("eissn") and isinstance(jmeta.get("eissn"), str) and jmeta.get("eissn").strip():
                cand = jmeta["eissn"].strip()
                if cand.lower() != str(issue_metadata.get("issn") or "").strip().lower():
                    issue_metadata["eissn"] = cand

        # --- Названия: JATS канон, HTML → page_title_* ---
        page_title_ru = (article_data.get("title_ru") or "").strip() or None
        page_title_en = (article_data.get("title_en") or "").strip() or None
        article_data["page_title_ru"] = page_title_ru
        article_data["page_title_en"] = page_title_en
        jats_title_ru = xml_parsed.get("title_ru")
        jats_title_en = xml_parsed.get("title_en")
        if isinstance(jats_title_ru, str) and jats_title_ru.strip():
            article_data["jats_title_ru"] = jats_title_ru.strip()
            article_data["title_ru"] = jats_title_ru.strip()
            article_data["title_ru_from_jats_only"] = not bool(page_title_ru)
        if isinstance(jats_title_en, str) and jats_title_en.strip():
            article_data["jats_title_en"] = jats_title_en.strip()
            article_data["title_en"] = jats_title_en.strip()
            article_data["title_en_from_jats_only"] = not bool(page_title_en)

        # --- Аннотации / keywords: канон из JATS, page_* уже с HTML ---
        if xml_parsed.get("abstract_ru"):
            article_data["abstract_ru"] = xml_parsed["abstract_ru"]
            article_data["abstract_ru_stats"] = self._abstract_stats(xml_parsed["abstract_ru"])
            article_data["abstract_ru_from_jats_only"] = not bool(
                (article_data.get("page_abstract_ru") or "").strip()
            )
        if xml_parsed.get("abstract_en"):
            article_data["abstract_en"] = xml_parsed["abstract_en"]
            article_data["abstract_en_stats"] = self._abstract_stats(xml_parsed["abstract_en"])
            article_data["abstract_en_from_jats_only"] = not bool(
                (article_data.get("page_abstract_en") or "").strip()
            )
        if xml_parsed.get("keywords_ru"):
            article_data["keywords_ru"] = xml_parsed["keywords_ru"]
            article_data["keywords_ru_count"] = len(xml_parsed["keywords_ru"])
            article_data["keywords_ru_from_jats_only"] = not bool(article_data.get("page_keywords_ru"))
        if xml_parsed.get("keywords_en"):
            article_data["keywords_en"] = xml_parsed["keywords_en"]
            article_data["keywords_en_count"] = len(xml_parsed["keywords_en"])
            article_data["keywords_en_from_jats_only"] = not bool(article_data.get("page_keywords_en"))

        # --- Авторы: JATS канон ---
        page_authors_ru = list(article_data.get("authors_ru") or [])
        page_authors_en = list(article_data.get("authors_en") or [])
        article_data["page_authors_ru"] = page_authors_ru
        article_data["page_authors_en"] = page_authors_en
        jats_authors_ru = xml_parsed.get("authors_ru")
        jats_authors_en = xml_parsed.get("authors_en")
        jats_authors = xml_parsed.get("authors")
        if isinstance(jats_authors_ru, list) and jats_authors_ru:
            article_data["authors_ru"] = list(jats_authors_ru)
        if isinstance(jats_authors_en, list) and jats_authors_en:
            article_data["authors_en"] = list(jats_authors_en)
        if isinstance(jats_authors, list) and jats_authors:
            article_data["authors"] = list(jats_authors)
        elif article_data.get("authors_ru") or article_data.get("authors_en"):
            article_data["authors"] = list(article_data.get("authors_ru") or article_data.get("authors_en") or [])
        if isinstance(xml_parsed.get("orcids"), list) and xml_parsed["orcids"]:
            article_data["orcids"] = list(xml_parsed["orcids"])
        if isinstance(xml_parsed.get("emails"), list) and xml_parsed["emails"]:
            article_data["emails"] = list(xml_parsed["emails"])
        if xml_parsed.get("has_corresponding_author") is not None:
            article_data["has_corresponding_author"] = xml_parsed["has_corresponding_author"]
        # Пересчёт числа авторов после JATS: число contrib, не длина одного языкового списка
        if isinstance(xml_parsed.get("authors_count"), int) and int(xml_parsed["authors_count"]) > 0:
            article_data["authors_count"] = int(xml_parsed["authors_count"])
        elif isinstance(article_data.get("authors"), list) and article_data["authors"]:
            article_data["authors_count"] = len(article_data["authors"])
        elif isinstance(article_data.get("authors_ru"), list) and article_data["authors_ru"]:
            article_data["authors_count"] = len(article_data["authors_ru"])
        elif isinstance(article_data.get("authors_en"), list) and article_data["authors_en"]:
            article_data["authors_count"] = len(article_data["authors_en"])

        # Организации из JATS, если на странице пусто
        jats_affs = xml_parsed.get("jats_affiliations")
        if isinstance(jats_affs, list) and jats_affs:
            article_data["jats_affiliations"] = jats_affs
            jats_org_names = [
                str(a.get("name")).strip()
                for a in jats_affs
                if isinstance(a, dict) and str(a.get("name") or "").strip()
            ]
            if jats_org_names and not article_data.get("organizations"):
                article_data["organizations"] = jats_org_names
                article_data["organizations_count"] = len(jats_org_names)
                article_data["affiliations"] = list(jats_org_names)
        if isinstance(xml_parsed.get("contributor_affiliation_refs"), list):
            article_data["contributor_affiliation_refs"] = xml_parsed["contributor_affiliation_refs"]
        if isinstance(xml_parsed.get("broken_affiliation_refs"), list):
            article_data["broken_affiliation_refs"] = xml_parsed["broken_affiliation_refs"]

        if xml_parsed.get("identifiers"):
            from ipsas.modules.issue_metadata.validators import looks_like_edn

            idents = article_data.get("identifiers")
            if not isinstance(idents, dict):
                idents = {}
                article_data["identifiers"] = idents
            for key, val in xml_parsed["identifiers"].items():
                if val is None:
                    continue
                if key == "edn":
                    if looks_like_edn(str(val)):
                        idents["edn"] = val
                    elif str(val).isdigit() and not idents.get("internal_id"):
                        idents["internal_id"] = str(val)
                    else:
                        idents["invalid_edn"] = str(val)
                    continue
                if key == "invalid_edn":
                    if not idents.get("invalid_edn"):
                        idents["invalid_edn"] = str(val)
                    continue
                if key == "internal_id":
                    # JATS/URL: не затирать уже найденный internal_id с URL
                    if not idents.get("internal_id"):
                        idents["internal_id"] = val
                    continue
                # DOI и прочее — JATS предпочтительнее, если есть
                if key == "doi" and val:
                    idents["doi"] = val
                    continue
                if val is not None and (not idents.get(key)):
                    idents[key] = val

        pages_sources = article_data.get("pages_sources")
        if not isinstance(pages_sources, dict):
            pages_sources = {}
        if xml_parsed.get("pages_jats"):
            pages_sources["jats"] = xml_parsed["pages_jats"]
        article_data["pages_sources"] = pages_sources
        # Итоговые страницы: JATS → HTML → …
        issue_validators.enrich_article_pages(article_data)

        if xml_parsed.get("article_type") is not None:
            article_data["article_type"] = xml_parsed["article_type"]

        pub_date = xml_parsed.get("publication_date")
        if isinstance(pub_date, str) and pub_date.strip():
            article_data["publication_date"] = pub_date.strip()
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", pub_date.strip())
            if m:
                article_data["publication_date_display"] = f"{m.group(3)}.{m.group(2)}.{m.group(1)}"

        article_data["data_source"] = "jats"

        refs_lang = xml_parsed.get("references_by_lang")
        ru = en = unk = None
        if isinstance(refs_lang, dict):
            ru = refs_lang.get("ru") if isinstance(refs_lang.get("ru"), dict) else None
            en = refs_lang.get("en") if isinstance(refs_lang.get("en"), dict) else None
            unk = refs_lang.get("unk") if isinstance(refs_lang.get("unk"), dict) else None

            html_refs = article_data.get("references")
            if not isinstance(html_refs, list):
                html_refs = []
            html_count = int(article_data.get("references_count") or 0)
            html_mode = str(article_data.get("references_mode") or "single_list")

            jats_items = refs_lang.get("items")
            total_refs = None
            if refs_lang.get("total_refs") is not None:
                try:
                    total_refs = int(refs_lang.get("total_refs") or 0)
                except (TypeError, ValueError):
                    total_refs = None
            if total_refs is None and isinstance(jats_items, list):
                total_refs = len(jats_items)

            # Библиография для анализа — предпочтительно из JATS (<ref>).
            if isinstance(jats_items, list) and jats_items:
                if html_refs:
                    article_data["references_html"] = html_refs
                    article_data["references_html_count"] = html_count or len(html_refs)
                article_data["references"] = list(jats_items)
                article_data["references_count"] = int(total_refs or len(jats_items))
                article_data["references_source"] = "jats"
                article_data["reference_first"] = jats_items[0]
                article_data["reference_last"] = jats_items[-1]
            elif total_refs and not html_count:
                article_data["references_count"] = total_refs
                article_data["references_source"] = "jats"
            else:
                article_data["references_source"] = article_data.get("references_source") or "html"

            # Режим: два блока на публичной странице важны для UI-проверки;
            # иначе берём режим JATS (single_list / parallel_citations).
            jats_mode = refs_lang.get("mode")
            if html_mode == "parallel_blocks":
                article_data["references_mode"] = "parallel_blocks"
            elif isinstance(jats_mode, str) and jats_mode.strip():
                article_data["references_mode"] = jats_mode.strip()

            if html_count and total_refs and abs(html_count - int(total_refs)) > 0:
                article_data["references_jats_count"] = int(total_refs)
                article_data["references_html_count"] = html_count
                if abs(html_count - int(total_refs)) >= 2:
                    article_data["references_count_mismatch_html_jats"] = True

            # Язык источников из xml:lang (не перезаписывать эвристикой по алфавиту)
            if refs_lang.get("lang_from_attr"):
                article_data["references_lang_source"] = "xml_lang"
                if isinstance(ru, dict):
                    article_data["references_ru_count"] = int(ru.get("count") or 0)
                if isinstance(en, dict):
                    article_data["references_en_count"] = int(en.get("count") or 0)
                if isinstance(unk, dict):
                    article_data["references_unk_count"] = int(unk.get("count") or 0)
            elif article_data.get("references_source") == "jats":
                # Без xml:lang — счётчики из JATS-эвристики, recompute может уточнить по items
                if isinstance(ru, dict):
                    article_data["references_ru_count"] = int(ru.get("count") or 0)
                if isinstance(en, dict):
                    article_data["references_en_count"] = int(en.get("count") or 0)
                if isinstance(unk, dict):
                    article_data["references_unk_count"] = int(unk.get("count") or 0)

        if isinstance(ru, dict) and not article_data.get("reference_ru_first"):
            article_data["reference_ru_first"] = ru.get("first")
            article_data["reference_ru_last"] = ru.get("last")
        if isinstance(en, dict) and not article_data.get("reference_en_first"):
            article_data["reference_en_first"] = en.get("first")
            article_data["reference_en_last"] = en.get("last")
        if isinstance(unk, dict) and not article_data.get("reference_unk_first"):
            article_data["reference_unk_first"] = unk.get("first")
            article_data["reference_unk_last"] = unk.get("last")

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
        # Сначала строгий парсинг (чтобы не получить silent corruption), затем recover с явным предупреждением.
        parser = etree.XMLParser(
            recover=False,
            huge_tree=False,
            resolve_entities=False,
            no_network=True,
        )
        recovered = False
        try:
            root = etree.fromstring(xml_bytes, parser=parser)
        except etree.XMLSyntaxError as e:
            logger.warning("JATS XML синтаксически некорректен, включаем recover=True: %s", e)
            recovered = True
            parser_recover = etree.XMLParser(
                recover=True,
                huge_tree=False,
                resolve_entities=False,
                no_network=True,
            )
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
            lang = (lang or "").strip().lower()
            if lang.startswith("ru") or lang in {"rus", "russian"}:
                return "ru"
            if lang.startswith("en") or lang in {"eng", "english"}:
                return "en"
            return lang

        def extract_text(node: etree._Element) -> Optional[str]:
            para_texts = [t.strip() for t in node.xpath(".//*[local-name()='p']//text()") if t and t.strip()]
            if para_texts:
                text = " ".join(para_texts)
            else:
                parts: List[str] = []
                if node.text and node.text.strip():
                    parts.append(node.text.strip())
                for child in node:
                    local_name = etree.QName(child).localname
                    if local_name in {"title", "label"}:
                        if child.tail and child.tail.strip():
                            parts.append(child.tail.strip())
                        continue
                    parts.extend(t.strip() for t in child.itertext() if t and t.strip())
                    if child.tail and child.tail.strip():
                        parts.append(child.tail.strip())
                text = " ".join(parts)
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
            result: List[str] = []
            allowed_types = {"", "author-generated", "author-keywords", "author"}

            def add_from_group(group: etree._Element) -> None:
                for node in group.xpath("./*[local-name()='kwd']"):
                    text = extract_text(node) or " ".join(
                        t.strip() for t in node.xpath(".//text()") if t and t.strip()
                    )
                    if text and text not in result:
                        result.append(text)

            for group in kwd_groups:
                if normalize_lang(get_lang_attr(group)) != lang:
                    continue
                group_type = (group.get("kwd-group-type") or "").strip().lower()
                if group_type not in allowed_types:
                    continue
                add_from_group(group)
            if result:
                return result

            for group in kwd_groups:
                group_type = (group.get("kwd-group-type") or "").strip().lower()
                if group_type not in allowed_types:
                    continue
                keywords: List[str] = []
                for node in group.xpath("./*[local-name()='kwd']"):
                    text = extract_text(node) or " ".join(
                        t.strip() for t in node.xpath(".//text()") if t and t.strip()
                    )
                    if text:
                        keywords.append(text)
                if not keywords:
                    continue
                sample = " ".join(keywords[:3])
                if detect_lang(sample) == lang:
                    for kw in keywords:
                        if kw not in result:
                            result.append(kw)
            return result

        def extract_article_titles() -> Dict[str, Optional[str]]:
            title_ru: Optional[str] = None
            title_en: Optional[str] = None
            for group in root.xpath(
                ".//*[local-name()='article-meta']/*[local-name()='title-group']"
            ):
                for node in group.xpath("./*[local-name()='article-title']"):
                    text = extract_text(node)
                    if not text:
                        continue
                    lang = normalize_lang(get_lang_attr(node))
                    if lang == "ru":
                        title_ru = title_ru or text
                    elif lang == "en":
                        title_en = title_en or text
                    else:
                        detected = detect_lang(text)
                        if detected == "ru":
                            title_ru = title_ru or text
                        elif detected == "en":
                            title_en = title_en or text
                for node in group.xpath(
                    ".//*[local-name()='trans-title-group']/*[local-name()='trans-title']"
                ):
                    text = extract_text(node)
                    if not text:
                        continue
                    parent = node.getparent()
                    lang = normalize_lang(get_lang_attr(parent) if parent is not None else "")
                    if not lang:
                        lang = normalize_lang(get_lang_attr(node))
                    if lang == "ru":
                        title_ru = title_ru or text
                    elif lang == "en":
                        title_en = title_en or text
                    else:
                        detected = detect_lang(text)
                        if detected == "ru":
                            title_ru = title_ru or text
                        elif detected == "en":
                            title_en = title_en or text
            return {"title_ru": title_ru, "title_en": title_en}

        def extract_affiliations() -> Dict[str, object]:
            aff_nodes = root.xpath(
                ".//*[local-name()='article-meta']//*[local-name()='aff']"
            )
            affiliations: List[Dict[str, object]] = []
            aff_ids: set[str] = set()
            for node in aff_nodes:
                aff_id = (node.get("id") or "").strip()
                name = extract_text(node)
                if name:
                    name = re.sub(r"^\d+\s*[.)]?\s*", "", name).strip() or name
                lang = normalize_lang(get_lang_attr(node)) or detect_lang(name)
                if aff_id:
                    aff_ids.add(aff_id)
                affiliations.append({
                    "id": aff_id or None,
                    "name": name,
                    "lang": lang,
                    "empty": not bool(name),
                })

            contrib_refs: List[Dict[str, object]] = []
            broken: List[Dict[str, object]] = []
            for contrib in root.xpath(
                ".//*[local-name()='article-meta']//*[local-name()='contrib']"
            ):
                name_parts = [
                    (n.text or "").strip()
                    for n in contrib.xpath(
                        ".//*[local-name()='surname' or local-name()='given-names'"
                        " or local-name()='string-name']"
                    )
                    if (n.text or "").strip()
                ]
                author_name = " ".join(name_parts).strip() or None
                rids: List[str] = []
                for xref in contrib.xpath(".//*[local-name()='xref']"):
                    ref_type = (xref.get("ref-type") or "").strip().lower()
                    if ref_type and ref_type not in {"aff", "affiliation"}:
                        continue
                    rid = (xref.get("rid") or "").strip()
                    if not rid:
                        continue
                    for part in re.split(r"\s+", rid):
                        if not part:
                            continue
                        rids.append(part)
                        if aff_ids and part not in aff_ids:
                            broken.append({
                                "author": author_name,
                                "rid": part,
                                "reason": "missing_aff",
                            })
                if rids:
                    contrib_refs.append({"author": author_name, "rid": rids})

            for aff in affiliations:
                if aff.get("empty") and aff.get("id"):
                    broken.append({
                        "author": None,
                        "rid": aff.get("id"),
                        "reason": "empty_aff",
                    })

            return {
                "jats_affiliations": affiliations,
                "contributor_affiliation_refs": contrib_refs,
                "broken_affiliation_refs": broken,
            }

        def collect_references_by_lang() -> Dict[str, object]:
            """
            JATS: один <ref> = одна запись списка.

            Язык citation берётся из xml:lang / lang у mixed-citation|element-citation
            (типично citation-alternatives с xml:lang=\"ru\" и xml:lang=\"en\").
            Эвристика по алфавиту — только если атрибут языка отсутствует.

            При наличии обеих языковых версий у ref считаем обе (parallel_citations),
            а не выбираем одну «доминирующую».
            """
            ru_refs: List[str] = []
            en_refs: List[str] = []
            unk_refs: List[str] = []
            primary_items: List[str] = []
            refs_with_both = 0
            citations_with_lang_attr = 0
            ref_nodes = root.xpath(".//*[local-name()='ref-list']//*[local-name()='ref']")

            for ref_node in ref_nodes:
                mixed_nodes: List[etree._Element] = ref_node.xpath(
                    ".//*[local-name()='mixed-citation' or local-name()='element-citation']"
                )
                by_lang: Dict[str, str] = {}
                unlanged: List[str] = []
                for node in mixed_nodes:
                    raw_lang = get_lang_attr(node)
                    lang = normalize_lang(raw_lang)
                    # Текст citation без <label> соседнего уровня; label обычно вне mixed-citation
                    txt = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
                    txt = re.sub(r"\s+", " ", txt).strip()
                    if not txt:
                        continue
                    if lang in {"ru", "en"}:
                        by_lang.setdefault(lang, txt)
                        citations_with_lang_attr += 1
                    else:
                        unlanged.append(txt)

                if "ru" in by_lang and "en" in by_lang:
                    refs_with_both += 1

                # Основная запись для списка/проверок качества — одна на <ref>
                primary = by_lang.get("ru") or by_lang.get("en")
                if not primary and unlanged:
                    primary = unlanged[0]
                if not primary:
                    txt = " ".join(t.strip() for t in ref_node.xpath(".//text()") if t and t.strip())
                    primary = re.sub(r"\s+", " ", txt).strip() or None
                if primary:
                    primary_items.append(primary)

                # Язык — строго по xml:lang, если он задан
                if by_lang:
                    if "ru" in by_lang:
                        ru_refs.append(by_lang["ru"])
                    if "en" in by_lang:
                        en_refs.append(by_lang["en"])
                    # Только неизвестный lang без ru/en — не сюда (уже в unlanged)
                elif primary:
                    # Нет xml:lang — эвристика по доминирующему алфавиту
                    lat = len(re.findall(r"[A-Za-z]", primary))
                    cyr = len(re.findall(r"[А-Яа-яЁё]", primary))
                    if cyr > lat:
                        ru_refs.append(primary)
                    elif lat > cyr:
                        en_refs.append(primary)
                    else:
                        unk_refs.append(primary)

            def stats(items: List[str]) -> Dict[str, Optional[object]]:
                return {
                    "count": len(items),
                    "first": items[0] if items else None,
                    "last": items[-1] if items else None,
                }

            total_refs = len(ref_nodes)
            # parallel_citations — у большинства ref есть обе языковые версии по xml:lang
            mode = "single_list"
            if total_refs >= 1 and refs_with_both >= max(1, (total_refs + 1) // 2):
                mode = "parallel_citations"

            return {
                "mode": mode,
                "total_refs": total_refs,
                "items": primary_items,
                "lang_from_attr": citations_with_lang_attr > 0,
                "ru": stats(ru_refs),
                "en": stats(en_refs),
                "unk": stats(unk_refs),
            }

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
        identifiers: Dict[str, Optional[str]] = {
            "doi": None,
            "edn": None,
            "internal_id": None,
            "invalid_edn": None,
        }
        from ipsas.modules.issue_metadata.validators import looks_like_edn

        codes_elems = root.xpath(".//*[local-name()='codes']")
        codes = codes_elems[0] if codes_elems else None
        if codes is not None:
            doi_el = codes.xpath(".//*[local-name()='doi']")
            if doi_el and doi_el[0].text and doi_el[0].text.strip():
                identifiers["doi"] = doi_el[0].text.strip()
            edn_el = codes.xpath(".//*[local-name()='edn']")
            if edn_el and edn_el[0].text and edn_el[0].text.strip():
                edn_candidate = edn_el[0].text.strip()
                if looks_like_edn(edn_candidate):
                    identifiers["edn"] = edn_candidate
                elif edn_candidate.isdigit():
                    identifiers["internal_id"] = edn_candidate
                else:
                    identifiers["invalid_edn"] = edn_candidate
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
                if looks_like_edn(text):
                    identifiers["edn"] = text
                elif text.isdigit() and not identifiers.get("internal_id"):
                    identifiers["internal_id"] = text
                else:
                    identifiers["invalid_edn"] = identifiers.get("invalid_edn") or text
            elif pt in {"publisher-id", "other", "articlenum", "artnum"} and not identifiers.get("internal_id"):
                if text.isdigit():
                    identifiers["internal_id"] = text
                elif not looks_like_edn(text):
                    identifiers["internal_id"] = text

        # Страницы из JATS (только article-meta, не библиография)
        pages_jats: Optional[str] = None
        fpage_vals = root.xpath(
            ".//*[local-name()='article-meta']//*[local-name()='fpage']/text()"
        )
        lpage_vals = root.xpath(
            ".//*[local-name()='article-meta']//*[local-name()='lpage']/text()"
        )
        elocation_vals = root.xpath(
            ".//*[local-name()='article-meta']//*[local-name()='elocation-id']/text()"
        )
        fpage = (fpage_vals[0] or "").strip() if fpage_vals else ""
        lpage = (lpage_vals[0] or "").strip() if lpage_vals else ""
        elocation = (elocation_vals[0] or "").strip() if elocation_vals else ""
        if fpage and lpage:
            pages_jats = f"{fpage}-{lpage}"
        elif fpage:
            pages_jats = fpage
        elif elocation:
            pages_jats = elocation

        # Тип статьи: сначала атрибут <article article-type="...">
        article_type: Optional[str] = None
        root_type = (root.get("article-type") or root.get("article_type") or "").strip()
        if root_type:
            article_type = root_type
        if not article_type:
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
                    parsed = date(int(year), int(month), int(day))
                    return parsed.isoformat()
                except ValueError:
                    continue
            return None

        titles = extract_article_titles()
        aff_data = extract_affiliations()

        def extract_authors() -> Dict[str, object]:
            authors_ru: List[str] = []
            authors_en: List[str] = []
            authors: List[str] = []
            orcids: List[str] = []
            emails: List[str] = []
            has_corresp = False

            def format_name_node(node: etree._Element) -> Optional[str]:
                """Одно <name> / <string-name> → строка ФИО."""
                local = etree.QName(node).localname
                if local == "string-name":
                    txt = extract_text(node) or " ".join(
                        t.strip() for t in node.xpath(".//text()") if t and t.strip()
                    )
                    return re.sub(r"\s+", " ", txt).strip() or None
                # <name>: только прямые/вложенные surname/given этого узла, не siblings
                surname = ""
                given = ""
                for sn in node.xpath(".//*[local-name()='surname']"):
                    surname = (sn.text or "").strip()
                    if surname:
                        break
                for gn in node.xpath(".//*[local-name()='given-names']"):
                    given = (gn.text or "").strip()
                    if given:
                        break
                if surname and given:
                    return f"{surname} {given}".strip()
                if surname or given:
                    return (surname or given).strip() or None
                txt = " ".join(t.strip() for t in node.xpath(".//text()") if t and t.strip())
                return re.sub(r"\s+", " ", txt).strip() or None

            def names_by_lang(contrib: etree._Element) -> Dict[str, str]:
                """
                Из name-alternatives / нескольких name взять RU и EN версии одного автора.
                Раньше брали только первое surname в contrib → часть авторов попадала
                только в RU, часть только в EN.
                """
                by_lang: Dict[str, str] = {}
                # Явные name/string-name (обычно внутри name-alternatives)
                name_nodes = contrib.xpath(
                    ".//*[local-name()='name-alternatives']"
                    "/*[local-name()='name' or local-name()='string-name']"
                    " | ./*[local-name()='name' or local-name()='string-name']"
                )
                if not name_nodes:
                    name_nodes = contrib.xpath(
                        ".//*[local-name()='name' or local-name()='string-name']"
                    )
                for name_node in name_nodes:
                    formatted = format_name_node(name_node)
                    if not formatted:
                        continue
                    lang = normalize_lang(get_lang_attr(name_node))
                    if lang not in {"ru", "en"}:
                        lang = detect_lang(formatted) or "unk"
                    if lang in {"ru", "en"}:
                        by_lang.setdefault(lang, formatted)
                    else:
                        by_lang.setdefault("unk", formatted)
                # Fallback: нет отдельных name — старый разбор всего contrib
                if not by_lang:
                    # Нельзя брать .//surname по всему contrib — смешает alternatives
                    formatted = None
                    string_names = contrib.xpath(".//*[local-name()='string-name']")
                    if string_names:
                        formatted = format_name_node(string_names[0])
                    if formatted:
                        lang = detect_lang(formatted) or "unk"
                        if lang in {"ru", "en"}:
                            by_lang[lang] = formatted
                        else:
                            by_lang["unk"] = formatted
                return by_lang

            contribs = root.xpath(
                ".//*[local-name()='article-meta']//*[local-name()='contrib']"
            )
            author_contrib_count = 0
            for contrib in contribs:
                ctype = (contrib.get("contrib-type") or "").strip().lower()
                if ctype and ctype not in {"author", "aut"}:
                    continue
                if (contrib.get("corresp") or "").strip().lower() in {"yes", "true", "1"}:
                    has_corresp = True

                by_lang = names_by_lang(contrib)
                if not by_lang:
                    continue
                author_contrib_count += 1

                if "ru" in by_lang:
                    authors_ru.append(by_lang["ru"])
                if "en" in by_lang:
                    authors_en.append(by_lang["en"])
                primary = by_lang.get("ru") or by_lang.get("en") or next(iter(by_lang.values()))
                authors.append(primary)

                for cid in contrib.xpath(".//*[local-name()='contrib-id']"):
                    cid_type = (cid.get("contrib-id-type") or "").strip().lower()
                    val = (cid.text or "").strip()
                    if not val:
                        continue
                    if "orcid" in cid_type or "orcid.org" in val.lower():
                        m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])", val)
                        orcids.append(m.group(1) if m else val)
                for em in contrib.xpath(".//*[local-name()='email']"):
                    val = (em.text or "").strip()
                    if val and val not in emails:
                        emails.append(val)

            # Уникальные с сохранением порядка
            def uniq(vals: List[str]) -> List[str]:
                seen: set[str] = set()
                out: List[str] = []
                for v in vals:
                    if v and v not in seen:
                        seen.add(v)
                        out.append(v)
                return out

            authors_ru = uniq(authors_ru)
            authors_en = uniq(authors_en)
            authors = uniq(authors) or authors_ru or authors_en
            # Количество авторов = число contrib author, не длина одного языкового списка
            count = author_contrib_count or len(authors) or len(authors_ru) or len(authors_en)
            return {
                "authors_ru": authors_ru,
                "authors_en": authors_en,
                "authors": authors,
                "orcids": uniq(orcids),
                "emails": uniq(emails),
                "has_corresponding_author": has_corresp or None,
                "authors_count": count,
            }

        authors_data = extract_authors()
        data = {
            "abstract_ru": collect_abstract("ru"),
            "abstract_en": collect_abstract("en"),
            "keywords_ru": collect_keywords("ru"),
            "keywords_en": collect_keywords("en"),
            "title_ru": titles.get("title_ru"),
            "title_en": titles.get("title_en"),
            "identifiers": identifiers,
            "pages_jats": pages_jats,
            "article_type": article_type,
            "journal_meta": extract_journal_meta(),
            "references_by_lang": collect_references_by_lang(),
            "publication_date": extract_publication_date(),
            "jats_affiliations": aff_data.get("jats_affiliations") or [],
            "contributor_affiliation_refs": aff_data.get("contributor_affiliation_refs") or [],
            "broken_affiliation_refs": aff_data.get("broken_affiliation_refs") or [],
            **authors_data,
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
