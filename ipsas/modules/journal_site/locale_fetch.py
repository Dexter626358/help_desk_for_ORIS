"""Переключение локали OJS при загрузке страниц журнала."""

from __future__ import annotations

import http.cookiejar
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from ipsas.common.ssrf import UnsafeUrlError, assert_safe_fetch_url
from ipsas.modules.issue_metadata.http_client import DEFAULT_HEADERS, HttpClient
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

LOCALE_VARIANTS: dict[str, tuple[str, ...]] = {
    "ru": ("ru_RU", "ru"),
    "en": ("en_US", "en"),
}

LOCALE_LABELS: dict[str, str] = {
    "ru": "Русский",
    "en": "English",
}


def with_locale_query(url: str, locale: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    query["locale"] = [locale]
    return urllib.parse.urlunparse(
        parsed._replace(query=urllib.parse.urlencode(query, doseq=True))
    )


def build_setlocale_url(page_url: str, locale: str) -> Optional[str]:
    parsed = urllib.parse.urlparse(page_url)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    slug = parts[0]
    source = urllib.parse.quote(parsed.path or "/", safe="")
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/{slug}/user/setLocale/{locale}?source={source}"


def _html_lang(data: bytes) -> str:
    head = data[:8000].decode("utf-8", errors="ignore").lower()
    for key in ('lang="', "lang='"):
        idx = head.find(key)
        if idx >= 0:
            start = idx + len(key)
            quote = key[-1]
            end = start
            while end < len(head) and head[end] != quote:
                end += 1
            return head[start:end].strip()
    return ""


def _locale_matches(data: bytes, expected: str) -> bool:
    lang = _html_lang(data)
    if not lang:
        return False
    if expected == "ru":
        return lang.startswith("ru")
    if expected == "en":
        return lang.startswith("en")
    return True


def _accept_language(lang: str) -> str:
    if lang == "en":
        return "en-US,en;q=0.9,ru;q=0.4"
    return "ru-RU,ru;q=0.9,en;q=0.4"


def _site_timeout_s() -> int:
    try:
        return int(os.getenv("JOURNAL_SITE_HTTP_TIMEOUT", os.getenv("ISSUE_PARSER_LOCALE_TIMEOUT", "12")))
    except ValueError:
        return 12


class LocaleSession:
    """Одна локаль на серию запросов: prime один раз, дальше ?locale= (потокобезопасно)."""

    def __init__(self, client: HttpClient, lang: str) -> None:
        self._client = client
        self.lang = lang
        self.locale_code = LOCALE_VARIANTS.get(lang, (lang,))[0]
        self._lock = threading.Lock()
        self._primed = False
        self._timeout_s = _site_timeout_s()
        self._headers = {
            **DEFAULT_HEADERS,
            "Accept-Language": _accept_language(lang),
            "Connection": "keep-alive",
        }

    def fetch(self, url: str) -> tuple[bytes, str]:
        assert_safe_fetch_url(url)
        with self._lock:
            if not self._primed:
                self._prime(url)
        target = with_locale_query(url, self.locale_code)
        data = self._client.fetch_bytes(
            target,
            is_issue_page=False,
            timeout_s=self._timeout_s,
            retries=1,
        )
        return data, self.locale_code

    def _prime(self, sample_url: str) -> None:
        variants = LOCALE_VARIANTS.get(self.lang, (self.lang,))
        for code in variants:
            locale_url = with_locale_query(sample_url, code)
            try:
                data = self._client.fetch_bytes(
                    locale_url,
                    is_issue_page=False,
                    timeout_s=self._timeout_s,
                    retries=1,
                )
                self.locale_code = code
                if _locale_matches(data, self.lang):
                    self._primed = True
                    logger.info("locale primed via query %s", code)
                    return
                self._primed = True
                logger.info("locale primed via query (unconfirmed lang) %s", code)
                return
            except Exception as exc:
                logger.info("locale query prime failed %s: %s", code, exc)

        for code in variants:
            setlocale_url = build_setlocale_url(sample_url, code)
            if not setlocale_url:
                continue
            try:
                assert_safe_fetch_url(setlocale_url)
                jar = http.cookiejar.CookieJar()
                from ipsas.common.ssrf import SafeRedirectHandler

                opener = urllib.request.build_opener(
                    urllib.request.HTTPCookieProcessor(jar),
                    SafeRedirectHandler(),
                )
                opener.addheaders = list(self._headers.items())
                try:
                    opener.open(setlocale_url, timeout=self._timeout_s)
                except Exception as exc:
                    logger.info("setLocale non-critical: %s", exc)
                req = urllib.request.Request(sample_url, headers=self._headers)
                with opener.open(req, timeout=self._timeout_s) as response:
                    HttpClient.read_response_limited(
                        response,
                        limit_bytes=int(self._client.max_bytes or 0),
                        read_timeout_s=float(self._timeout_s),
                    )
                self.locale_code = code
                self._primed = True
                logger.info("locale primed via setLocale %s", code)
                return
            except Exception as exc:
                logger.warning("setLocale prime failed (%s): %s", code, exc)

        self._primed = True
        logger.info("locale prime fallback (%s)", self.lang)


def fetch_bytes_for_locale(
    client: HttpClient,
    url: str,
    *,
    lang: str,
    session: LocaleSession | None = None,
) -> tuple[bytes, str]:
    """Загрузить URL в языковой версии ``lang`` (``ru`` / ``en``).

    Returns:
        ``(body, used_locale_code)``
    """
    try:
        assert_safe_fetch_url(url)
    except UnsafeUrlError as e:
        raise ValueError(str(e)) from e

    if session is not None:
        return session.fetch(url)

    variants = LOCALE_VARIANTS.get(lang, (lang,))
    timeout_s = _site_timeout_s()
    code = variants[0]
    locale_url = with_locale_query(url, code)
    try:
        data = client.fetch_bytes(
            locale_url,
            is_issue_page=False,
            timeout_s=timeout_s,
            retries=1,
        )
        return data, code
    except Exception as e:
        logger.info("locale query failed %s: %s", locale_url, e)

    data = client.fetch_bytes(url, is_issue_page=False, timeout_s=timeout_s, retries=1)
    return data, code
