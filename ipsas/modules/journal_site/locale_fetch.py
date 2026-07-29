"""Переключение локали OJS при загрузке страниц журнала."""

from __future__ import annotations

import http.cookiejar
import os
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


def _fetch_via_setlocale(
    client: HttpClient,
    url: str,
    code: str,
    *,
    lang: str,
    timeout_s: int,
) -> bytes:
    setlocale_url = build_setlocale_url(url, code)
    if not setlocale_url:
        raise ValueError("Не удалось построить setLocale URL")
    assert_safe_fetch_url(setlocale_url)

    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookie_jar),
        urllib.request.HTTPRedirectHandler(),
    )
    headers = dict(DEFAULT_HEADERS)
    headers["Accept-Language"] = _accept_language(lang)
    opener.addheaders = list(headers.items())

    logger.info("setLocale %s → %s", code, setlocale_url)
    try:
        opener.open(setlocale_url, timeout=timeout_s)
    except Exception as exc:
        logger.info("setLocale non-critical error: %s", exc)

    req = urllib.request.Request(url, headers=headers)
    with opener.open(req, timeout=timeout_s) as response:
        return HttpClient.read_response_limited(
            response,
            limit_bytes=int(client.max_bytes or 0),
            read_timeout_s=float(timeout_s),
        )


def fetch_bytes_for_locale(
    client: HttpClient,
    url: str,
    *,
    lang: str,
) -> tuple[bytes, str]:
    """Загрузить URL в языковой версии ``lang`` (``ru`` / ``en``).

    Returns:
        ``(body, used_locale_code)``
    """
    try:
        assert_safe_fetch_url(url)
    except UnsafeUrlError as e:
        raise ValueError(str(e)) from e

    variants = LOCALE_VARIANTS.get(lang, (lang,))
    try:
        timeout_s = int(os.getenv("ISSUE_PARSER_LOCALE_TIMEOUT", "20"))
    except ValueError:
        timeout_s = 20

    fallback: tuple[bytes, str] | None = None

    for code in variants:
        locale_url = with_locale_query(url, code)
        try:
            data = client.fetch_bytes(locale_url, is_issue_page=True, timeout_s=timeout_s)
            fallback = (data, code)
            if _locale_matches(data, lang):
                return data, code
            logger.info("locale query mismatch for %s (%s)", locale_url, code)
        except Exception as e:
            logger.info("locale query failed %s: %s", locale_url, e)

    for code in variants:
        try:
            data = _fetch_via_setlocale(
                client, url, code, lang=lang, timeout_s=timeout_s
            )
            fallback = (data, code)
            if _locale_matches(data, lang):
                return data, code
            logger.info("setLocale language mismatch for %s", code)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            logger.warning("setLocale fetch failed (%s): %s", code, e)

    if fallback is not None:
        return fallback

    data = client.fetch_bytes(url, is_issue_page=True, timeout_s=timeout_s)
    return data, variants[0]
