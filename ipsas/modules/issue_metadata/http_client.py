"""HTTP-клиент для парсеров выпуска.

Цель: изолировать сетевой слой (таймауты/ретраи/заголовки/лимит размера) от парсинга.
Использует stdlib `urllib`, чтобы не добавлять зависимости.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
import socket
import time
import urllib.error
import urllib.request
from typing import Any, Optional, Tuple

from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


DEFAULT_HEADERS: dict[str, str] = {
    # Более “браузерные” заголовки помогают на некоторых хостингах/CDN.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) IPSAS-Issue-Metadata-Parser/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
    "Connection": "close",
}


@dataclass(slots=True)
class HttpClient:
    """Мини-клиент с ретраями и лимитом размера."""

    max_bytes: int = 0
    timeout_s: int = 30
    retries: int = 3
    backoff_s: float = 0.8
    issue_timeout_s: int = 60
    issue_retries: int = 4
    headers: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_HEADERS))

    @classmethod
    def from_env(cls, *, max_bytes: int = 0) -> "HttpClient":
        timeout_s = int(os.getenv("ISSUE_PARSER_HTTP_TIMEOUT", "30"))
        retries = int(os.getenv("ISSUE_PARSER_HTTP_RETRIES", "3"))
        backoff_s = float(os.getenv("ISSUE_PARSER_HTTP_BACKOFF", "0.8"))
        issue_timeout_s = int(os.getenv("ISSUE_PARSER_ISSUE_TIMEOUT", str(max(timeout_s, 60))))
        issue_retries = int(os.getenv("ISSUE_PARSER_ISSUE_RETRIES", str(max(retries, 4))))
        return cls(
            max_bytes=int(max_bytes or 0),
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            issue_timeout_s=issue_timeout_s,
            issue_retries=issue_retries,
        )

    def make_request(self, url: str) -> urllib.request.Request:
        return urllib.request.Request(url, headers=self.headers)

    @staticmethod
    def read_response_limited(
        response: Any,
        *,
        limit_bytes: int = 0,
        read_timeout_s: Optional[float] = None,
    ) -> bytes:
        """Прочитать тело ответа с лимитом размера и таймаутом на чтение.

        В `urllib` таймаут, переданный в `urlopen(..., timeout=...)`, не всегда гарантирует
        таймаут на чтение каждого следующего чанка тела. Поэтому дополнительно пытаемся
        выставить таймаут на underlying socket.
        """
        sock: Any = None
        old_timeout: Any = None
        if read_timeout_s is not None:
            try:
                # http.client.HTTPResponse -> .fp (BufferedReader) -> .raw (SocketIO) -> ._sock (socket)
                sock = getattr(response, "fp", None)
                sock = getattr(sock, "raw", sock)
                sock = getattr(sock, "_sock", sock)
                if hasattr(sock, "settimeout"):
                    old_timeout = sock.gettimeout() if hasattr(sock, "gettimeout") else None
                    sock.settimeout(float(read_timeout_s))
            except Exception:
                sock = None
                old_timeout = None
        chunks: list[bytes] = []
        total = 0
        try:
            while True:
                chunk = response.read(1024 * 64)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if limit_bytes and total > limit_bytes:
                    raise ValueError("Превышен допустимый размер загружаемых данных")
            return b"".join(chunks)
        finally:
            if read_timeout_s is not None and sock is not None and old_timeout is not None and hasattr(sock, "settimeout"):
                try:
                    sock.settimeout(old_timeout)
                except Exception:
                    pass

    def fetch_bytes(
        self,
        url: str,
        *,
        is_issue_page: bool = False,
        timeout_s: Optional[int] = None,
        retries: Optional[int] = None,
    ) -> bytes:
        """Скачать URL в память (bytes) с ретраями и лимитом размера."""
        from ipsas.common.ssrf import UnsafeUrlError, assert_safe_fetch_url

        try:
            assert_safe_fetch_url(url)
        except UnsafeUrlError as e:
            raise ValueError(str(e)) from e

        req = self.make_request(url)
        effective_timeout = int(
            timeout_s
            if timeout_s is not None
            else (self.issue_timeout_s if is_issue_page else self.timeout_s)
        )
        attempts = max(
            1,
            int(retries if retries is not None else (self.issue_retries if is_issue_page else self.retries)),
        )

        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            t0 = time.monotonic()
            logger.info(
                "HTTP fetch attempt %d/%d (timeout=%ss): %s",
                attempt,
                attempts,
                effective_timeout,
                url,
            )
            try:
                with urllib.request.urlopen(req, timeout=effective_timeout) as response:
                    logger.info(
                        "HTTP fetch ok in %.2fs: %s",
                        time.monotonic() - t0,
                        url,
                    )
                    return HttpClient.read_response_limited(
                        response,
                        limit_bytes=int(self.max_bytes or 0),
                        read_timeout_s=float(effective_timeout),
                    )
            except (TimeoutError, socket.timeout) as e:
                last_exc = e
                logger.warning(
                    "HTTP fetch timeout in %.2fs: %s",
                    time.monotonic() - t0,
                    url,
                )
            except urllib.error.HTTPError as e:
                raise ValueError(f"HTTP ошибка при загрузке: {e.code}") from e
            except urllib.error.URLError as e:
                last_exc = e
                logger.warning(
                    "HTTP fetch URLError in %.2fs: %s (%s)",
                    time.monotonic() - t0,
                    url,
                    getattr(e, "reason", e),
                )

            if attempt < attempts:
                time.sleep(self.backoff_s * attempt)

        if last_exc is not None:
            if isinstance(last_exc, (TimeoutError, socket.timeout)):
                raise ValueError("Таймаут при загрузке страницы. Попробуйте повторить позже.") from last_exc
            if isinstance(last_exc, urllib.error.URLError):
                raise ValueError(f"Ошибка при загрузке: {getattr(last_exc, 'reason', last_exc)}") from last_exc
            raise ValueError(f"Ошибка при загрузке: {last_exc}") from last_exc
        raise ValueError("Ошибка при загрузке: неизвестная причина")

    def fetch_bytes_with_content_type(
        self,
        url: str,
        *,
        is_issue_page: bool = False,
        timeout_s: Optional[int] = None,
        retries: Optional[int] = None,
    ) -> Tuple[bytes, Optional[str]]:
        """Скачать URL и вернуть (bytes, Content-Type)."""
        from ipsas.common.ssrf import UnsafeUrlError, assert_safe_fetch_url

        try:
            assert_safe_fetch_url(url)
        except UnsafeUrlError as e:
            raise ValueError(str(e)) from e

        req = self.make_request(url)
        effective_timeout = int(
            timeout_s
            if timeout_s is not None
            else (self.issue_timeout_s if is_issue_page else self.timeout_s)
        )
        attempts = max(
            1,
            int(retries if retries is not None else (self.issue_retries if is_issue_page else self.retries)),
        )

        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            t0 = time.monotonic()
            logger.info(
                "HTTP fetch attempt %d/%d (timeout=%ss): %s",
                attempt,
                attempts,
                effective_timeout,
                url,
            )
            try:
                with urllib.request.urlopen(req, timeout=effective_timeout) as response:
                    content_type = response.headers.get("Content-Type")
                    body = HttpClient.read_response_limited(
                        response,
                        limit_bytes=int(self.max_bytes or 0),
                        read_timeout_s=float(effective_timeout),
                    )
                    logger.info(
                        "HTTP fetch ok in %.2fs: %s (Content-Type=%s, bytes=%d)",
                        time.monotonic() - t0,
                        url,
                        content_type,
                        len(body),
                    )
                    return body, content_type
            except (TimeoutError, socket.timeout) as e:
                last_exc = e
                logger.warning(
                    "HTTP fetch timeout in %.2fs: %s",
                    time.monotonic() - t0,
                    url,
                )
            except urllib.error.HTTPError as e:
                raise ValueError(f"HTTP ошибка при загрузке: {e.code}") from e
            except urllib.error.URLError as e:
                last_exc = e
                logger.warning(
                    "HTTP fetch URLError in %.2fs: %s (%s)",
                    time.monotonic() - t0,
                    url,
                    getattr(e, "reason", e),
                )

            if attempt < attempts:
                time.sleep(self.backoff_s * attempt)

        if last_exc is not None:
            if isinstance(last_exc, (TimeoutError, socket.timeout)):
                raise ValueError("Таймаут при загрузке страницы. Попробуйте повторить позже.") from last_exc
            if isinstance(last_exc, urllib.error.URLError):
                raise ValueError(f"Ошибка при загрузке: {getattr(last_exc, 'reason', last_exc)}") from last_exc
            raise ValueError(f"Ошибка при загрузке: {last_exc}") from last_exc
        raise ValueError("Ошибка при загрузке: неизвестная причина")

