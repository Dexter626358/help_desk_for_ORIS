"""HTTP-клиент платформы RCSI для apply ENG-метаданных."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthClient,
    PlatformAuthError,
    ensure_platform_auth,
)

logger = logging.getLogger(__name__)


class PlatformClientError(RuntimeError):
    pass


@dataclass
class PlatformClientSettings:
    base_url: str
    username: str
    password: str
    cookie_file: Path
    request_timeout: float = 60.0
    request_delay: float = 0.35


class PlatformClient:
    """Обёртка над PlatformAuthClient: GET/POST форм OJS."""

    def __init__(
        self,
        settings: PlatformClientSettings,
        auth: PlatformAuthClient | None = None,
    ) -> None:
        self.settings = settings
        self.auth = auth or ensure_platform_auth(
            username=settings.username,
            password=settings.password,
            base_url=settings.base_url,
            cookie_file=settings.cookie_file,
            timeout=settings.request_timeout,
        )
        self.auth.timeout = float(settings.request_timeout)

    def _sleep(self) -> None:
        if self.settings.request_delay > 0:
            time.sleep(self.settings.request_delay)

    def get_bytes(self, url: str) -> tuple[int, str, bytes]:
        self._sleep()
        try:
            return self.auth.request(url, method="GET")
        except PlatformAuthError as exc:
            raise PlatformClientError(str(exc)) from exc

    def get_text(self, url: str, encoding: str = "utf-8") -> str:
        status, _, body = self.get_bytes(url)
        if status >= 400:
            raise PlatformClientError(f"HTTP {status} for {url}")
        text = body.decode(encoding, "replace")
        if "signinForm" in text or "loginUsername" in text:
            raise PlatformClientError(f"Требуется авторизация: {url}")
        return text

    def post_form(
        self,
        url: str,
        data: dict[str, str],
        *,
        referer: str | None = None,
    ) -> tuple[int, str, bytes]:
        self._sleep()
        headers: dict[str, str] = {}
        if referer:
            headers["Referer"] = referer
            headers["Origin"] = self.settings.base_url.rstrip("/")
        try:
            return self.auth.request(url, data=data, headers=headers or None)
        except PlatformAuthError as exc:
            raise PlatformClientError(str(exc)) from exc
