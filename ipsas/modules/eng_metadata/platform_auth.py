"""Авторизация на journals.rcsi.science (OJS) для apply ENG-метаданных."""

from __future__ import annotations

import http.cookiejar
import logging
import re
import uuid
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from ipsas.common.ssrf import UnsafeUrlError, assert_safe_fetch_url, build_safe_opener

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://journals.rcsi.science"
LOGIN_PATH = "/index/login"
SIGN_IN_PATH = "/index/login/signIn"
DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; ipsas-eng-metadata/1.0)"

_SAFE_UPLOAD_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _ascii_upload_filename(filename: str) -> str:
    """Имя файла для Content-Disposition: только ASCII без пробелов."""
    name = Path(filename).name.strip() or "file.bin"
    stem = Path(name).stem
    suffix = Path(name).suffix
    safe_stem = _SAFE_UPLOAD_NAME_RE.sub("_", stem).strip("._") or "file"
    safe_suffix = _SAFE_UPLOAD_NAME_RE.sub("", suffix)
    return f"{safe_stem}{safe_suffix}"


class PlatformAuthError(RuntimeError):
    """Ошибка входа или сессии на платформе."""


@dataclass
class PlatformAuthClient:
    """HTTP-клиент с cookie-сессией после логина."""

    base_url: str = DEFAULT_BASE_URL
    user_agent: str = DEFAULT_USER_AGENT
    timeout: float = 60.0
    _cookie_jar: http.cookiejar.CookieJar = field(
        default_factory=http.cookiejar.CookieJar, repr=False
    )
    _opener: urllib.request.OpenerDirector | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self._rebuild_opener()

    def _rebuild_opener(self) -> None:
        self._opener = build_safe_opener(
            urllib.request.HTTPCookieProcessor(self._cookie_jar),
        )

    def _headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        }
        if extra:
            headers.update(extra)
        return headers

    def request(
        self,
        url: str,
        *,
        data: Mapping[str, str] | None = None,
        method: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> tuple[int, str, bytes]:
        assert self._opener is not None
        assert_safe_fetch_url(url, resolve_dns=True)
        body: bytes | None = None
        req_headers = self._headers(headers)
        if data is not None:
            body = urllib.parse.urlencode(dict(data)).encode("utf-8")
            req_headers.setdefault(
                "Content-Type", "application/x-www-form-urlencoded"
            )
            method = method or "POST"
        req = urllib.request.Request(
            url,
            data=body,
            headers=req_headers,
            method=method,
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                content = resp.read()
                final_url = resp.geturl()
                status = getattr(resp, "status", 200) or 200
                return status, final_url, content
        except UnsafeUrlError:
            raise
        except urllib.error.HTTPError as exc:
            content = exc.read() if exc.fp else b""
            return exc.code, url, content

    def request_multipart(
        self,
        url: str,
        *,
        fields: Mapping[str, str],
        files: Mapping[str, tuple[str, bytes, str]],
        headers: Mapping[str, str] | None = None,
    ) -> tuple[int, str, bytes]:
        """POST multipart/form-data (загрузка файлов на платформу).

        files: name -> (filename, content, content_type)
        """
        assert self._opener is not None
        assert_safe_fetch_url(url, resolve_dns=True)
        boundary = f"----ipsasBoundary{uuid.uuid4().hex}"
        body = bytearray()
        for name, value in fields.items():
            body.extend(f"--{boundary}\r\n".encode("ascii"))
            body.extend(
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("ascii")
            )
            body.extend(str(value).encode("utf-8"))
            body.extend(b"\r\n")
        for name, (filename, content, content_type) in files.items():
            safe_name = _ascii_upload_filename(filename)
            body.extend(f"--{boundary}\r\n".encode("ascii"))
            body.extend(
                (
                    f'Content-Disposition: form-data; name="{name}"; '
                    f'filename="{safe_name}"\r\n'
                    f"Content-Type: {content_type or 'application/octet-stream'}\r\n\r\n"
                ).encode("ascii")
            )
            body.extend(content)
            body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("ascii"))

        req_headers = self._headers(headers)
        req_headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        req = urllib.request.Request(
            url,
            data=bytes(body),
            headers=req_headers,
            method="POST",
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                content = resp.read()
                final_url = resp.geturl()
                status = getattr(resp, "status", 200) or 200
                return status, final_url, content
        except UnsafeUrlError:
            raise
        except urllib.error.HTTPError as exc:
            content = exc.read() if exc.fp else b""
            return exc.code, url, content

    def get_text(self, url: str, encoding: str = "utf-8") -> str:
        _, _, content = self.request(url, method="GET")
        return content.decode(encoding, "replace")

    def cookie_header(self) -> str:
        parts = [f"{c.name}={c.value}" for c in self._cookie_jar]
        return "; ".join(parts)

    def save_cookies(self, path: Path | str) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.cookie_header() + "\n", encoding="utf-8")

    def load_cookies(self, path: Path | str) -> None:
        path = Path(path)
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            raise PlatformAuthError(f"Пустой файл cookies: {path}")
        host = urllib.parse.urlparse(self.base_url).hostname or "journals.rcsi.science"
        jar = http.cookiejar.CookieJar()
        for part in text.replace("\n", ";").split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            name, value = part.split("=", 1)
            cookie = http.cookiejar.Cookie(
                version=0,
                name=name.strip(),
                value=value.strip(),
                port=None,
                port_specified=False,
                domain=host,
                domain_specified=True,
                domain_initial_dot=False,
                path="/",
                path_specified=True,
                secure=True,
                expires=None,
                discard=True,
                comment=None,
                comment_url=None,
                rest={},
                rfc2109=False,
            )
            jar.set_cookie(cookie)
        self._cookie_jar = jar
        self._rebuild_opener()

    def is_logged_in(self) -> bool:
        html = self.get_text(f"{self.base_url}/index/user")
        low = html.casefold()
        if "login/signout" in low or "logout" in low or "/user/profile" in low:
            return True
        if "/index/login" in low and "signinForm" in html:
            return False
        if "My Account" in html or "Мой профиль" in html or "userHome" in low:
            return True
        return False

    def login(self, username: str, password: str, *, remember: bool = True) -> None:
        if not username or not password:
            raise PlatformAuthError("Нужны username и password")
        login_url = f"{self.base_url}{LOGIN_PATH}"
        self.get_text(login_url)
        payload = {
            "username": username,
            "password": password,
            "source": "",
        }
        if remember:
            payload["remember"] = "1"
        status, final_url, body = self.request(
            f"{self.base_url}{SIGN_IN_PATH}",
            data=payload,
            headers={
                "Referer": login_url,
                "Origin": self.base_url,
            },
        )
        text = body.decode("utf-8", "replace")
        if status >= 400:
            raise PlatformAuthError(f"Ошибка входа HTTP {status}")
        if self._looks_like_failed_login(text, final_url):
            reason = self._extract_login_error(text) or "неверный логин или пароль"
            raise PlatformAuthError(reason)
        if not self.is_logged_in():
            if "login" in final_url.casefold() and "signinForm" in text:
                raise PlatformAuthError("Сессия после входа не установлена")
        logger.info("Вход на платформу выполнен (%s)", username)

    @staticmethod
    def _looks_like_failed_login(html: str, final_url: str) -> bool:
        low = html.casefold()
        markers = (
            "invalid username",
            "invalid password",
            "неправильный пароль",
            "неверн",
            "ошибка входа",
            "authentication failed",
            "incorrect username",
            "incorrect password",
        )
        if any(m in low for m in markers):
            return True
        if "signinForm" in html and "loginUsername" in html:
            if "/login" in final_url.casefold():
                return True
        return False

    @staticmethod
    def _extract_login_error(html: str) -> str | None:
        marker = 'class="pkp_form_error"'
        start = html.find(marker)
        if start < 0:
            return None
        gt = html.find(">", start)
        lt = html.find("<", gt + 1) if gt >= 0 else -1
        if gt < 0 or lt < 0:
            return None
        msg = html[gt + 1 : lt].strip()
        return msg or None


def ensure_platform_auth(
    *,
    username: str,
    password: str,
    base_url: str,
    cookie_file: Path,
    timeout: float,
) -> PlatformAuthClient:
    """Переиспользовать cookies или выполнить логин."""
    auth = PlatformAuthClient(base_url=base_url, timeout=timeout)
    if cookie_file.is_file():
        try:
            auth.load_cookies(cookie_file)
            if auth.is_logged_in():
                return auth
        except (OSError, PlatformAuthError) as exc:
            logger.info("Cookies недействительны, повторный вход: %s", exc)
    auth.login(username, password)
    try:
        auth.save_cookies(cookie_file)
    except OSError as exc:
        logger.warning("Не удалось сохранить cookies: %s", exc)
    return auth
