"""Защита от SSRF при исходящих HTTP-запросах (проверка выпусков OJS)."""

from __future__ import annotations

import ipaddress
import os
import socket
import urllib.request
from urllib.parse import urljoin, urlparse


class UnsafeUrlError(ValueError):
    """URL отклонён политикой SSRF."""


def _allowed_hosts() -> set[str]:
    raw = os.getenv("ISSUE_FETCH_ALLOWED_HOSTS", "").strip()
    if not raw:
        return set()
    return {h.strip().lower() for h in raw.split(",") if h.strip()}


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or (ip.version == 4 and ip in ipaddress.ip_network("169.254.0.0/16"))
    )


def assert_safe_fetch_url(
    url: str,
    *,
    allow_private: bool = False,
    resolve_dns: bool = True,
) -> str:
    if not url or not str(url).strip():
        raise UnsafeUrlError("Пустой URL")
    parsed = urlparse(str(url).strip())
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        raise UnsafeUrlError(f"Разрешены только http/https, получено: {scheme or 'нет'}")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise UnsafeUrlError("URL без hostname")
    if host in {"localhost", "127.0.0.1", "::1", "0.0.0.0", "metadata.google.internal"}:
        raise UnsafeUrlError("Обращение к localhost/metadata запрещено")

    allowlist = _allowed_hosts()
    if allowlist and host not in allowlist:
        raise UnsafeUrlError(f"Хост не в белом списке: {host}")

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        if not allow_private and _is_blocked_ip(ip):
            raise UnsafeUrlError(f"Запрещённый IP-адрес: {host}")
        return url

    if allow_private or not resolve_dns:
        return url

    try:
        infos = socket.getaddrinfo(
            host,
            parsed.port or (443 if scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as e:
        raise UnsafeUrlError(f"Не удалось разрешить DNS для {host}") from e
    if not infos:
        raise UnsafeUrlError(f"Пустой DNS-ответ для {host}")
    for info in infos:
        addr = info[4][0]
        try:
            resolved = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if _is_blocked_ip(resolved):
            raise UnsafeUrlError(f"DNS {host} указывает на запрещённый адрес {addr}")
    return url


def is_safe_fetch_url(url: str, *, resolve_dns: bool = False) -> bool:
    try:
        assert_safe_fetch_url(url, resolve_dns=resolve_dns)
        return True
    except UnsafeUrlError:
        return False


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_hops: int = 5

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        if newurl is None:
            return None
        absolute = urljoin(req.full_url, newurl)
        assert_safe_fetch_url(absolute, resolve_dns=True)
        hops = int(getattr(req, "ssrf_redirect_hops", 0)) + 1
        if hops > self.max_hops:
            raise UnsafeUrlError(f"Слишком много HTTP-редиректов (>{self.max_hops})")
        new_req = super().redirect_request(req, fp, code, msg, headers, absolute)
        if new_req is not None:
            new_req.ssrf_redirect_hops = hops  # type: ignore[attr-defined]
        return new_req


def build_safe_opener(
    *handlers: urllib.request.BaseHandler,
) -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(SafeRedirectHandler(), *handlers)


def urlopen_safe(req: urllib.request.Request | str, *, timeout: float | None = None):
    opener = build_safe_opener()
    if timeout is None:
        return opener.open(req)
    return opener.open(req, timeout=timeout)
