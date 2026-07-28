"""Защита от SSRF при исходящих HTTP-запросах (проверка выпусков OJS)."""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urlparse


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
    )


def assert_safe_fetch_url(
    url: str,
    *,
    allow_private: bool = False,
    resolve_dns: bool = True,
) -> str:
    """
    Проверить URL перед исходящим запросом.

    Разрешены только http/https. Блокируются localhost и private IP.
    Если задан ``ISSUE_FETCH_ALLOWED_HOSTS`` — hostname должен быть в списке.
    """
    if not url or not str(url).strip():
        raise UnsafeUrlError("Пустой URL")
    parsed = urlparse(str(url).strip())
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        raise UnsafeUrlError(f"Разрешены только http/https, получено: {scheme or 'нет'}")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise UnsafeUrlError("URL без hostname")
    if host in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        raise UnsafeUrlError("Обращение к localhost запрещено")

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
    """Быстрая проверка для форм (без DNS по умолчанию)."""
    try:
        assert_safe_fetch_url(url, resolve_dns=resolve_dns)
        return True
    except UnsafeUrlError:
        return False
