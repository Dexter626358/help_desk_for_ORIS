"""Оркестрация: URL журнала + CSV → создание недостающих разделов.

Работает и с песочницей (f23g45), и с платформой (journals.rcsi.science).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from ipsas.config.settings import Settings, get_settings
from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthError,
    ensure_platform_auth,
)
from ipsas.modules.journal_sections.service import sync_journal_sections
from ipsas.modules.sandbox_journal_setup.auth import ensure_sandbox_auth
from ipsas.modules.sandbox_journal_setup.urls import parse_journal_url
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

AuthKind = Literal["sandbox", "platform"]


def _host(url: str) -> str:
    return (urlparse(url).netloc or "").casefold()


def is_sandbox_host(base_url: str, sandbox_base_url: str) -> bool:
    host = _host(base_url)
    sandbox_host = _host(sandbox_base_url)
    if not host:
        return False
    if sandbox_host and host == sandbox_host:
        return True
    return "f23g45" in host


def resolve_auth_kind(base_url: str, settings: Settings) -> AuthKind:
    """Песочница или платформа; иначе — понятная ошибка."""
    if is_sandbox_host(base_url, settings.sandbox_base_url):
        return "sandbox"
    host = _host(base_url)
    platform_host = _host(settings.platform_base_url) or "journals.rcsi.science"
    if host == platform_host or host == "journals.rcsi.science":
        return "platform"
    raise ValueError(
        f"Хост «{host or '—'}» не поддерживается. "
        f"Укажите ссылку на песочницу ({settings.sandbox_base_url}) "
        f"или платформу ({settings.platform_base_url}), "
        f"например …/2079-7877/manager/sections."
    )


def execute(
    *,
    journal_url: str,
    csv_path: Path | None = None,
    dry_run: bool = True,
    sort_alpha: bool = False,
) -> dict[str, Any]:
    """Синхронизировать разделы из CSV и/или отсортировать по алфавиту."""
    if csv_path is None and not sort_alpha:
        raise ValueError("Укажите CSV и/или включите сортировку разделов")

    settings = get_settings()
    base_url, _journal = parse_journal_url(journal_url)
    auth_kind = resolve_auth_kind(base_url, settings)

    try:
        if auth_kind == "sandbox":
            if not (settings.sandbox_gate_username and settings.sandbox_gate_password):
                raise ValueError(
                    "Для песочницы задайте SANDBOX_GATE_* / SANDBOX_USER1_* в .env"
                )
            if not (settings.sandbox_ojs_username and settings.sandbox_ojs_password):
                raise ValueError(
                    "Для песочницы задайте SANDBOX_OJS_* / SANDBOX_USER2_* в .env"
                )
            auth = ensure_sandbox_auth(
                base_url=base_url,
                gate_username=settings.sandbox_gate_username,
                gate_password=settings.sandbox_gate_password,
                ojs_username=settings.sandbox_ojs_username,
                ojs_password=settings.sandbox_ojs_password,
                cookie_file=settings.sandbox_cookie_file,
                timeout=float(settings.request_timeout),
            )
            delay = float(settings.sandbox_request_delay)
        else:
            if not settings.platform_username or not settings.platform_password:
                raise ValueError(
                    "Задайте PLATFORM_USERNAME / PLATFORM_PASSWORD "
                    "(или RCSI_USERNAME / RCSI_PASSWORD) в .env"
                )
            # base_url из ссылки пользователя (journals.rcsi.science/…)
            auth = ensure_platform_auth(
                username=settings.platform_username,
                password=settings.platform_password,
                base_url=base_url,
                cookie_file=settings.platform_cookie_file,
                timeout=float(settings.request_timeout),
                ssl_verify=bool(settings.platform_ssl_verify),
            )
            delay = float(settings.platform_request_delay)

        report = sync_journal_sections(
            auth,
            journal_url_raw=journal_url,
            csv_path=Path(csv_path) if csv_path is not None else None,
            dry_run=dry_run,
            sort_alpha=sort_alpha,
            delay=delay,
        )
    except PlatformAuthError as exc:
        raise ValueError(f"Ошибка доступа: {exc}") from exc

    data = report.to_dict()
    data["auth_kind"] = auth_kind
    logger.info(
        "journal_sections journal=%s auth=%s dry_run=%s sort_alpha=%s "
        "created=%s exists=%s errors=%s sort=%s",
        data.get("journal"),
        auth_kind,
        dry_run,
        sort_alpha,
        data.get("created"),
        data.get("exists"),
        data.get("errors"),
        data.get("sort_status"),
    )
    if report.error:
        raise ValueError(report.error)
    return data
