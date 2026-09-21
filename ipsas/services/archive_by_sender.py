"""Оркестрация: ссылка на журнал → архивация «Новые» по отправителю."""

from __future__ import annotations

from collections import Counter
from typing import Any

from ipsas.config.settings import get_settings
from ipsas.modules.archive_by_sender.constants import DEFAULT_SENDERS
from ipsas.modules.archive_by_sender.parser import parse_journal_ref
from ipsas.modules.archive_by_sender.service import process_journal
from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthError,
    ensure_platform_auth,
)
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def parse_senders_text(raw: str | None) -> list[str]:
    """Разбор ФИО: по одному на строку; пусто → список по умолчанию."""
    if raw is None or not str(raw).strip():
        return list(DEFAULT_SENDERS)
    names = [line.strip() for line in str(raw).splitlines() if line.strip()]
    return names or list(DEFAULT_SENDERS)


def execute(
    *,
    journal_url: str,
    senders_text: str | None = None,
    dry_run: bool = True,
    limit: int | None = None,
    verify: bool = True,
) -> dict[str, Any]:
    """
    Обработать «Новые» одного журнала.

    Returns:
        summary с journal, dry_run, counts и списком results.
    """
    settings = get_settings()
    if not settings.platform_username or not settings.platform_password:
        raise ValueError(
            "Задайте PLATFORM_USERNAME / PLATFORM_PASSWORD "
            "(или RCSI_USERNAME / RCSI_PASSWORD) в .env"
        )

    journal = parse_journal_ref(journal_url)
    sender_names = parse_senders_text(senders_text)

    auth = ensure_platform_auth(
        username=settings.platform_username,
        password=settings.platform_password,
        base_url=settings.platform_base_url,
        cookie_file=settings.platform_cookie_file,
        timeout=float(settings.request_timeout),
    )

    try:
        results = process_journal(
            auth,
            journal,
            sender_names=sender_names,
            limit=limit,
            delay=float(settings.platform_request_delay),
            dry_run=dry_run,
            verify=verify and not dry_run,
        )
    except PlatformAuthError as exc:
        raise ValueError(f"Ошибка доступа к платформе: {exc}") from exc

    counts = Counter(r.status for r in results)
    summary = {
        "journal": journal,
        "journal_url": journal_url.strip(),
        "dry_run": dry_run,
        "sender_count": len(sender_names),
        "senders": sender_names,
        "total": len(results),
        "counts": dict(counts),
        "archived": counts.get("archived", 0),
        "dry_run_hits": counts.get("dry_run", 0),
        "skipped_sender": counts.get("skipped_sender", 0),
        "errors": counts.get("error", 0) + counts.get("verify_failed", 0),
        "results": [r.to_dict() for r in results],
    }
    logger.info(
        "archive_by_sender journal=%s dry_run=%s total=%s archived=%s skipped=%s errors=%s",
        journal,
        dry_run,
        summary["total"],
        summary["archived"],
        summary["skipped_sender"],
        summary["errors"],
    )
    return summary
