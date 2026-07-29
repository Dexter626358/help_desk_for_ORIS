"""Сценарий: проверка заполненности сайта журнала."""

from __future__ import annotations

from typing import Any

from ipsas.config.settings import get_settings
from ipsas.modules.journal_site.checker import JournalSiteChecker
from ipsas.modules.journal_site.editorial_letter import build_journal_site_editorial_letter


def execute(journal_url: str) -> dict[str, Any]:
    """Проверить сайт журнала и вернуть отчёт (dict)."""
    settings = get_settings()
    checker = JournalSiteChecker(max_bytes=min(settings.max_file_size, 2_500_000))
    report = checker.check(journal_url)
    return report.to_dict()


def build_editorial_letter_text(
    report: dict[str, Any],
    *,
    journal_url: str = "",
    generated_at: str | None = None,
) -> str:
    """Текст письма редакции по уже посчитанному отчёту."""
    return build_journal_site_editorial_letter(
        report,
        journal_url=journal_url,
        generated_at=generated_at,
    )
