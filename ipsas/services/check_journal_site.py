"""Сценарий: проверка заполненности сайта журнала и/или OJS .data."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Union

from ipsas.config.settings import get_settings
from ipsas.modules.journal_site.checker import JournalSiteChecker
from ipsas.modules.journal_site.data_check import build_data_report_dict
from ipsas.modules.journal_site.data_export import parse_journal_data
from ipsas.modules.journal_site.editorial_letter import (
    build_journal_site_editorial_letter,
    build_journal_site_editorial_letter_html,
    wrap_editorial_letter_html_document,
)


def execute(
    journal_url: str | None = None,
    data_file: Union[str, Path, bytes, None] = None,
    *,
    data_filename: str = "",
) -> dict[str, Any]:
    """Проверить сайт и/или файл .data. Нужен хотя бы один источник."""
    url = (journal_url or "").strip()
    has_data = data_file is not None and data_file != b"" and data_file != ""
    if not url and not has_data:
        raise ValueError("Укажите ссылку на журнал и/или загрузите файл .data")

    settings = get_settings()
    max_bytes = min(settings.max_file_size, settings.max_content_length)

    site_report: Optional[dict[str, Any]] = None
    if url:
        from ipsas.modules.issue_metadata.http_client import HttpClient

        http = HttpClient(
            max_bytes=min(settings.max_file_size, 2_500_000),
            timeout_s=12,
            retries=1,
            backoff_s=0.2,
            issue_timeout_s=12,
            issue_retries=1,
        )
        checker = JournalSiteChecker(http=http, max_bytes=min(settings.max_file_size, 2_500_000))
        site_report = checker.check(url).to_dict()
        site_report["check_mode"] = "site"

    data_report: Optional[dict[str, Any]] = None
    if has_data:
        payload = _read_payload(data_file)
        export = parse_journal_data(payload, max_bytes=max_bytes)
        data_report = build_data_report_dict(export, source_name=data_filename or "")
        # если есть URL — плагины всё равно из .data
        if site_report is not None:
            site_report["plugins_results"] = data_report.get("plugins_results") or []
            site_report["plugins_must_fix"] = data_report.get("plugins_must_fix") or []
            site_report["data_source"] = data_report.get("data_source")
            site_report["settings_locales"] = data_report.get("locales") or {}
            site_report["settings_must_fix"] = data_report.get("must_fix") or []
            site_report["settings_recommendations"] = data_report.get("recommendations") or []
            site_report["settings_completeness_percent"] = data_report.get("completeness_percent")
            site_report["settings_completeness_ru"] = data_report.get("completeness_ru")
            site_report["settings_completeness_en"] = data_report.get("completeness_en")
            site_report["check_mode"] = "site+data"
            if not site_report.get("journal_title") and data_report.get("journal_title"):
                site_report["journal_title"] = data_report["journal_title"]
            return site_report
        return data_report

    assert site_report is not None
    site_report.setdefault("plugins_results", [])
    site_report.setdefault("plugins_must_fix", [])
    return site_report


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


def build_editorial_letter_html(
    report: dict[str, Any],
    *,
    journal_url: str = "",
    generated_at: str | None = None,
) -> str:
    """HTML-фрагмент письма редакции по уже посчитанному отчёту."""
    return build_journal_site_editorial_letter_html(
        report,
        journal_url=journal_url,
        generated_at=generated_at,
    )


def build_editorial_letter_html_document(
    report: dict[str, Any],
    *,
    journal_url: str = "",
    generated_at: str | None = None,
) -> str:
    """Полный HTML-документ письма для скачивания."""
    fragment = build_editorial_letter_html(
        report,
        journal_url=journal_url,
        generated_at=generated_at,
    )
    return wrap_editorial_letter_html_document(fragment)


def _read_payload(data_file: Union[str, Path, bytes]) -> bytes:
    if isinstance(data_file, bytes):
        return data_file
    path = Path(data_file)
    return path.read_bytes()
