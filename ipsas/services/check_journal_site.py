"""Сценарий: проверка настроек журнала по файлу OJS .data."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Union

from ipsas.config.settings import get_settings
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
    """Проверить файл настроек .data.

    Параметр ``journal_url`` оставлен для совместимости вызовов и игнорируется.
    Проверка по ссылке на сайт больше не поддерживается.
    """
    url = (journal_url or "").strip()
    has_data = data_file is not None and data_file != b"" and data_file != ""
    if not has_data:
        if url:
            raise ValueError(
                "Проверка по ссылке больше не поддерживается. Загрузите файл .data"
            )
        raise ValueError("Загрузите файл настроек журнала (.data)")

    settings = get_settings()
    max_bytes = min(settings.max_file_size, settings.max_content_length)
    payload = _read_payload(data_file)
    export = parse_journal_data(payload, max_bytes=max_bytes)
    return build_data_report_dict(export, source_name=data_filename or "")


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
    return Path(data_file).read_bytes()
