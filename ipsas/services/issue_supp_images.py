"""Оркестрация: URL выпуска + images.zip → загрузка рисунков в доп. файлы."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ipsas.config.settings import get_settings
from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthError,
    ensure_platform_auth,
)
from ipsas.modules.issue_supp_images.service import process_issue_images
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def execute(
    *,
    issue_url: str,
    zip_path: Path,
    extract_dir: Path,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Загрузить рисунки из ZIP в доп. файлы статей выпуска."""
    settings = get_settings()
    if not settings.platform_username or not settings.platform_password:
        raise ValueError(
            "Задайте PLATFORM_USERNAME / PLATFORM_PASSWORD "
            "(или RCSI_USERNAME / RCSI_PASSWORD) в .env"
        )

    auth = ensure_platform_auth(
        username=settings.platform_username,
        password=settings.platform_password,
        base_url=settings.platform_base_url,
        cookie_file=settings.platform_cookie_file,
        timeout=float(settings.request_timeout),
    )

    try:
        summary = process_issue_images(
            auth,
            issue_url=issue_url,
            zip_path=Path(zip_path),
            extract_dir=Path(extract_dir),
            dry_run=dry_run,
            delay=float(settings.platform_request_delay),
        )
    except PlatformAuthError as exc:
        raise ValueError(f"Ошибка доступа к платформе: {exc}") from exc

    logger.info(
        "issue_supp_images journal=%s issue=%s dry_run=%s files=%s uploaded=%s errors=%s",
        summary.get("journal"),
        summary.get("issue_id"),
        dry_run,
        summary.get("total_files"),
        summary.get("uploaded"),
        summary.get("errors"),
    )
    return summary
