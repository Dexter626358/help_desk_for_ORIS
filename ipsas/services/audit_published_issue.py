"""Сценарий аудита опубликованного выпуска OJS."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable

from ipsas.config.settings import get_settings
from ipsas.modules.issue_metadata.orchestrator import IssueMetadataParser
from ipsas.modules.issue_metadata.report_summary import group_findings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def parse_issue(issue_url: str) -> dict[str, Any]:
    """Разобрать выпуск по URL (страница или файл)."""
    settings = get_settings()
    parser = IssueMetadataParser(max_download_size=settings.max_file_size)
    lower_url = issue_url.lower()

    if lower_url.endswith(".xml") or lower_url.endswith(".zip"):
        original_name = issue_url.split("/")[-1] or "issue.xml"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_path = settings.temp_dir / f"{timestamp}_{original_name}"
        try:
            download = parser.download(issue_url, temp_path)
            issue_metadata = parser.parse_issue_metadata(download.path)
            return {
                "issue": issue_metadata,
                "articles": [],
                "notice": (
                    "Ссылка указывает на файл. Для анализа статей "
                    "используйте ссылку на страницу выпуска."
                ),
            }
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError as exc:
                logger.warning(
                    "Не удалось удалить временный файл %s: %s", temp_path, exc
                )

    result = parser.parse_issue_url(issue_url)
    result["notice"] = None
    return result


def summarize_findings(result: dict[str, Any]) -> Any:
    issue_data = result.get("issue") if isinstance(result.get("issue"), dict) else {}
    articles_data = (
        result.get("articles") if isinstance(result.get("articles"), list) else []
    )
    return group_findings(issue_data.get("warnings") or [], articles_data)


def run_parse_task(
    *,
    task_id: str,
    issue_url: str,
    user_key: str,
    task_set: Callable[..., None],
    release_inflight: Callable[..., None],
) -> None:
    """Фоновый worker: парсинг + запись статуса задачи."""
    try:
        result = parse_issue(issue_url)
        task_set(task_id, status="done", result=result, finished_at=time.time())
    except Exception as exc:
        logger.error("Фоновый парсинг выпуска упал: %s", exc, exc_info=True)
        task_set(
            task_id,
            status="error",
            error=str(exc),
            finished_at=time.time(),
        )
    finally:
        release_inflight(user_key=user_key, issue_url=issue_url)
