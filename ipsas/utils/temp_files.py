"""Утилиты для временных файлов: безопасные пути и TTL-очистка."""

from __future__ import annotations

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TEMP_TTL_SECONDS = 6 * 60 * 60  # 6 часов


def safe_temp_path(temp_dir: Path, filename: str) -> Path | None:
    """Путь внутри temp_dir без path traversal. None если имя недопустимо."""
    from werkzeug.utils import secure_filename

    safe_name = Path(secure_filename(filename)).name
    if not safe_name or safe_name in {".", ".."}:
        return None
    candidate = (temp_dir / safe_name).resolve()
    try:
        candidate.relative_to(temp_dir.resolve())
    except ValueError:
        return None
    return candidate


def cleanup_temp_dir(
    temp_dir: Path,
    *,
    ttl_seconds: int = DEFAULT_TEMP_TTL_SECONDS,
    suffixes: tuple[str, ...] = (".xml", ".html", ".json", ".lock", ".csv"),
) -> int:
    """Удалить устаревшие файлы в temp_dir (рекурсивно).

    Returns:
        Число удалённых файлов.
    """
    if not temp_dir.exists() or ttl_seconds <= 0:
        return 0

    now = time.time()
    removed = 0
    suffix_set = {s.lower() for s in suffixes}

    for path in temp_dir.rglob("*"):
        try:
            if not path.is_file():
                continue
            if path.suffix.lower() not in suffix_set:
                continue
            if now - path.stat().st_mtime < ttl_seconds:
                continue
            path.unlink(missing_ok=True)
            removed += 1
        except OSError as e:
            logger.debug("Не удалось удалить temp %s: %s", path, e)

    if removed:
        logger.info("Очистка temp: удалено %s файлов старше %sс", removed, ttl_seconds)
    return removed
