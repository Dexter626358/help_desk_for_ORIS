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


def _dir_has_lock(directory: Path) -> bool:
    try:
        if (directory / "job.lock").exists():
            return True
        return any(directory.glob("*.lock"))
    except OSError:
        return False


def is_path_protected(path: Path, temp_dir: Path) -> bool:
    """True если файл/каталог относится к активной операции (есть lock)."""
    if path.name.endswith(".lock") or path.name == "job.lock":
        return True
    current = path if path.is_dir() else path.parent
    temp_resolved = temp_dir.resolve()
    while True:
        try:
            current.resolve().relative_to(temp_resolved)
        except ValueError:
            break
        if _dir_has_lock(current):
            return True
        if current.resolve() == temp_resolved:
            break
        if current.parent == current:
            break
        current = current.parent
    return False


def cleanup_temp_dir(
    temp_dir: Path,
    *,
    ttl_seconds: int = DEFAULT_TEMP_TTL_SECONDS,
    suffixes: tuple[str, ...] = (".xml", ".html", ".json", ".csv", ".zip"),
) -> int:
    """Удалить устаревшие файлы в temp_dir (рекурсивно).

    Не удаляет ``*.lock`` и файлы внутри каталогов с lock (активные jobs).
    """
    if not temp_dir.exists() or ttl_seconds <= 0:
        return 0

    now = time.time()
    removed = 0
    suffix_set = {s.lower() for s in suffixes}

    for path in list(temp_dir.rglob("*")):
        try:
            if not path.is_file():
                continue
            if path.name.endswith(".lock"):
                continue
            if path.suffix.lower() not in suffix_set:
                continue
            if is_path_protected(path, temp_dir):
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
