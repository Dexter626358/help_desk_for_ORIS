"""Файловое хранилище фоновых задач (совместимо с несколькими gunicorn workers)."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from ipsas.config.settings import get_settings

logger = logging.getLogger(__name__)

DEFAULT_TASK_TTL_SECONDS = 2 * 60 * 60  # 2 часа


def _tasks_dir() -> Path:
    path = get_settings().temp_dir / "issue_metadata_tasks"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _task_path(task_id: str) -> Path:
    safe = "".join(ch for ch in task_id if ch.isalnum())
    if not safe or safe != task_id:
        raise ValueError("Некорректный task_id")
    return _tasks_dir() / f"{safe}.json"


def task_ttl_seconds() -> int:
    try:
        return int(os.getenv("ISSUE_PARSER_TASK_TTL_S", str(DEFAULT_TASK_TTL_SECONDS)))
    except ValueError:
        return DEFAULT_TASK_TTL_SECONDS


def cleanup_expired_tasks(ttl_seconds: int | None = None) -> int:
    ttl = task_ttl_seconds() if ttl_seconds is None else ttl_seconds
    now = time.time()
    removed = 0
    for path in _tasks_dir().glob("*.json"):
        try:
            age = now - path.stat().st_mtime
            if age >= ttl:
                path.unlink(missing_ok=True)
                removed += 1
        except OSError:
            continue
    return removed


def task_set(task_id: str, **kwargs: Any) -> None:
    path = _task_path(task_id)
    data: dict[str, Any] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data = loaded
        except Exception:
            data = {}
    data.update(kwargs)
    data["updated_at"] = time.time()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def task_get(task_id: str) -> dict[str, Any] | None:
    try:
        path = _task_path(task_id)
    except ValueError:
        return None
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def task_pop(task_id: str) -> None:
    try:
        path = _task_path(task_id)
    except ValueError:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        logger.warning("Не удалось удалить task %s: %s", task_id, e)
