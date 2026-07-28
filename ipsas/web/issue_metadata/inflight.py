"""Межпроцессная блокировка повторного парсинга одного выпуска."""

from __future__ import annotations

import hashlib
import os
import threading
import time
from pathlib import Path

from ipsas.config.settings import get_settings

_inflight_lock = threading.Lock()


def inflight_lock_path(*, user_key: str, issue_url: str) -> Path:
    settings = get_settings()
    lock_dir = settings.temp_dir / "issue_metadata_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(f"{user_key}\n{issue_url}".encode("utf-8")).hexdigest()[:32]
    return lock_dir / f"{digest}.lock"


def try_acquire_inflight(*, user_key: str, issue_url: str, ttl_s: int = 15 * 60) -> bool:
    """Атомарный lockfile: защита от двойного запуска между gunicorn workers."""
    lock_path = inflight_lock_path(user_key=user_key, issue_url=issue_url)
    now = time.time()
    with _inflight_lock:
        if lock_path.exists():
            try:
                age = now - lock_path.stat().st_mtime
                if age > ttl_s:
                    lock_path.unlink(missing_ok=True)
            except OSError:
                return False
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(f"started_at={now}\nuser={user_key}\nurl={issue_url}\n")
            return True
        except FileExistsError:
            return False
        except OSError:
            return False


def inflight_age_s(*, user_key: str, issue_url: str) -> float | None:
    lock_path = inflight_lock_path(user_key=user_key, issue_url=issue_url)
    if not lock_path.exists():
        return None
    try:
        return max(0.0, time.time() - lock_path.stat().st_mtime)
    except OSError:
        return None


def release_inflight(*, user_key: str, issue_url: str) -> None:
    lock_path = inflight_lock_path(user_key=user_key, issue_url=issue_url)
    with _inflight_lock:
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass
