"""Локальная история операций для дашборда (файл в temp/)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ipsas.config.settings import get_settings

_HISTORY_LIMIT = 30


def _history_path() -> Path:
    path = get_settings().temp_dir / "operation_history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def record_operation(
    *,
    tool: str,
    title: str,
    status: str,
    detail: str = "",
    url: str | None = None,
) -> None:
    """Добавить запись об операции (best-effort, без исключений наружу)."""
    entry = {
        "ts": time.time(),
        "tool": tool,
        "title": title,
        "status": status,
        "detail": detail[:300],
        "url": url,
    }
    try:
        with _history_path().open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        return


def list_operations(limit: int = 8) -> list[dict[str, Any]]:
    """Вернуть последние операции (новые сверху)."""
    path = _history_path()
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []

    items: list[dict[str, Any]] = []
    for line in reversed(lines[-_HISTORY_LIMIT:]):
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            items.append(data)
        if len(items) >= limit:
            break
    return items
