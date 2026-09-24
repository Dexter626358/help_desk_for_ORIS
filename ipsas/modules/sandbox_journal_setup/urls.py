"""Разбор ссылки на журнал песочницы."""

from __future__ import annotations

import re
from urllib.parse import urlparse

# https://f23g45.rcsi.science/257/index
# https://f23g45.rcsi.science/257/
# https://f23g45.rcsi.science/257/manager/languages
_JOURNAL_PATH_RE = re.compile(r"^/([^/]+)(?:/|$)")


def parse_journal_url(url: str) -> tuple[str, str]:
    """Вернуть (base_url, journal_path) из ссылки на журнал.

    Примеры journal_path: ``257``, ``0869-5733``.
    """
    raw = (url or "").strip()
    if not raw:
        raise ValueError("Укажите ссылку на журнал (например …/257/index)")
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    if not parsed.netloc:
        raise ValueError(f"Некорректный URL: {url}")
    path = parsed.path or "/"
    m = _JOURNAL_PATH_RE.match(path)
    if not m:
        raise ValueError(
            "Ожидается ссылка вида https://host/JOURNAL/… "
            f"(например https://f23g45.rcsi.science/257/index), получено: {url}"
        )
    journal = m.group(1)
    if journal.lower() in {"index.php", "index", "login", "user"}:
        raise ValueError(
            "В URL не найден путь журнала. "
            "Пример: https://f23g45.rcsi.science/257/index"
        )
    base = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return base, journal


def journal_url(base_url: str, journal: str, *parts: str) -> str:
    """Собрать URL внутри ветки журнала."""
    base = base_url.rstrip("/")
    path = "/".join(str(p).strip("/") for p in parts if str(p).strip("/"))
    if path:
        return f"{base}/{journal.strip('/')}/{path}"
    return f"{base}/{journal.strip('/')}"
