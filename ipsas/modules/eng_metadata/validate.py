"""Валидация flat-JSON ENG-метаданных."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REQUIRED_KEYS = ("title_eng", "authors", "abstract_eng", "keywords_eng")


def load_json_file(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("JSON должен быть объектом")
    return data


def validate_article_json(data: dict[str, Any]) -> list[str]:
    """Вернуть список ошибок; пустой список = OK."""
    errors: list[str] = []
    for key in REQUIRED_KEYS:
        if key not in data:
            errors.append(f"Нет обязательного поля «{key}»")

    title = data.get("title_eng")
    if title is not None and not str(title).strip():
        errors.append("Поле title_eng пустое")

    abstract = data.get("abstract_eng")
    if abstract is not None and not str(abstract).strip():
        errors.append("Поле abstract_eng пустое")

    keywords = data.get("keywords_eng")
    if keywords is not None and not isinstance(keywords, list):
        errors.append("keywords_eng должен быть списком")

    authors = data.get("authors")
    if authors is None:
        pass
    elif not isinstance(authors, list):
        errors.append("authors должен быть списком")
    elif not authors:
        errors.append("Список authors пуст")
    else:
        for idx, author in enumerate(authors, start=1):
            if not isinstance(author, dict):
                errors.append(f"Автор #{idx}: ожидался объект")
                continue
            surname = str(author.get("surname_en") or "").strip()
            full = str(author.get("full_name_en") or "").strip()
            if not surname and not full:
                errors.append(
                    f"Автор #{idx}: нужен surname_en или full_name_en"
                )
    return errors
