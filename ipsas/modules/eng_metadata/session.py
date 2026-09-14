"""Файловые сессии ENG-метаданных в TEMP_DIR."""

from __future__ import annotations

import json
import re
import secrets
import shutil
from pathlib import Path
from typing import Any

from ipsas.config.settings import get_settings
from ipsas.modules.eng_metadata.archive import build_article_pairs, unpack_eng_archive
from ipsas.modules.eng_metadata.validate import load_json_file, validate_article_json

SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
ARTICLE_ID_RE = re.compile(r"^\d{1,12}$")
MANIFEST_NAME = "manifest.json"
EDITED_NAME = "edited.json"
ORIGINAL_NAME = "original.json"
PDF_NAME = "article.pdf"


def sessions_root() -> Path:
    path = get_settings().temp_dir / "eng_metadata_sessions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def new_session_id() -> str:
    return secrets.token_urlsafe(24)


def is_safe_session_id(session_id: str) -> bool:
    return bool(session_id and SESSION_ID_RE.fullmatch(session_id))


def is_safe_article_id(article_id: str) -> bool:
    return bool(article_id and ARTICLE_ID_RE.fullmatch(article_id))


def get_session_dir(session_id: str) -> Path:
    if not is_safe_session_id(session_id):
        raise ValueError("Некорректный session_id")
    path = sessions_root() / session_id
    if not path.is_dir():
        raise FileNotFoundError("Сессия не найдена")
    return path


def get_article_dir(session_id: str, article_id: str) -> Path:
    if not is_safe_article_id(article_id):
        raise ValueError("Некорректный article_id")
    path = get_session_dir(session_id) / "articles" / article_id
    if not path.is_dir():
        raise FileNotFoundError("Статья не найдена в сессии")
    return path


def load_manifest(session_id: str) -> dict[str, Any]:
    path = get_session_dir(session_id) / MANIFEST_NAME
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Повреждённый manifest")
    return data


def save_manifest(session_id: str, manifest: dict[str, Any]) -> None:
    path = get_session_dir(session_id) / MANIFEST_NAME
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_article_json(session_id: str, article_id: str) -> dict[str, Any]:
    article_dir = get_article_dir(session_id, article_id)
    edited = article_dir / EDITED_NAME
    original = article_dir / ORIGINAL_NAME
    path = edited if edited.is_file() else original
    return load_json_file(path)


def save_article_json(session_id: str, article_id: str, data: dict[str, Any]) -> None:
    article_dir = get_article_dir(session_id, article_id)
    (article_dir / EDITED_NAME).write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def create_session_from_zip(zip_path: Path, *, source_name: str = "") -> dict[str, Any]:
    """Распаковать ZIP, проверить пары, создать сессию. Возвращает manifest."""
    session_id = new_session_id()
    root = sessions_root() / session_id
    extract_dir = root / "_extract"
    articles_root = root / "articles"
    root.mkdir(parents=True, exist_ok=True)

    try:
        unpack_eng_archive(zip_path, extract_dir)
        pairs = build_article_pairs(extract_dir)
        articles: list[dict[str, Any]] = []

        for pair in pairs:
            entry: dict[str, Any] = {
                "stem": pair.stem,
                "article_id": pair.article_id,
                "pdf_name": pair.pdf_name,
                "json_name": pair.json_name,
                "pair_issues": list(pair.issues),
                "validation_errors": [],
                "ok": False,
                "approved": False,
            }
            if not pair.article_id:
                articles.append(entry)
                continue

            article_dir = articles_root / pair.article_id
            article_dir.mkdir(parents=True, exist_ok=True)

            if pair.pdf_name:
                src_pdf = extract_dir / pair.pdf_name
                if src_pdf.is_file():
                    shutil.copy2(src_pdf, article_dir / PDF_NAME)

            data: dict[str, Any] | None = None
            if pair.json_name:
                src_json = extract_dir / pair.json_name
                if src_json.is_file():
                    try:
                        data = load_json_file(src_json)
                        (article_dir / ORIGINAL_NAME).write_text(
                            json.dumps(data, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8",
                        )
                        shutil.copy2(
                            article_dir / ORIGINAL_NAME,
                            article_dir / EDITED_NAME,
                        )
                    except (OSError, ValueError, json.JSONDecodeError) as exc:
                        entry["validation_errors"].append(f"JSON: {exc}")

            if data is not None:
                entry["validation_errors"].extend(validate_article_json(data))
                if data.get("article_url"):
                    entry["article_url"] = data.get("article_url")
                entry["title_eng"] = str(data.get("title_eng") or "")[:200]

            entry["ok"] = (
                bool(pair.pdf_name)
                and bool(pair.json_name)
                and not pair.issues
                and not entry["validation_errors"]
                and data is not None
            )
            articles.append(entry)

        # дедуп article_id: если несколько stem с одним id — пометить
        seen: dict[str, int] = {}
        for entry in articles:
            aid = entry.get("article_id") or ""
            if not aid:
                continue
            seen[aid] = seen.get(aid, 0) + 1
        for entry in articles:
            aid = entry.get("article_id") or ""
            if aid and seen.get(aid, 0) > 1:
                entry["pair_issues"].append("Дублирующий article_id в архиве")
                entry["ok"] = False

        manifest: dict[str, Any] = {
            "session_id": session_id,
            "source_name": source_name or zip_path.name,
            "articles": articles,
            "ok_count": sum(1 for a in articles if a.get("ok")),
            "total": len(articles),
        }
        (root / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        # extract можно оставить для отладки или удалить — удаляем для экономии
        shutil.rmtree(extract_dir, ignore_errors=True)
        return manifest
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise
