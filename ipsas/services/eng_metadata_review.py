"""Оркестрация: ZIP → сессия сверки ENG-метаданных → dry-run / apply."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ipsas.config.settings import get_settings
from ipsas.modules.eng_metadata.forms import apply_form_to_metadata
from ipsas.modules.eng_metadata.payload import build_update_payload, summarize_payload
from ipsas.modules.eng_metadata.platform_client import (
    PlatformClient,
    PlatformClientError,
    PlatformClientSettings,
)
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError
from ipsas.modules.eng_metadata.platform_update import ApplyResult, UpdateService
from ipsas.modules.eng_metadata.session import (
    create_session_from_zip,
    get_article_dir,
    load_article_json,
    load_manifest,
    save_article_json,
    save_manifest,
)
from ipsas.modules.eng_metadata.validate import validate_article_json
from ipsas.utils.logger import get_logger
from werkzeug.datastructures import MultiDict

logger = get_logger(__name__)

_ISSN_RE = re.compile(r"/(\d{4}-\d{3}[\dXx])/", re.I)


def execute_upload(zip_path: Path, *, source_name: str = "") -> dict[str, Any]:
    return create_session_from_zip(zip_path, source_name=source_name)


def save_article_edits(
    session_id: str,
    article_id: str,
    form: MultiDict[str, str],
) -> dict[str, Any]:
    base = load_article_json(session_id, article_id)
    data = apply_form_to_metadata(base, form)
    errors = validate_article_json(data)
    if errors:
        raise ValueError("; ".join(errors))
    save_article_json(session_id, article_id, data)

    article_dir = get_article_dir(session_id, article_id)
    has_pdf = (article_dir / "article.pdf").is_file()
    manifest = load_manifest(session_id)
    for entry in manifest.get("articles") or []:
        if str(entry.get("article_id")) == str(article_id):
            entry["validation_errors"] = []
            entry["title_eng"] = str(data.get("title_eng") or "")[:200]
            entry["approved"] = False
            entry["ok"] = has_pdf and not entry.get("pair_issues")
            break
    manifest["ok_count"] = sum(
        1 for a in (manifest.get("articles") or []) if a.get("ok")
    )
    save_manifest(session_id, manifest)
    return data


def prepare_platform_dry_run(session_id: str, article_id: str) -> dict[str, Any]:
    data = load_article_json(session_id, article_id)
    errors = validate_article_json(data)
    if errors:
        raise ValueError("; ".join(errors))

    payload = build_update_payload(
        article_id=article_id, data=data, approved=True
    )
    article_dir = get_article_dir(session_id, article_id)
    (article_dir / "update_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    manifest = load_manifest(session_id)
    for entry in manifest.get("articles") or []:
        if str(entry.get("article_id")) == str(article_id):
            entry["approved"] = True
            break
    save_manifest(session_id, manifest)
    return summarize_payload(payload)


def issn_from_article_url(article_url: str | None) -> str | None:
    if not article_url:
        return None
    m = _ISSN_RE.search(str(article_url))
    return m.group(1) if m else None


def apply_platform_update(session_id: str, article_id: str) -> dict[str, Any]:
    """Собрать payload и при PLATFORM_APPLY_ENABLED отправить на OJS."""
    settings = get_settings()
    data = load_article_json(session_id, article_id)
    errors = validate_article_json(data)
    if errors:
        raise ValueError("; ".join(errors))

    payload = build_update_payload(
        article_id=article_id, data=data, approved=True
    )
    article_dir = get_article_dir(session_id, article_id)
    (article_dir / "update_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    manifest = load_manifest(session_id)
    for entry in manifest.get("articles") or []:
        if str(entry.get("article_id")) == str(article_id):
            entry["approved"] = True
            break
    save_manifest(session_id, manifest)

    summary = summarize_payload(payload)
    if not settings.platform_apply_enabled:
        summary["apply"] = {
            "ok": True,
            "dry_run": True,
            "message": "PLATFORM_APPLY_ENABLED выключен — POST не отправлялся",
            "applied_fields": [],
            "errors": [],
        }
        return summary

    if not settings.platform_username or not settings.platform_password:
        raise ValueError(
            "Для отправки задайте PLATFORM_USERNAME/PLATFORM_PASSWORD "
            "(или RCSI_USERNAME/RCSI_PASSWORD)"
        )

    issn = issn_from_article_url(str(data.get("article_url") or ""))
    if not issn:
        raise ValueError(
            "Не удалось извлечь ISSN из article_url "
            "(ожидается …/{issn}/article/view/{id})"
        )

    try:
        client = PlatformClient(
            PlatformClientSettings(
                base_url=settings.platform_base_url,
                username=settings.platform_username,
                password=settings.platform_password,
                cookie_file=settings.platform_cookie_file,
                request_timeout=float(settings.request_timeout),
                request_delay=float(settings.platform_request_delay),
            )
        )
        service = UpdateService(client, issn=issn)
        result: ApplyResult = service.apply_payload(payload, allow_apply=True)
    except PlatformAuthError as exc:
        logger.error("platform auth failed: %s", exc)
        raise ValueError(
            f"Не удалось войти на платформу: {exc}. "
            "Проверьте PLATFORM_USERNAME / PLATFORM_PASSWORD."
        ) from exc
    except PlatformClientError as exc:
        logger.error("platform apply failed: %s", exc, exc_info=True)
        raise ValueError(f"Ошибка связи с платформой: {exc}") from exc

    result_dict = {
        "ok": result.ok,
        "dry_run": result.dry_run,
        "message": result.message,
        "applied_fields": result.applied_fields,
        "errors": result.errors,
        "issn": issn,
        "platform_host": urlparse(settings.platform_base_url).hostname,
    }
    (article_dir / "update_result.json").write_text(
        json.dumps(result_dict, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    for entry in manifest.get("articles") or []:
        if str(entry.get("article_id")) == str(article_id):
            entry["applied"] = bool(result.ok)
            entry["apply_message"] = result.message
            break
    save_manifest(session_id, manifest)

    summary["apply"] = result_dict
    summary["dry_run"] = False
    summary["note"] = result.message
    return summary
