"""Общие операции загрузки/скачивания временных файлов для web-сервисов."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from flask import Response, flash, redirect
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.utils.download_names import (
    attachment_filename_from_xml,
    content_disposition_attachment,
)
from ipsas.utils.logger import get_logger
from ipsas.utils.temp_files import cleanup_temp_dir, safe_temp_path

logger = get_logger(__name__)


def temp_ttl_seconds() -> int:
    try:
        return int(os.getenv("TEMP_FILE_TTL_SECONDS", str(6 * 60 * 60)))
    except ValueError:
        return 6 * 60 * 60


def cleanup_temp(*, suffixes: tuple[str, ...] = (".xml", ".html", ".json", ".lock", ".csv")) -> int:
    settings = get_settings()
    return cleanup_temp_dir(settings.temp_dir, ttl_seconds=temp_ttl_seconds(), suffixes=suffixes)


def build_temp_name(original_filename: str, *, suffix: str = "", ext: str = ".xml") -> str:
    """``YYYYMMDD_HHMMSS_uuid8_{stem}{suffix}{ext}``."""
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = secure_filename(original_filename) or f"upload{ext}"
    stem = Path(safe).stem
    if not ext.startswith("."):
        ext = f".{ext}"
    return f"{timestamp}_{unique_id}_{stem}{suffix}{ext}"


def save_uploaded_xml(
    file: FileStorage,
    *,
    suffix: str = "",
) -> tuple[Path, str]:
    """
    Сохранить загруженный XML во temp.

    Returns:
        (path, original_secure_name)
    """
    settings = get_settings()
    original = secure_filename(file.filename or "") or "upload.xml"
    name = build_temp_name(original, suffix=suffix, ext=".xml")
    path = settings.temp_dir / name
    path.parent.mkdir(parents=True, exist_ok=True)
    file.save(str(path))
    return path, original


def require_xml_upload(
    *,
    field: str = "xml_file",
    redirect_endpoint: str,
) -> tuple[Optional[FileStorage], Optional[Response]]:
    """
    Проверить наличие XML в request.files.

    Returns:
        (file, None) при успехе; (None, redirect_response) при ошибке.
    """
    from flask import request, url_for

    if field not in request.files:
        flash("Файл не был загружен", "error")
        return None, redirect(url_for(redirect_endpoint))
    file = request.files[field]
    if not file or not file.filename:
        flash("Файл не выбран", "error")
        return None, redirect(url_for(redirect_endpoint))
    if not str(file.filename).lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return None, redirect(url_for(redirect_endpoint))
    return file, None


def stream_and_delete(
    file_path: Path,
    *,
    download_name: str,
    mimetype: str,
    on_missing_redirect: str,
) -> Response:
    """Отдать файл как attachment и удалить после чтения."""
    from flask import url_for

    if not file_path.exists():
        flash("Файл не найден", "error")
        return redirect(url_for(on_missing_redirect))

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info("Удалён файл после скачивания: %s", file_path.name)
            except OSError as e:
                logger.warning("Не удалось удалить %s: %s", file_path.name, e)

    return Response(
        generate(),
        mimetype=mimetype,
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )


def download_xml_and_delete(
    filename: str,
    *,
    on_missing_redirect: str,
    fallback_name: str = "output",
    resolve: Optional[Callable[[str], Optional[Path]]] = None,
) -> Response:
    """Скачать XML из temp с именем по ISSN/год/том/номер_report."""
    settings = get_settings()
    if resolve is not None:
        file_path = resolve(filename)
    else:
        file_path = safe_temp_path(settings.temp_dir, filename)
        if file_path is None:
            # fallback: прямое имя в temp (как раньше)
            candidate = settings.temp_dir / Path(filename).name
            file_path = candidate if candidate.exists() else None

    if file_path is None or not file_path.exists():
        flash("Файл не найден", "error")
        from flask import url_for

        return redirect(url_for(on_missing_redirect))

    download_name = attachment_filename_from_xml(
        file_path, extension=".xml", fallback=fallback_name
    )
    return stream_and_delete(
        file_path,
        download_name=download_name,
        mimetype="application/xml",
        on_missing_redirect=on_missing_redirect,
    )
