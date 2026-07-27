"""Роуты для скачивания HTML-отчёта по метаданным XML (совместимость)."""

from __future__ import annotations

import os
from pathlib import Path

from flask import Blueprint, Response, flash, redirect, url_for

from ipsas.config.settings import get_settings
from ipsas.modules.xml_report_generator import generate_xml_html_report
from ipsas.utils.logger import get_logger
from ipsas.utils.temp_files import cleanup_temp_dir, safe_temp_path

logger = get_logger(__name__)

xml_report_bp = Blueprint("xml_report", __name__, template_folder="templates")


def _unlink_quiet(*paths: Path) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError as e:
            logger.warning("Не удалось удалить временный файл %s: %s", path.name, e)


def _temp_ttl_seconds() -> int:
    try:
        return int(os.getenv("TEMP_FILE_TTL_SECONDS", str(6 * 60 * 60)))
    except ValueError:
        return 6 * 60 * 60


@xml_report_bp.route("/xml-report")
def xml_report_page():
    """Совместимость: старый URL ведёт в объединённый валидатор XML."""
    return redirect(url_for("xml_validation.xml_validator_page"))


@xml_report_bp.route("/xml-report/generate", methods=["POST"])
def generate_report():
    """Совместимость: POST старого маршрута перенаправляет на единый валидатор."""
    return redirect(url_for("xml_validation.xml_validator_page"))


@xml_report_bp.route("/xml-report/download/<filename>")
def download_report(filename: str):
    """Сгенерировать HTML из сохранённого XML и отдать на скачивание."""
    settings = get_settings()
    cleanup_temp_dir(settings.temp_dir, ttl_seconds=_temp_ttl_seconds())

    xml_path = safe_temp_path(settings.temp_dir, filename)
    if xml_path is None or not xml_path.exists() or xml_path.suffix.lower() != ".xml":
        flash("Исходный XML для отчёта не найден или устарел. Загрузите файл снова.", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))

    report_temp_path = xml_path.with_name(f"{xml_path.stem}_report.html")
    try:
        generate_xml_html_report(xml_path, report_temp_path)
        html_bytes = report_temp_path.read_bytes()
    except Exception as e:
        logger.error("Ошибка ленивой генерации HTML: %s", e)
        flash(f"Не удалось сформировать HTML-отчёт: {e}", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))
    finally:
        _unlink_quiet(report_temp_path)

    from ipsas.utils.download_names import (
        attachment_filename_from_xml,
        content_disposition_attachment,
    )

    download_name = attachment_filename_from_xml(
        xml_path, extension=".html", fallback="report"
    )

    def generate():
        try:
            yield html_bytes
        finally:
            _unlink_quiet(xml_path)

    return Response(
        generate(),
        mimetype="text/html",
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )
