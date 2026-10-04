"""Веб: проверка JATS XML для Метафоры (файл или ZIP выпуска)."""

from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.services.validate_metafora_jats import execute as validate_execute
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import cleanup_temp_dir

logger = get_logger(__name__)

metafora_jats_bp = Blueprint("metafora_jats", __name__, template_folder="templates")


@metafora_jats_bp.route("/metafora-jats")
def upload_page():
    return render_template("metafora_jats.html")


@metafora_jats_bp.route("/metafora-jats/process", methods=["POST"])
def process_upload():
    settings = get_settings()
    cleanup_temp_dir(
        settings.temp_dir,
        ttl_seconds=settings.temp_file_ttl_seconds,
    )

    file = request.files.get("xml_file")
    if file is None or not file.filename:
        flash("Выберите XML-файл или ZIP-архив выпуска.", "error")
        return redirect(url_for("metafora_jats.upload_page"))

    original = file.filename
    low = original.lower()
    if not (low.endswith(".xml") or low.endswith(".zip")):
        flash("Поддерживаются файлы .xml или .zip", "error")
        return redirect(url_for("metafora_jats.upload_page"))

    data = file.read()
    if not data:
        flash("Файл пуст.", "error")
        return redirect(url_for("metafora_jats.upload_page"))
    if len(data) > settings.max_file_size:
        max_mb = settings.max_file_size / (1024 * 1024)
        flash(f"Файл слишком большой (максимум {max_mb:.0f} МБ).", "error")
        return redirect(url_for("metafora_jats.upload_page"))

    safe_name = secure_filename(original) or (
        "issue.zip" if low.endswith(".zip") else "article.xml"
    )
    # сохранить расширение, если secure_filename его съел
    if low.endswith(".zip") and not safe_name.lower().endswith(".zip"):
        safe_name = f"{safe_name}.zip"
    if low.endswith(".xml") and not safe_name.lower().endswith(".xml"):
        safe_name = f"{safe_name}.xml"

    try:
        result = validate_execute(xml_bytes=data, filename=safe_name)
    except Exception as exc:  # noqa: BLE001
        logger.error("metafora_jats failed: %s", exc, exc_info=True)
        flash("Не удалось проверить файл.", "error")
        return redirect(url_for("metafora_jats.upload_page"))

    if result.batch_error and not result.reports:
        flash(result.batch_error, "error")
        return redirect(url_for("metafora_jats.upload_page"))

    detail = safe_name
    if result.is_batch:
        detail = f"{safe_name}: {result.ok_count}/{result.article_count} OK"

    record_operation(
        tool="metafora_jats",
        title="Проверка JATS для Метафоры",
        detail=detail,
        status="ok" if result.ok else "error",
        url=url_for("metafora_jats.upload_page"),
    )

    return render_template(
        "metafora_jats_result.html",
        filename=result.source_name,
        result=result,
        report=result.report,
    )
