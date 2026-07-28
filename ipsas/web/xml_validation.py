"""Роуты для валидации XML: XSD-схема и контроль метаданных."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.services.validate_xml import execute as validate_xml_service
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import cleanup_temp_dir

logger = get_logger(__name__)

xml_validation_bp = Blueprint("xml_validation", __name__, template_folder="templates")


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


def _schema_ok(result: dict) -> bool:
    if result.get("schemas_results") is not None:
        return bool(result.get("overall_valid"))
    return bool(result.get("valid"))


@xml_validation_bp.route("/xml-validator")
def xml_validator_page():
    """Единая страница валидатора XML (схема + метаданные)."""
    settings = get_settings()
    schemas: list[str] = []
    if settings.schemas_dir.exists():
        schemas = [f.name for f in settings.schemas_dir.glob("*.xsd")]
    return render_template("xml_validator.html", schemas=schemas)


@xml_validation_bp.route("/xml-validator/validate", methods=["POST"])
def validate_xml():
    """Проверка XML по XSD и анализ метаданных journal."""
    settings = get_settings()
    cleanup_temp_dir(settings.temp_dir, ttl_seconds=_temp_ttl_seconds())

    if "xml_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))

    file = request.files["xml_file"]
    if not file.filename:
        flash("Файл не выбран", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))

    if not file.filename.lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))

    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_upload = secure_filename(file.filename) or f"upload_{unique_id}.xml"
    display_name = safe_upload if safe_upload != f"upload_{unique_id}.xml" else file.filename
    temp_path = settings.temp_dir / f"{timestamp}_{unique_id}_{safe_upload}"

    schema_name = (request.form.get("schema") or "").strip()
    check_schema = request.form.get("check_schema") == "1"
    check_metadata = request.form.get("check_metadata") == "1"

    try:
        file.save(str(temp_path))

        try:
            size = temp_path.stat().st_size
        except OSError:
            size = None
        if size is not None and size > settings.max_file_size:
            _unlink_quiet(temp_path)
            max_mb = settings.max_file_size / (1024 * 1024)
            flash(f"Файл слишком большой. Максимальный размер: {max_mb:.1f} MB", "error")
            return redirect(url_for("xml_validation.xml_validator_page"))

        try:
            outcome = validate_xml_service(
                temp_path,
                schema_name=schema_name,
                check_schema=check_schema,
                check_metadata=check_metadata,
            )
        except FileNotFoundError as e:
            flash(str(e), "error")
            _unlink_quiet(temp_path)
            return redirect(url_for("xml_validation.xml_validator_page"))

        schema_result = outcome.schema_result
        schema_label = outcome.schema_label
        report = outcome.report
        metadata_error = outcome.metadata_error
        check_schema = outcome.check_schema
        check_metadata = outcome.check_metadata

        schema_passed = True if schema_result is None else _schema_ok(schema_result)
        meta_errors = 0
        if report and isinstance(report.get("summary"), dict):
            meta_errors = int(report["summary"].get("articles_with_errors") or 0)

        status = "ok"
        if (schema_result is not None and not schema_passed) or metadata_error or meta_errors:
            status = (
                "error"
                if (schema_result is not None and not schema_passed) or metadata_error
                else "ok"
            )

        detail_parts = [display_name]
        if schema_result is not None:
            detail_parts.append("схема OK" if schema_passed else "ошибки схемы")
        if report is not None:
            detail_parts.append(
                f"статей {report.get('summary', {}).get('articles_total', '?')}, "
                f"ошибок метаданных {meta_errors}"
            )
        elif check_metadata and metadata_error:
            detail_parts.append("метаданные недоступны")

        record_operation(
            tool="xml_validator",
            title="Валидатор XML",
            status=status if status == "ok" else "error",
            detail="; ".join(detail_parts),
            url=url_for("xml_validation.xml_validator_page"),
        )

        keep_file = report is not None
        xml_filename = temp_path.name if keep_file else None
        if not keep_file:
            _unlink_quiet(temp_path)

        return render_template(
            "xml_validation_result.html",
            filename=display_name,
            schema_name=schema_label,
            schema_result=schema_result,
            check_schema=check_schema,
            check_metadata=check_metadata,
            report=report,
            metadata_error=metadata_error,
            xml_filename=xml_filename,
        )

    except Exception as e:
        logger.error("Ошибка при валидации XML: %s", e, exc_info=True)
        flash(f"Ошибка при обработке файла: {str(e)}", "error")
        _unlink_quiet(temp_path)
        return redirect(url_for("xml_validation.xml_validator_page"))
