"""Роуты для форматирования списков литературы в XML."""

from __future__ import annotations

from flask import Blueprint, render_template, url_for, flash

from ipsas.services.process_references import format_structure
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.web.file_ops import (
    download_xml_and_delete,
    require_xml_upload,
    save_uploaded_xml,
)

logger = get_logger(__name__)

reference_formatting_bp = Blueprint(
    "reference_formatting", __name__, template_folder="templates"
)

_PAGE = "reference_formatting.reference_formatting_page"


@reference_formatting_bp.route("/reference-formatting")
def reference_formatting_page():
    """Страница загрузки XML файла со списком литературы."""
    return render_template("reference_formatting.html")


@reference_formatting_bp.route("/reference-formatting/process", methods=["POST"])
def process_reference_formatting():
    """Обработка XML файла: форматирование списка литературы."""
    file, err = require_xml_upload(redirect_endpoint=_PAGE)
    if err is not None:
        return err

    temp_path, original_filename = save_uploaded_xml(file)
    try:
        result = format_structure(temp_path)
        if not result["success"]:
            flash(
                f"Ошибка при обработке файла: {result.get('error', 'Неизвестная ошибка')}",
                "error",
            )
            return _redirect_cleanup(temp_path)

        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass

        record_operation(
            tool="bibliography",
            title="Форматирование библиографии",
            status="ok",
            detail=f"{original_filename}: изменено {result.get('processed_count', 0)}",
            url=url_for(_PAGE),
        )
        return render_template(
            "reference_formatting_result.html",
            result=result,
            original_filename=original_filename,
            output_filename=result["output_path"].name,
            samples=result.get("samples") or [],
        )
    except Exception as e:
        logger.error("Ошибка при обработке XML: %s", e, exc_info=True)
        flash(f"Ошибка при обработке файла: {e}", "error")
        return _redirect_cleanup(temp_path)


def _redirect_cleanup(temp_path):
    from flask import redirect

    try:
        if temp_path.exists():
            temp_path.unlink()
    except OSError:
        pass
    return redirect(url_for(_PAGE))


@reference_formatting_bp.route("/reference-formatting/download/<filename>")
def download_formatted_file(filename: str):
    """Скачивание отформатированного XML файла."""
    return download_xml_and_delete(
        filename,
        on_missing_redirect=_PAGE,
        fallback_name="formatted",
    )
