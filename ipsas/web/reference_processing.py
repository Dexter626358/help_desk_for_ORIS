"""Роуты для обработки XML файлов: удаление нумерации источников."""

from __future__ import annotations

from flask import Blueprint, render_template, url_for, flash

from ipsas.services.process_references import remove_numbering
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.web.file_ops import (
    download_xml_and_delete,
    require_xml_upload,
    save_uploaded_xml,
)

logger = get_logger(__name__)

reference_processing_bp = Blueprint(
    "reference_processing", __name__, template_folder="templates"
)

_PAGE = "reference_processing.reference_processing_page"


@reference_processing_bp.route("/reference-processing")
def reference_processing_page():
    """Страница обработки XML файлов: удаление нумерации источников."""
    return render_template("reference_processing.html")


@reference_processing_bp.route("/reference-processing/process", methods=["POST"])
def process_references():
    """Обработка загруженного XML файла: удаление нумерации из источников."""
    file, err = require_xml_upload(redirect_endpoint=_PAGE)
    if err is not None:
        return err

    temp_path, original_filename = save_uploaded_xml(file)
    try:
        result = remove_numbering(temp_path)
        if not result["success"]:
            flash(f"Ошибка при обработке файла: {result['error']}", "error")
            return redirect_cleanup(temp_path)

        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass

        record_operation(
            tool="bibliography",
            title="Удаление нумерации",
            status="ok",
            detail=f"{original_filename}: изменено {result['processed_count']}",
            url=url_for(_PAGE),
        )
        return render_template(
            "reference_processing_result.html",
            result=result,
            filename=original_filename,
            processed_filename=result["output_path"].name if result["output_path"] else None,
            processed_count=result["processed_count"],
            samples=result.get("samples") or [],
        )
    except Exception as e:
        logger.error("Ошибка при обработке XML: %s", e)
        flash(f"Ошибка при обработке файла: {e}", "error")
        return redirect_cleanup(temp_path)


def redirect_cleanup(temp_path):
    from flask import redirect

    try:
        if temp_path.exists():
            temp_path.unlink()
    except OSError:
        pass
    return redirect(url_for(_PAGE))


@reference_processing_bp.route("/reference-processing/download/<filename>")
def download_processed_file(filename: str):
    """Скачивание обработанного файла."""
    return download_xml_and_delete(
        filename,
        on_missing_redirect=_PAGE,
        fallback_name="processed",
    )
