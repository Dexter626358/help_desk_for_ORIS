"""Роуты для очистки <references>/<reference>: нормализация refinfo/text и удаление нумерации."""

from __future__ import annotations

from flask import Blueprint, Response, flash, redirect, render_template, url_for
from lxml import etree

from ipsas.config.settings import get_settings
from ipsas.services.process_references import clean_tree
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.web.file_ops import (
    build_temp_name,
    download_xml_and_delete,
    require_xml_upload,
    save_uploaded_xml,
)

logger = get_logger(__name__)

reference_cleaning_bp = Blueprint(
    "reference_cleaning", __name__, template_folder="templates"
)

_PAGE = "reference_cleaning.reference_cleaning_page"


@reference_cleaning_bp.route("/reference-cleaning")
def reference_cleaning_page():
    return render_template("reference_cleaning.html")


@reference_cleaning_bp.route("/reference-cleaning/process", methods=["POST"])
def process_reference_cleaning():
    settings = get_settings()
    file, err = require_xml_upload(redirect_endpoint=_PAGE)
    if err is not None:
        return err

    input_path, original_filename = save_uploaded_xml(file)
    output_name = build_temp_name(original_filename, suffix="_references_cleaned")
    output_path = settings.temp_dir / output_name

    try:
        stats, samples = clean_tree(input_path, output_path)
        try:
            input_path.unlink(missing_ok=True)
        except OSError:
            pass

        record_operation(
            tool="bibliography",
            title="Очистка библиографии",
            status="ok",
            detail=f"{original_filename}: изменено {stats.changed_references}",
            url=url_for(_PAGE),
        )
        return render_template(
            "reference_cleaning_result.html",
            filename=original_filename,
            processed_filename=output_path.name,
            stats=stats,
            samples=samples,
        )
    except etree.XMLSyntaxError as e:
        logger.error("Ошибка синтаксиса XML: %s", e)
        flash(f"Ошибка синтаксиса XML: {e}", "error")
        _unlink_quiet(input_path)
        return redirect(url_for(_PAGE))
    except Exception as e:
        logger.error("Ошибка при очистке references: %s", e, exc_info=True)
        flash(f"Ошибка при обработке файла: {e}", "error")
        _unlink_quiet(input_path)
        _unlink_quiet(output_path)
        return redirect(url_for(_PAGE))


def _unlink_quiet(path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


@reference_cleaning_bp.route("/reference-cleaning/download/<filename>")
def download_reference_cleaned_file(filename: str) -> Response:
    return download_xml_and_delete(
        filename,
        on_missing_redirect=_PAGE,
        fallback_name="references_cleaned",
    )
