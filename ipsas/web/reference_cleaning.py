"""Роуты для очистки <references>/<reference>: нормализация refinfo/text и удаление нумерации."""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for
from flask_login import login_required
from lxml import etree
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.modules.reference_cleaner import clean_references_with_stats
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

reference_cleaning_bp = Blueprint("reference_cleaning", __name__, template_folder="templates")


def _create_strict_parser() -> etree.XMLParser:
    return etree.XMLParser(
        recover=False,
        remove_blank_text=False,
        resolve_entities=False,
        huge_tree=True,
    )


@reference_cleaning_bp.route("/reference-cleaning")
@login_required
def reference_cleaning_page():
    return render_template("reference_cleaning.html")


def _build_temp_filename(original_filename: str, suffix: str) -> str:
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = secure_filename(original_filename) or "input.xml"
    stem = Path(safe).stem
    return f"{timestamp}_{unique_id}_{stem}{suffix}.xml"


@reference_cleaning_bp.route("/reference-cleaning/process", methods=["POST"])
@login_required
def process_reference_cleaning():
    settings = get_settings()

    if "xml_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("reference_cleaning.reference_cleaning_page"))

    file = request.files["xml_file"]
    if not file or not file.filename:
        flash("Файл не выбран", "error")
        return redirect(url_for("reference_cleaning.reference_cleaning_page"))

    if not file.filename.lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return redirect(url_for("reference_cleaning.reference_cleaning_page"))

    original_filename = secure_filename(file.filename)
    input_name = _build_temp_filename(original_filename, suffix="")
    input_path = settings.temp_dir / input_name
    output_name = _build_temp_filename(original_filename, suffix="_references_cleaned")
    output_path = settings.temp_dir / output_name

    try:
        file.save(str(input_path))

        parser = _create_strict_parser()
        tree = etree.parse(str(input_path), parser)
        _, stats = clean_references_with_stats(tree)
        tree.write(
            str(output_path),
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=False,
        )

        try:
            input_path.unlink()
        except Exception:
            pass

        return render_template(
            "reference_cleaning_result.html",
            filename=original_filename,
            processed_filename=output_path.name,
            stats=stats,
        )
    except etree.XMLSyntaxError as e:
        logger.error(f"Ошибка синтаксиса XML: {e}")
        flash(f"Ошибка синтаксиса XML: {e}", "error")
        try:
            if input_path.exists():
                input_path.unlink()
        except Exception:
            pass
        return redirect(url_for("reference_cleaning.reference_cleaning_page"))
    except Exception as e:
        logger.error(f"Ошибка при очистке references: {e}", exc_info=True)
        flash(f"Ошибка при обработке файла: {e}", "error")
        try:
            if input_path.exists():
                input_path.unlink()
        except Exception:
            pass
        try:
            if output_path.exists():
                output_path.unlink()
        except Exception:
            pass
        return redirect(url_for("reference_cleaning.reference_cleaning_page"))


@reference_cleaning_bp.route("/reference-cleaning/download/<filename>")
@login_required
def download_reference_cleaned_file(filename: str) -> Response:
    settings = get_settings()
    file_path = settings.temp_dir / filename

    if not file_path.exists():
        flash("Файл не найден", "error")
        return redirect(url_for("reference_cleaning.reference_cleaning_page"))

    # Отдаём как attachment и удаляем после отправки
    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"Удален файл после скачивания: {file_path.name}")
            except Exception as e:
                logger.warning(f"Не удалось удалить файл {file_path.name}: {e}")

    download_name: Optional[str] = None
    parts = filename.split("_", 2)
    if len(parts) >= 3:
        download_name = parts[2]
    else:
        download_name = filename

    return Response(
        generate(),
        mimetype="application/xml",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )

