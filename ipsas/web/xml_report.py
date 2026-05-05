"""Роуты для анализа XML и генерации HTML-отчёта."""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for
from flask_login import login_required
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.modules.xml_report_generator import generate_xml_html_report
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

xml_report_bp = Blueprint("xml_report", __name__, template_folder="templates")


@xml_report_bp.route("/xml-report")
@login_required
def xml_report_page():
    """Страница генерации HTML-отчёта по XML."""
    return render_template("xml_report.html")


@xml_report_bp.route("/xml-report/generate", methods=["POST"])
@login_required
def generate_report():
    """Принять XML, сгенерировать HTML-отчёт и показать страницу результата."""
    settings = get_settings()

    if "xml_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("xml_report.xml_report_page"))

    file = request.files["xml_file"]
    if file.filename == "":
        flash("Файл не выбран", "error")
        return redirect(url_for("xml_report.xml_report_page"))

    if not file.filename.lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return redirect(url_for("xml_report.xml_report_page"))

    original_filename = secure_filename(file.filename)
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    xml_temp_name = f"{timestamp}_{unique_id}_{original_filename}"
    xml_temp_path = settings.temp_dir / xml_temp_name

    report_temp_name = f"{timestamp}_{unique_id}_{Path(original_filename).stem}_report.html"
    report_temp_path = settings.temp_dir / report_temp_name

    try:
        file.save(str(xml_temp_path))

        # Контроль размера после сохранения (надёжнее, чем request.content_length для multipart).
        try:
            size = xml_temp_path.stat().st_size
        except OSError:
            size = None

        if size is not None and size > settings.max_file_size:
            try:
                xml_temp_path.unlink()
            except Exception:
                pass
            max_mb = settings.max_file_size / (1024 * 1024)
            flash(f"Файл слишком большой. Максимальный размер: {max_mb:.1f} MB", "error")
            return redirect(url_for("xml_report.xml_report_page"))

        generate_xml_html_report(xml_temp_path, report_temp_path)

        # Исходный XML больше не нужен.
        try:
            if xml_temp_path.exists():
                xml_temp_path.unlink()
        except Exception as e:
            logger.warning("Не удалось удалить временный XML %s: %s", xml_temp_path.name, e)

        return render_template(
            "xml_report_result.html",
            filename=original_filename,
            report_filename=report_temp_path.name,
        )

    except Exception as e:
        logger.error("Ошибка при генерации HTML-отчёта: %s", e)
        flash(f"Ошибка при обработке файла: {str(e)}", "error")

        # Чистим временные файлы в случае ошибки.
        for p in (xml_temp_path, report_temp_path):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass

        return redirect(url_for("xml_report.xml_report_page"))


@xml_report_bp.route("/xml-report/download/<filename>")
@login_required
def download_report(filename: str):
    """Скачать HTML-отчёт и удалить его после скачивания."""
    settings = get_settings()
    file_path = settings.temp_dir / filename

    if not file_path.exists():
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("xml_report.xml_report_page"))

    # Попытаемся вернуть пользователю «красивое» имя.
    download_name = filename
    parts = filename.split("_", 2)
    if len(parts) >= 3:
        # parts[2] содержит <original_stem>_report.html
        download_name = parts[2]
        if not download_name.lower().endswith(".html"):
            download_name = f"{download_name}.html"

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info("Удалён HTML-отчёт после скачивания: %s", file_path.name)
            except Exception as e:
                logger.warning("Не удалось удалить HTML-отчёт %s: %s", file_path.name, e)

    return Response(
        generate(),
        mimetype="text/html",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )

