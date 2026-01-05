"""Роуты для обработки XML файлов: удаление нумерации источников."""

from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from pathlib import Path
from ipsas.modules.reference_processor import remove_reference_numbering
from ipsas.config.settings import get_settings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

# Создание Blueprint для обработки источников
reference_processing_bp = Blueprint("reference_processing", __name__, template_folder="templates")


@reference_processing_bp.route("/reference-processing")
@login_required
def reference_processing_page():
    """Страница обработки XML файлов: удаление нумерации источников."""
    return render_template("reference_processing.html")


@reference_processing_bp.route("/reference-processing/process", methods=["POST"])
@login_required
def process_references():
    """Обработка загруженного XML файла: удаление нумерации из источников."""
    settings = get_settings()
    
    # Проверка наличия файла
    if "xml_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("reference_processing.reference_processing_page"))
    
    file = request.files["xml_file"]
    
    if file.filename == "":
        flash("Файл не выбран", "error")
        return redirect(url_for("reference_processing.reference_processing_page"))
    
    # Проверка расширения
    if not file.filename.lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return redirect(url_for("reference_processing.reference_processing_page"))
    
    # Сохранение файла во временную директорию
    filename = secure_filename(file.filename)
    temp_path = settings.temp_dir / filename
    
    try:
        file.save(str(temp_path))
        
        # Обработка файла
        result = remove_reference_numbering(temp_path)
        
        if not result["success"]:
            flash(f"Ошибка при обработке файла: {result['error']}", "error")
            # Удаление временного файла
            try:
                temp_path.unlink()
            except:
                pass
            return redirect(url_for("reference_processing.reference_processing_page"))
        
        # Удаляем исходный временный файл после успешной обработки
        try:
            temp_path.unlink()
        except:
            pass
        
        # Отображение результатов
        return render_template(
            "reference_processing_result.html",
            result=result,
            filename=filename,
            processed_count=result["processed_count"]
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке XML: {e}")
        flash(f"Ошибка при обработке файла: {str(e)}", "error")
        
        # Удаление временного файла в случае ошибки
        try:
            if temp_path.exists():
                temp_path.unlink()
        except:
            pass
        
        return redirect(url_for("reference_processing.reference_processing_page"))


@reference_processing_bp.route("/reference-processing/download/<filename>")
@login_required
def download_processed_file(filename):
    """Скачивание обработанного файла."""
    settings = get_settings()
    file_path = settings.temp_dir / filename
    
    if not file_path.exists():
        flash("Файл не найден", "error")
        return redirect(url_for("reference_processing.reference_processing_page"))
    
    return send_file(
        str(file_path),
        as_attachment=True,
        download_name=filename
    )

