"""Роуты для форматирования списков литературы в XML."""

import uuid
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from pathlib import Path
from ipsas.modules.reference_formatter import ReferenceFormatter
from ipsas.config.settings import get_settings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

# Создание Blueprint для форматирования списков литературы
reference_formatting_bp = Blueprint("reference_formatting", __name__, template_folder="templates")


@reference_formatting_bp.route("/reference-formatting")
@login_required
def reference_formatting_page():
    """Страница загрузки XML файла со списком литературы."""
    return render_template("reference_formatting.html")


@reference_formatting_bp.route("/reference-formatting/process", methods=["POST"])
@login_required
def process_reference_formatting():
    """Обработка XML файла: форматирование списка литературы."""
    settings = get_settings()
    
    # Проверка наличия файла
    if "xml_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("reference_formatting.reference_formatting_page"))
    
    file = request.files["xml_file"]
    
    if file.filename == "":
        flash("Файл не выбран", "error")
        return redirect(url_for("reference_formatting.reference_formatting_page"))
    
    # Проверка расширения
    if not file.filename.lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return redirect(url_for("reference_formatting.reference_formatting_page"))
    
    # Сохранение файла во временную директорию с уникальным именем
    original_filename = secure_filename(file.filename)
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{unique_id}_{original_filename}"
    temp_path = settings.temp_dir / filename
    
    try:
        # Сохраняем XML файл
        file.save(str(temp_path))
        
        # Обработка файла
        formatter = ReferenceFormatter()
        result = formatter.format_references(temp_path)
        
        if not result["success"]:
            flash(f"Ошибка при обработке файла: {result.get('error', 'Неизвестная ошибка')}", "error")
            # Удаление временного файла
            try:
                temp_path.unlink()
            except:
                pass
            return redirect(url_for("reference_formatting.reference_formatting_page"))
        
        # Удаляем исходный временный файл после успешной обработки
        try:
            temp_path.unlink()
        except:
            pass
        
        # Отображение результатов
        return render_template(
            "reference_formatting_result.html",
            result=result,
            original_filename=original_filename,
            output_filename=result['output_path'].name
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке XML: {e}", exc_info=True)
        flash(f"Ошибка при обработке файла: {str(e)}", "error")
        
        # Удаление временного файла в случае ошибки
        try:
            if temp_path.exists():
                temp_path.unlink()
        except:
            pass
        
        return redirect(url_for("reference_formatting.reference_formatting_page"))


@reference_formatting_bp.route("/reference-formatting/download/<filename>")
@login_required
def download_formatted_file(filename):
    """Скачивание отформатированного XML файла."""
    settings = get_settings()
    
    # Ищем файл в temp_dir
    file_path = settings.temp_dir / filename
    
    if not file_path.exists():
        flash("Файл не найден", "error")
        return redirect(url_for("reference_formatting.reference_formatting_page"))
    
    # Определяем оригинальное имя для скачивания
    if "_formatted.xml" in filename:
        original_name = filename.replace("_formatted.xml", ".xml")
    else:
        # Убираем timestamp и UUID из начала имени
        parts = filename.split("_", 2)
        if len(parts) >= 3:
            original_name = parts[2]
        else:
            original_name = filename
    
    # Отправляем файл и удаляем его после скачивания
    try:
        def generate():
            try:
                with open(file_path, 'rb') as f:
                    data = f.read()
                    yield data
            finally:
                # Удаляем файл после чтения
                try:
                    if file_path.exists():
                        file_path.unlink()
                        logger.info(f"Удален отформатированный файл: {file_path.name}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить файл {file_path}: {e}")
        
        from flask import Response
        return Response(
            generate(),
            mimetype='application/xml',
            headers={
                'Content-Disposition': f'attachment; filename="{original_name}"'
            }
        )
    except Exception as e:
        logger.error(f"Ошибка при скачивании файла: {e}")
        flash("Ошибка при скачивании файла", "error")
        return redirect(url_for("reference_formatting.reference_formatting_page"))
