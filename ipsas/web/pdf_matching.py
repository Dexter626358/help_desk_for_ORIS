"""Роуты для добавления PDF файлов в XML."""

import uuid
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from pathlib import Path
from ipsas.modules.pdf_matcher import PDFMatcher
from ipsas.config.settings import get_settings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

# Создание Blueprint для добавления PDF файлов
pdf_matching_bp = Blueprint("pdf_matching", __name__, template_folder="templates")


@pdf_matching_bp.route("/pdf-matching")
@login_required
def pdf_matching_page():
    """Страница загрузки ZIP архива с XML и PDF файлами."""
    return render_template("pdf_matching.html")


@pdf_matching_bp.route("/pdf-matching/process", methods=["POST"])
@login_required
def process_pdf_matching():
    """Обработка ZIP архива: сопоставление PDF файлов со статьями."""
    settings = get_settings()
    
    # Проверка наличия файла
    if "zip_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("pdf_matching.pdf_matching_page"))
    
    file = request.files["zip_file"]
    
    if file.filename == "":
        flash("Файл не выбран", "error")
        return redirect(url_for("pdf_matching.pdf_matching_page"))
    
    # Проверка расширения
    if not file.filename.lower().endswith(".zip"):
        flash("Поддерживаются только ZIP архивы", "error")
        return redirect(url_for("pdf_matching.pdf_matching_page"))
    
    # Сохранение файла во временную директорию с уникальным именем
    original_filename = secure_filename(file.filename)
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{unique_id}_{original_filename}"
    temp_path = settings.temp_dir / filename
    
    # Создаем уникальную директорию для извлечения
    extract_dir = settings.temp_dir / f"{timestamp}_{unique_id}_extract"
    
    try:
        # Сохраняем ZIP файл
        file.save(str(temp_path))
        
        # Обработка архива
        matcher = PDFMatcher()
        result = matcher.process_zip(temp_path, extract_dir)
        
        # Удаляем исходный ZIP файл
        try:
            temp_path.unlink()
        except Exception as e:
            logger.warning(f"Не удалось удалить исходный ZIP файл: {e}")
        
        # Отображение результатов
        return render_template(
            "pdf_matching_result.html",
            result=result,
            original_filename=original_filename,
            output_xml_filename=result['output_xml'].name
        )
        
    except ValueError as e:
        logger.error(f"Ошибка валидации при обработке ZIP: {e}")
        flash(f"Ошибка при обработке архива: {str(e)}", "error")
        
        # Очистка временных файлов
        try:
            if temp_path.exists():
                temp_path.unlink()
            if extract_dir.exists():
                import shutil
                shutil.rmtree(extract_dir)
        except Exception:
            pass
        
        return redirect(url_for("pdf_matching.pdf_matching_page"))
        
    except Exception as e:
        logger.error(f"Ошибка при обработке ZIP архива: {e}", exc_info=True)
        flash(f"Ошибка при обработке архива: {str(e)}", "error")
        
        # Очистка временных файлов
        try:
            if temp_path.exists():
                temp_path.unlink()
            if extract_dir.exists():
                import shutil
                shutil.rmtree(extract_dir)
        except Exception:
            pass
        
        return redirect(url_for("pdf_matching.pdf_matching_page"))


@pdf_matching_bp.route("/pdf-matching/download/<filename>")
@login_required
def download_processed_xml(filename):
    """Скачивание обработанного XML файла."""
    settings = get_settings()
    
    # Ищем файл в temp_dir и поддиректориях
    file_path = None
    for path in settings.temp_dir.rglob(filename):
        if path.is_file() and path.suffix == '.xml':
            file_path = path
            break
    
    if not file_path or not file_path.exists():
        flash("Файл не найден", "error")
        return redirect(url_for("pdf_matching.pdf_matching_page"))
    
    # Определяем оригинальное имя для скачивания
    if "_processed.xml" in filename:
        original_name = filename.replace("_processed.xml", ".xml")
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
                # Удаляем файл и директорию извлечения после чтения
                try:
                    if file_path.exists():
                        file_path.unlink()
                    # Удаляем директорию извлечения, если она существует
                    extract_dir = file_path.parent
                    if extract_dir.exists() and extract_dir.is_dir():
                        import shutil
                        shutil.rmtree(extract_dir)
                    logger.info(f"Удален обработанный XML файл: {file_path.name}")
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
        return redirect(url_for("pdf_matching.pdf_matching_page"))
