"""Роуты для обработки XML файлов: удаление нумерации источников."""

import uuid
from datetime import datetime, timedelta
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
    
    # Сохранение файла во временную директорию с уникальным именем
    original_filename = secure_filename(file.filename)
    # Добавляем UUID для уникальности и timestamp для отслеживания
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{unique_id}_{original_filename}"
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
        
        # Отображение результатов (используем оригинальное имя для отображения)
        return render_template(
            "reference_processing_result.html",
            result=result,
            filename=original_filename,
            processed_filename=result["output_path"].name if result["output_path"] else None,
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
    
    # Определяем оригинальное имя для скачивания
    # Формат файла: timestamp_uuid_originalname_processed.xml
    # Нужно получить originalname_processed.xml или просто originalname.xml
    if "_processed" in filename:
        # Убираем timestamp_uuid_ из начала
        # Находим позицию после второго подчеркивания
        parts = filename.split("_", 2)
        if len(parts) >= 3:
            # parts[2] содержит originalname_processed.xml
            original_name = parts[2]
        else:
            original_name = filename
    else:
        # Если нет _processed, убираем только timestamp_uuid_
        parts = filename.split("_", 2)
        if len(parts) >= 3:
            original_name = parts[2]
        else:
            original_name = filename
    
    # Отправляем файл и удаляем его после скачивания
    try:
        # Используем генератор для удаления файла после отправки
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
                        logger.info(f"Удален файл после скачивания: {file_path.name}")
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
        return redirect(url_for("reference_processing.reference_processing_page"))


@reference_processing_bp.route("/reference-processing/cleanup")
@login_required
def cleanup_old_files():
    """Очистка старых файлов из временной директории (старше 24 часов)."""
    if not current_user.is_admin:
        flash("Доступ запрещен", "error")
        return redirect(url_for("main.dashboard"))
    
    settings = get_settings()
    temp_dir = settings.temp_dir
    
    if not temp_dir.exists():
        flash("Временная директория не найдена", "error")
        return redirect(url_for("main.dashboard"))
    
    cutoff_time = datetime.now() - timedelta(hours=24)
    deleted_count = 0
    
    try:
        for file_path in temp_dir.glob("*_processed.xml"):
            try:
                # Получаем время модификации файла
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if mtime < cutoff_time:
                    file_path.unlink()
                    deleted_count += 1
                    logger.info(f"Удален старый файл: {file_path.name}")
            except Exception as e:
                logger.warning(f"Не удалось удалить файл {file_path}: {e}")
        
        flash(f"Очищено файлов: {deleted_count}", "success")
    except Exception as e:
        logger.error(f"Ошибка при очистке файлов: {e}")
        flash(f"Ошибка при очистке: {str(e)}", "error")
    
    return redirect(url_for("main.dashboard"))

