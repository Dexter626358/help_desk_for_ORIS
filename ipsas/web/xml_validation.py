"""Роуты для валидации XML файлов."""

from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from pathlib import Path
import os
from ipsas.modules.xml_validator import XMLValidator
from ipsas.config.settings import get_settings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

# Создание Blueprint для валидации XML
xml_validation_bp = Blueprint("xml_validation", __name__, template_folder="templates")


@xml_validation_bp.route("/xml-validator")
@login_required
def xml_validator_page():
    """Страница валидации XML файлов."""
    settings = get_settings()
    
    # Ищем XSD схемы в директории schemas
    schemas_dir = settings.schemas_dir
    schemas = []
    if schemas_dir.exists():
        schemas = [f.name for f in schemas_dir.glob("*.xsd")]
    
    return render_template("xml_validator.html", schemas=schemas)


@xml_validation_bp.route("/xml-validator/validate", methods=["POST"])
@login_required
def validate_xml():
    """Валидация загруженного XML файла."""
    settings = get_settings()
    
    # Проверка наличия файла
    if "xml_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))
    
    file = request.files["xml_file"]
    
    if file.filename == "":
        flash("Файл не выбран", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))
    
    # Проверка расширения
    if not file.filename.lower().endswith(".xml"):
        flash("Поддерживаются только XML файлы", "error")
        return redirect(url_for("xml_validation.xml_validator_page"))
    
    # Сохранение файла во временную директорию
    filename = secure_filename(file.filename)
    temp_path = settings.temp_dir / filename
    
    try:
        file.save(str(temp_path))
        
        # Получение выбранной схемы
        schema_name = request.form.get("schema", "")
        schema_paths = []
        
        if schema_name:
            # Если выбрана схема
            schema_path = settings.schemas_dir / schema_name
            if not schema_path.exists():
                flash(f"Схема {schema_name} не найдена", "error")
                return redirect(url_for("xml_validation.xml_validator_page"))
            schema_paths = [schema_path]
        
        # Инициализация валидатора
        validator = XMLValidator()
        
        # Валидация файла
        if len(schema_paths) == 1:
            # Проверка по одной схеме
            validator.load_schema(schema_paths[0])
            result = validator.validate_xml_file(temp_path)
            schema_name = schema_paths[0].name
        else:
            # Проверка только синтаксиса XML
            result = validator.validate_xml_file(temp_path)
            schema_name = "Не указана"
        
        # Удаление временного файла
        try:
            temp_path.unlink()
        except:
            pass
        
        # Отображение результатов
        return render_template(
            "xml_validation_result.html",
            result=result,
            filename=filename,
            schema_name=schema_name if schema_name else "Не указана"
        )
        
    except Exception as e:
        logger.error(f"Ошибка при валидации XML: {e}")
        flash(f"Ошибка при обработке файла: {str(e)}", "error")
        
        # Удаление временного файла в случае ошибки
        try:
            if temp_path.exists():
                temp_path.unlink()
        except:
            pass
        
        return redirect(url_for("xml_validation.xml_validator_page"))

