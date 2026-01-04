"""Модуль для валидации XML файлов по XSD схемам."""

import re
from typing import Dict, List, Any, Optional
from pathlib import Path
from io import BytesIO
from lxml import etree
from lxml.etree import XMLSyntaxError, DocumentInvalid, XMLSchemaParseError
from ipsas.utils.logger import get_logger
from ipsas.config.settings import get_settings

logger = get_logger(__name__)


def _create_strict_parser() -> etree.XMLParser:
    """
    Создает строгий парсер XML/XSD с сохранением line numbers.
    
    Returns:
        Настроенный XMLParser
    """
    return etree.XMLParser(
        recover=False,
        remove_blank_text=False,
        resolve_entities=False,
        huge_tree=True,
    )


def _translate_error_to_russian(message: str) -> str:
    """
    Переводит типичные сообщения об ошибках валидации XSD на русский язык.
    
    Args:
        message: Сообщение об ошибке на английском
        
    Returns:
        Переведенное сообщение на русском
    """
    # Словарь переводов типичных ошибок
    translations = {
        # Общие ошибки
        "This element is not expected": "Этот элемент не ожидается",
        "Element is not expected": "Элемент не ожидается",
        "Expected is": "Ожидается",
        "Expected is one of": "Ожидается один из",
        "Expected is (": "Ожидается (",
        "is required": "обязателен",
        "is missing": "отсутствует",
        "is not a valid value": "не является допустимым значением",
        "is not a valid value of the atomic type": "не является допустимым значением атомарного типа",
        
        # Ошибки элементов
        "Element": "Элемент",
        "element": "элемент",
        "The element": "Элемент",
        "The element has invalid child element": "Элемент имеет недопустимый дочерний элемент",
        "The element has invalid content": "Элемент имеет недопустимое содержимое",
        "Missing child element": "Отсутствует дочерний элемент",
        "Missing required element": "Отсутствует обязательный элемент",
        
        # Ошибки атрибутов
        "attribute": "атрибут",
        "Attribute": "Атрибут",
        "The attribute": "Атрибут",
        "is not allowed": "не разрешен",
        "is not declared for element": "не объявлен для элемента",
        "Missing required attribute": "Отсутствует обязательный атрибут",
        
        # Ошибки типов данных
        "is not a valid value of the atomic type": "не является допустимым значением атомарного типа",
        "is not a valid value": "не является допустимым значением",
        "value is not valid": "значение недопустимо",
        "Invalid value": "Недопустимое значение",
        "The value": "Значение",
        
        # Ошибки структуры
        "The content is not valid": "Содержимое недопустимо",
        "The content model is not determinist": "Модель содержимого недетерминирована",
        "The content model is not deterministic": "Модель содержимого недетерминирована",
        "The content is incomplete": "Содержимое неполное",
        
        # Ошибки схемы
        "Schema validation error": "Ошибка валидации схемы",
        "Document is invalid": "Документ невалиден",
        "Validation error": "Ошибка валидации",
        "No matching global declaration available for the validation root": "Не найдено соответствующей глобальной декларации для корневого элемента валидации",
        "No matching global declaration": "Не найдено соответствующей глобальной декларации",
        "matching global declaration": "соответствующей глобальной декларации",
        "for the validation root": "для корневого элемента валидации",
        "validation root": "корневой элемент валидации",
        "global declaration": "глобальная декларация",
    }
    
    # Пытаемся найти точное совпадение или частичное
    original_message = message
    translated = message
    
    # Сначала применяем специальные паттерны для полных фраз (более длинные сначала)
    # "No matching global declaration available for the validation root" -> полный перевод
    translated = re.sub(
        r"No matching global declaration available for the validation root",
        "Не найдено соответствующей глобальной декларации для корневого элемента валидации",
        translated,
        flags=re.IGNORECASE
    )
    
    # "No matching global declaration" -> "Не найдено соответствующей глобальной декларации"
    translated = re.sub(
        r"No matching global declaration",
        "Не найдено соответствующей глобальной декларации",
        translated,
        flags=re.IGNORECASE
    )
    
    # "Element 'name': ..." -> "Элемент 'name': ..."
    translated = re.sub(r"Element\s+'([^']+)':", r"Элемент '\1':", translated)
    
    # "The element 'name' ..." -> "Элемент 'name' ..."
    translated = re.sub(r"The element\s+'([^']+)'", r"Элемент '\1'", translated)
    
    # "Expected is one of ( ... )" -> "Ожидается один из ( ... )"
    translated = re.sub(r"Expected is one of\s*\(", "Ожидается один из (", translated)
    
    # "Expected is ( ... )" -> "Ожидается ( ... )"
    translated = re.sub(r"Expected is\s*\(", "Ожидается (", translated)
    
    # Затем проверяем словарь переводов (сначала длинные фразы, потом короткие)
    # Сортируем по длине (от больших к меньшим), чтобы длинные фразы заменялись первыми
    sorted_translations = sorted(translations.items(), key=lambda x: len(x[0]), reverse=True)
    for eng, rus in sorted_translations:
        if eng in translated:
            translated = translated.replace(eng, rus)
    
    # Если перевод не изменился, возвращаем оригинал
    if translated == original_message:
        return message
    
    return translated


def _parse_error_log(error_log: etree._ListErrorLog) -> List[Dict[str, Any]]:
    """
    Парсит lxml error_log с line/col в список словарей.
    
    Args:
        error_log: Лог ошибок из lxml
        
    Returns:
        Список ошибок в виде словарей
    """
    errors = []
    for err in error_log:
        line = getattr(err, "line", None) or 0
        column = getattr(err, "column", None) or 0
        message = getattr(err, "message", None) or str(err)
        path = getattr(err, "path", None)
        
        # Переводим сообщение на русский
        translated_message = _translate_error_to_russian(message.strip())
        
        error_dict = {
            "line": line,
            "column": column,
            "message": translated_message,
            "element": None
        }
        
        if path:
            error_dict["path"] = path
            
        errors.append(error_dict)
    
    return errors


class XMLValidationError:
    """Класс для представления ошибки валидации XML."""

    def __init__(self, line: int, column: int, message: str, element: Optional[str] = None):
        """
        Инициализация ошибки валидации.

        Args:
            line: Номер строки
            column: Номер колонки
            message: Сообщение об ошибке
            element: Элемент, в котором обнаружена ошибка
        """
        self.line = line
        self.column = column
        self.message = message
        self.element = element

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь."""
        return {
            "line": self.line,
            "column": self.column,
            "message": self.message,
            "element": self.element
        }

    def __str__(self) -> str:
        element_info = f" (элемент: {self.element})" if self.element else ""
        return f"Строка {self.line}, колонка {self.column}{element_info}: {self.message}"


class XMLValidator:
    """Класс для валидации XML файлов по XSD схемам."""

    def __init__(self, schema_path: Optional[Path] = None):
        """
        Инициализация валидатора.

        Args:
            schema_path: Путь к XSD схеме (опционально)
        """
        self.logger = logger
        self.schema_path = schema_path
        self.schema = None
        self.schema_parser = None

        if schema_path and schema_path.exists():
            self.load_schema(schema_path)

    def load_schema(self, schema_path: Path) -> bool:
        """
        Загрузка XSD схемы с проверкой:
        1) XSD на корректность синтаксиса (XMLSyntaxError)
        2) XSD на корректность как схемы (XMLSchemaParseError / компиляция)

        Args:
            schema_path: Путь к XSD схеме

        Returns:
            True если схема загружена успешно
        """
        if not schema_path.exists():
            self.logger.error(f"XSD файл не найден: {schema_path}")
            return False
        
        # (1) XSD синтаксис (это XML)
        try:
            parser = _create_strict_parser()
            schema_doc = etree.parse(str(schema_path), parser)
        except XMLSyntaxError as e:
            self.logger.error(f"[XSD:СИНТАКСИС] Ошибка разбора XSD как XML: {schema_path}")
            if hasattr(e, "error_log"):
                errors = _parse_error_log(e.error_log)
                for err in errors:
                    self.logger.error(f"  Строка {err['line']}, колонка {err['column']}: {err['message']}")
            else:
                self.logger.error(f"  {e}")
            return False
        except OSError as e:
            self.logger.error(f"[XSD:ФАЙЛ] Не удалось открыть XSD: {schema_path}\n  {e}")
            return False

        # (2) XSD компиляция (это уже "валидная схема")
        try:
            self.schema = etree.XMLSchema(schema_doc)
            self.schema_parser = etree.XMLParser(schema=self.schema)
            self.schema_path = schema_path
            self.logger.info(f"XSD схема загружена: {schema_path}")
            return True
        except XMLSchemaParseError as e:
            self.logger.error(
                f"[XSD:СХЕМА] XSD синтаксически XML-корректна, но НЕ компилируется как XSD: {schema_path}"
            )
            if hasattr(e, "error_log"):
                errors = _parse_error_log(e.error_log)
                for err in errors:
                    self.logger.error(f"  Строка {err['line']}, колонка {err['column']}: {err['message']}")
            else:
                self.logger.error(f"  {e}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при загрузке XSD схемы: {e}")
            return False

    def validate_xml_file(self, xml_path: Path) -> Dict[str, Any]:
        """
        Валидация XML файла.

        Args:
            xml_path: Путь к XML файлу

        Returns:
            Словарь с результатами валидации
        """
        result = {
            "valid": False,
            "file_path": str(xml_path),
            "errors": [],
            "warnings": [],
            "schema_loaded": self.schema is not None
        }

        if not xml_path.exists():
            result["errors"].append({
                "line": 0,
                "column": 0,
                "message": f"Файл не найден: {xml_path}",
                "element": None
            })
            return result

        try:
            # Сначала проверяем, что файл является валидным XML
            try:
                parser = _create_strict_parser()
                xml_doc = etree.parse(str(xml_path), parser)
            except XMLSyntaxError as e:
                # Парсим ошибки синтаксиса XML через error_log
                self.logger.error(f"[XML:СИНТАКСИС] Ошибка разбора XML: {xml_path}")
                if hasattr(e, "error_log"):
                    errors = _parse_error_log(e.error_log)
                    result["errors"].extend(errors)
                else:
                    # Fallback на старый способ
                    error_msg = str(e.msg) if hasattr(e, 'msg') else str(e)
                    translated_msg = _translate_error_to_russian(error_msg)
                    error = XMLValidationError(
                        e.lineno if hasattr(e, 'lineno') and e.lineno else 0,
                        e.offset if hasattr(e, 'offset') and e.offset else 0,
                        translated_msg,
                        None
                    )
                    result["errors"].append(error.to_dict())
                return result
            except OSError as e:
                result["errors"].append({
                    "line": 0,
                    "column": 0,
                    "message": f"[XML:ФАЙЛ] Не удалось открыть XML: {xml_path}\n  {e}",
                    "element": None
                })
                return result
            except Exception as e:
                result["errors"].append({
                    "line": 0,
                    "column": 0,
                    "message": f"Ошибка чтения XML файла: {str(e)}",
                    "element": None
                })
                return result

            # Если схема загружена, валидируем по схеме
            if self.schema:
                is_valid = self.schema.validate(xml_doc)
                if is_valid:
                    result["valid"] = True
                    self.logger.info(f"[OK] XSD корректна, XML соответствует схеме: {xml_path.name}")
                else:
                    # Обрабатываем ошибки валидации по схеме через error_log
                    result["valid"] = False
                    self.logger.warning(f"[INVALID] XML НЕ соответствует XSD: {xml_path.name}")
                    if hasattr(self.schema, "error_log"):
                        errors = _parse_error_log(self.schema.error_log)
                        # Дополняем информацией об элементах
                        errors = self._enrich_errors_with_elements(errors, xml_path)
                        result["errors"].extend(errors)
                    else:
                        # Fallback на старый способ
                        try:
                            self.schema.assertValid(xml_doc)
                        except DocumentInvalid as e:
                            errors = self._parse_validation_errors(e, xml_doc, xml_path)
                            result["errors"].extend(errors)
                    self.logger.warning(f"Найдено ошибок: {len(result['errors'])}")
            else:
                # Если схема не загружена, просто проверяем, что XML валиден
                result["valid"] = True
                result["warnings"].append({
                    "message": "XSD схема не загружена. Проверяется только синтаксис XML."
                })
                self.logger.info(f"XML файл синтаксически корректен (схема не загружена): {xml_path}")

        except Exception as e:
            result["errors"].append({
                "line": 0,
                "column": 0,
                "message": f"Неожиданная ошибка при валидации: {str(e)}",
                "element": None
            })
            self.logger.error(f"Ошибка валидации XML: {e}")

        return result

    def validate_xml_content(self, xml_content: bytes) -> Dict[str, Any]:
        """
        Валидация XML из памяти.

        Args:
            xml_content: Содержимое XML файла в виде bytes

        Returns:
            Словарь с результатами валидации
        """
        result = {
            "valid": False,
            "file_path": None,
            "errors": [],
            "warnings": [],
            "schema_loaded": self.schema is not None
        }

        try:
            # Парсим XML из памяти
            try:
                parser = _create_strict_parser()
                xml_doc = etree.parse(BytesIO(xml_content), parser)
            except XMLSyntaxError as e:
                if hasattr(e, "error_log"):
                    errors = _parse_error_log(e.error_log)
                    result["errors"].extend(errors)
                else:
                    error_msg = str(e.msg) if hasattr(e, 'msg') else str(e)
                    translated_msg = _translate_error_to_russian(error_msg)
                    error = XMLValidationError(
                        e.lineno if hasattr(e, 'lineno') and e.lineno else 0,
                        e.offset if hasattr(e, 'offset') and e.offset else 0,
                        translated_msg,
                        None
                    )
                    result["errors"].append(error.to_dict())
                return result
            except Exception as e:
                result["errors"].append({
                    "line": 0,
                    "column": 0,
                    "message": f"Ошибка чтения XML: {str(e)}",
                    "element": None
                })
                return result

            # Валидация по схеме
            if self.schema:
                is_valid = self.schema.validate(xml_doc)
                if is_valid:
                    result["valid"] = True
                else:
                    result["valid"] = False
                    if hasattr(self.schema, "error_log"):
                        errors = _parse_error_log(self.schema.error_log)
                        result["errors"].extend(errors)
                    else:
                        try:
                            self.schema.assertValid(xml_doc)
                        except DocumentInvalid as e:
                            errors = self._parse_validation_errors(e, xml_doc)
                            result["errors"].extend(errors)
            else:
                result["valid"] = True
                result["warnings"].append({
                    "message": "XSD схема не загружена. Проверяется только синтаксис XML."
                })

        except Exception as e:
            result["errors"].append({
                "line": 0,
                "column": 0,
                "message": f"Неожиданная ошибка: {str(e)}",
                "element": None
            })

        return result

    def _parse_validation_errors(
        self, 
        error: DocumentInvalid, 
        xml_doc: etree._ElementTree,
        xml_path: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Парсинг ошибок валидации из DocumentInvalid.

        Args:
            error: Объект DocumentInvalid с ошибками
            xml_doc: XML документ
            xml_path: Путь к XML файлу (опционально, для более точного определения элементов)

        Returns:
            Список ошибок в виде словарей
        """
        errors = []
        
        # Получаем путь к файлу для чтения содержимого
        file_path = None
        if xml_path:
            file_path = str(xml_path)
        elif hasattr(xml_doc, 'docinfo') and hasattr(xml_doc.docinfo, 'URL'):
            file_path = xml_doc.docinfo.URL
        
        # Читаем содержимое файла для определения элементов
        file_lines = []
        if file_path and Path(file_path).exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_lines = f.readlines()
            except:
                pass
        
        # Получаем все ошибки из лога валидации
        for error_obj in error.error_log:
            line = error_obj.line if hasattr(error_obj, 'line') and error_obj.line else 0
            column = error_obj.column if hasattr(error_obj, 'column') and error_obj.column else 0
            message = error_obj.message if hasattr(error_obj, 'message') else str(error_obj)
            
            # Переводим сообщение на русский
            translated_message = _translate_error_to_russian(message.strip())
            
            # Пытаемся найти элемент, в котором ошибка
            element = None
            if line > 0 and file_lines and line <= len(file_lines):
                try:
                    line_content = file_lines[line - 1].strip()
                    # Ищем открывающий тег в строке
                    if '<' in line_content:
                        # Находим первый открывающий тег
                        start = line_content.find('<')
                        if start >= 0:
                            # Пропускаем комментарии и инструкции обработки
                            if line_content[start + 1:start + 4] not in ['!--', '?xml', '!DO']:
                                end = line_content.find('>', start)
                                if end > start:
                                    tag_content = line_content[start + 1:end]
                                    # Убираем атрибуты и получаем только имя элемента
                                    element = tag_content.split()[0].split('/')[0]
                except Exception:
                    pass

            errors.append({
                "line": line,
                "column": column,
                "message": translated_message,
                "element": element
            })

        return errors

    def _enrich_errors_with_elements(
        self,
        errors: List[Dict[str, Any]],
        xml_path: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Дополняет ошибки информацией об элементах из файла.
        
        Args:
            errors: Список ошибок
            xml_path: Путь к XML файлу
            
        Returns:
            Список ошибок с дополненной информацией об элементах
        """
        if not xml_path or not xml_path.exists():
            return errors
        
        # Читаем содержимое файла для определения элементов
        file_lines = []
        try:
            with open(xml_path, 'r', encoding='utf-8') as f:
                file_lines = f.readlines()
        except Exception:
            return errors
        
        # Обогащаем ошибки информацией об элементах
        for error in errors:
            line = error.get("line", 0)
            if line > 0 and file_lines and line <= len(file_lines):
                try:
                    line_content = file_lines[line - 1].strip()
                    # Ищем открывающий тег в строке
                    if '<' in line_content:
                        start = line_content.find('<')
                        if start >= 0:
                            # Пропускаем комментарии и инструкции обработки
                            if line_content[start + 1:start + 4] not in ['!--', '?xml', '!DO']:
                                end = line_content.find('>', start)
                                if end > start:
                                    tag_content = line_content[start + 1:end]
                                    # Убираем атрибуты и получаем только имя элемента
                                    element = tag_content.split()[0].split('/')[0]
                                    error["element"] = element
                except Exception:
                    pass
        
        return errors

    def validate_xml_file_multiple_schemas(
        self, 
        xml_path: Path, 
        schema_paths: List[Path]
    ) -> Dict[str, Any]:
        """
        Валидация XML файла по нескольким схемам.

        Args:
            xml_path: Путь к XML файлу
            schema_paths: Список путей к XSD схемам

        Returns:
            Словарь с результатами валидации для каждой схемы
        """
        result = {
            "valid": False,
            "file_path": str(xml_path),
            "schemas_results": [],
            "overall_valid": False,
            "errors": [],
            "warnings": []
        }

        if not xml_path.exists():
            result["errors"].append({
                "line": 0,
                "column": 0,
                "message": f"Файл не найден: {xml_path}",
                "element": None
            })
            return result

        # Парсим XML один раз
        try:
            parser = _create_strict_parser()
            xml_doc = etree.parse(str(xml_path), parser)
        except XMLSyntaxError as e:
            self.logger.error(f"[XML:СИНТАКСИС] Ошибка разбора XML: {xml_path}")
            if hasattr(e, "error_log"):
                errors = _parse_error_log(e.error_log)
                result["errors"].extend(errors)
            else:
                error = XMLValidationError(
                    e.lineno if hasattr(e, 'lineno') and e.lineno else 0,
                    e.offset if hasattr(e, 'offset') and e.offset else 0,
                    str(e.msg) if hasattr(e, 'msg') else str(e),
                    None
                )
                result["errors"].append(error.to_dict())
            return result
        except OSError as e:
            result["errors"].append({
                "line": 0,
                "column": 0,
                "message": f"[XML:ФАЙЛ] Не удалось открыть XML: {xml_path}\n  {e}",
                "element": None
            })
            return result
        except Exception as e:
            result["errors"].append({
                "line": 0,
                "column": 0,
                "message": f"Ошибка чтения XML файла: {str(e)}",
                "element": None
            })
            return result

        # Проверяем по каждой схеме
        valid_count = 0
        all_errors = []
        
        for schema_path in schema_paths:
            if not schema_path.exists():
                all_errors.append({
                    "line": 0,
                    "column": 0,
                    "message": f"Схема не найдена: {schema_path.name}",
                    "element": None,
                    "schema": schema_path.name
                })
                continue

            try:
                # Загружаем схему с проверкой
                parser = _create_strict_parser()
                schema_doc = etree.parse(str(schema_path), parser)
                schema = etree.XMLSchema(schema_doc)

                # Валидируем
                is_valid = schema.validate(xml_doc)
                if is_valid:
                    valid_count += 1
                    self.logger.info(
                        f"[OK] XML файл валиден по схеме {schema_path.name}: {xml_path.name}"
                    )
                else:
                    # Получаем ошибки через error_log
                    self.logger.warning(
                        f"[INVALID] XML НЕ соответствует XSD {schema_path.name}: {xml_path.name}"
                    )
                    if hasattr(schema, "error_log"):
                        errors = _parse_error_log(schema.error_log)
                        # Дополняем информацией об элементах
                        errors = self._enrich_errors_with_elements(errors, xml_path)
                    else:
                        # Fallback на старый способ
                        try:
                            schema.assertValid(xml_doc)
                        except DocumentInvalid as e:
                            errors = self._parse_validation_errors(e, xml_doc, xml_path)
                    
                    # Добавляем информацию о схеме к каждой ошибке
                    for error in errors:
                        error["schema"] = schema_path.name
                    all_errors.extend(errors)
                    self.logger.warning(f"Найдено ошибок: {len(errors)}")

            except XMLSyntaxError as e:
                all_errors.append({
                    "line": 0,
                    "column": 0,
                    "message": f"[XSD:СИНТАКСИС] Ошибка разбора XSD как XML: {schema_path.name}",
                    "element": None,
                    "schema": schema_path.name
                })
                self.logger.error(f"Ошибка синтаксиса XSD {schema_path}: {e}")
            except XMLSchemaParseError as e:
                all_errors.append({
                    "line": 0,
                    "column": 0,
                    "message": f"[XSD:СХЕМА] XSD не компилируется как схема: {schema_path.name}",
                    "element": None,
                    "schema": schema_path.name
                })
                self.logger.error(f"Ошибка компиляции XSD {schema_path}: {e}")
            except Exception as e:
                all_errors.append({
                    "line": 0,
                    "column": 0,
                    "message": f"Ошибка загрузки или валидации схемы {schema_path.name}: {str(e)}",
                    "element": None,
                    "schema": schema_path.name
                })
                self.logger.error(f"Ошибка при работе со схемой {schema_path}: {e}")

        # Файл считается валидным, если он проходит ВСЕ схемы
        result["overall_valid"] = valid_count == len(schema_paths)
        result["valid"] = result["overall_valid"]
        result["errors"] = all_errors
        
        if not result["valid"]:
            result["warnings"].append({
                "message": f"Файл не прошел валидацию по {len(schema_paths) - valid_count} из {len(schema_paths)} схем"
            })

        return result

