"""Модуль для обработки XML файлов: удаление нумерации из источников литературы."""

import re
from typing import Dict, Any, Optional
from pathlib import Path
from lxml import etree
from lxml.etree import XMLSyntaxError
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def _create_strict_parser() -> etree.XMLParser:
    """
    Создает строгий парсер XML с сохранением структуры.
    
    Returns:
        Настроенный XMLParser
    """
    return etree.XMLParser(
        recover=False,
        remove_blank_text=False,
        resolve_entities=False,
        huge_tree=True,
    )


def remove_reference_numbering(xml_path: Path) -> Dict[str, Any]:
    """
    Удаляет нумерацию из элементов <reference> в XML файле.
    
    Нумерация определяется как цифры с точкой в начале текста элемента,
    например: "1. АНТИПОВ..." -> "АНТИПОВ..."
    
    Args:
        xml_path: Путь к XML файлу
        
    Returns:
        Словарь с результатами обработки:
        - success: bool - успешность обработки
        - processed_count: int - количество обработанных элементов
        - output_path: Optional[Path] - путь к обработанному файлу
        - error: Optional[str] - сообщение об ошибке
    """
    result = {
        "success": False,
        "processed_count": 0,
        "output_path": None,
        "error": None
    }
    
    if not xml_path.exists():
        result["error"] = f"Файл не найден: {xml_path}"
        return result
    
    try:
        # Парсим XML
        parser = _create_strict_parser()
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()
        
        # Находим все элементы <reference>
        references = root.findall(".//reference")
        
        if not references:
            logger.warning(f"Элементы <reference> не найдены в файле: {xml_path}")
            result["success"] = True
            result["processed_count"] = 0
            return result
        
        # Паттерн для поиска нумерации: цифры с точкой в начале текста
        # Пример: "1. ", "12. ", "123. " и т.д.
        numbering_pattern = re.compile(r'^\s*\d+\.\s+')
        
        processed = 0
        for ref in references:
            if ref.text:
                # Проверяем, есть ли нумерация в начале текста
                if numbering_pattern.match(ref.text):
                    # Удаляем нумерацию
                    ref.text = numbering_pattern.sub('', ref.text)
                    processed += 1
                    logger.debug(f"Удалена нумерация из reference: {ref.text[:50]}...")
        
        # Сохраняем обработанный файл во временную директорию
        from ipsas.config.settings import get_settings
        settings = get_settings()
        output_path = settings.temp_dir / f"{xml_path.stem}_processed{xml_path.suffix}"
        
        # Сохраняем с красивым форматированием
        tree.write(
            str(output_path),
            encoding='utf-8',
            xml_declaration=True,
            pretty_print=True
        )
        
        result["success"] = True
        result["processed_count"] = processed
        result["output_path"] = output_path
        
        logger.info(
            f"Обработка завершена: обработано {processed} из {len(references)} элементов reference"
        )
        
        return result
        
    except XMLSyntaxError as e:
        error_msg = f"Ошибка синтаксиса XML: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
        return result
    except Exception as e:
        error_msg = f"Неожиданная ошибка при обработке файла: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
        return result

