"""Модуль для обработки XML файлов: удаление нумерации из источников литературы."""

import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from lxml import etree
from lxml.etree import XMLSyntaxError

from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def _create_strict_parser() -> etree.XMLParser:
    """Создает строгий XML parser с сохранением структуры."""
    return etree.XMLParser(
        recover=False,
        remove_blank_text=False,
        resolve_entities=False,
        huge_tree=True,
    )


def _strip_numbering(text: Optional[str], pattern: re.Pattern[str]) -> Tuple[Optional[str], bool]:
    """Удаляет префикс вида `1. ` в начале строки."""
    if not text:
        return text, False
    if not pattern.match(text):
        return text, False
    return pattern.sub("", text), True


def remove_reference_numbering(xml_path: Path) -> Dict[str, Any]:
    """
    Удаляет нумерацию из ссылок в XML файле.

    Поддерживаемые форматы:
    1) `<reference>1. ...</reference>`
    2) `<reference><refInfo ...><text>1. ...</text></refInfo></reference>`
    """
    result = {
        "success": False,
        "processed_count": 0,
        "output_path": None,
        "error": None,
    }

    if not xml_path.exists():
        result["error"] = f"Файл не найден: {xml_path}"
        return result

    try:
        parser = _create_strict_parser()
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()

        references = root.findall(".//reference")
        if not references:
            logger.warning(f"Элементы <reference> не найдены в файле: {xml_path}")
            result["success"] = True
            result["processed_count"] = 0
            return result

        numbering_pattern = re.compile(r"^\s*\d+\.\s+")

        processed = 0
        for ref in references:
            # Формат 1: текст прямо в <reference>
            updated_text, changed = _strip_numbering(ref.text, numbering_pattern)
            if changed:
                ref.text = updated_text
                processed += 1
                logger.debug(f"Удалена нумерация из reference: {(ref.text or '')[:50]}...")
                continue

            # Формат 2: текст в <reference>/<refInfo>/<text>
            text_elem = ref.find("refInfo/text")
            if text_elem is not None:
                updated_text, changed = _strip_numbering(text_elem.text, numbering_pattern)
                if changed:
                    text_elem.text = updated_text
                    processed += 1
                    logger.debug(
                        "Удалена нумерация из reference/refInfo/text: "
                        f"{(text_elem.text or '')[:50]}..."
                    )

        from ipsas.config.settings import get_settings

        settings = get_settings()
        output_path = settings.temp_dir / f"{xml_path.stem}_processed{xml_path.suffix}"

        tree.write(
            str(output_path),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )

        result["success"] = True
        result["processed_count"] = processed
        result["output_path"] = output_path

        logger.info(
            f"Обработка завершена: обработано {processed} из {len(references)} элементов reference"
        )
        return result

    except XMLSyntaxError as e:
        error_msg = f"Ошибка синтаксиса XML: {e}"
        logger.error(error_msg)
        result["error"] = error_msg
        return result
    except Exception as e:
        error_msg = f"Неожиданная ошибка при обработке файла: {e}"
        logger.error(error_msg)
        result["error"] = error_msg
        return result
