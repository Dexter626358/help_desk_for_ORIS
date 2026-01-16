"""Модуль для форматирования списков литературы в XML."""

from pathlib import Path
from typing import Dict, Optional
from lxml import etree
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


class ReferenceFormatter:
    """Класс для форматирования списков литературы."""

    def __init__(self):
        """Инициализация форматтера."""
        pass

    def format_references(self, xml_path: Path) -> Dict:
        """
        Форматировать список литературы в XML файле.
        
        Преобразует простой формат:
        <reference>Текст ссылки</reference>
        
        В формат согласно схеме:
        <reference>
        <refInfo lang="ANY">
        <text>Текст ссылки</text>
        </refInfo>
        </reference>
        
        Args:
            xml_path: Путь к XML файлу
            
        Returns:
            Словарь с результатами обработки
        """
        try:
            # Парсим XML
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(str(xml_path), parser)
            root = tree.getroot()
            
            # Находим все элементы references
            references_elements = root.findall('.//references')
            
            if not references_elements:
                # Если нет элементов references, ищем reference напрямую
                reference_elements = root.findall('.//reference')
                if reference_elements:
                    # Создаем контейнер references, если его нет
                    # Находим родительский элемент для reference
                    if len(reference_elements) > 0:
                        parent = reference_elements[0].getparent()
                        if parent is not None:
                            # Создаем references, если его нет
                            references_elem = parent.find('references')
                            if references_elem is None:
                                references_elem = etree.Element('references')
                                # Вставляем перед первым reference
                                first_ref = reference_elements[0]
                                parent.insert(list(parent).index(first_ref), references_elem)
                                # Перемещаем все reference в references
                                for ref in reference_elements[:]:
                                    parent.remove(ref)
                                    references_elem.append(ref)
                                references_elements = [references_elem]
            
            processed_count = 0
            total_count = 0
            
            for references_elem in references_elements:
                # Находим все элементы reference внутри references
                reference_list = references_elem.findall('reference')
                total_count += len(reference_list)
                
                for ref_elem in reference_list:
                    # Проверяем, есть ли уже refInfo
                    ref_info = ref_elem.find('refInfo')
                    
                    if ref_info is None:
                        # Если refInfo нет, нужно преобразовать
                        # Получаем текст из reference
                        ref_text = ref_elem.text
                        if ref_text is None:
                            # Пробуем получить весь текст из элемента
                            ref_text = ''.join(ref_elem.itertext()).strip()
                        
                        if ref_text and ref_text.strip():
                            # Очищаем элемент от текста и дочерних элементов
                            ref_elem.clear()
                            
                            # Создаем структуру refInfo
                            ref_info = etree.Element('refInfo')
                            ref_info.set('lang', 'ANY')
                            
                            text_elem = etree.Element('text')
                            text_elem.text = ref_text.strip()
                            
                            ref_info.append(text_elem)
                            ref_elem.append(ref_info)
                            
                            processed_count += 1
                    else:
                        # Проверяем, есть ли атрибут lang
                        if not ref_info.get('lang'):
                            ref_info.set('lang', 'ANY')
                        
                        # Проверяем, есть ли элемент text
                        text_elem = ref_info.find('text')
                        if text_elem is None:
                            # Если текста нет, но есть текст в refInfo, перемещаем его
                            if ref_info.text and ref_info.text.strip():
                                text_elem = etree.Element('text')
                                text_elem.text = ref_info.text.strip()
                                ref_info.text = None  # Убираем текст из refInfo
                                ref_info.insert(0, text_elem)
                                processed_count += 1
            
            # Сохраняем обновленный XML
            output_path = xml_path.parent / f"{xml_path.stem}_formatted{xml_path.suffix}"
            tree.write(
                str(output_path),
                encoding='UTF-8',
                xml_declaration=True,
                pretty_print=True
            )
            
            return {
                'success': True,
                'output_path': output_path,
                'processed_count': processed_count,
                'total_count': total_count
            }
            
        except etree.XMLSyntaxError as e:
            logger.error(f"Ошибка синтаксиса XML: {e}")
            return {
                'success': False,
                'error': f"Ошибка синтаксиса XML: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Ошибка при форматировании списка литературы: {e}", exc_info=True)
            return {
                'success': False,
                'error': f"Ошибка при обработке: {str(e)}"
            }
