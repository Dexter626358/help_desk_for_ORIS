"""Генерация HTML-отчёта по XML.

Модуль-обёртка над существующим генератором отчёта `report_generator.py`.
Нужен, чтобы web-слой не зависел напрямую от скрипта и имел стабильный API.
"""

from __future__ import annotations

from pathlib import Path

from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def generate_xml_html_report(xml_path: Path, output_path: Path) -> Path:
    """
    Сгенерировать HTML-отчёт по XML и сохранить его в `output_path`.

    Логика построения отчёта и HTML намеренно остаются такими же,
    как в `report_generator.py`.

    Args:
        xml_path: Путь к исходному XML.
        output_path: Путь, куда сохранить HTML-отчёт.

    Returns:
        Path: Путь к сохранённому HTML-отчёту (обычно равен `output_path`).
    """
    if not xml_path.exists():
        raise FileNotFoundError(f"XML файл не найден: {xml_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # `report_generator.py` лежит в корне репозитория и используется как модуль.
    # Это позволяет не дублировать большой объём логики генерации HTML.
    import report_generator

    logger.info("Генерация HTML-отчёта по XML: %s", xml_path.name)
    return report_generator.generate_html_report(xml_path, output_file=output_path)

