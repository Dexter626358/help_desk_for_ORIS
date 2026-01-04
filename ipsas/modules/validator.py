"""Модуль для валидации данных."""

from typing import Any, Dict, List, Optional
from pathlib import Path
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


class Validator:
    """Класс для валидации различных типов данных."""

    def __init__(self):
        """Инициализация валидатора."""
        self.logger = logger

    def validate_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Валидация файла.

        Args:
            file_path: Путь к файлу

        Returns:
            Словарь с результатами валидации
        """
        results = {
            "file_path": str(file_path),
            "exists": False,
            "is_file": False,
            "is_readable": False,
            "size": 0,
            "extension": "",
            "valid": False,
            "errors": []
        }

        # Проверка существования
        if not file_path.exists():
            results["errors"].append("Файл не существует")
            return results

        results["exists"] = True

        # Проверка что это файл
        if not file_path.is_file():
            results["errors"].append("Указанный путь не является файлом")
            return results

        results["is_file"] = True

        # Проверка расширения
        results["extension"] = file_path.suffix.lower()

        # Проверка размера
        try:
            size = file_path.stat().st_size
            results["size"] = size
        except Exception as e:
            results["errors"].append(f"Ошибка получения размера: {e}")
            return results

        # Проверка читаемости
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                f.read(1)
            results["is_readable"] = True
        except Exception as e:
            results["errors"].append(f"Файл не читается: {e}")
            return results

        # Если все проверки пройдены
        if not results["errors"]:
            results["valid"] = True

        return results

    def validate_data(self, data: Any, rules: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Валидация данных по правилам.

        Args:
            data: Данные для валидации
            rules: Правила валидации

        Returns:
            Словарь с результатами валидации
        """
        results = {
            "valid": False,
            "errors": [],
            "warnings": []
        }

        if rules is None:
            rules = {}

        # Проверка на None
        if data is None:
            results["errors"].append("Данные не могут быть None")
            return results

        # Проверка типа данных
        if "type" in rules:
            expected_type = rules["type"]
            if not isinstance(data, expected_type):
                results["errors"].append(
                    f"Неверный тип данных. Ожидается {expected_type}, получен {type(data)}"
                )

        # Проверка обязательных полей (для словарей)
        if isinstance(data, dict) and "required_fields" in rules:
            required = rules["required_fields"]
            missing = [field for field in required if field not in data]
            if missing:
                results["errors"].append(f"Отсутствуют обязательные поля: {', '.join(missing)}")

        # Проверка длины (для строк и списков)
        if "min_length" in rules:
            min_len = rules["min_length"]
            if len(data) < min_len:
                results["errors"].append(f"Длина меньше минимальной: {min_len}")

        if "max_length" in rules:
            max_len = rules["max_length"]
            if len(data) > max_len:
                results["errors"].append(f"Длина больше максимальной: {max_len}")

        # Если ошибок нет, данные валидны
        if not results["errors"]:
            results["valid"] = True

        return results

    def validate_email(self, email: str) -> bool:
        """
        Простая валидация email адреса.

        Args:
            email: Email адрес для проверки

        Returns:
            True если email валиден
        """
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    def validate_url(self, url: str) -> bool:
        """
        Простая валидация URL.

        Args:
            url: URL для проверки

        Returns:
            True если URL валиден
        """
        import re
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, url))

