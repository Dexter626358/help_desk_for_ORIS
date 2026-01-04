"""Тесты для модуля обработки данных."""

import pytest
from pathlib import Path
from ipsas.modules.data_processor import DataProcessor


class TestDataProcessor:
    """Тесты для DataProcessor."""

    def test_processor_initialization(self):
        """Тест инициализации процессора."""
        processor = DataProcessor()
        assert processor is not None
        assert processor.logger is not None

    def test_process_nonexistent_file(self):
        """Тест обработки несуществующего файла."""
        processor = DataProcessor()
        fake_path = Path("nonexistent_file.txt")
        
        with pytest.raises(FileNotFoundError):
            processor.process_file(fake_path)

    # TODO: Добавить больше тестов после создания тестовых данных

