"""Тесты для модуля валидации."""

import pytest
from pathlib import Path
from ipsas.modules.validator import Validator


class TestValidator:
    """Тесты для Validator."""

    def test_validator_initialization(self):
        """Тест инициализации валидатора."""
        validator = Validator()
        assert validator is not None
        assert validator.logger is not None

    def test_validate_nonexistent_file(self):
        """Тест валидации несуществующего файла."""
        validator = Validator()
        fake_path = Path("nonexistent_file.txt")
        
        result = validator.validate_file(fake_path)
        assert result["exists"] is False
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_validate_email(self):
        """Тест валидации email."""
        validator = Validator()
        
        assert validator.validate_email("test@example.com") is True
        assert validator.validate_email("invalid-email") is False
        assert validator.validate_email("test@") is False

    def test_validate_url(self):
        """Тест валидации URL."""
        validator = Validator()
        
        assert validator.validate_url("https://example.com") is True
        assert validator.validate_url("http://example.com") is True
        assert validator.validate_url("invalid-url") is False

