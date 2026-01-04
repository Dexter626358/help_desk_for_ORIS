"""Настройки приложения."""

from typing import Optional
from pathlib import Path
import os


class Settings:
    """Класс для управления настройками приложения."""

    def __init__(self):
        """Инициализация настроек."""
        # Базовые пути
        self.base_dir: Path = Path(__file__).parent.parent.parent
        self.data_dir: Path = self.base_dir / "data"
        self.logs_dir: Path = self.base_dir / "logs"
        self.temp_dir: Path = self.base_dir / "temp"
        self.schemas_dir: Path = self.base_dir / "schemas"  # Директория для XSD схем

        # Создание необходимых директорий
        self._create_directories()

        # Настройки логирования
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.log_file: Optional[str] = str(self.logs_dir / "ipsas.log")

        # Настройки обработки данных
        self.max_file_size: int = int(os.getenv("MAX_FILE_SIZE", "10485760"))  # 10MB
        self.allowed_extensions: list[str] = [
            ".txt", ".csv", ".json", ".xml", ".xlsx", ".xls"
        ]

        # Настройки веб-приложения
        self.secret_key: str = os.getenv(
            "SECRET_KEY",
            "dev-secret-key-change-in-production-please"
        )
        self.database_uri: str = os.getenv(
            "DATABASE_URI",
            f"sqlite:///{self.base_dir / 'ipsas.db'}"
        )

    def _create_directories(self) -> None:
        """Создание необходимых директорий, если они не существуют."""
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        self.schemas_dir.mkdir(exist_ok=True)


# Глобальный экземпляр настроек
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Получить экземпляр настроек (singleton)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings

