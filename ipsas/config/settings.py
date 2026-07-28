"""Настройки приложения."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    """Класс для управления настройками приложения."""

    def __init__(self) -> None:
        self.base_dir: Path = Path(__file__).parent.parent.parent
        self.data_dir: Path = self.base_dir / "data"
        self.logs_dir: Path = self.base_dir / "logs"
        self.temp_dir: Path = self.base_dir / "temp"
        self.schemas_dir: Path = self.base_dir / "schemas"
        self.templates_dir: Path = self.base_dir / "ipsas" / "web" / "templates"

        self.environment: str = (
            os.getenv("IPSAS_ENV")
            or os.getenv("FLASK_ENV")
            or os.getenv("ENV")
            or "development"
        ).strip().lower()
        self.is_production: bool = self.environment in {"production", "prod"}

        self._create_directories()

        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        # В production по умолчанию только stdout (Railway); файл — для локальной разработки.
        log_to_file_default = not self.is_production
        self.log_to_file: bool = _env_bool("LOG_TO_FILE", log_to_file_default)
        self.log_file: Optional[str] = (
            str(self.logs_dir / "ipsas.log") if self.log_to_file else None
        )

        self.max_file_size: int = int(os.getenv("MAX_FILE_SIZE", "10485760"))  # 10MB
        self.max_content_length: int = int(
            os.getenv("MAX_CONTENT_LENGTH", str(self.max_file_size))
        )
        self.allowed_extensions: list[str] = [".txt", ".csv", ".json", ".xml", ".zip"]

        secret = os.getenv("SECRET_KEY")
        if self.is_production:
            if not secret:
                raise RuntimeError("SECRET_KEY is required in production")
            self.secret_key = secret
        else:
            self.secret_key = secret or "dev-secret-key-change-in-production-please"

        self.xml_typo_fixes_url: str = os.getenv("XML_TYPO_FIXES_URL", "").strip()
        self.journal_site_check_url: str = os.getenv("JOURNAL_SITE_CHECK_URL", "").strip()
        self.editorial_board_url: str = os.getenv(
            "EDITORIAL_BOARD_URL",
            "https://parseeditorialboard-production.up.railway.app",
        ).strip()

        # SSRF / исходящие запросы к OJS (пусто = любые публичные хосты после SSRF-проверок)
        self.issue_fetch_allowed_hosts: str = os.getenv(
            "ISSUE_FETCH_ALLOWED_HOSTS", ""
        ).strip()

        self.temp_file_ttl_seconds: int = int(
            os.getenv("TEMP_FILE_TTL_SECONDS", str(6 * 60 * 60))
        )

        # Нагрузка на публичные POST /services/*
        self.max_concurrent_jobs: int = int(os.getenv("MAX_CONCURRENT_JOBS", "4"))
        self.rate_limit_per_minute: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "30"))

    def _create_directories(self) -> None:
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        self.schemas_dir.mkdir(exist_ok=True)
        (self.temp_dir / "jobs").mkdir(exist_ok=True)


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Получить экземпляр настроек (singleton)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Сброс singleton (для тестов)."""
    global _settings
    _settings = None
