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


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    return float(raw)


class Settings:
    """Класс для управления настройками приложения."""

    def __init__(self) -> None:
        self.base_dir: Path = Path(__file__).parent.parent.parent

        self.environment: str = (
            os.getenv("IPSAS_ENV")
            or os.getenv("FLASK_ENV")
            or os.getenv("ENV")
            or "development"
        ).strip().lower()
        self.is_production: bool = self.environment in {"production", "prod"}

        self.data_dir: Path = Path(
            os.getenv("DATA_DIR") or (self.base_dir / "data")
        ).expanduser().resolve()
        self.logs_dir: Path = Path(
            os.getenv("LOGS_DIR") or (self.base_dir / "logs")
        ).expanduser().resolve()
        self.temp_dir: Path = Path(
            os.getenv("TEMP_DIR") or (self.base_dir / "temp")
        ).expanduser().resolve()
        self.schemas_dir: Path = Path(
            os.getenv("SCHEMAS_DIR") or (self.base_dir / "schemas")
        ).expanduser().resolve()
        self.templates_dir: Path = self.base_dir / "ipsas" / "web" / "templates"

        self._create_directories()

        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        # В production по умолчанию только stdout; файл — для локальной разработки.
        log_to_file_default = not self.is_production
        self.log_to_file: bool = _env_bool("LOG_TO_FILE", log_to_file_default)
        self.log_file: Optional[str] = (
            str(self.logs_dir / "ipsas.log") if self.log_to_file else None
        )

        # Не путать с LOG_LEVEL: Flask debug / reloader только через явный флаг.
        self.flask_debug: bool = _env_bool("FLASK_DEBUG", False)
        if self.is_production and self.flask_debug:
            raise RuntimeError("FLASK_DEBUG must be disabled in production")

        self.max_file_size: int = int(os.getenv("MAX_FILE_SIZE", "10485760"))  # 10MB
        self.max_content_length: int = int(
            os.getenv("MAX_CONTENT_LENGTH", str(self.max_file_size))
        )
        self.allowed_extensions: list[str] = [".txt", ".csv", ".json", ".xml", ".zip", ".data"]

        secret = os.getenv("SECRET_KEY")
        if self.is_production:
            if not secret:
                raise RuntimeError("SECRET_KEY is required in production")
            self.secret_key = secret
        else:
            self.secret_key = secret or "dev-secret-key-change-in-production-please"

        # Таймауты исходящих HTTP (секунды): connect/read через urllib timeout
        self.request_timeout: float = _env_float("REQUEST_TIMEOUT", 30.0)
        self.request_max_retries: int = int(os.getenv("REQUEST_MAX_RETRIES", "3"))

        self.xml_typo_fixes_url: str = os.getenv("XML_TYPO_FIXES_URL", "").strip()
        self.journal_site_check_url: str = os.getenv("JOURNAL_SITE_CHECK_URL", "").strip()
        # Без жёсткого внешнего URL по умолчанию в production
        editorial_default = "" if self.is_production else ""
        self.editorial_board_url: str = os.getenv(
            "EDITORIAL_BOARD_URL",
            editorial_default,
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

        # Cookie / session (для HTTPS за reverse-proxy в production)
        self.session_cookie_secure: bool = _env_bool(
            "SESSION_COOKIE_SECURE", self.is_production
        )
        self.session_cookie_httponly: bool = _env_bool("SESSION_COOKIE_HTTPONLY", True)
        self.session_cookie_samesite: str = (
            os.getenv("SESSION_COOKIE_SAMESITE", "Lax").strip() or "Lax"
        )

        # Доверять X-Forwarded-For только если явно включено (за корректным proxy)
        self.trust_proxy_headers: bool = _env_bool("TRUST_PROXY_HEADERS", False)

        # Gunicorn (документация / примеры; in-memory rate limit → 1 worker)
        self.gunicorn_workers: int = int(os.getenv("GUNICORN_WORKERS", "1"))
        self.gunicorn_threads: int = int(os.getenv("GUNICORN_THREADS", "4"))
        self.gunicorn_timeout: int = int(os.getenv("GUNICORN_TIMEOUT", "120"))

    def _create_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        # schemas обычно в репозитории — не создаём пустой каталог поверх mount
        if not self.schemas_dir.exists():
            self.schemas_dir.mkdir(parents=True, exist_ok=True)
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
