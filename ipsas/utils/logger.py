"""Настройка логирования."""

import logging
import sys
from pathlib import Path
from typing import Optional
from ipsas.config.settings import get_settings


def setup_logger(
    name: str = "ipsas",
    log_file: Optional[str] = None,
    log_level: str = "INFO"
) -> logging.Logger:
    """
    Настройка логгера.

    Args:
        name: Имя логгера
        log_file: Путь к файлу лога (опционально)
        log_level: Уровень логирования

    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Удаление существующих обработчиков
    logger.handlers.clear()

    # Формат логов
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Файловый обработчик (если указан)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "ipsas") -> logging.Logger:
    """
    Получить логгер с настройками из конфигурации.

    Args:
        name: Имя логгера

    Returns:
        Настроенный логгер
    """
    settings = get_settings()
    return setup_logger(
        name=name,
        log_file=settings.log_file,
        log_level=settings.log_level
    )

