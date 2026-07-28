"""Предметные исключения IPSAS."""

from __future__ import annotations


class IpsasError(Exception):
    """Базовая ошибка приложения."""


class InvalidUploadError(IpsasError):
    """Некорректная загрузка файла."""


class XmlParsingError(IpsasError):
    """Ошибка разбора XML."""


class SchemaValidationError(IpsasError):
    """Ошибка XSD-валидации."""


class RemoteIssueUnavailableError(IpsasError):
    """Не удалось получить удалённый выпуск."""


class UnsafeRemoteUrlError(IpsasError):
    """URL отклонён (SSRF / политика доступа)."""
