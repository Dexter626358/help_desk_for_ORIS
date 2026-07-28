"""Пакет helpers для парсинга метаданных выпуска (JATS, HTML, валидация).

Публичный оркестратор: ``IssueMetadataParser`` в ``orchestrator``
(совместимый импорт: ``ipsas.modules.issue_metadata_parser``).
"""

from ipsas.modules.issue_metadata.orchestrator import IssueMetadataParser

__all__ = ["IssueMetadataParser"]
