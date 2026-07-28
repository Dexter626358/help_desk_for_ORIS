"""Пакет обработки списков литературы (journal XML references)."""

from ipsas.modules.bibliography.cleaner import (
    ReferenceCleaningStats,
    clean_references,
    clean_references_with_stats,
    remove_reference_number,
)
from ipsas.modules.bibliography.formatter import ReferenceFormatter
from ipsas.modules.bibliography.processor import remove_reference_numbering

__all__ = [
    "remove_reference_numbering",
    "clean_references",
    "clean_references_with_stats",
    "ReferenceCleaningStats",
    "remove_reference_number",
    "ReferenceFormatter",
]
