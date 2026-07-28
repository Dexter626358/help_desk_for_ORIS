"""Compatibility shim → ``ipsas.modules.bibliography.cleaner``."""

from ipsas.modules.bibliography.cleaner import (
    ReferenceCleaningStats,
    clean_references,
    clean_references_with_stats,
    remove_reference_number,
)

__all__ = [
    "ReferenceCleaningStats",
    "clean_references",
    "clean_references_with_stats",
    "remove_reference_number",
]
