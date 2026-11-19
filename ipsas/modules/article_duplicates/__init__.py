"""Поиск дублей статей: journal XML ↔ рукописи редактора OJS."""

from ipsas.modules.article_duplicates.service import (
    archive_confirmed,
    find_duplicates,
)

__all__ = ["archive_confirmed", "find_duplicates"]
