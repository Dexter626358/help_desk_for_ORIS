"""Модель отчёта journal XML (словарь analyze_journal_xml).

Пока отчёт — ``dict[str, Any]``; типы зафиксированы контрактом analyzer.
При ужесточении типизации сюда можно вынести TypedDict / dataclass.
"""

from __future__ import annotations

from typing import Any, TypedDict


class JournalXmlSummary(TypedDict, total=False):
    articles_total: int
    articles_ok: int
    articles_with_errors: int
    articles_with_warnings: int
    missing_eng_title: int
    missing_eng_abstract: int
    missing_eng_keywords: int


class JournalXmlReport(TypedDict, total=False):
    source_file: str
    generated_at: str
    root_tag: str
    journal: dict[str, Any]
    issue: dict[str, Any]
    summary: JournalXmlSummary
    articles: list[dict[str, Any]]
