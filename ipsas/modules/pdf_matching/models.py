"""Data models for PDF–article matching."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from lxml import etree


class MatchMethod(Enum):
    """Методы сопоставления PDF и статей"""
    EDN_EXACT = "edn_exact"              # Точное совпадение EDN
    DOI_EXACT = "doi_exact"              # Точное совпадение DOI
    DOI_PARTIAL = "doi_partial"          # Частичное совпадение DOI (обрезанный)
    TITLE_HIGH = "title_high"            # Высокое совпадение по названию (>0.85)
    TITLE_AUTHORS = "title_authors"      # Совпадение по названию + авторам
    PAGES_TITLE = "pages_title"          # Совпадение по страницам + названию
    FALLBACK = "fallback"                # Общий fallback
    UNMATCHED = "unmatched"              # Не сопоставлено


@dataclass(frozen=True)
class PDFEntry:
    """PDF файл на диске + исходный путь (arcname) внутри ZIP."""
    path: Path
    arcname: str


@dataclass
class ArticleInfo:
    """Информация о статье из XML"""
    index: int
    element: etree.Element
    article_id: Optional[str]
    num: Optional[str]
    pages: Optional[Tuple[int, int]]
    title_rus: Optional[str]
    title_eng: Optional[str]
    authors_rus: List[str]
    authors_eng: List[str]
    doi: Optional[str]
    edn: Optional[str]  # eLIBRARY Document Number (6 латинских символов)


@dataclass
class PDFMetadata:
    """Метаданные извлечённые из PDF"""
    title: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    doi: Optional[str] = None
    doi_candidates: List[str] = field(default_factory=list)  # Все найденные DOI
    edn: Optional[str] = None  # eLIBRARY Document Number
    text_length: int = 0
    extraction_quality: str = "unknown"  # low, medium, high


@dataclass
class MatchResult:
    """Результат сопоставления"""
    article_index: int
    article_id: Optional[str]
    article_title: str
    pdf_filename: Optional[str]
    pdf_lang: Optional[str]
    pages_start: int
    pages_end: int
    score: float
    method: MatchMethod
    doi: Optional[str]
    confidence: str = "low"  # low, medium, high
    details: Dict[str, Any] = field(default_factory=dict)
    pdf_metadata: Optional[Dict[str, Any]] = None  # Для совместимости с шаблоном
