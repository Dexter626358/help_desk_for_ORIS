"""Модели поиска дублей: XML-статья ↔ рукопись OJS."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class XmlArticle:
    """Эталонная статья (journal XML или опубликованный выпуск)."""

    index: int
    title: str
    title_ru: str
    title_en: str
    authors: str
    abstract: str
    pages: str = ""
    keywords: str = ""
    keywords_en: str = ""
    publication_date: str = ""


@dataclass
class QueueSubmission:
    """Рукопись из любой очереди редактора OJS."""

    article_id: int
    submit_date: str
    section: str
    authors: str
    title: str
    url: str
    queue: str
    queue_label: str
    ojs_status: str = ""
    sender: str = ""
    title_from_list: str = ""
    title_en: str = ""
    abstract: str = ""
    keywords: str = ""
    keywords_en: str = ""
    detail_loaded: bool = False

    @property
    def is_published(self) -> bool:
        status = (self.ojs_status or "").casefold()
        return "published" in status or "опубликован" in status


@dataclass
class MatchCandidate:
    """Кандидат-дубль для одной XML-статьи."""

    submission: QueueSubmission
    score: float
    title_score: float
    author_score: float
    abstract_score: float
    authors_full_match: bool
    decision: str = "unchecked"  # unchecked | match | no_match
    authors_order_match: bool = False
    match_reasons: list[str] = field(default_factory=list)
    confidence: str = "medium"  # high | medium | low
    uniqueness: str = ""  # "" | unique | likely_unique

    def to_dict(self) -> dict[str, Any]:
        sub = self.submission
        return {
            "article_id": sub.article_id,
            "title": sub.title,
            "title_en": sub.title_en,
            "title_from_list": sub.title_from_list or sub.title,
            "authors": sub.authors,
            "url": sub.url,
            "queue": sub.queue,
            "queue_label": sub.queue_label,
            "ojs_status": sub.ojs_status,
            "is_published": sub.is_published,
            "submit_date": sub.submit_date,
            "section": sub.section,
            "sender": sub.sender,
            "abstract": sub.abstract,
            "keywords": sub.keywords,
            "keywords_en": sub.keywords_en,
            "detail_loaded": sub.detail_loaded,
            "score": round(self.score, 4),
            "title_score": round(self.title_score, 4),
            "author_score": round(self.author_score, 4),
            "abstract_score": round(self.abstract_score, 4),
            "authors_full_match": self.authors_full_match,
            "authors_order_match": self.authors_order_match,
            "match_reasons": list(self.match_reasons),
            "confidence": self.confidence,
            "uniqueness": self.uniqueness,
            "decision": self.decision,
        }


@dataclass
class ArticleMatchGroup:
    """XML-статья и до пяти кандидатов."""

    xml: XmlArticle
    candidates: list[MatchCandidate] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.xml.index,
            "title": self.xml.title,
            "title_ru": self.xml.title_ru,
            "title_en": self.xml.title_en,
            "authors": self.xml.authors,
            "abstract": self.xml.abstract,
            "pages": self.xml.pages,
            "keywords": self.xml.keywords,
            "keywords_en": self.xml.keywords_en,
            "publication_date": self.xml.publication_date,
            "candidates": [c.to_dict() for c in self.candidates],
        }


@dataclass
class ArchiveActionResult:
    journal: str
    article_id: int
    title: str
    status: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
