"""Модели данных для парсинга выпуска.

Web-шаблоны ожидают dict-структуры. Модели предоставляют `.to_dict()` для обратной совместимости.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(slots=True)
class ValidationMessage:
    text: str
    severity: str = "warning"
    field: Optional[str] = None

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {"text": self.text, "severity": self.severity}
        if self.field:
            d["field"] = self.field
        return d


@dataclass(slots=True)
class ArticleIdentifiers:
    doi: Optional[str] = None
    edn: Optional[str] = None
    pdf_url: Optional[str] = None
    internal_id: Optional[str] = None

    @classmethod
    def from_mapping(cls, data: Any) -> "ArticleIdentifiers":
        if not isinstance(data, dict):
            return cls()
        return cls(
            doi=(data.get("doi") or None),
            edn=(data.get("edn") or None),
            pdf_url=(data.get("pdf_url") or None),
            internal_id=(data.get("internal_id") or None),
        )

    def to_dict(self) -> dict[str, Optional[str]]:
        return {
            "doi": self.doi,
            "edn": self.edn,
            "pdf_url": self.pdf_url,
            "internal_id": self.internal_id,
        }


@dataclass(slots=True)
class TextStats:
    length: Optional[int] = None
    first_10: Optional[str] = None
    last_10: Optional[str] = None

    @classmethod
    def from_mapping(cls, data: Any) -> "TextStats":
        if not isinstance(data, dict):
            return cls()
        length = data.get("length")
        return cls(
            length=int(length) if isinstance(length, int) else None,
            first_10=(data.get("first_10") or None),
            last_10=(data.get("last_10") or None),
        )

    def to_dict(self) -> dict[str, object]:
        return {"length": self.length, "first_10": self.first_10, "last_10": self.last_10}


@dataclass(slots=True)
class Article:
    url: str
    title_ru: Optional[str] = None
    title_en: Optional[str] = None
    publication_date: Optional[str] = None
    publication_date_display: Optional[str] = None
    article_type: Optional[str] = None
    issn: Optional[str] = None

    authors_count: int = 0
    authors_ru: list[str] = field(default_factory=list)
    authors_en: list[str] = field(default_factory=list)
    authors: list[str] = field(default_factory=list)
    affiliations: list[str] = field(default_factory=list)
    organizations: list[str] = field(default_factory=list)
    organizations_count: int = 0

    identifiers: ArticleIdentifiers = field(default_factory=ArticleIdentifiers)

    abstract_ru: Optional[str] = None
    abstract_en: Optional[str] = None
    abstract_ru_stats: TextStats = field(default_factory=TextStats)
    abstract_en_stats: TextStats = field(default_factory=TextStats)

    keywords_ru: list[str] = field(default_factory=list)
    keywords_en: list[str] = field(default_factory=list)
    keywords_ru_count: int = 0
    keywords_en_count: int = 0

    references_count: int = 0
    references_ru_count: int = 0
    references_en_count: int = 0
    references_unk_count: int = 0
    reference_first: Optional[str] = None
    reference_last: Optional[str] = None
    reference_ru_first: Optional[str] = None
    reference_ru_last: Optional[str] = None
    reference_en_first: Optional[str] = None
    reference_en_last: Optional[str] = None
    reference_unk_first: Optional[str] = None
    reference_unk_last: Optional[str] = None

    pdf_files: list[dict[str, object]] = field(default_factory=list)

    problems: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, data: Any) -> "Article":
        if not isinstance(data, dict):
            raise TypeError("Article.from_mapping ожидает dict")

        url = str(data.get("url") or "") or "-"

        affiliations = data.get("affiliations") or []
        if isinstance(affiliations, str):
            affiliations = [affiliations]

        pdf_raw = data.get("pdf_files") or []
        pdf_files: list[dict[str, object]] = []
        if isinstance(pdf_raw, list):
            for item in pdf_raw:
                if isinstance(item, dict):
                    pdf_files.append(dict(item))

        return cls(
            url=url,
            title_ru=(data.get("title_ru") or None),
            title_en=(data.get("title_en") or None),
            publication_date=(data.get("publication_date") or None),
            publication_date_display=(data.get("publication_date_display") or None),
            article_type=(data.get("article_type") or None),
            issn=(data.get("issn") or None),
            authors_count=int(data.get("authors_count") or 0),
            authors_ru=list(data.get("authors_ru") or []),
            authors_en=list(data.get("authors_en") or []),
            authors=list(data.get("authors") or []),
            affiliations=list(affiliations),
            organizations=list(data.get("organizations") or []),
            organizations_count=int(data.get("organizations_count") or 0),
            identifiers=ArticleIdentifiers.from_mapping(data.get("identifiers")),
            abstract_ru=(data.get("abstract_ru") or None),
            abstract_en=(data.get("abstract_en") or None),
            abstract_ru_stats=TextStats.from_mapping(data.get("abstract_ru_stats")),
            abstract_en_stats=TextStats.from_mapping(data.get("abstract_en_stats")),
            keywords_ru=list(data.get("keywords_ru") or []),
            keywords_en=list(data.get("keywords_en") or []),
            keywords_ru_count=int(data.get("keywords_ru_count") or 0),
            keywords_en_count=int(data.get("keywords_en_count") or 0),
            references_count=int(data.get("references_count") or 0),
            references_ru_count=int(data.get("references_ru_count") or 0),
            references_en_count=int(data.get("references_en_count") or 0),
            references_unk_count=int(data.get("references_unk_count") or 0),
            reference_first=(data.get("reference_first") or None),
            reference_last=(data.get("reference_last") or None),
            reference_ru_first=(data.get("reference_ru_first") or None),
            reference_ru_last=(data.get("reference_ru_last") or None),
            reference_en_first=(data.get("reference_en_first") or None),
            reference_en_last=(data.get("reference_en_last") or None),
            reference_unk_first=(data.get("reference_unk_first") or None),
            reference_unk_last=(data.get("reference_unk_last") or None),
            pdf_files=pdf_files,
            problems=list(data.get("problems") or []),
            errors=list(data.get("errors") or []),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "url": self.url,
            "title_ru": self.title_ru,
            "title_en": self.title_en,
            "publication_date": self.publication_date,
            "publication_date_display": self.publication_date_display,
            "article_type": self.article_type,
            "issn": self.issn,
            "authors_count": self.authors_count,
            "authors_ru": self.authors_ru,
            "authors_en": self.authors_en,
            "authors": self.authors,
            "affiliations": self.affiliations,
            "organizations": self.organizations,
            "organizations_count": self.organizations_count,
            "identifiers": self.identifiers.to_dict(),
            "abstract_ru": self.abstract_ru,
            "abstract_en": self.abstract_en,
            "abstract_ru_stats": self.abstract_ru_stats.to_dict(),
            "abstract_en_stats": self.abstract_en_stats.to_dict(),
            "keywords_ru": self.keywords_ru,
            "keywords_en": self.keywords_en,
            "keywords_ru_count": self.keywords_ru_count,
            "keywords_en_count": self.keywords_en_count,
            "references_count": self.references_count,
            "references_ru_count": self.references_ru_count,
            "references_en_count": self.references_en_count,
            "references_unk_count": self.references_unk_count,
            "reference_first": self.reference_first,
            "reference_last": self.reference_last,
            "reference_ru_first": self.reference_ru_first,
            "reference_ru_last": self.reference_ru_last,
            "reference_en_first": self.reference_en_first,
            "reference_en_last": self.reference_en_last,
            "reference_unk_first": self.reference_unk_first,
            "reference_unk_last": self.reference_unk_last,
            "pdf_files": self.pdf_files,
            "problems": self.problems,
            "errors": self.errors,
        }


@dataclass(slots=True)
class Issue:
    issue_url: Optional[str] = None
    journal_title_ru: Optional[str] = None
    journal_title: Optional[str] = None
    issue_title: Optional[str] = None
    issn: Optional[str] = None
    eissn: Optional[str] = None
    year: Optional[object] = None
    volume: Optional[object] = None
    issue: Optional[object] = None
    issue_serial: Optional[object] = None
    article_count: Optional[int] = None
    article_urls: list[str] = field(default_factory=list)
    cover_url: Optional[str] = None
    cover_thumb_url: Optional[str] = None
    issue_galleys: list[dict[str, object]] = field(default_factory=list)
    warnings: list[ValidationMessage] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, data: Any) -> "Issue":
        if not isinstance(data, dict):
            raise TypeError("Issue.from_mapping ожидает dict")
        warnings_raw = data.get("warnings") or []
        warnings: list[ValidationMessage] = []
        for w in warnings_raw:
            if isinstance(w, dict):
                warnings.append(
                    ValidationMessage(
                        text=str(w.get("text") or ""),
                        severity=str(w.get("severity") or "warning"),
                        field=(str(w.get("field")) if w.get("field") else None),
                    )
                )
            else:
                warnings.append(ValidationMessage(text=str(w), severity="warning"))

        galleys_raw = data.get("issue_galleys") or []
        issue_galleys: list[dict[str, object]] = []
        if isinstance(galleys_raw, list):
            for g in galleys_raw:
                if isinstance(g, dict):
                    issue_galleys.append(dict(g))

        return cls(
            issue_url=(data.get("issue_url") or None),
            journal_title_ru=(data.get("journal_title_ru") or None),
            journal_title=(data.get("journal_title") or None),
            issue_title=(data.get("issue_title") or None),
            issn=(data.get("issn") or None),
            eissn=(data.get("eissn") or None),
            year=data.get("year"),
            volume=data.get("volume"),
            issue=data.get("issue"),
            issue_serial=data.get("issue_serial"),
            article_count=(int(data.get("article_count")) if data.get("article_count") is not None else None),
            article_urls=list(data.get("article_urls") or []),
            cover_url=(data.get("cover_url") or None),
            cover_thumb_url=(data.get("cover_thumb_url") or None),
            issue_galleys=issue_galleys,
            warnings=warnings,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "issue_url": self.issue_url,
            "journal_title_ru": self.journal_title_ru,
            "journal_title": self.journal_title,
            "issue_title": self.issue_title,
            "issn": self.issn,
            "eissn": self.eissn,
            "year": self.year,
            "volume": self.volume,
            "issue": self.issue,
            "issue_serial": self.issue_serial,
            "article_count": self.article_count,
            "article_urls": self.article_urls,
            "cover_url": self.cover_url,
            "cover_thumb_url": self.cover_thumb_url,
            "issue_galleys": self.issue_galleys,
            "warnings": [w.to_dict() for w in self.warnings],
        }


@dataclass(slots=True)
class IssueParseResult:
    issue: Issue
    articles: list[Article]
    notice: Optional[str] = None

    @classmethod
    def from_mapping(cls, data: dict[str, object]) -> "IssueParseResult":
        issue = Issue.from_mapping(data.get("issue"))
        articles_raw = data.get("articles") or []
        articles: list[Article] = []
        if isinstance(articles_raw, list):
            for a in articles_raw:
                if isinstance(a, dict):
                    articles.append(Article.from_mapping(a))
        return cls(
            issue=issue,
            articles=articles,
            notice=(data.get("notice") if isinstance(data.get("notice"), str) else None),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "issue": self.issue.to_dict(),
            "articles": [a.to_dict() for a in self.articles],
            "notice": self.notice,
        }
