"""Модели статей ENG-метаданных."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ArticlePair:
    """Пара PDF+JSON одной статьи в архиве."""

    stem: str
    article_id: str
    pdf_name: str | None = None
    json_name: str | None = None
    # Из префикса имени: «3-21__article_288752» → 3, 21, «3-21»
    page_start: int | None = None
    page_end: int | None = None
    pages_label: str = ""
    issues: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.pdf_name and self.json_name and not self.issues)


@dataclass
class ArticleCheckResult:
    pair: ArticlePair
    data: dict[str, Any] | None = None
    validation_errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.pair.ok and not self.validation_errors and self.data is not None
