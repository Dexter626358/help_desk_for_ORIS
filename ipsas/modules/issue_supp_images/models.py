"""Модели загрузки рисунков в доп. файлы."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PageRange:
    start: int
    end: int

    @property
    def key(self) -> str:
        return f"{self.start}-{self.end}"

    def matches(self, other: PageRange) -> bool:
        return self.start == other.start and self.end == other.end


@dataclass
class IssueArticle:
    article_id: int
    pages: PageRange
    title: str = ""
    authors: str = ""


@dataclass
class ImageFile:
    path: Path
    original_name: str
    folder_key: str


@dataclass
class ImageBundle:
    page_range: PageRange
    folder_name: str
    files: list[ImageFile] = field(default_factory=list)


@dataclass
class UploadResult:
    article_id: int
    pages: str
    filename: str
    status: str
    message: str
    supp_file_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
