"""Модели результата архивации."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class SubmissionInfo:
    article_id: int
    submit_date: str
    section: str
    authors: str
    title: str
    url: str


@dataclass
class ArchiveResult:
    journal: str
    article_id: int
    title: str
    sender: str
    status: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
