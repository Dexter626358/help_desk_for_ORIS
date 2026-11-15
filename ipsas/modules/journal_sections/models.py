"""Модели синхронизации разделов журнала из CSV."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class SectionSpec:
    """Строка CSV с описанием раздела."""

    number: int
    title_ru: str
    short_title_ru: str
    title_en: str
    short_title_en: str


@dataclass(frozen=True)
class ExistingSection:
    """Раздел со страницы manager/sections."""

    section_id: str
    title: str
    abbrev: str
    edit_url: str


@dataclass
class SectionSyncResult:
    number: int
    title_ru: str
    status: str  # exists | created | dry_run | error
    message: str
    section_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SyncReport:
    journal_url: str
    base_url: str
    journal: str
    dry_run: bool
    sort_alpha: bool = False
    existing_count: int = 0
    results: list[SectionSyncResult] = field(default_factory=list)
    sort_status: str | None = None  # sorted | unchanged | dry_run | error
    sort_message: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        if self.error:
            return False
        if self.sort_status == "error":
            return False
        return all(r.status != "error" for r in self.results)

    def to_dict(self) -> dict[str, Any]:
        return {
            "journal_url": self.journal_url,
            "base_url": self.base_url,
            "journal": self.journal,
            "dry_run": self.dry_run,
            "sort_alpha": self.sort_alpha,
            "existing_count": self.existing_count,
            "ok": self.ok,
            "error": self.error,
            "created": sum(1 for r in self.results if r.status == "created"),
            "exists": sum(1 for r in self.results if r.status == "exists"),
            "dry_run_hits": sum(1 for r in self.results if r.status == "dry_run"),
            "errors": sum(1 for r in self.results if r.status == "error"),
            "sort_status": self.sort_status,
            "sort_message": self.sort_message,
            "results": [r.to_dict() for r in self.results],
        }
