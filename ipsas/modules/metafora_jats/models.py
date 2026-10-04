"""Модели валидатора JATS XML для Метафоры."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

Severity = Literal["error", "warning", "info"]
CheckCategory = Literal["metafora_required", "metadata_quality"]


@dataclass
class Author:
    surname: str = ""
    given_names: str = ""
    orcid: str = ""
    xpath: str = ""
    affiliation_ids: list[str] = field(default_factory=list)
    has_affiliation: bool = False

    @property
    def has_name(self) -> bool:
        return bool((self.surname or "").strip() or (self.given_names or "").strip())

    @property
    def display_name(self) -> str:
        parts = [p for p in ((self.surname or "").strip(), (self.given_names or "").strip()) if p]
        return " ".join(parts)


@dataclass
class TitleEntry:
    text: str
    lang: str = ""
    xpath: str = ""


@dataclass
class Publication:
    publication_type: str = ""
    publication_type_xpath: str = "/article/@article-type"
    titles: list[TitleEntry] = field(default_factory=list)
    publication_date_raw: str = ""
    publication_date_xpath: str = ""
    fpage: str = ""
    lpage: str = ""
    page_range: str = ""
    elocation_id: str = ""
    pagination_xpath: str = ""
    doi: str = ""
    doi_xpath: str = ""
    doi_declared: bool = False
    edn: str = ""
    edn_xpath: str = ""
    authors: list[Author] = field(default_factory=list)
    reference_count: int = 0
    references_xpath: str = "/article/back/ref-list"
    article_url: str = ""
    source_filename: str = ""


@dataclass
class ValidationIssue:
    code: str
    severity: Severity
    message: str
    field: str = ""
    value: str = ""
    xpath: str = ""
    details: str = ""
    category: CheckCategory = "metafora_required"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PassedCheck:
    code: str
    label: str
    category: CheckCategory = "metafora_required"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    valid_for_metafora: bool
    filename: str = ""
    article_title: str = ""
    publication_type: str = ""
    publication_type_label: str = ""
    article_url: str = ""
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    passed_count: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)
    passed_checks: list[PassedCheck] = field(default_factory=list)
    parse_ok: bool = True
    is_jats: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid_for_metafora": self.valid_for_metafora,
            "filename": self.filename,
            "article_title": self.article_title,
            "publication_type": self.publication_type,
            "publication_type_label": self.publication_type_label,
            "article_url": self.article_url,
            "parse_ok": self.parse_ok,
            "is_jats": self.is_jats,
            "summary": {
                "errors": self.error_count,
                "warnings": self.warning_count,
                "info": self.info_count,
                "passed": self.passed_count,
            },
            "issues": [i.to_dict() for i in self.issues],
            "passed_checks": [p.to_dict() for p in self.passed_checks],
        }


def issue(
    code: str,
    severity: Severity,
    message: str,
    *,
    field: str = "",
    value: str = "",
    xpath: str = "",
    details: str = "",
) -> ValidationIssue:
    return ValidationIssue(
        code=code,
        severity=severity,
        message=message,
        field=field,
        value=value,
        xpath=xpath,
        details=details,
        category="metafora_required",
    )
