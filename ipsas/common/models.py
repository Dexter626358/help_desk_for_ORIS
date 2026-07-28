"""Общие модели замечаний / severity для отчётов IPSAS."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field as dc_field
from enum import Enum
from typing import Any, Optional


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    OK = "ok"

    @classmethod
    def from_value(cls, value: Any, default: "Severity" = None) -> "Severity":
        if default is None:
            default = cls.WARNING
        if isinstance(value, cls):
            return value
        text = str(value or "").strip().lower()
        aliases = {
            "err": cls.ERROR,
            "error": cls.ERROR,
            "critical": cls.ERROR,
            "warn": cls.WARNING,
            "warning": cls.WARNING,
            "info": cls.INFO,
            "ok": cls.OK,
            "success": cls.OK,
        }
        return aliases.get(text, default)


@dataclass(frozen=True)
class Finding:
    """Единый формат замечания для journal XML / аудита выпуска."""

    code: str
    severity: Severity
    message: str
    field: str | None = None
    article_id: str | None = None
    meta: dict[str, Any] = dc_field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["severity"] = self.severity.value
        return data

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "Finding":
        return cls(
            code=str(raw.get("code") or raw.get("rule_id") or raw.get("id") or "unknown"),
            severity=Severity.from_value(raw.get("severity") or raw.get("level")),
            message=str(raw.get("message") or raw.get("text") or ""),
            field=(str(raw["field"]) if raw.get("field") is not None else None),
            article_id=(
                str(raw["article_id"]) if raw.get("article_id") is not None else None
            ),
            meta={
                k: v
                for k, v in raw.items()
                if k
                not in {
                    "code",
                    "rule_id",
                    "id",
                    "severity",
                    "level",
                    "message",
                    "text",
                    "field",
                    "article_id",
                }
            },
        )
