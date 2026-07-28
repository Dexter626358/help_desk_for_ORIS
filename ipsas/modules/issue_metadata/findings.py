"""Хелперы замечаний: единый формат dict для шаблонов + мост к Finding."""

from __future__ import annotations

from typing import Any, Optional

from ipsas.common.models import Finding, Severity
from ipsas.modules.issue_metadata.models import ValidationMessage


def warning_dict(
    text: str,
    *,
    severity: str = "warning",
    field: Optional[str] = None,
    code: Optional[str] = None,
    rule_id: Optional[str] = None,
) -> dict[str, object]:
    """Словарь замечания в формате, ожидаемом шаблонами/group_findings."""
    finding = Finding(
        code=code or rule_id or (f"field.{field}" if field else "message"),
        severity=Severity.from_value(severity),
        message=text,
        field=field,
    )
    data = finding.to_dict()
    # Совместимость со старым ключом text
    out: dict[str, object] = {
        "text": finding.message,
        "severity": finding.severity.value,
        "code": finding.code,
    }
    if field:
        out["field"] = field
    if rule_id:
        out["rule_id"] = rule_id
    # убрать дубли message если не нужен шаблонам — оставляем оба
    out["message"] = finding.message
    return out


def validation_message_from_dict(raw: dict[str, Any]) -> ValidationMessage:
    finding = Finding.from_mapping(raw)
    return ValidationMessage(
        text=finding.message,
        severity=finding.severity.value,
        field=finding.field,
    )


def finding_from_validation_message(msg: ValidationMessage) -> Finding:
    return Finding(
        code=f"field.{msg.field}" if msg.field else "message",
        severity=Severity.from_value(msg.severity),
        message=msg.text,
        field=msg.field,
    )
