"""Текст письма для редакции по результатам валидации journal XML."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional

from ipsas.modules.issue_metadata.editorial_letter import build_editorial_letter


def _schema_error_text(err: Any) -> str:
    if isinstance(err, Mapping):
        msg = str(err.get("message") or err.get("msg") or "").strip()
        if not msg:
            msg = str(err)
        line = err.get("line")
        if line is not None and str(line).strip():
            return f"Строка {line}: {msg}"
        return msg
    return str(err).strip() or "Ошибка схемы"


def _add_finding(
    bucket: MutableMapping[str, dict[str, Any]],
    text: str,
    *,
    article_no: Any = None,
    issue_level: bool = False,
) -> None:
    key = text.strip()
    if not key:
        return
    item = bucket.get(key)
    if item is None:
        item = {
            "text": key,
            "count": 0,
            "articles": [],
            "issue_level": bool(issue_level),
        }
        bucket[key] = item
    item["count"] = int(item["count"]) + 1
    if issue_level:
        item["issue_level"] = True
    if article_no is not None and article_no not in item["articles"]:
        item["articles"].append(article_no)


def group_xml_findings(
    report: Optional[Mapping[str, Any]] = None,
    schema_result: Optional[Mapping[str, Any]] = None,
    metadata_error: Optional[str] = None,
) -> dict[str, list[dict[str, Any]]]:
    """Свести замечания XML-валидатора к формату findings для письма."""
    errors: dict[str, dict[str, Any]] = {}
    warnings: dict[str, dict[str, Any]] = {}

    if metadata_error and str(metadata_error).strip():
        _add_finding(errors, str(metadata_error).strip(), issue_level=True)

    if isinstance(schema_result, Mapping):
        for err in schema_result.get("errors") or []:
            _add_finding(
                errors,
                f"[Схема] {_schema_error_text(err)}",
                issue_level=True,
            )
        for warn in schema_result.get("warnings") or []:
            text = _schema_error_text(warn)
            if text:
                _add_finding(warnings, f"[Схема] {text}", issue_level=True)

    if isinstance(report, Mapping):
        for art in report.get("articles") or []:
            if not isinstance(art, Mapping):
                continue
            idx = art.get("index")
            for text in art.get("critical_issues") or []:
                if text:
                    _add_finding(errors, str(text), article_no=idx)
            for text in art.get("secondary_issues") or []:
                if text:
                    _add_finding(warnings, str(text), article_no=idx)

    def _sorted(items: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            items.values(),
            key=lambda x: (-int(x.get("count") or 0), str(x.get("text") or "")),
        )

    return {
        "errors": _sorted(errors),
        "warnings": _sorted(warnings),
        "by_category": [],
    }


def _result_from_xml_report(
    report: Optional[Mapping[str, Any]],
    *,
    source_file: str = "",
) -> dict[str, Any]:
    journal = report.get("journal") if isinstance(report, Mapping) else {}
    issue = report.get("issue") if isinstance(report, Mapping) else {}
    if not isinstance(journal, Mapping):
        journal = {}
    if not isinstance(issue, Mapping):
        issue = {}

    date_uni = str(issue.get("date_uni") or "").strip()
    year = date_uni[:4] if len(date_uni) >= 4 and date_uni[:4].isdigit() else date_uni
    title = str(
        journal.get("title_ru") or journal.get("title_en") or source_file or "журнал"
    ).strip()

    articles = []
    if isinstance(report, Mapping) and isinstance(report.get("articles"), list):
        articles = list(report["articles"])

    return {
        "issue": {
            "journal_title_ru": title,
            "volume": issue.get("volume") or "",
            "issue": issue.get("number") or "",
            "year": year,
        },
        "articles": articles,
    }


def build_xml_editorial_letter(
    *,
    report: Optional[Mapping[str, Any]] = None,
    schema_result: Optional[Mapping[str, Any]] = None,
    metadata_error: Optional[str] = None,
    source_file: str = "",
    generated_at: Optional[str] = None,
) -> str:
    """Письмо редакции по итогам проверки XML (схема + метаданные)."""
    findings = group_xml_findings(
        report,
        schema_result=schema_result,
        metadata_error=metadata_error,
    )
    result = _result_from_xml_report(report, source_file=source_file)
    when = generated_at or datetime.now().strftime("%d.%m.%Y %H:%M")

    issue = result["issue"]
    journal = str(issue.get("journal_title_ru") or "журнал").strip()
    bits = [f'«{journal}»']
    if issue.get("volume"):
        bits.append(f"том {issue['volume']}")
    if issue.get("issue"):
        bits.append(f"№ {issue['issue']}")
    if issue.get("year"):
        bits.append(str(issue["year"]))
    issue_line = ", ".join(bits)

    if source_file:
        intro = (
            "Направляем результаты автоматической проверки XML-файла "
            f"«{source_file}» ({issue_line})."
        )
    else:
        intro = (
            "Направляем результаты автоматической проверки XML-файла выпуска "
            f"{issue_line}."
        )

    return build_editorial_letter(
        result=result,
        findings=findings,
        generated_at=when,
        intro=intro,
    )
