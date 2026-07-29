"""Текст письма для редакции по результатам проверки выпуска."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Sequence


def _fmt_articles(article_nos: Sequence[Any]) -> str:
    nums = [str(n) for n in article_nos if n is not None]
    if not nums:
        return ""
    return "статьи: " + ", ".join(nums)


def _format_finding_line(item: Mapping[str, Any], *, index: int) -> str:
    text = str(item.get("text") or "").strip() or "—"
    count = int(item.get("count") or 1)
    articles = item.get("articles") if isinstance(item.get("articles"), list) else []
    issue_level = bool(item.get("issue_level"))

    where_parts: list[str] = []
    if issue_level:
        where_parts.append("на уровне выпуска")
    art_label = _fmt_articles(articles)
    if art_label:
        where_parts.append(art_label)
    if count > 1 and not articles:
        where_parts.append(f"встречается: {count}")

    suffix = f" ({'; '.join(where_parts)})" if where_parts else ""
    return f"{index}. {text}{suffix}"


def build_editorial_letter(
    *,
    result: Mapping[str, Any],
    findings: Mapping[str, Any],
    issue_url: str = "",
    generated_at: Optional[str] = None,
    intro: Optional[str] = None,
) -> str:
    """
    Собрать письмо редакции: итог проверки и перечень того, что нужно исправить.

    Формат — обычный текст (.txt), удобно вставить в электронную почту.
    ``intro`` — первая содержательная фраза после обращения; если не задан,
    используется формулировка для проверки опубликованного выпуска.
    """
    issue = result.get("issue") if isinstance(result.get("issue"), dict) else {}
    articles = result.get("articles") if isinstance(result.get("articles"), list) else []
    when = generated_at or datetime.now().strftime("%d.%m.%Y %H:%M")

    journal = str(
        issue.get("journal_title_ru") or issue.get("journal_title") or "журнал"
    ).strip()
    volume = str(issue.get("volume") or "").strip()
    number = str(issue.get("issue") or issue.get("number") or "").strip()
    year = str(issue.get("year") or "").strip()

    issue_bits: list[str] = [f'«{journal}»']
    if volume:
        issue_bits.append(f"том {volume}")
    if number:
        issue_bits.append(f"№ {number}")
    if year:
        issue_bits.append(str(year))
    issue_line = ", ".join(issue_bits)

    errors = findings.get("errors") if isinstance(findings.get("errors"), list) else []
    warnings = findings.get("warnings") if isinstance(findings.get("warnings"), list) else []
    by_category = (
        findings.get("by_category") if isinstance(findings.get("by_category"), list) else []
    )

    error_count = sum(int(x.get("count") or 1) for x in errors if isinstance(x, dict))
    warning_count = sum(int(x.get("count") or 1) for x in warnings if isinstance(x, dict))
    articles_total = len(articles)
    ok = error_count == 0 and warning_count == 0

    opening = intro or (
        "Направляем результаты автоматической проверки метаданных "
        f"опубликованного выпуска {issue_line}."
    )

    lines: list[str] = [
        "Уважаемые коллеги!",
        "",
        opening,
    ]
    if issue_url:
        lines.append(f"Ссылка на выпуск: {issue_url}")
    lines.extend(
        [
            f"Дата проверки: {when}.",
            "",
            "Краткие итоги:",
            f"— статей в выпуске: {articles_total};",
            f"— ошибок (требуют исправления): {error_count};",
            f"— предупреждений (рекомендуется проверить): {warning_count}.",
            "",
        ]
    )

    if ok:
        lines.extend(
            [
                "По результатам проверки критичных замечаний не выявлено.",
                "Дополнительных правок по замечаниям системы не требуется.",
                "",
            ]
        )
    else:
        lines.append("Просим устранить замечания ниже и при необходимости повторно опубликовать/обновить метаданные.")
        lines.append("")

        if errors:
            lines.append("1. Ошибки (необходимо исправить)")
            lines.append("")
            for i, item in enumerate(errors, start=1):
                if isinstance(item, dict):
                    lines.append(_format_finding_line(item, index=i))
            lines.append("")

        if warnings:
            section_no = 2 if errors else 1
            lines.append(f"{section_no}. Предупреждения (рекомендуется исправить)")
            lines.append("")
            for i, item in enumerate(warnings, start=1):
                if isinstance(item, dict):
                    lines.append(_format_finding_line(item, index=i))
            lines.append("")

        # Краткая сводка по категориям (только непустые)
        cat_lines: list[str] = []
        for cat in by_category:
            if not isinstance(cat, dict):
                continue
            total = int(cat.get("total") or 0)
            if total <= 0:
                continue
            title = str(cat.get("title") or cat.get("id") or "Категория")
            ec = int(cat.get("error_count") or 0)
            wc = int(cat.get("warning_count") or 0)
            cat_lines.append(f"— {title}: ошибок {ec}, предупреждений {wc}")
        if cat_lines:
            lines.append("Сводка по разделам:")
            lines.extend(cat_lines)
            lines.append("")

    notice = result.get("notice")
    if notice:
        lines.extend([f"Примечание: {notice}", ""])

    lines.extend(
        [
            "При необходимости можем направить подробный HTML-отчёт по проверке.",
            "",
            "С уважением,",
            "служба поддержки национальной платформы периодических научных изданий",
        ]
    )
    return "\n".join(lines) + "\n"
