"""Письмо для редакции по результатам проверки заполненности сайта журнала."""

from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Any, Mapping, Optional, Sequence

_STATUS_LABELS = {
    "missing": "не заполнено",
    "formal": "указано формально / недостаточно",
    "partial": "заполнено частично",
    "conflict": "конфликт / не засчитывается",
    "error": "не удалось проверить",
    # совместимость со старыми статусами
    "empty": "не заполнено",
    "weak": "заполнено слабо / недостаточно",
}

_STATUS_LABELS_EN = {
    "missing": "not provided",
    "formal": "provided formally / insufficient",
    "partial": "provided partially",
    "conflict": "conflict / not accepted",
    "error": "unable to verify",
    # совместимость со старыми статусами
    "empty": "not provided",
    "weak": "weak / insufficient",
}

_LEVEL_LABELS_EN = {
    "high": "well completed",
    "medium": "average completeness",
    "low": "low completeness",
}

_TITLE_EN_BY_ID = {
    # Editorial Team
    "team.editor_in_chief": "Editor-in-Chief",
    "team.editor_in_chief_affiliation": "Affiliation of the Editor-in-Chief",
    "team.deputy_editors": "Deputy Editors",
    "team.editorial_board": "Editorial Board",
    "team.member_affiliations": "Affiliations of Board Members",
    # Editorial Policies
    "policy.aims_scope": "Aims and Scope",
    "policy.sections": "Sections and Directions",
    "policy.peer_review": "Peer Review",
    "policy.frequency": "Publication Frequency",
    "policy.access": "Open Access Policy",
    "policy.archiving": "Archiving",
    "policy.fees": "Publication Fees",
    "policy.ethics": "Publication Ethics / Journal Code of Ethics",
    "policy.indexing": "Indexing",
    "policy.metadata_oa": "Open Access to Publication Metadata",
    "policy.personal_data": "Personal Data Processing Policy",
    "policy.ai_generative": "Generative AI Policy",
    "policy.retraction": "Article Retraction Policy",
    # Submissions
    "submission.receiving": "Submissions / Manuscript Reception",
    "submission.online": "Online Submissions",
    "submission.guidelines": "Author Guidelines",
    "submission.copyright": "Copyright Notice",
    "submission.privacy": "Privacy Statement",
    # Contacts
    "contact.map": "Map",
    "contact.persons": "Contact Persons",
    # Other
    "journal.access_model": "Access Model",
}

_PAGE_TITLE_EN = {
    "/index": "Home",
    "/about/editorialPolicies": "About the Journal → Editorial Policies",
    "/about/editorialTeam": "About the Journal → Editorial Team",
    "/about/submissions": "About the Journal → Submissions",
    "/about/contact": "About the Journal → Journal Contact",
    "/about/journalSponsorship": "About the Journal → Journal Sponsorship",
    "/about/subscriptions": "About the Journal → Subscriptions",
    "/about/history": "About the Journal → History",
}

_LEVEL_LABELS = {
    "high": "хорошо заполнен",
    "medium": "средняя заполненность",
    "low": "низкая заполненность",
}


def _as_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [x for x in value if isinstance(x, Mapping)]


def _problem_required(fields: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for item in fields:
        if item.get("status") == "not_applicable":
            continue
        if item.get("requirement") == "optional":
            continue
        if not item.get("applicable", True):
            continue
        if item.get("status") in {"missing", "formal", "conflict", "error", "empty", "weak"}:
            out.append(item)
        elif item.get("status") == "partial" and item.get("critical"):
            out.append(item)
    return out


def _optional_tips(fields: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for item in fields:
        if item.get("requirement") != "optional":
            continue
        if item.get("status") in {"missing", "formal", "partial", "error", "empty", "weak"}:
            out.append(item)
    return out


def build_journal_site_editorial_letter(
    report: Mapping[str, Any],
    *,
    journal_url: str = "",
    generated_at: Optional[str] = None,
) -> str:
    """Собрать текст письма: обязательные пробелы отдельно от рекомендаций."""
    when = generated_at or str(report.get("generated_at") or "") or datetime.now().strftime(
        "%d.%m.%Y %H:%M"
    )
    title = str(report.get("journal_title") or "журнал").strip() or "журнал"
    base_url = str(report.get("base_url") or journal_url or "").strip()
    url = journal_url or base_url
    overall = report.get("completeness_percent")
    quality = report.get("quality_percent")
    ru_pct = report.get("completeness_ru")
    en_pct = report.get("completeness_en")
    level = str(report.get("level") or "")
    level_label = str(report.get("level_label") or _LEVEL_LABELS.get(level, level))

    locales = report.get("locales") if isinstance(report.get("locales"), dict) else {}
    has_en = isinstance(locales.get("en"), Mapping)

    check_mode = str(report.get("check_mode") or "site")
    data_source = report.get("data_source") if isinstance(report.get("data_source"), Mapping) else {}

    if check_mode == "data":
        return _build_data_checklist_letter(
            report,
            title=title,
            when=when,
            data_source=data_source,
            overall=overall,
            level_label=level_label or level,
        )

    if check_mode == "site+data":
        intro = (
            "Направляем результаты автоматизированной проверки публичного сайта "
            f"и файла настроек OJS (.data) журнала «{title}»."
        )
    else:
        intro = (
            "Направляем результаты автоматизированной проверки заполненности "
            f"публичного сайта журнала «{title}»."
        )

    lines: list[str] = [
        "Уважаемые коллеги!",
        "",
        intro,
    ]
    if url:
        lines.append(f"Ссылка на сайт: {url}")
    if data_source:
        fname = str(data_source.get("filename") or "").strip()
        path = str(data_source.get("path") or "").strip()
        if fname:
            lines.append(f"Файл настроек: {fname}")
        if path:
            lines.append(f"Path журнала в OJS: {path}")
    lines.extend(
        [
            f"Дата проверки: {when}.",
            "",
            "Краткие итоги:",
            f"— базовая заполненность (обязательные и условно обязательные сведения): {overall}%;",
            f"— качество представления (дополнительные сведения): {quality if quality is not None else '—'}%;",
            f"— русская версия (RU): {ru_pct}%;",
            f"— английская версия (EN): {en_pct}%;",
            f"— итоговая оценка: {level_label or level or '—'}.",
            "",
        ]
    )

    any_must = False
    any_tips = False
    section_no = 1

    for lang_key in ("ru", "en"):
        loc = locales.get(lang_key)
        if not isinstance(loc, Mapping):
            continue
        label = str(loc.get("label") or lang_key.upper())
        fields = loc.get("fields") if isinstance(loc.get("fields"), list) else []
        must = _as_list(loc.get("must_fix")) or _problem_required(fields)
        tips = _as_list(loc.get("recommendations")) or _optional_tips(fields)
        req_filled = loc.get("required_filled")
        req_total = loc.get("required_total")
        if req_filled is None or req_total is None:
            summary = loc.get("summary") if isinstance(loc.get("summary"), Mapping) else {}
            req_filled = summary.get("required_filled")
            req_total = summary.get("required_total")

        lines.append(f"{section_no}. {label}")
        if req_total:
            if lang_key == "en":
                lines.append(f"Required information — {req_filled} of {req_total} provided.")
            else:
                lines.append(f"Обязательные сведения — {req_filled} из {req_total} заполнены.")
        lines.append("")

        if must:
            any_must = True
            lines.append("Please complete:" if lang_key == "en" else "Необходимо заполнить:")
            for i, item in enumerate(must, start=1):
                field_title_ru = str(item.get("title") or item.get("id") or "Поле").strip()
                field_title = (
                    _TITLE_EN_BY_ID.get(item.get("id") or "", field_title_ru)
                    if lang_key == "en"
                    else field_title_ru
                )
                status = str(item.get("status") or "")
                if lang_key == "en":
                    status_label = _STATUS_LABELS_EN.get(status, status or "requires attention")
                    page_path = str(item.get("page") or "").strip()
                    page_label = _PAGE_TITLE_EN.get(page_path, str(item.get("page_label") or "").strip())
                else:
                    status_label = _STATUS_LABELS.get(status, status or "требует внимания")
                    page_label = str(item.get("page_label") or item.get("page") or "").strip()
                note = str(item.get("note") or "").strip()
                line = f"{i}. {field_title} — {status_label}"
                if page_label:
                    if lang_key == "en":
                        line += f". Page: {page_label}."
                    else:
                        line += f". Страница: {page_label}."
                if note and status in {"error", "conflict"}:
                    line += f" ({note})"
                lines.append(line)
            lines.append("")
        else:
            lines.append(
                "No issues were found for required and conditionally required information."
                if lang_key == "en"
                else "По обязательным и условно обязательным сведениям замечаний нет."
            )
            lines.append("")

        if tips:
            any_tips = True
            lines.append(
                "Additional recommendations:" if lang_key == "en" else "Дополнительные рекомендации:"
            )
            for i, item in enumerate(tips[:12], start=1):
                field_title_ru = str(item.get("title") or item.get("id") or "Поле").strip()
                field_title = (
                    _TITLE_EN_BY_ID.get(item.get("id") or "", field_title_ru)
                    if lang_key == "en"
                    else field_title_ru
                )
                lines.append(f"{i}. {field_title}.")
            lines.append("")

        section_no += 1

    plugins = _as_list(report.get("plugins_must_fix")) or [
        p
        for p in _as_list(report.get("plugins_results"))
        if p.get("status") in {"missing", "error"}
    ]
    if plugins or report.get("plugins_results"):
        lines.append(f"{section_no}. Настройки плагинов (из .data)")
        lines.append("")
        if plugins:
            lines.append("Необходимо исправить:")
            for i, item in enumerate(plugins, start=1):
                title_p = str(item.get("title") or item.get("id") or "Плагин").strip()
                note = str(item.get("note") or "").strip()
                status = str(item.get("status") or "")
                status_label = _STATUS_LABELS.get(status, status or "требует внимания")
                line = f"{i}. {title_p} — {status_label}"
                if note:
                    line += f" ({note})"
                lines.append(line)
            lines.append("")
            any_must = True
        else:
            lines.append("По эталонному списку плагинов замечаний нет.")
            lines.append("")
        section_no += 1

    settings_must = _as_list(report.get("settings_must_fix"))
    if settings_must and check_mode == "site+data":
        lines.append(f"{section_no}. Замечания по файлу настроек .data")
        lines.append("")
        lines.append("Необходимо заполнить / исправить в настройках OJS:")
        for i, item in enumerate(settings_must[:20], start=1):
            field_title = str(item.get("title") or item.get("id") or "Поле").strip()
            status = str(item.get("status") or "")
            status_label = _STATUS_LABELS.get(status, status or "требует внимания")
            lines.append(f"{i}. {field_title} — {status_label}")
        lines.append("")
        any_must = True
        section_no += 1

    if not any_must and not any_tips:
        lines.extend(
            [
                "По результатам проверки незаполненных обязательных полей не выявлено.",
                "Дополнительных правок по замечаниям системы не требуется.",
                "",
            ]
        )
    elif any_must:
        lines.extend(
            [
                "Базовая оценка строится только по обязательным и условно обязательным полям.",
                "Дополнительные сведения влияют на показатель качества представления, "
                "но их отсутствие не считается ошибкой.",
                "",
            ]
        )

    lines.extend(
        [
            "С уважением,",
            "служба поддержки национальной платформы периодических научных изданий",
        ]
    )
    if has_en:
        lines.extend(
            [
                "",
                "Sincerely,",
                "Support service of the national platform of periodical scientific publications",
            ]
        )
    return "\n".join(lines) + "\n"


def build_journal_site_editorial_letter_html(
    report: Mapping[str, Any],
    *,
    journal_url: str = "",
    generated_at: Optional[str] = None,
) -> str:
    """HTML-фрагмент письма (абзацы, список, ссылки) для показа и копирования."""
    when = generated_at or str(report.get("generated_at") or "") or datetime.now().strftime(
        "%d.%m.%Y %H:%M"
    )
    title = str(report.get("journal_title") or "журнал").strip() or "журнал"
    check_mode = str(report.get("check_mode") or "site")
    if check_mode == "data":
        return _build_data_checklist_letter_html(report, title=title, when=when)
    text = build_journal_site_editorial_letter(
        report,
        journal_url=journal_url,
        generated_at=when,
    )
    return _plain_letter_to_html(text)


def wrap_editorial_letter_html_document(fragment: str) -> str:
    """Минимальный HTML-документ для скачивания."""
    return (
        "<!DOCTYPE html>\n"
        '<html lang="ru">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        "<title>Письмо для редакции</title>\n"
        "</head>\n"
        "<body>\n"
        f"{fragment.rstrip()}\n"
        "</body>\n"
        "</html>\n"
    )


def _plain_letter_to_html(text: str) -> str:
    """Простой HTML из текстового письма (режим проверки сайта)."""
    blocks = [b.strip() for b in text.strip().split("\n\n") if b.strip()]
    parts: list[str] = []
    for block in blocks:
        inner = "<br>\n".join(escape(line) for line in block.split("\n"))
        parts.append(f"<p>{inner}</p>")
    return "\n".join(parts) + "\n"


def _append_letter_item_plain(lines: list[str], index: int, item: Mapping[str, Any]) -> None:
    """Добавить пункт письма с инструкцией в TXT."""
    text = str(item.get("text") or "").strip()
    lines.append(f"{index}. {text}")
    path = str(item.get("fix_path") or "").strip()
    role = str(item.get("fix_role") or "").strip()
    steps = [str(s).strip() for s in (item.get("fix_steps") or []) if str(s).strip()]
    doc_url = str(item.get("fix_doc_url") or item.get("doc_url") or "").strip()
    if path:
        lines.append(f"   Где исправить: {path}")
    if role:
        lines.append(f"   Роль: {role}")
    if steps:
        lines.append("   Как исправить:")
        for step in steps:
            lines.append(f"   — {step}")
    if doc_url:
        lines.append(f"   Инструкция: {doc_url}")


def _format_letter_item_html(item: Mapping[str, Any]) -> str:
    """HTML одного пункта письма с инструкцией."""
    text = escape(str(item.get("text") or "").strip())
    path = str(item.get("fix_path") or "").strip()
    role = str(item.get("fix_role") or "").strip()
    steps = [str(s).strip() for s in (item.get("fix_steps") or []) if str(s).strip()]
    doc_url = str(item.get("fix_doc_url") or item.get("doc_url") or "").strip()
    parts = [f"<div>{text}</div>"]
    if path:
        parts.append(f"<div><strong>Где исправить:</strong> {escape(path)}</div>")
    if role:
        parts.append(f'<div style="color:#555;font-size:0.95em;">Роль: {escape(role)}</div>')
    if steps:
        parts.append("<div><strong>Как исправить:</strong></div>")
        parts.append("<ul>")
        for step in steps:
            parts.append(f"  <li>{escape(step)}</li>")
        parts.append("</ul>")
    if doc_url:
        href = escape(doc_url, quote=True)
        parts.append(
            f'<div><a href="{href}" target="_blank" rel="noopener noreferrer">Открыть инструкцию</a></div>'
        )
    return "<li>\n" + "\n".join(f"  {p}" for p in parts) + "\n</li>"


def _build_data_checklist_letter(
    report: Mapping[str, Any],
    *,
    title: str,
    when: str,
    data_source: Mapping[str, Any],
    overall: Any,
    level_label: str,
) -> str:
    """Письмо редакции: фактические пробелы + краткая инструкция, как исправить."""
    from ipsas.modules.journal_site.checklist_messages import (
        format_letter_date,
        letter_fix_items,
    )

    items = letter_fix_items(report)
    date_line = format_letter_date(when)
    lines: list[str] = [
        "Уважаемые коллеги!",
        "",
        f"Мы проверили настройки сайта журнала «{title}» "
        "на Национальной платформе периодических научных изданий.",
        "",
    ]

    if not items:
        lines.extend(
            [
                "По результатам проверки обязательных замечаний нет — "
                "дополнительных изменений не требуется.",
                "",
                f"Дата проверки: {date_line}.",
                "",
                "Если останутся вопросы по отдельным настройкам, пожалуйста, напишите нам.",
                "",
            ]
        )
    else:
        lines.append("По результатам проверки просим внести следующие изменения:")
        lines.append("")
        for i, item in enumerate(items, start=1):
            _append_letter_item_plain(lines, i, item)
            lines.append("")
        lines.append(f"Дата проверки: {date_line}.")
        lines.append("")
        lines.append(
            "Если при внесении изменений потребуется помощь или пример заполнения, "
            "пожалуйста, напишите нам."
        )
        lines.append("")

    lines.extend(
        [
            "С уважением,",
            "служба поддержки",
            "Национальной платформы периодических научных изданий",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_data_checklist_letter_html(
    report: Mapping[str, Any],
    *,
    title: str,
    when: str,
) -> str:
    from ipsas.modules.journal_site.checklist_messages import (
        format_letter_date,
        letter_fix_items,
    )

    items = letter_fix_items(report)
    date_line = escape(format_letter_date(when))
    title_html = escape(title)
    parts: list[str] = [
        "<p>Уважаемые коллеги!</p>",
        "<p>",
        "  Мы проверили настройки сайта журнала",
        f"  <strong>«{title_html}»</strong>",
        "  на Национальной платформе периодических научных изданий.",
        "</p>",
    ]
    if not items:
        parts.extend(
            [
                "<p>По результатам проверки обязательных замечаний нет — "
                "дополнительных изменений не требуется.</p>",
                f"<p><strong>Дата проверки:</strong> {date_line}.</p>",
                "<p>Если останутся вопросы по отдельным настройкам, пожалуйста, напишите нам.</p>",
            ]
        )
    else:
        parts.append("<p>По результатам проверки просим внести следующие изменения:</p>")
        parts.append("<ol>")
        for item in items:
            parts.append(_format_letter_item_html(item))
        parts.append("</ol>")
        parts.append(f"<p><strong>Дата проверки:</strong> {date_line}.</p>")
        parts.append(
            "<p>Если при внесении изменений потребуется помощь или пример заполнения, "
            "пожалуйста, напишите нам.</p>"
        )
    parts.extend(
        [
            "<p>",
            "  С уважением,<br>",
            "  служба поддержки<br>",
            "  Национальной платформы периодических научных изданий",
            "</p>",
        ]
    )
    return "\n".join(parts) + "\n"
