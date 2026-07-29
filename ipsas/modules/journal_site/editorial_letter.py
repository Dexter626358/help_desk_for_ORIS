"""Письмо для редакции по результатам проверки заполненности сайта журнала."""

from __future__ import annotations

from datetime import datetime
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

    lines: list[str] = [
        "Уважаемые коллеги!",
        "",
        (
            "Направляем результаты автоматизированной проверки заполненности "
            f"публичного сайта журнала «{title}»."
        ),
    ]
    if url:
        lines.append(f"Ссылка на сайт: {url}")
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
