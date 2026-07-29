"""Текст письма для редакции по результатам проверки выпуска."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Sequence


# Мягкие формулировки тем по категориям (для блока «Основные замечания связаны»).
_CATEGORY_THEME: dict[str, str] = {
    "issue": "с реквизитами и оформлением выпуска",
    "identifiers": "с идентификаторами статей (DOI, EDN и др.)",
    "texts": "с оформлением названий, аннотаций и ключевых слов",
    "authors_orgs": (
        "с отсутствием англоязычных вариантов названий организаций "
        "и связями между авторами и организациями в метаданных"
    ),
    "files": "с отсутствием или недоступностью PDF-файлов выпуска и статей",
    "references": (
        "с возможным ошибочным включением фрагментов текста статьи "
        "в список литературы и оформлением отдельных библиографических записей"
    ),
    "consistency": "с согласованностью страниц и принадлежностью статей выпуску",
}


def _theme_lines_from_findings(
    by_category: Sequence[Any],
    errors: Sequence[Any],
    warnings: Sequence[Any],
) -> list[str]:
    """Собрать до 5 мягких тем замечаний для письма."""
    themes: list[str] = []
    seen: set[str] = set()

    def add(theme: str) -> None:
        t = theme.strip()
        if not t or t in seen:
            return
        seen.add(t)
        themes.append(t)

    for cat in by_category:
        if not isinstance(cat, dict):
            continue
        if int(cat.get("total") or 0) <= 0:
            continue
        cat_id = str(cat.get("id") or "")
        theme = _CATEGORY_THEME.get(cat_id)
        if theme:
            add(theme)
        elif cat.get("title"):
            add(f"с разделом «{cat.get('title')}»")

    samples = [
        str(x.get("text") or "")
        for x in list(errors) + list(warnings)
        if isinstance(x, dict)
    ]
    blob = " ".join(samples).lower()
    if "организац" in blob and "англи" in blob:
        add("с отсутствием англоязычных вариантов названий организаций")
    if "аффилиац" in blob:
        add(
            "с некорректными связями между авторами и организациями "
            "в метаданных отдельных статей"
        )
    if "литератур" in blob or "библиографи" in blob:
        add(
            "с возможным ошибочным включением фрагментов текста статьи "
            "в список литературы"
        )
    if "ключев" in blob:
        add("с оформлением ключевых слов и отдельных библиографических записей")
    if "pdf" in blob and "выпуск" in blob:
        add("с отсутствием общего PDF-файла выпуска")

    return themes[:5]


def build_editorial_letter(
    *,
    result: Mapping[str, Any],
    findings: Mapping[str, Any],
    issue_url: str = "",
    generated_at: Optional[str] = None,
    intro: Optional[str] = None,
) -> str:
    """
    Собрать письмо редакции: фиксация результатов проверки без прямого предписания.

    Формат — обычный текст (.txt) для вставки в электронную почту.
    ``intro`` — замена стандартного вводного абзаца (после обращения).
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

    issue_bits: list[str] = [f"«{journal}»"]
    if volume:
        issue_bits.append(f"том {volume}")
    if number:
        issue_bits.append(f"№ {number}")
    if year:
        issue_bits.append(f"за {year} год" if year.isdigit() else str(year))
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

    default_intro = (
        "В рамках контроля качества размещения материалов на Национальной платформе "
        "периодических научных изданий проведена автоматическая проверка метаданных "
        f"выпуска журнала {issue_line}."
    )
    opening = intro or default_intro

    lines: list[str] = [
        "Уважаемые коллеги!",
        "",
        opening,
        "",
    ]
    if issue_url:
        lines.append("Ссылка на выпуск:")
        lines.append(issue_url)
        lines.append("")
    lines.extend(
        [
            f"Дата и время проверки: {when}.",
            "",
        ]
    )

    if ok:
        lines.extend(
            [
                "По результатам проверки замечаний высокой значимости и замечаний, "
                "требующих дополнительной проверки, не выявлено.",
                "",
                "Краткие результаты:",
                f"— статей в выпуске — {articles_total};",
                "— выявлено замечаний высокой значимости — 0;",
                "— выявлено замечаний, требующих дополнительной проверки, — 0.",
                "",
                "Приложение: HTML-отчёт о качестве метаданных выпуска.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "По результатам проверки обращаем ваше внимание на возможные неточности "
                "в метаданных и оформлении материалов выпуска.",
                "",
                "Краткие результаты:",
                f"— статей в выпуске — {articles_total};",
                f"— выявлено замечаний высокой значимости — {error_count};",
                (
                    "— выявлено замечаний, требующих дополнительной проверки, — "
                    f"{warning_count}."
                ),
                "",
            ]
        )

        themes = _theme_lines_from_findings(by_category, errors, warnings)
        if themes:
            lines.append("Основные замечания связаны:")
            lines.append("")
            for theme in themes:
                lines.append(f"— {theme};")
            # последняя точка вместо точки с запятой
            if lines[-1].endswith(";"):
                lines[-1] = lines[-1][:-1] + "."
            lines.append("")

        lines.extend(
            [
                "Подробная информация по каждой статье, включая название материала, "
                "страницы, прямую ссылку и описание обнаруженных особенностей, "
                "представлена в приложенном HTML-отчёте.",
                "",
                "Просим ознакомиться с результатами проверки и учитывать выявленные "
                "замечания при размещении и последующем обновлении материалов на платформе. "
                "При наличии возможности рекомендуем проверить указанные метаданные и "
                "скорректировать те из них, которые действительно содержат неточности.",
                "",
                "Обращаем внимание, что проверка выполняется автоматически. Отдельные "
                "замечания могут быть обусловлены особенностями оформления конкретной "
                "статьи и не всегда свидетельствуют об ошибке. Если сведения в метаданных "
                "соответствуют опубликованному материалу, внесение изменений не требуется.",
                "",
                "Приложение: HTML-отчёт о качестве метаданных выпуска.",
                "",
            ]
        )

    notice = result.get("notice")
    if notice:
        lines.extend([f"Примечание: {notice}", ""])

    lines.extend(
        [
            "С уважением,",
            "служба поддержки",
            "Национальной платформы периодических научных изданий",
        ]
    )
    return "\n".join(lines) + "\n"
