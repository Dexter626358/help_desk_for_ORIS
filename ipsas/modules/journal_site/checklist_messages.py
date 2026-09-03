"""Человекочитаемые формулировки для отчёта проверяющего и письма редакции."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping, Sequence

from ipsas.modules.journal_site.default_setup_checklist import (
    DEFAULT_SETUP_CHECKLIST,
    SECTION_TITLES,
)
from ipsas.modules.journal_site.fix_guides import attach_fix_guide

_CHECKLIST_ORDER: dict[str, int] = {
    item.id: i for i, item in enumerate(DEFAULT_SETUP_CHECKLIST)
}
_SECTION_ORDER: dict[str, int] = {sid: i for i, sid in enumerate(SECTION_TITLES)}

# Базовые действия (полное отсутствие / оба языка). Инфинитив.
FRIENDLY_ACTION_BY_ID: dict[str, str] = {
    "pages.lang.ru": (
        "Включить русский язык для интерфейса, отправки рукописей и форм"
    ),
    "pages.lang.en": (
        "Включить английский язык для интерфейса, отправки рукописей и форм"
    ),
    "pages.reader_tools": "Включить и настроить инструменты читателя",
    "pages.editorial_board": "Заполнить раздел «Редакция» на русском и английском языках",
    "pages.section_articles": "Настроить раздел «Статьи» по инструкции",
    "pages.sections_other": "Добавить и настроить остальные разделы журнала",
    "modules.mets_gateway": "Выключить плагин шлюза METS",
    "modules.recognition": "Включить плагин распознавания",
    "modules.doi": (
        "Если журнал присваивает DOI — включить DOI и указать префикс DOI"
    ),
    "modules.edn": "Включить модуль EDN",
    "modules.urn": "Выключить модуль URN",
    "modules.url_pubid": "Выключить публичный идентификатор URL",
    "modules.browse": "Включить плагин «Браузер»",
    "modules.coins": "Включить плагин COinS",
    "modules.custom_blocks": "Включить плагин «Управление блоками пользователя»",
    "modules.driver": "Включить плагин DRIVER",
    "modules.pdfjs": "Включить PDF-просмотрщик PDF.JS",
    "modules.sehl": "Включить плагин SEHL",
    "modules.static_pages": "Включить плагин статических страниц",
    "modules.tinymce": "Включить плагин TinyMCE",
    "modules.webfeed": (
        "Настроить новостную ленту: отображение на страницах выпуска и текущего выпуска"
    ),
    "modules.fundref": "Включить FundRef и обновить базу организаций",
    "modules.acron": "Проверить, что ACRON включён на уровне администратора сайта",
    "modules.extra_off": "Выключить лишние основные модули вне эталона настройки",
    "metrics.dimensions": "При наличии DOI включить метрику Dimensions",
    "metrics.plumx": "Если журнал в Scopus — включить метрику PlumX",
    "metrics.citedby": "При наличии DOI включить плагин Cited-by",
    "metrics.alm": "Включить плагин ALM",
    "metrics.altmetrics": "Выключить метрику Altmetrics",
    "metrics.publons": "Выключить метрику Publons",
    "step1.title": "Указать название журнала на русском и английском языках",
    "step1.initials": "Указать инициалы журнала на русском и английском языках",
    "step1.abbreviation": "Указать сокращённое название журнала на русском и английском языках",
    "step1.issn": "Указать ISSN (печатный и/или онлайн)",
    "step1.elibrary": "Указать идентификатор журнала в eLIBRARY.RU",
    "step1.mailing": "Заполнить почтовый адрес и карту на русском и английском языках",
    "step1.board": "Заполнить раздел «Редакция» на русском и английском языках",
    "step1.support": "Заполнить блок технической поддержки (имя и адрес электронной почты)",
    "step1.email": "Проверить исходящую почту или оставить глобальные настройки сайта",
    "step1.publisher": (
        "Указать наименование издателя и сведения об издателе "
        "на русском и английском языках"
    ),
    "step1.indexing_kw": (
        "Добавить ключевые слова для индексации на русском и английском языках. "
        "Ключевые слова необходимо разделять запятыми"
    ),
    "step2.focus": "Заполнить сведения о предметной области и целях журнала",
    "step2.review": "Разместить принципы рецензирования на русском и английском языках",
    "step3.guidelines": "Разместить правила для авторов на русском и английском языках",
    "step3.checklist": "Добавить список требований к статьям на русском и английском языках",
    "step3.copyright": (
        "Заполнить условия использования материалов: добавить текст на русском "
        "и английском языках, указать правообладателя и сведения о лицензии"
    ),
    "step3.submission_ack": (
        "Включить отправку уведомления о поступлении новой рукописи контактному лицу "
        "журнала либо указать адрес электронной почты для отправки копии уведомления"
    ),
    "step4.access": "Проверить доступ к журналу и роли при регистрации",
    "step4.library": "Если включён режим библиотеки — указать URL исходного сайта",
    "step4.issue_id": "Задать формат идентификации выпусков (том, номер и/или год)",
    "step4.pagination": "Включить пагинацию",
    "step5.home_header": (
        "Указать название журнала для отображения в верхнем колонтитуле "
        "главной страницы на русском и английском языках"
    ),
    "step5.thumbnail": "Загрузить миниатюру журнала",
    "step5.description": "Заполнить описание журнала на главной на русском и английском языках",
    "step5.cover": "Загрузить обложку на главную страницу",
    "step5.current_issue": "Включить отображение текущего выпуска на главной",
    "step5.additional": "Заполнить дополнительное содержание на главной",
    "step5.page_header": (
        "Указать название журнала для отображения в верхнем колонтитуле "
        "всех страниц на русском и английском языках"
    ),
    "step5.dates": (
        "В разделе настройки внешнего вида журнала выбрать даты, "
        "которые должны отображаться на страницах статей"
    ),
}

# Действия по кодам дефицита (только то, что реально не выполнено)
DEFICIT_ACTIONS: dict[str, str] = {
    "section_articles.not_found": "Добавить раздел «Статьи» и настроить его по инструкции",
    "section_articles.title": (
        "Указать название раздела «Статьи» на русском и английском языках"
    ),
    "section_articles.abbrev": (
        "Указать сокращение раздела «Статьи» на русском и английском языках"
    ),
    "section_articles.hide_about": (
        "В настройках раздела «Статьи» скрыть раздел из блока «О журнале»"
    ),
    "section_articles.editor_restriction": (
        "В настройках раздела «Статьи» установить ограничение, согласно которому "
        "отправлять материалы в этот раздел могут только редакторы"
    ),
    "checklist.ru": (
        "Добавить список требований к статьям на русском языке. "
        "Английская версия списка уже заполнена"
    ),
    "checklist.en": (
        "Добавить список требований к статьям на английском языке. "
        "Русская версия списка уже заполнена"
    ),
    "checklist.both": "Добавить список требований к статьям на русском и английском языках",
    "board.ru": (
        "Заполнить раздел «Редакция» на русском языке. "
        "Английская версия уже заполнена"
    ),
    "board.en": (
        "Заполнить раздел «Редакция» на английском языке. "
        "Русская версия уже заполнена"
    ),
    "board.both": "Заполнить раздел «Редакция» на русском и английском языках",
    "submission_ack": (
        "Включить отправку уведомления о поступлении новой рукописи контактному лицу "
        "журнала либо указать адрес электронной почты для отправки копии уведомления"
    ),
    "issn.print": "У журнала действительно отсутствует печатный ISSN",
    "issn.online": "У журнала действительно отсутствует онлайн ISSN",
    "issn.any": "Указать ISSN (печатный и/или онлайн)",
}

REQUIREMENT_LABEL_BY_ID: dict[str, str] = {
    "step3.checklist": "Список требований к статьям на русском и английском",
    "step1.abbreviation": "Сокращённое название на русском и английском",
    "step1.elibrary": "Идентификатор журнала в eLIBRARY.RU",
    "step1.publisher": "Сведения об издателе на русском и английском",
    "step1.issn": "ISSN печатной и/или электронной версии",
    "step2.focus": "Предметная область и цели журнала",
    "modules.browse": "Плагин «Браузер»",
    "modules.webfeed": "Новостная лента выпуска",
    "modules.doi": "DOI (если журнал присваивает DOI)",
    "pages.editorial_board": "Редакция журнала",
    "step1.board": "Редакция журнала",
    "pages.section_articles": "Раздел «Статьи»",
}

# Базовая формулировка поля → для частичного заполнения по языкам
_FIELD_SUBJECT_BY_ID: dict[str, str] = {
    "step1.title": "название журнала",
    "step1.initials": "инициалы журнала",
    "step1.abbreviation": "сокращённое название журнала",
    "step1.publisher": "наименование издателя и сведения об издателе",
    "step1.indexing_kw": "ключевые слова для индексации",
    "step1.mailing": "почтовый адрес",
    "step1.support": "сведения о технической поддержке",
    "step2.focus": "сведения о предметной области и целях журнала",
    "step2.review": "принципы рецензирования",
    "step3.guidelines": "правила для авторов",
    "step5.home_header": "название журнала для верхнего колонтитула главной страницы",
    "step5.page_header": "название журнала для верхнего колонтитула всех страниц",
    "step5.description": "описание журнала на главной",
}

_DEDUPE_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"pages.editorial_board", "step1.board"}),
)

RESULT_LABELS = {
    "ok": "Выполнено",
    "fail": "Не выполнено",
    "warn": "Частично выполнено",
    "manual": "Проверить вручную",
    "na": "Не применяется",
}

_MONTHS_RU = (
    "",
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def _section_title_for_staff(section: str, existing: str = "") -> str:
    title = existing or SECTION_TITLES.get(section, "")
    if "→" in title:
        title = title.split("→", 1)[-1].strip()
    return title


def enrich_result_row(row: dict[str, Any]) -> dict[str, Any]:
    """Добавить поля для UI проверяющего и письма."""
    iid = str(row.get("id") or "")
    status = str(row.get("status") or "")
    severity = str(row.get("severity") or "required")
    actual = str(row.get("actual") or "")
    deficits = [str(x) for x in (row.get("deficits") or []) if x]

    ui_status = _ui_status_for(iid, status)
    bucket = _bucket(ui_status, severity, iid)
    letter_actions = precise_actions(iid, status, actual, deficits, row)
    action = "; ".join(letter_actions) if letter_actions else (
        FRIENDLY_ACTION_BY_ID.get(iid) or _fallback_action(row)
    )
    requirement = REQUIREMENT_LABEL_BY_ID.get(iid) or _clean_requirement_title(
        str(row.get("title") or iid)
    )
    detected = _detected_label(row)
    tech = _tech_detail(row)
    letter_fail_action = _letter_fail_action(iid, ui_status, deficits, actual, letter_actions)

    section = str(row.get("section") or "")
    if not section and "." in iid:
        prefix = iid.split(".", 1)[0]
        if prefix in SECTION_TITLES:
            section = prefix
        elif prefix == "modules":
            section = "modules_generic"
        elif prefix == "metrics":
            section = "modules_metrics"
        elif prefix == "pages":
            section = "pages"

    existing_title = str(row.get("section_title") or "")
    section_title = _section_title_for_staff(section, existing_title)

    row = dict(row)
    row.update(
        {
            "ui_status": ui_status,
            "result_label": RESULT_LABELS.get(ui_status, status or "—"),
            "requirement_label": requirement,
            "action": action,
            "letter_actions": letter_actions,
            "letter_fail_action": letter_fail_action,
            "sort_index": _CHECKLIST_ORDER.get(iid, 999),
            "detected": detected,
            "tech_detail": tech,
            "bucket": bucket,
            "deficits": deficits,
            "section": section or row.get("section") or "",
            "section_title": section_title,
        }
    )
    return attach_fix_guide(row)


def build_staff_summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Сводка для шапки отчёта проверяющего."""
    rows = [enrich_result_row(dict(r)) for r in results]
    applicable = [r for r in rows if r.get("status") != "na"]
    required = [r for r in applicable if r.get("severity") == "required"]
    required_ok = [r for r in required if r.get("ui_status") == "ok"]
    required_fail = [r for r in required if r.get("bucket") == "fix"]
    required_partial = [r for r in required if r.get("bucket") == "partial"]
    required_manual = [r for r in required if r.get("bucket") == "manual"]
    mandatory_total = len(required)
    mandatory_done = len(required_ok)
    mandatory_pct = (
        round(100.0 * mandatory_done / mandatory_total, 1) if mandatory_total else 0.0
    )

    must_fix = _dedupe_rows(required_fail)
    partial = _dedupe_rows(required_partial)
    # ручные могут быть и среди recommended
    manual = _dedupe_rows([r for r in rows if r.get("bucket") == "manual"])
    rec_open = _dedupe_rows([r for r in rows if r.get("bucket") == "rec"])

    if not must_fix and not partial and not manual:
        conclusion = "Обязательные настройки в порядке. Можно завершать проверку."
        conclusion_level = "ok"
    elif must_fix:
        conclusion = "Настройки требуют доработки перед завершением проверки."
        conclusion_level = "error"
    else:
        conclusion = "Есть частичные заполнения или пункты для ручной проверки."
        conclusion_level = "warning"

    return {
        "rows": rows,
        "mandatory_percent": mandatory_pct,
        "mandatory_done": mandatory_done,
        "mandatory_total": mandatory_total,
        "must_fix": must_fix,
        "partial": partial,
        "manual": manual,
        "recommendations_open": rec_open,
        "must_fix_count": len(must_fix),
        "fail_check_count": len(required_fail),
        "partial_count": len(partial),
        "manual_count": len(manual),
        "recommendations_open_count": len(rec_open),
        "conclusion": conclusion,
        "conclusion_level": conclusion_level,
    }


def letter_fix_items(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Пункты письма: действие + путь/шаги исправления (для редакции)."""
    rows: list[Mapping[str, Any]] = []
    for key in ("staff_must_fix", "staff_partial"):
        value = report.get(key)
        if value is None:
            continue
        rows.extend(list(value))
    if not rows:
        for key in ("must_fix",):
            rows.extend(list(report.get(key) or []))
        for row in report.get("fields") or []:
            enriched = enrich_result_row(dict(row))
            if enriched.get("bucket") == "partial":
                rows.append(enriched)

    enriched_rows = _dedupe_rows([enrich_result_row(dict(x)) for x in rows])
    enriched_rows = [r for r in enriched_rows if r.get("bucket") in {"fix", "partial"}]
    enriched_rows.sort(key=_row_sort_key)

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in enriched_rows:
        fix_fields = _letter_fix_fields(row)
        for text in row.get("letter_actions") or []:
            text = str(text).strip()
            if not text:
                continue
            if not text.endswith("."):
                text += "."
            if text in seen:
                continue
            seen.add(text)
            item: dict[str, Any] = {
                "id": str(row.get("id") or ""),
                "text": text,
                "doc_url": fix_fields["fix_doc_url"],
                "kind": "auto",
                "order": str(_CHECKLIST_ORDER.get(str(row.get("id") or ""), 999)),
            }
            item.update(fix_fields)
            items.append(item)
    return items


def _letter_fix_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    """Поля инструкции для пункта письма."""
    return {
        "fix_path": str(row.get("fix_path") or ""),
        "fix_role": str(row.get("fix_role") or ""),
        "fix_steps": [str(s).strip() for s in (row.get("fix_steps") or []) if str(s).strip()],
        "fix_doc_url": _safe_http_url(
            str(row.get("fix_doc_url") or row.get("doc_url") or "")
        ),
    }


def letter_fix_actions(report: Mapping[str, Any]) -> list[str]:
    """Плоский список действий для письма: только фактические пробелы (fix + partial)."""
    return [item["text"] for item in letter_fix_items(report)]


def build_letter_payload(report: Mapping[str, Any]) -> dict[str, Any]:
    """JSON для пересборки письма после ручных отметок проверяющего."""
    auto_items = letter_fix_items(report)
    manuals: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in report.get("staff_manual") or []:
        enriched = enrich_result_row(dict(row))
        iid = str(enriched.get("id") or "")
        if iid in seen:
            continue
        seen.add(iid)
        text = str(enriched.get("letter_fail_action") or "").strip()
        if not text:
            continue
        if not text.endswith("."):
            text += "."
        item: dict[str, Any] = {
            "id": iid,
            "text": text,
            "kind": "manual",
            "order": str(_CHECKLIST_ORDER.get(iid, 999)),
            "doc_url": "",
        }
        item.update(_letter_fix_fields(enriched))
        if not item["doc_url"]:
            item["doc_url"] = item["fix_doc_url"]
        manuals.append(item)
    manuals.sort(key=lambda x: int(x.get("order") or 999))
    return {
        "title": str(report.get("journal_title") or "журнал"),
        "date_line": format_letter_date(str(report.get("generated_at") or "")),
        "auto_items": auto_items,
        "manual_items": manuals,
        "pending_manual": len(manuals),
    }


def _row_sort_key(row: Mapping[str, Any]) -> tuple[int, int]:
    iid = str(row.get("id") or "")
    section = str(row.get("section") or "")
    return (
        _SECTION_ORDER.get(section, 99),
        _CHECKLIST_ORDER.get(iid, 999),
    )


def _letter_fail_action(
    iid: str,
    ui_status: str,
    deficits: Sequence[str],
    actual: str,
    letter_actions: Sequence[str],
) -> str:
    """Текст для письма, если ручная проверка отмечена как «не выполнено»."""
    if ui_status != "manual":
        return ""
    if iid == "step1.issn":
        joined = " ".join(deficits) + " " + actual.lower()
        if "issn.print" in deficits or "печатный=нет" in joined.replace(" ", ""):
            return "Указать печатный ISSN журнала"
        if "issn.online" in deficits or "онлайн=нет" in joined.replace(" ", ""):
            return "Указать онлайн ISSN журнала"
        return "Указать ISSN (печатный и/или онлайн)"
    if iid == "modules.webfeed":
        return (
            "Настроить новостную ленту так, чтобы она отображалась "
            "на страницах выпуска и текущего выпуска"
        )
    if iid == "modules.browse":
        return "Включить плагин «Браузер»"
    if letter_actions:
        return str(letter_actions[0])
    return FRIENDLY_ACTION_BY_ID.get(iid, "")


def letter_actions_by_section(report: Mapping[str, Any]) -> list[tuple[str, list[str]]]:
    """Совместимость: одна группа без заголовков разделов."""
    actions = letter_fix_actions(report)
    return [("", actions)] if actions else []


def format_letter_date(when: str = "") -> str:
    """«03.09.2026, 09:20» → «3 сентября 2026 года»."""
    text = (when or "").strip()
    match = re.match(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})", text)
    if match:
        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        if 1 <= month <= 12:
            return f"{day} {_MONTHS_RU[month]} {year} года"
    now = datetime.now()
    return f"{now.day} {_MONTHS_RU[now.month]} {now.year} года"


def precise_actions(
    iid: str,
    status: str,
    actual: str,
    deficits: Sequence[str],
    row: Mapping[str, Any],
) -> list[str]:
    """Сформировать действия только по фактическим пробелам."""
    if status in {"ok", "na"}:
        return []

    # явные дефициты от оценщика
    if deficits:
        if iid == "step3.copyright" or any(d.startswith("copyright.") for d in deficits):
            return [_copyright_action(list(deficits))]
        mapped = [DEFICIT_ACTIONS[d] for d in deficits if d in DEFICIT_ACTIONS]
        if mapped:
            return mapped
        # дефициты вида abbreviation/ru из setting_text
        locale_actions = _actions_from_locale_missing(iid, list(deficits))
        if locale_actions:
            return locale_actions

    # эвристики без deficits (старые строки / ручные кейсы)
    if iid == "pages.section_articles":
        return _section_articles_from_note(str(row.get("note") or ""), actual)
    if iid == "step3.checklist":
        return _checklist_from_actual(actual)
    if iid in {"pages.editorial_board", "step1.board"}:
        return _board_from_actual(actual, status)
    if iid == "step1.issn" and status == "warn":
        low = actual.lower()
        if "печатный=нет" in low:
            return [DEFICIT_ACTIONS["issn.print"]]
        if "онлайн=нет" in low:
            return [DEFICIT_ACTIONS["issn.online"]]
    if iid == "modules.webfeed" and status != "ok":
        return [
            "Новостная лента отображается на страницах выпуска и текущего выпуска"
        ]

    note = str(row.get("note") or "")
    if "Частично:" in note or "Не заполнено:" in note:
        missing = _parse_missing_tokens(note)
        locale_actions = _actions_from_locale_missing(iid, missing)
        if locale_actions:
            return locale_actions

    base = FRIENDLY_ACTION_BY_ID.get(iid)
    if base:
        return [base]
    return [_fallback_action(row)]


def _copyright_action(deficits: list[str]) -> str:
    need_notice_ru = "copyright.notice_ru" in deficits
    need_notice_en = "copyright.notice_en" in deficits
    need_holder = "copyright.holder" in deficits or "copyright.holder_other" in deficits
    need_license = "copyright.license" in deficits

    parts: list[str] = []
    if need_notice_ru and need_notice_en:
        parts.append("добавить текст на русском и английском языках")
    elif need_notice_ru:
        parts.append("добавить текст на русском языке")
    elif need_notice_en:
        parts.append("добавить текст на английском языке")
    if need_holder:
        parts.append("указать правообладателя")
    if need_license:
        parts.append("сведения о лицензии")
    if not parts:
        return FRIENDLY_ACTION_BY_ID["step3.copyright"]
    if len(parts) == 1:
        return "Заполнить условия использования материалов: " + parts[0]
    return (
        "Заполнить условия использования материалов: "
        + ", ".join(parts[:-1])
        + " и "
        + parts[-1]
    )


def _actions_from_locale_missing(iid: str, missing: list[str]) -> list[str]:
    langs = set()
    for token in missing:
        token = token.strip()
        if "/" in token:
            langs.add(token.rsplit("/", 1)[-1].lower())
        elif token.lower() in {"ru", "en"}:
            langs.add(token.lower())
    if not langs:
        return []

    subject = _FIELD_SUBJECT_BY_ID.get(iid)
    base = FRIENDLY_ACTION_BY_ID.get(iid)
    if not subject and not base:
        return []

    only_ru = langs == {"ru"}
    only_en = langs == {"en"}
    if only_en and subject:
        verb = "Добавить" if iid in {"step1.indexing_kw", "step3.guidelines", "step2.review"} else "Указать"
        if iid in {"step2.review", "step3.guidelines"}:
            verb = "Разместить"
        if iid in {"step1.publisher", "step2.focus", "pages.editorial_board", "step1.board"}:
            verb = "Заполнить"
        if iid == "step1.indexing_kw":
            return [
                "Добавить ключевые слова для индексации на английском языке. "
                "Русская версия уже заполнена. Ключевые слова необходимо разделять запятыми"
            ]
        return [
            f"{verb} {subject} на английском языке. Русская версия уже заполнена"
        ]
    if only_ru and subject:
        verb = "Указать"
        if iid in {"step2.review", "step3.guidelines"}:
            verb = "Разместить"
        if iid in {"step1.publisher", "step2.focus", "pages.editorial_board", "step1.board"}:
            verb = "Заполнить"
        if iid == "step1.indexing_kw":
            return [
                "Добавить ключевые слова для индексации на русском языке. "
                "Английская версия уже заполнена. Ключевые слова необходимо разделять запятыми"
            ]
        return [
            f"{verb} {subject} на русском языке. Английская версия уже заполнена"
        ]
    if base:
        return [base]
    return []


def _parse_missing_tokens(note: str) -> list[str]:
    for prefix in ("Частично: нет ", "Не заполнено: "):
        if prefix in note:
            tail = note.split(prefix, 1)[1].rstrip(".")
            return [x.strip() for x in tail.split(",") if x.strip()]
    return []


def _section_articles_from_note(note: str, actual: str) -> list[str]:
    deficits: list[str] = []
    note_l = note.lower()
    actual_compact = actual.lower().replace(" ", "")
    if "не найден" in note_l:
        return [DEFICIT_ACTIONS["section_articles.not_found"]]
    if "название" in note_l:
        deficits.append("section_articles.title")
    if "сокращен" in note_l or "сокращён" in note_l:
        deficits.append("section_articles.abbrev")
    if "скрыт" in note_l or "о журнале" in note_l:
        deficits.append("section_articles.hide_about")
    if "редактор" in note_l or "editorrestriction=false" in actual_compact:
        deficits.append("section_articles.editor_restriction")
    # убрать дубли с сохранением порядка
    uniq: list[str] = []
    for d in deficits:
        if d not in uniq:
            uniq.append(d)
    return [DEFICIT_ACTIONS[d] for d in uniq] or [
        FRIENDLY_ACTION_BY_ID["pages.section_articles"]
    ]


def _checklist_from_actual(actual: str) -> list[str]:
    counts = {"ru": 0, "en": 0}
    for chunk in actual.split(","):
        chunk = chunk.strip().lower()
        if "=" not in chunk:
            continue
        lang, n = chunk.split("=", 1)
        try:
            counts[lang.strip()] = int(n.strip())
        except ValueError:
            continue
    ru_ok = counts.get("ru", 0) >= 3
    en_ok = counts.get("en", 0) >= 3
    if ru_ok and not en_ok:
        return [DEFICIT_ACTIONS["checklist.en"]]
    if en_ok and not ru_ok:
        return [DEFICIT_ACTIONS["checklist.ru"]]
    return [DEFICIT_ACTIONS["checklist.both"]]


def _ui_status_for(iid: str, status: str) -> str:
    if status in {"ok", "na"}:
        return status
    if status == "manual":
        return "manual"
    if iid in {"modules.webfeed", "metrics.plumx"} and status in {
        "fail",
        "warn",
    }:
        return "manual"
    if iid == "step1.issn" and status == "warn":
        return "manual"
    return status


def _bucket(ui_status: str, severity: str, iid: str) -> str:
    if ui_status == "na":
        return "na"
    if ui_status == "ok":
        return "ok"
    if ui_status == "manual":
        return "manual"
    if severity == "required":
        if ui_status == "fail":
            return "fix"
        if ui_status == "warn":
            return "partial"
        return "fix"
    if ui_status in {"fail", "warn", "manual"}:
        return "rec"
    return "ok"


def _dedupe_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        iid = str(row.get("id") or "")
        if iid in seen:
            continue
        skip = False
        for group in _DEDUPE_GROUPS:
            if iid in group and (group & seen):
                skip = True
                break
        if skip:
            continue
        seen.add(iid)
        out.append(dict(row))
    return out


def _clean_requirement_title(title: str) -> str:
    text = title.strip()
    while text and (text[0].isdigit() or text[0] in ". "):
        if text[0].isdigit() or text[0] == ".":
            text = text[1:].lstrip(" .")
            continue
        break
    return text or title


def _fallback_action(row: Mapping[str, Any]) -> str:
    note = str(row.get("note") or "").strip()
    title = _clean_requirement_title(str(row.get("title") or "Пункт"))
    if note:
        return f"{title}: {_humanize_note(note)}"
    return title


def _detected_label(row: Mapping[str, Any]) -> str:
    actual = str(row.get("actual") or "").strip()
    note = str(row.get("note") or "").strip()
    iid = str(row.get("id") or "")

    if iid == "step3.checklist" and actual:
        parts: list[str] = []
        for chunk in actual.split(","):
            chunk = chunk.strip()
            if "=" not in chunk:
                continue
            lang, n = chunk.split("=", 1)
            lang = lang.strip().lower()
            lang_label = {"ru": "русский", "en": "английский"}.get(lang, lang.upper())
            try:
                num = int(n.strip())
            except ValueError:
                parts.append(chunk)
                continue
            if num <= 0:
                parts.append(f"{lang_label}: отсутствует")
            else:
                parts.append(f"{lang_label}: {num} {_plural_points(num)}")
        if parts:
            return "; ".join(parts)

    if iid == "step1.issn" and actual:
        return actual.replace("printIssn", "печатный ISSN").replace("onlineIssn", "онлайн ISSN")

    if iid == "pages.section_articles" and actual:
        if "только редакторы" in actual or "скрыт в" in actual:
            return actual
        bits = []
        low = actual.lower().replace(" ", "")
        if "editorrestriction=false" in low:
            bits.append("ограничение «только редакторы»: нет")
        if "editorrestriction=true" in low:
            bits.append("ограничение «только редакторы»: да")
        if "hideabout=false" in low:
            bits.append("скрытие в «О журнале»: нет")
        if "hideabout=true" in low:
            bits.append("скрытие в «О журнале»: да")
        if bits:
            return "; ".join(bits)

    if iid == "step4.access" and actual:
        return _humanize_access_actual(actual)

    locale_keys = _humanize_setting_locale_actual(actual)
    if locale_keys:
        return locale_keys

    if actual:
        return _humanize_actual_generic(actual)
    if note:
        return _humanize_note(note)
    return "—"


def _humanize_setting_locale_actual(actual: str) -> str:
    """homeHeaderTitle/ru, homeHeaderTitle/en → «колонтитул главной: русский, английский»."""
    labels = {
        "homeHeaderTitle": "колонтитул главной",
        "pageHeaderTitle": "колонтитул всех страниц",
        "abbreviation": "сокращённое название",
        "publisherInstitution": "наименование издателя",
        "publisherNote": "сведения об издателе",
        "searchKeywords": "ключевые слова",
        "authorGuidelines": "правила для авторов",
        "reviewPolicy": "принципы рецензирования",
        "focusAndScope": "предметная область",
        "description": "описание на главной",
    }
    lang_labels = {"ru": "русский", "en": "английский"}
    by_field: dict[str, list[str]] = {}
    for chunk in actual.split(","):
        token = chunk.strip()
        if "/" not in token or "=" in token:
            return ""
        field, lang = token.split("/", 1)
        field = field.strip()
        lang = lang.strip().lower()
        if field not in labels or lang not in lang_labels:
            return ""
        by_field.setdefault(field, []).append(lang_labels[lang])
    if not by_field:
        return ""
    parts = []
    for field, langs in by_field.items():
        parts.append(f"{labels[field]}: {', '.join(langs)}")
    return "; ".join(parts)


def _humanize_access_actual(actual: str) -> str:
    """disableUserReg=False, author=True… → читаемый текст."""
    low = actual.lower().replace(" ", "")
    # уже человекочитаемый формат
    if "регистрация" in actual.lower() and "автор" in actual.lower():
        return actual

    def _flag(name: str) -> bool | None:
        for token in (f"{name}=true", f"{name}=false", f"{name}=да", f"{name}=нет"):
            if token in low:
                return token.endswith("true") or token.endswith("да")
        return None

    disable = _flag("disableuserreg")
    author = _flag("author")
    reader = _flag("reader")
    reviewer = _flag("reviewer")
    if disable is None and author is None:
        return _humanize_actual_generic(actual)

    parts: list[str] = []
    if disable is True:
        parts.append("регистрация пользователей отключена")
    elif disable is False:
        parts.append("регистрация открыта")
    roles = []
    if author is not None:
        roles.append(f"автор — {'да' if author else 'нет'}")
    if reader is not None:
        roles.append(f"читатель — {'да' if reader else 'нет'}")
    if reviewer is not None:
        roles.append(f"рецензент — {'да' if reviewer else 'нет'}")
    if roles:
        parts.append("; ".join(roles))
    return "; ".join(parts) if parts else _humanize_actual_generic(actual)


def _humanize_actual_generic(actual: str) -> str:
    """Заменить техярлыки True/False и известные ключи на понятный текст."""
    text = actual
    replacements = (
        ("disableUserReg", "регистрация отключена"),
        ("enablePageNumber", "пагинация"),
        ("displayCurrentIssue", "текущий выпуск на главной"),
        ("rtEnabled", "инструменты читателя"),
        ("libraryMode", "режим библиотеки"),
        ("displayPage", "страница отображения"),
        ("licenseURL", "ссылка на лицензию"),
        ("emailFrom", "исходящая почта"),
        ("hideAbout", "скрыт в «О журнале»"),
        ("editorRestriction", "только редакторы"),
        ("primary", "контактному лицу"),
        ("specified", "отдельный адрес"),
        ("address", "адрес"),
        ("holder", "правообладатель"),
        ("True", "да"),
        ("False", "нет"),
        ("true", "да"),
        ("false", "нет"),
        ("=yes", "=есть"),
        ("=no", "=нет"),
        ("author=", "автор="),
        ("reader=", "читатель="),
        ("reviewer=", "рецензент="),
        ("volume=", "том="),
        ("number=", "номер="),
        ("year=", "год="),
        ("title=", "название="),
        ("ru=", "русский="),
        ("en=", "английский="),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    # аккуратно заменить = на « — » в простых парах
    if "=" in text and " — " not in text:
        text = text.replace("=", " — ")
    return text


def _board_from_actual(actual: str, status: str) -> list[str]:
    # русский: 12 симв.; английский: 0 симв.  или  ru=12 симв., en=0
    ru_n = en_n = 0
    match_ru = re.search(r"(?:ru|русский)\s*[:=]\s*(\d+)", actual, re.I)
    match_en = re.search(r"(?:en|английский)\s*[:=]\s*(\d+)", actual, re.I)
    if match_ru:
        ru_n = int(match_ru.group(1))
    if match_en:
        en_n = int(match_en.group(1))
    if ru_n >= 40 and en_n < 40:
        return [DEFICIT_ACTIONS["board.en"]]
    if en_n >= 40 and ru_n < 40:
        return [DEFICIT_ACTIONS["board.ru"]]
    return [DEFICIT_ACTIONS["board.both"]]


def _tech_detail(row: Mapping[str, Any]) -> str:
    bits = []
    note = str(row.get("note") or "").strip()
    actual = str(row.get("actual") or "").strip()
    deficits = row.get("deficits") or []
    if note:
        bits.append(note)
    if actual:
        bits.append(f"факт: {actual}")
    if deficits:
        bits.append("дефициты: " + ", ".join(str(x) for x in deficits))
    return " · ".join(bits)


def _plural_points(n: int) -> str:
    n_abs = abs(n) % 100
    n1 = n_abs % 10
    if 11 <= n_abs <= 14:
        return "пунктов"
    if n1 == 1:
        return "пункт"
    if 2 <= n1 <= 4:
        return "пункта"
    return "пунктов"


def _safe_http_url(url: str) -> str:
    text = (url or "").strip()
    if text.startswith("https://") or text.startswith("http://"):
        return text
    return ""


def _humanize_note(note: str) -> str:
    text = note
    replacements = (
        ("boardCustomText", "текст редакции"),
        ("focusAndScope", "предметная область"),
        ("authorGuidelines", "правила для авторов"),
        ("reviewPolicy", "принципы рецензирования"),
        ("copyrightNotice", "уведомление об авторских правах"),
        ("searchKeywords", "ключевые слова"),
        ("pageHeaderTitle", "название в колонтитуле"),
        ("publisherInstitution", "название издателя"),
        ("publisherNote", "сведения об издателе"),
        ("elibraryId", "идентификатор eLIBRARY.RU"),
        ("abbreviation", "сокращённое название"),
        ("printIssn", "печатный ISSN"),
        ("onlineIssn", "онлайн ISSN"),
        ("hideAbout", "скрытие в «О журнале»"),
        ("editorRestriction", "ограничение «только редакторы»"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    if "Не заполнено:" in text:
        return "Не заполнено на русском и/или английском"
    if "Не хватает:" in text:
        return "Заполнены не все сведения"
    return text
