"""Проверка экспорта OJS .data по чек-листу «Настройка журнала по умолчанию»."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from ipsas.modules.journal_site.data_export import JournalDataExport, html_to_text
from ipsas.modules.journal_site.default_setup_checklist import (
    ALLOWED_ENABLED_GENERIC,
    DEFAULT_SETUP_CHECKLIST,
    SECTION_TITLES,
    ChecklistItem,
)
from ipsas.modules.journal_site.parser import norm_space, preview_value

_ISSN_RE = re.compile(r"\d{4}-?\d{3}[\dXx]")
_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
_MAP_RE = re.compile(r"(google\.com/maps|yandex\.(ru|com)/maps|maps\.google|iframe)", re.I)
_SCOPUS_RE = re.compile(r"scopus", re.I)


def build_data_report_dict(
    export: JournalDataExport,
    *,
    source_name: str = "",
) -> dict[str, Any]:
    """Отчёт по чек-листу настройки журнала (режим .data)."""
    from ipsas.modules.journal_site.checklist_messages import (
        build_letter_payload,
        build_staff_summary,
    )

    ctx = _detect_flags(export)
    results = [evaluate_checklist_item(export, item, ctx) for item in DEFAULT_SETUP_CHECKLIST]
    staff = build_staff_summary(results)
    rows = staff["rows"]

    sections: list[dict[str, Any]] = []
    by_sec: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_sec.setdefault(r["section"], []).append(r)
    for sec_id, title in SECTION_TITLES.items():
        items = by_sec.get(sec_id) or []
        if not items:
            continue
        app = [i for i in items if i["status"] != "na"]
        sec_ok = sum(1 for i in app if i.get("ui_status") == "ok")
        sections.append(
            {
                "id": sec_id,
                "title": title,
                "entries": items,
                "ok": sec_ok,
                "total": len(app),
                "completeness_percent": round(100.0 * sec_ok / len(app), 1) if app else 0.0,
            }
        )

    print_issn = str(export.get_raw_setting("printIssn") or "").strip()
    online_issn = str(export.get_raw_setting("onlineIssn") or "").strip()
    issn_display = " / ".join(x for x in (print_issn, online_issn) if x) or export.path

    title = export.journal_title("ru") or export.journal_title("en") or export.path
    mandatory_pct = float(staff["mandatory_percent"])
    must_fix = staff["must_fix"]
    level = staff["conclusion_level"]
    level_labels = {
        "ok": "готово к завершению проверки",
        "warning": "нужна уточняющая проверка",
        "error": "настройки требуют доработки",
    }

    out = {
        "journal_url": "",
        "base_url": export.path,
        "generated_at": datetime.now().strftime("%d.%m.%Y, %H:%M"),
        "journal_title": title,
        "journal_issn": issn_display,
        "check_mode": "data",
        "checklist_name": "Настройка журнала по умолчанию",
        "completeness_percent": mandatory_pct,
        "completeness_ru": mandatory_pct,
        "completeness_en": mandatory_pct,
        "quality_percent": mandatory_pct,
        "level": level,
        "level_label": level_labels.get(level, level),
        "conclusion": staff["conclusion"],
        "locales": {},
        "fields": rows,
        "pages": {},
        "by_category": sections,
        "checklist_sections": sections,
        "by_requirement": {},
        "must_fix": must_fix,
        "staff_must_fix": must_fix,
        "staff_partial": staff["partial"],
        "staff_manual": staff["manual"],
        "staff_recommendations": staff["recommendations_open"],
        "recommendations": staff["recommendations_open"],
        "required_filled": staff["mandatory_done"],
        "required_total": staff["mandatory_total"],
        "mandatory_percent": mandatory_pct,
        "mandatory_done": staff["mandatory_done"],
        "mandatory_total": staff["mandatory_total"],
        "must_fix_count": staff["must_fix_count"],
        "fail_check_count": staff.get("fail_check_count", staff["must_fix_count"]),
        "partial_count": staff["partial_count"],
        "manual_count": staff["manual_count"],
        "recommendations_open_count": staff["recommendations_open_count"],
        "summary": {
            "completeness_percent": mandatory_pct,
            "must_fix": must_fix,
            "recommendations": staff["recommendations_open"],
            "required_filled": staff["mandatory_done"],
            "required_total": staff["mandatory_total"],
            "conclusion": staff["conclusion"],
        },
        "filled_count": staff["mandatory_done"],
        "weak_count": staff["partial_count"] + staff["manual_count"],
        "empty_count": 0,
        "missing_count": staff["must_fix_count"],
        "error_count": 0,
        "data_source": {
            "kind": "ojs_data",
            "filename": source_name,
            **export.to_summary(),
            "flags": ctx,
            "issn": issn_display,
        },
        "plugins_results": [r for r in rows if str(r.get("section", "")).startswith("modules")],
        "plugins_must_fix": [
            r
            for r in must_fix
            if str(r.get("section", "")).startswith("modules")
        ],
        "context_flags": ctx,
    }
    out["letter_payload"] = build_letter_payload(out)
    return out


def evaluate_checklist_item(
    export: JournalDataExport,
    item: ChecklistItem,
    ctx: dict[str, bool],
) -> dict[str, Any]:
    handlers = {
        "locale_flags": _eval_locale_flags,
        "setting_text": _eval_setting_text,
        "setting_bool": _eval_setting_bool,
        "setting_present_or_empty_ok": _eval_present_or_empty,
        "plugin": _eval_plugin,
        "plugin_conditional": _eval_plugin_conditional,
        "sections_articles": _eval_sections_articles,
        "sections_other": _eval_sections_other,
        "reader_tools": _eval_reader_tools,
        "email_outgoing": _eval_email_outgoing,
        "copyright_license": _eval_copyright_license,
        "submission_checklist": _eval_submission_checklist,
        "library_mode": _eval_library_mode,
        "issue_identification": _eval_issue_identification,
        "browse_plugin": _eval_browse_plugin,
        "webfeed_plugin": _eval_webfeed_plugin,
        "extra_generic_plugins": _eval_extra_generic,
        "dates_display": _eval_dates_display,
        "custom_about": _eval_custom_about,
        "board": _eval_board,
        "map_address": _eval_map_address,
        "manual": _eval_manual,
    }
    fn = handlers.get(item.kind, _eval_manual)
    raw = fn(export, item, ctx)
    if len(raw) == 4:
        status, note, actual, deficits = raw
    else:
        status, note, actual = raw
        deficits = []
    return {
        "id": item.id,
        "title": item.title,
        "section": item.section,
        "section_title": SECTION_TITLES.get(item.section, item.section),
        "status": status,
        "severity": item.severity,
        "note": note,
        "actual": actual,
        "deficits": list(deficits or []),
        "doc_url": item.doc_url,
        "value_preview": preview_value(actual, limit=160) if actual else "",
    }


def _detect_flags(export: JournalDataExport) -> dict[str, bool]:
    doi = _plugin_enabled(export, "DOIPubIdPlugin")
    prefix = ""
    info = _find_plugin(export, "DOIPubIdPlugin")
    if info:
        prefix = str(info.settings.get("doiPrefix") or "").strip()
    has_doi = bool(doi and prefix)

    scopus_hit = False
    for lang in ("ru", "en"):
        text = " ".join(
            [
                export.get_setting_text("searchKeywords", lang),
                export.find_custom_about(lang, "индексац", "indexing", "scopus"),
            ]
        )
        if _SCOPUS_RE.search(text):
            scopus_hit = True
            break

    return {
        "has_doi": has_doi,
        "in_scopus": scopus_hit,
        "has_en": "en" in export.supported_locales,
        "has_ru": "ru" in export.supported_locales,
    }


def _find_plugin(export: JournalDataExport, *names: str):
    lower_map = {k.lower(): v for k, v in export.plugins.items()}
    for name in names:
        hit = export.plugins.get(name) or lower_map.get(name.lower())
        if hit is not None:
            return hit
    return None


def _plugin_enabled(export: JournalDataExport, *names: str) -> bool:
    info = _find_plugin(export, *names)
    return bool(info and info.enabled)


def _truthy(value: Any) -> bool:
    if value is True or value == 1:
        return True
    if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}:
        return True
    return False


def _eval_locale_flags(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    lang = item.locales[0] if item.locales else "ru"
    prefix = "ru" if lang.startswith("ru") else "en"

    def _has(setting_name: str) -> bool:
        raw = export.get_raw_setting(setting_name)
        if not isinstance(raw, list):
            return False
        return any(str(x).lower().startswith(prefix) for x in raw)

    interface_ok = _has("supportedLocales")
    submission_ok = _has("supportedSubmissionLocales")
    forms_ok = _has("supportedFormLocales")
    parts = [
        f"интерфейс={'да' if interface_ok else 'нет'}",
        f"отправка={'да' if submission_ok else 'нет'}",
        f"формы={'да' if forms_ok else 'нет'}",
    ]
    actual = "; ".join(parts)
    if interface_ok and submission_ok and forms_ok:
        return "ok", "Локаль включена для интерфейса, отправки и форм.", actual
    return "fail", "Нужно включить локаль в интерфейсе, отправке рукописи и формах.", actual


def _eval_setting_text(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    locales = item.locales or ("",)
    missing: list[str] = []
    previews: list[str] = []
    for key in item.setting_keys:
        if not locales or locales == ("",):
            raw = export.get_raw_setting(key)
            text = html_to_text(str(raw)) if raw not in (None, "") else ""
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                text = str(raw)
            if "issn" in key.lower() and text and not _ISSN_RE.search(text):
                text = ""
            if len(text) < item.min_chars:
                missing.append(key)
            else:
                previews.append(f"{key}={preview_value(text, 40)}")
            continue
        for lang in locales:
            text = export.get_setting_text(key, lang)
            if key == "supportEmail" and not text:
                text = str(export.get_raw_setting("supportEmail") or "")
            if len(text) < item.min_chars:
                missing.append(f"{key}/{lang}")
            else:
                previews.append(f"{key}/{lang}")
    actual = ", ".join(previews) if previews else ""

    if item.id == "step1.issn":
        print_v = str(export.get_raw_setting("printIssn") or "").strip()
        online_v = str(export.get_raw_setting("onlineIssn") or "").strip()
        print_ok = bool(print_v and _ISSN_RE.search(print_v))
        online_ok = bool(online_v and _ISSN_RE.search(online_v))
        actual = f"печатный={'да' if print_ok else 'нет'}; онлайн={'да' if online_ok else 'нет'}"
        if print_ok and online_ok:
            return "ok", "Указаны печатный и онлайн ISSN.", actual, []
        if print_ok or online_ok:
            miss = "печатный" if not print_ok else "онлайн"
            deficit = "issn.print" if not print_ok else "issn.online"
            return (
                "warn",
                f"Указан только один ISSN; уточните, нужен ли {miss}.",
                actual,
                [deficit],
            )
        return "fail", "ISSN не указан.", actual, ["issn.any"]

    if not missing:
        return "ok", "Заполнено.", actual, []
    if previews and missing:
        return "warn", f"Частично: нет {', '.join(missing)}.", actual, list(missing)
    return "fail", f"Не заполнено: {', '.join(missing)}.", actual, list(missing)


def _eval_setting_bool(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    if item.id == "step3.submission_ack":
        primary = _truthy(export.get_raw_setting("copySubmissionAckPrimaryContact"))
        specified = _truthy(export.get_raw_setting("copySubmissionAckSpecified"))
        addr = str(export.get_raw_setting("copySubmissionAckAddress") or "").strip()
        ok = primary or (specified and bool(addr))
        actual = (
            f"контактному лицу={'да' if primary else 'нет'}; "
            f"адрес копии={'да' if (specified and bool(addr)) else 'нет'}"
        )
        if ok:
            return "ok", "Уведомление о подаче настроено.", actual, []
        return (
            "fail",
            "Включите уведомление контактному лицу или укажите адрес копии.",
            actual,
            ["submission_ack"],
        )
    if item.id == "step4.access":
        disable = _truthy(export.get_raw_setting("disableUserReg"))
        author = _truthy(export.get_raw_setting("allowRegAuthor"))
        reader = _truthy(export.get_raw_setting("allowRegReader"))
        reviewer = _truthy(export.get_raw_setting("allowRegReviewer"))
        roles = []
        roles.append(f"автор — {'да' if author else 'нет'}")
        roles.append(f"читатель — {'да' if reader else 'нет'}")
        roles.append(f"рецензент — {'да' if reviewer else 'нет'}")
        if disable:
            actual = "регистрация пользователей отключена; " + "; ".join(roles)
            return "fail", "Глобально отключена регистрация пользователей.", actual
        actual = "регистрация открыта; " + "; ".join(roles)
        if author and reader and reviewer:
            return "ok", "Роли регистрации разрешены.", actual
        return "warn", "Проверьте роли регистрации (автор/читатель/рецензент).", actual

    if item.id == "step4.pagination":
        on = _truthy(export.get_raw_setting("enablePageNumber"))
        return (
            ("ok", "Пагинация включена.", "пагинация включена")
            if on
            else ("fail", "Включите пагинацию.", "пагинация выключена")
        )

    if item.id == "step5.current_issue":
        on = _truthy(export.get_raw_setting("displayCurrentIssue"))
        return (
            ("ok", "Показ текущего выпуска включён.", "текущий выпуск на главной: да")
            if on
            else (
                "fail",
                "Включите отображение текущего выпуска.",
                "текущий выпуск на главной: нет",
            )
        )

    # общий случай: все указанные bool должны быть True
    bad: list[str] = []
    for key in item.setting_keys:
        if not _truthy(export.get_raw_setting(key)):
            bad.append(key)
    actual = ", ".join(item.setting_keys)
    if not bad:
        return "ok", "Ок.", actual
    return "fail", f"Выключено: {', '.join(bad)}.", actual


def _eval_present_or_empty(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    chunks: list[str] = []
    for key in item.setting_keys:
        raw = export.get_raw_setting(key)
        if isinstance(raw, list):
            if raw:
                chunks.append(f"{key}: {len(raw)} шт.")
        else:
            for lang in ("ru", "en"):
                text = export.get_setting_text(key, lang)
                if text:
                    chunks.append(f"{key}/{lang}")
    if not chunks:
        return "ok", "Не заполнено — допустимо по чек-листу.", "пусто"
    return "ok", "Заполнено (проверьте корректность вручную при необходимости).", "; ".join(chunks)


def _eval_plugin(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    expect = item.expect_enabled
    info = _find_plugin(export, *item.plugin_keys)
    enabled = bool(info and info.enabled)
    name = info.name if info else item.plugin_keys[0]

    # ACRON — сайт/админский плагин: в .data журнала часто settings=[] без enabled
    if item.id == "modules.acron":
        if info is not None:
            return (
                "ok",
                "Плагин ACRON есть в установке (управляется администратором сайта).",
                name,
            )
        return (
            "warn",
            "ACRON не найден в экспорте — проверьте на уровне администратора сайта.",
            "",
        )

    if item.id == "modules.doi":
        if not enabled:
            return "fail", "DOI-плагин выключен или отсутствует.", name
        prefix = str((info.settings if info else {}).get("doiPrefix") or "").strip()
        if not prefix:
            return "fail", "DOI включён, но doiPrefix пуст.", name
        return "ok", f"DOI включён, prefix={prefix}.", f"{name}; {prefix}"

    if item.id == "modules.url_pubid" and info is None:
        return "ok", "Плагин URL pubIds отсутствует — считаем выключенным.", "нет в экспорте"

    if expect is True:
        if enabled:
            return "ok", f"Плагин «{name}» включён.", name
        return "fail", f"Ожидается включённым ({'/'.join(item.plugin_keys)}).", name
    if expect is False:
        if enabled:
            return "fail", f"Должен быть выключен, но включён ({name}).", name
        return "ok", "Выключен или отсутствует — ок.", name
    return "manual", item.note_hint or "Проверьте вручную.", name


def _eval_plugin_conditional(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    cond = item.condition or ""
    cond_true = bool(ctx.get(cond))
    enabled = _plugin_enabled(export, *item.plugin_keys)
    name = item.plugin_keys[0]

    if cond == "has_doi":
        if not cond_true:
            if enabled:
                return "warn", "DOI не настроен, но метрика включена — обычно лишнее.", name
            return "na", "DOI нет — пункт не применяется.", name
        if enabled:
            return "ok", "DOI есть, плагин включён.", name
        return "fail", "При наличии DOI плагин должен быть включён.", name

    if cond == "in_scopus":
        if not cond_true:
            if enabled:
                return "ok", "PlumX включён (Scopus в настройках не подтверждён автоматически).", name
            return "manual", "Включите PlumX, если журнал в Scopus (в .data признак не найден).", name
        if enabled:
            return "ok", "Найден Scopus, PlumX включён.", name
        return "fail", "Журнал в Scopus — включите PlumX.", name

    return "manual", "Условие не распознано.", name


def _eval_sections_articles(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple:
    from ipsas.modules.journal_site.data_export import _value_to_text

    articles = None
    for sec in export.sections:
        title_ru = _value_to_text(sec.get("title"), "ru").lower()
        title_en = _value_to_text(sec.get("title"), "en").lower()
        abbrev_en = _value_to_text(sec.get("abbrev"), "en").upper()
        if "стать" in title_ru or title_en == "articles" or abbrev_en in {"ART", "ARTICLES"}:
            articles = sec
            break
    if articles is None:
        return "fail", "Раздел «Статьи»/Articles не найден.", "", ["section_articles.not_found"]

    title_ru = _value_to_text(articles.get("title"), "ru")
    title_en = _value_to_text(articles.get("title"), "en")
    abbrev_ru = _value_to_text(articles.get("abbrev"), "ru")
    abbrev_en = _value_to_text(articles.get("abbrev"), "en")
    hide_about = str(articles.get("hideAbout") or "") in {"1", "true", "True"}
    editor_only = str(articles.get("editorRestriction") or "") in {"1", "true", "True"}

    deficits: list[str] = []
    problems: list[str] = []
    if not title_ru or not title_en:
        deficits.append("section_articles.title")
        problems.append("название")
    if not abbrev_ru or not abbrev_en:
        deficits.append("section_articles.abbrev")
        problems.append("сокращение")
    if not hide_about:
        deficits.append("section_articles.hide_about")
        problems.append("не скрыт в «О журнале»")
    if not editor_only:
        deficits.append("section_articles.editor_restriction")
        problems.append("нет ограничения «только редакторы»")

    actual = (
        f"название: {title_ru or '—'} / {title_en or '—'}; "
        f"сокращение: {abbrev_ru or '—'} / {abbrev_en or '—'}; "
        f"скрыт в «О журнале»: {'да' if hide_about else 'нет'}; "
        f"только редакторы: {'да' if editor_only else 'нет'}"
    )
    if deficits:
        return "fail", "Проблемы: " + ", ".join(problems) + ".", actual, deficits
    return "ok", "Раздел «Статьи» настроен по чек-листу.", actual, []


def _eval_sections_other(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    n = len(export.sections)
    if n >= 2:
        return "ok", f"Всего разделов: {n}.", str(n)
    if n == 1:
        return "warn", "Только один раздел — обычно нужны дополнительные рубрики.", "1"
    return "fail", "Разделы журнала не найдены.", "0"


def _eval_reader_tools(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    enabled = _truthy(export.get_raw_setting("rtEnabled"))
    # базовый набор по инструкции
    expected_on = (
        "rtAbstract",
        "rtViewMetadata",
        "rtSupplementaryFiles",
        "rtPrinterFriendly",
        "rtCaptureCite",
    )
    on = [k for k in expected_on if _truthy(export.get_raw_setting(k))]
    actual = (
        f"инструменты читателя={'включены' if enabled else 'выключены'}; "
        f"опций: {len(on)} из {len(expected_on)}"
    )
    if enabled and len(on) >= 4:
        return "ok", "Инструменты читателя включены.", actual
    if enabled:
        return "warn", "RT включены, но набор опций неполный — сверьте с инструкцией.", actual
    return "fail", "Включите инструменты читателя (Reading Tools).", actual


def _eval_email_outgoing(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    # emailFrom: 0 часто = глобальные настройки сайта
    email_from = export.get_raw_setting("emailFrom")
    addr = str(export.get_raw_setting("emailFromAddress") or export.get_raw_setting("emailFromEmail") or "").strip()
    actual = (
        f"исходящая почта журнала={'задана' if addr else 'не задана'}; "
        f"режим={'глобальные настройки сайта' if email_from in (0, '0', None, False) else 'настройки журнала'}"
    )
    if email_from in (0, "0", None, False) or not addr:
        # 0 / пусто → глобальные настройки
        return "ok", "Используются глобальные настройки сайта или пустой SMTP журнала.", actual
    if addr and _EMAIL_RE.search(addr):
        return "ok", "Исходящая почта журнала заполнена.", actual
    return "warn", "Проверьте настройки исходящей почты.", actual


def _eval_copyright_license(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple:
    notice_ru = export.get_setting_text("copyrightNotice", "ru")
    notice_en = export.get_setting_text("copyrightNotice", "en")
    holder_type = str(export.get_raw_setting("copyrightHolderType") or "").strip()
    license_url = str(export.get_raw_setting("licenseURL") or "").strip()
    holder_other = export.get_setting_text("copyrightHolderOther", "ru") or export.get_setting_text(
        "copyrightHolderOther", "en"
    )
    deficits: list[str] = []
    problems: list[str] = []
    if len(notice_ru) < 20:
        deficits.append("copyright.notice_ru")
        problems.append("текст на русском")
    if len(notice_en) < 20:
        deficits.append("copyright.notice_en")
        problems.append("текст на английском")
    if not holder_type:
        deficits.append("copyright.holder")
        problems.append("правообладатель")
    if holder_type == "other" and not holder_other:
        deficits.append("copyright.holder_other")
        problems.append("имя правообладателя")
    if not license_url and "creative commons" not in (notice_ru + notice_en).lower() and "cc by" not in (
        notice_ru + notice_en
    ).lower():
        deficits.append("copyright.license")
        problems.append("лицензия")
    holder_label = {
        "author": "автор",
        "journal": "журнал",
        "other": "иное",
    }.get(holder_type.lower(), holder_type or "не указан")
    actual = (
        f"правообладатель={holder_label}; "
        f"ссылка на лицензию={'есть' if license_url else 'нет'}"
    )
    if deficits:
        return "fail", "Не хватает: " + ", ".join(problems) + ".", actual, deficits
    return "ok", "Условия использования заполнены.", actual, []


def _eval_submission_checklist(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple:
    counts: dict[str, int] = {}
    for lang in ("ru", "en"):
        raw = export.get_raw_setting("submissionChecklist")
        items: list[Any] = []
        if isinstance(raw, dict):
            from ipsas.modules.journal_site.data_export import _pick_localized

            picked = _pick_localized(raw, lang)
            if isinstance(picked, list):
                items = picked
        elif isinstance(raw, list):
            items = raw
        counts[lang] = len(items)
    actual = ", ".join(f"{k}={v}" for k, v in counts.items())
    ru_n, en_n = counts.get("ru", 0), counts.get("en", 0)
    if ru_n >= 3 and en_n >= 3:
        return "ok", "Чек-лист требований к статье составлен на RU и EN.", actual, []
    if ru_n >= 3 and en_n < 3:
        return "warn", "Список есть не на обоих языках (не хватает EN).", actual, ["checklist.en"]
    if en_n >= 3 and ru_n < 3:
        return "warn", "Список есть не на обоих языках (не хватает RU).", actual, ["checklist.ru"]
    return "fail", "Нужен список требований по пунктам (RU и EN).", actual, ["checklist.both"]


def _eval_library_mode(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    on = _truthy(export.get_raw_setting("libraryMode"))
    url = str(export.get_raw_setting("originalSiteURL") or "").strip()
    actual = (
        f"режим библиотеки={'включён' if on else 'выключен'}; "
        f"URL исходного сайта={'указан' if url else 'не указан'}"
    )
    if not on:
        return "ok", "Режим библиотеки выключен.", actual
    if url.startswith("http"):
        return "ok", "Режим библиотеки включён, URL указан.", actual
    return "fail", "При включённом режиме библиотеки укажите URL исходного сайта.", actual


def _eval_issue_identification(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    flags = {
        "том": _truthy(export.get_raw_setting("publicationFormatVolume")),
        "номер": _truthy(export.get_raw_setting("publicationFormatNumber")),
        "год": _truthy(export.get_raw_setting("publicationFormatYear")),
        "название": _truthy(export.get_raw_setting("publicationFormatTitle")),
    }
    actual = "; ".join(f"{k} — {'да' if v else 'нет'}" for k, v in flags.items())
    if any(flags.values()):
        return "ok", "Формат идентификации выпусков задан.", actual
    return "fail", "Установите формат идентификации выпусков (том/номер/год).", actual


def _eval_browse_plugin(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    info = _find_plugin(export, "browseplugin")
    if not info or not info.enabled:
        return "fail", "Плагин «Браузер» выключен или отсутствует.", "browseplugin"
    # В типовом .data есть только enabled; browseBy в экспорт не попадает.
    browse_by = info.settings.get("browseBy") or info.settings.get("browseBlocks")
    if browse_by is not None and browse_by != "":
        if browse_by in {"sections", "section", "2", 2} or str(browse_by).lower() == "sections":
            return "ok", "Браузер включён, просмотр по разделам.", str(browse_by)
        return (
            "warn",
            f"Браузер включён; режим просмотра: {browse_by}.",
            str(browse_by),
        )
    return "ok", "Плагин «Браузер» включён.", "enabled"


def _eval_webfeed_plugin(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    info = _find_plugin(export, "webfeedplugin")
    if not info or not info.enabled:
        return "fail", "Новостная лента выпуска выключена.", "webfeedplugin"
    page = str(info.settings.get("displayPage") or "").lower()
    page_labels = {
        "issue": "страницы выпуска",
        "issuepage": "страницы выпуска",
        "currentissue": "текущий выпуск",
        "homepage": "главная страница",
        "all": "все страницы",
        "": "не задано",
    }
    page_label = page_labels.get(page, page or "не задано")
    actual = f"отображение: {page_label}"
    if page in {"issue", "issuepage", "currentissue"}:
        return "ok", "Лента на страницах выпуска.", actual
    if page in {"homepage", "all", ""}:
        return "warn", "Ожидаются страницы выпуска / текущий выпуск; сейчас: " + page_label + ".", actual
    return "warn", f"Проверьте страницу отображения ленты ({page_label}).", actual


def _eval_extra_generic(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    extras: list[str] = []
    for name, info in export.plugins.items():
        if info.category not in {"generic", ""}:
            continue
        if not info.enabled:
            continue
        if name.lower() in ALLOWED_ENABLED_GENERIC:
            continue
        # метрики / pubIds не сюда
        if info.category in {"metrics", "pubIds", "importexport", "blocks", "gateways"}:
            continue
        extras.append(name)
    if not extras:
        return "ok", "Лишних включённых основных модулей не найдено.", ""
    return "warn", "Включены модули вне эталона: " + ", ".join(sorted(extras)[:12]) + ".", ", ".join(extras[:8])


def _eval_dates_display(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    keys = (
        "displayIssuePublishDate",
        "displaySubmissionPublishDate",
        "displaySubmissionPublishOnlineDate",
        "displaySubmissionSubmitDate",
        "displaySubmissionAcceptDate",
    )
    on = [k for k in keys if _truthy(export.get_raw_setting(k))]
    actual = ", ".join(on) if on else "ничего не выбрано"
    if on:
        return "ok", f"Выбраны даты: {len(on)}.", actual
    return "fail", "В шаге 5.9 выберите даты для отображения.", actual


def _eval_custom_about(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    texts: list[str] = []
    for key in item.setting_keys:
        for lang in ("ru", "en"):
            t = export.get_setting_text(key, lang)
            if t:
                texts.append(t)
    for lang in ("ru", "en"):
        t = export.find_custom_about(
            lang,
            "focus and scope",
            "aims and scope",
            "тематика",
            "цели и задачи",
            "предметн",
        )
        if t:
            texts.append(t)
    body = norm_space(" ".join(texts))
    if len(body) >= 40:
        return "ok", "Предметная область / цели найдены.", preview_value(body, 80)
    return "fail", "Заполните предметную область и цели (focusAndScope или custom about).", ""


def _eval_board(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple:
    ru = export.get_setting_text("boardCustomText", "ru")
    en = export.get_setting_text("boardCustomText", "en")
    actual = f"русский: {len(ru)} симв.; английский: {len(en)} симв."
    if len(ru) >= 40 and len(en) >= 40:
        return "ok", "Текст редакции заполнен на русском и английском.", actual, []
    if len(ru) >= 40 and len(en) < 40:
        return "warn", "Редакция заполнена не на обоих языках.", actual, ["board.en"]
    if len(en) >= 40 and len(ru) < 40:
        return "warn", "Редакция заполнена не на обоих языках.", actual, ["board.ru"]
    return "fail", "Заполните редакцию на русском и английском.", actual, ["board.both"]


def _eval_map_address(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    from ipsas.modules.journal_site.data_export import _pick_localized

    problems: list[str] = []
    has_map = False
    for lang in ("ru", "en"):
        text = export.get_setting_text("mailingAddress", lang)
        raw = export.get_raw_setting("mailingAddress")
        html = ""
        if isinstance(raw, dict):
            picked = _pick_localized(raw, lang)
            html = str(picked or "")
        elif isinstance(raw, str):
            html = raw
        if len(text) < 10 and "iframe" not in html.lower():
            problems.append(f"адрес/{lang}")
        if html and _MAP_RE.search(html):
            has_map = True
    if problems:
        return "fail", "Не заполнен почтовый адрес: " + ", ".join(problems) + ".", ""
    if not has_map:
        return "warn", "Адрес есть, карту (iframe maps) не нашли.", "адрес без карты"
    return "ok", "Адрес и карта найдены.", "ok"


def _eval_manual(
    export: JournalDataExport, item: ChecklistItem, ctx: dict[str, bool]
) -> tuple[str, str, str]:
    return "manual", item.note_hint or "Проверьте вручную по инструкции.", ""
