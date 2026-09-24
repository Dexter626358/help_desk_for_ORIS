"""Шаги базовой настройки журнала в песочнице."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Callable

from ipsas.modules.eng_metadata.platform_auth import PlatformAuthClient
from ipsas.modules.journal_site.default_setup_checklist import ALLOWED_ENABLED_GENERIC
from ipsas.modules.sandbox_journal_setup.forms import (
    find_post_form,
    plugin_action_links,
)
from ipsas.modules.sandbox_journal_setup.urls import journal_url

logger = logging.getLogger(__name__)

REQUIRED_LOCALES = frozenset({"ru_RU", "en_US"})
LOCALE_FIELD_NAMES = (
    "supportedLocales[]",
    "supportedSubmissionLocales[]",
    "supportedFormLocales[]",
)

SECTION_RU_TITLE = "Статьи"
SECTION_RU_ABBREV = "СТАТ"
SECTION_EN_TITLE = "Articles"
SECTION_EN_ABBREV = "ART"

CITATION_PARSER_TEMPLATE_IDS = ("26", "27", "28", "29")  # FreeCite, ParsCit, ParaCite, RegEx
VANCOUVER_OUTPUT_ID = "22"

METRICS_ENABLE = frozenset(
    {"dimensionsplugin", "crossrefcitedbyplugin", "almplugin"}
)
METRICS_DISABLE = frozenset(
    {"altmetricsplugin", "publonsbadgeplugin", "publonsplugin"}
)
PUBIDS_ENABLE = frozenset({"doipubidplugin", "ednpubidplugin"})


@dataclass
class StepResult:
    id: str
    title: str
    ok: bool
    message: str
    changed: bool = False
    dry_run: bool = True


def _delay(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def step_languages(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    url = journal_url(base_url, journal, "manager/languages")
    html = auth.get_text(url)
    form = find_post_form(html, url, action_contains="saveLanguageSettings")
    if form is None:
        return StepResult("languages", "Языки", False, "Форма saveLanguageSettings не найдена")

    before = {name: set(form.values_for(name)) for name in LOCALE_FIELD_NAMES}
    for name in LOCALE_FIELD_NAMES:
        form.ensure_multi_values(name, set(REQUIRED_LOCALES), keep_others=True)
    after = {name: set(form.values_for(name)) for name in LOCALE_FIELD_NAMES}
    need = before != after
    # primaryLocale оставить как есть (часто ru_RU)
    if not need:
        return StepResult(
            "languages",
            "Языки RU/EN",
            True,
            "ru_RU и en_US уже во всех трёх колонках",
            changed=False,
            dry_run=dry_run,
        )
    detail = "; ".join(f"{k}: {sorted(after[k])}" for k in LOCALE_FIELD_NAMES)
    if dry_run:
        return StepResult(
            "languages",
            "Языки RU/EN",
            True,
            f"dry-run: будет POST saveLanguageSettings ({detail})",
            changed=True,
            dry_run=True,
        )
    status, _, _ = auth.request(form.action, data=form.fields)
    _delay(delay)
    if status >= 400:
        return StepResult("languages", "Языки RU/EN", False, f"HTTP {status}")
    return StepResult(
        "languages",
        "Языки RU/EN",
        True,
        f"Сохранено ({detail})",
        changed=True,
        dry_run=False,
    )


def _find_articles_section(html: str) -> tuple[str | None, str]:
    """Вернуть (edit_url_or_None, label)."""
    from lxml import html as lxml_html

    root = lxml_html.fromstring(html)
    rows = root.xpath(".//tr[starts-with(@id,'section-')]")
    candidates: list[tuple[str, str, str]] = []
    for row in rows:
        sid = (row.get("id") or "").replace("section-", "")
        cells = ["".join(td.itertext()).strip() for td in row.xpath("./td")]
        title = cells[0] if cells else ""
        abbrev = cells[1] if len(cells) > 1 else ""
        hrefs = row.xpath(".//a[contains(@href,'editSection')]/@href")
        if not hrefs:
            continue
        candidates.append((hrefs[0], title, abbrev))
        joined = f"{title} {abbrev}".casefold()
        if "стат" in joined or "article" in joined:
            return hrefs[0], f"{title}/{abbrev} (id={sid})"
    if candidates:
        href, title, abbrev = candidates[0]
        return href, f"{title}/{abbrev} (первый раздел)"
    return None, ""


def step_section_articles(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    list_url = journal_url(base_url, journal, "manager/sections")
    html = auth.get_text(list_url)
    edit_url, label = _find_articles_section(html)
    created = False
    if edit_url is None:
        create_url = journal_url(base_url, journal, "manager/createSection")
        if dry_run:
            return StepResult(
                "section",
                "Раздел Статьи",
                True,
                f"dry-run: раздела нет — создать и заполнить RU/EN",
                changed=True,
                dry_run=True,
            )
        html = auth.get_text(create_url)
        form = find_post_form(html, create_url, action_contains="updateSection")
        if form is None:
            # create may post to createSection/save
            form = find_post_form(html, create_url)
        if form is None:
            return StepResult("section", "Раздел Статьи", False, "Форма создания раздела не найдена")
        created = True
    else:
        html = auth.get_text(edit_url)
        form = find_post_form(html, edit_url, action_contains="updateSection")
        if form is None:
            return StepResult(
                "section",
                "Раздел Статьи",
                False,
                f"Форма updateSection не найдена ({label})",
            )

    # RU + EN поля (OJS принимает оба locale в одном POST)
    form.set_value("title[ru_RU]", SECTION_RU_TITLE)
    form.set_value("abbrev[ru_RU]", SECTION_RU_ABBREV)
    form.set_value("policy[ru_RU]", "")
    form.set_value("title[en_US]", SECTION_EN_TITLE)
    form.set_value("abbrev[en_US]", SECTION_EN_ABBREV)
    form.set_value("policy[en_US]", "")
    form.set_checkbox("hideAbout", True, "1")
    form.set_checkbox("editorRestriction", True, "1")
    # снять metaReviewed / hideTitle и т.п. — оставляем как были, кроме целевых

    summary = (
        f"RU «{SECTION_RU_TITLE}/{SECTION_RU_ABBREV}», "
        f"EN «{SECTION_EN_TITLE}/{SECTION_EN_ABBREV}», "
        f"policy очищен, hideAbout, editorRestriction"
    )
    if dry_run:
        return StepResult(
            "section",
            "Раздел Статьи",
            True,
            f"dry-run: {label or 'новый раздел'} — {summary}",
            changed=True,
            dry_run=True,
        )
    status, _, body = auth.request(form.action, data=form.fields)
    _delay(delay)
    if status >= 400:
        return StepResult("section", "Раздел Статьи", False, f"HTTP {status}")
    # если EN не сохранилось через один POST — дописать через formLocale
    verify = auth.get_text(edit_url or form.action.replace("updateSection", "editSection"))
    if "title[en_US]" not in verify and "Articles" not in verify:
        _save_section_locale(
            auth,
            edit_url=edit_url or _find_articles_section(auth.get_text(list_url))[0] or "",
            locale="en_US",
            title=SECTION_EN_TITLE,
            abbrev=SECTION_EN_ABBREV,
            delay=delay,
        )
    return StepResult(
        "section",
        "Раздел Статьи",
        True,
        ("Создан и настроен: " if created else "Обновлён: ") + summary,
        changed=True,
        dry_run=False,
    )


def _save_section_locale(
    auth: PlatformAuthClient,
    *,
    edit_url: str,
    locale: str,
    title: str,
    abbrev: str,
    delay: float,
) -> None:
    if not edit_url:
        return
    html = auth.get_text(edit_url)
    form = find_post_form(html, edit_url, action_contains="updateSection")
    if form is None:
        return
    form.set_value("formLocale", locale)
    # смена локали часто требует промежуточного POST — пробуем сразу с полями
    form.set_value(f"title[{locale}]", title)
    form.set_value(f"abbrev[{locale}]", abbrev)
    form.set_value(f"policy[{locale}]", "")
    form.set_checkbox("hideAbout", True, "1")
    form.set_checkbox("editorRestriction", True, "1")
    auth.request(form.action, data=form.fields)
    _delay(delay)


def step_reader_tools(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    url = journal_url(base_url, journal, "rtadmin/settings")
    html = auth.get_text(url)
    form = find_post_form(html, url, action_contains="saveSettings")
    if form is None:
        return StepResult("rt", "Инструменты читателя", False, "Форма rtadmin/saveSettings не найдена")
    # enabled checkbox value="1"; checked → уже в fields
    enabled_now = bool(form.values_for("enabled"))
    form.set_checkbox("enabled", True, "1")
    if enabled_now:
        return StepResult(
            "rt",
            "Инструменты читателя",
            True,
            "Уже включены",
            changed=False,
            dry_run=dry_run,
        )
    if dry_run:
        return StepResult(
            "rt",
            "Инструменты читателя",
            True,
            "dry-run: будет включено (enabled=1)",
            changed=True,
            dry_run=True,
        )
    status, _, _ = auth.request(form.action, data=form.fields)
    _delay(delay)
    if status >= 400:
        return StepResult("rt", "Инструменты читателя", False, f"HTTP {status}")
    return StepResult(
        "rt",
        "Инструменты читателя",
        True,
        "Включены",
        changed=True,
        dry_run=False,
    )


def _toggle_plugins(
    auth: PlatformAuthClient,
    *,
    page_url: str,
    to_enable: frozenset[str],
    to_disable: frozenset[str] = frozenset(),
    dry_run: bool,
    delay: float,
    step_id: str,
    title: str,
) -> StepResult:
    html = auth.get_text(page_url)
    links = plugin_action_links(html)
    actions: list[str] = []
    for key in sorted(to_enable):
        info = links.get(key) or links.get(key.replace("plugin", ""))
        # keys in page are like DOIPubIdPlugin → lower doipubidplugin
        matched = None
        for k, v in links.items():
            if k == key or k.endswith(key) or key in k:
                matched = v
                break
        if matched is None:
            actions.append(f"{key}: не найден")
            continue
        if "disable" in matched:
            actions.append(f"{matched.get('key', key)}: уже включён")
            continue
        enable_url = matched.get("enable")
        if not enable_url:
            actions.append(f"{key}: нет ссылки enable")
            continue
        actions.append(f"ENABLE {matched.get('key', key)}")
        if not dry_run:
            status, _, _ = auth.request(enable_url, method="GET")
            _delay(delay)
            if status >= 400:
                return StepResult(step_id, title, False, f"enable {key}: HTTP {status}")

    for key in sorted(to_disable):
        matched = None
        for k, v in links.items():
            if k == key or key in k:
                matched = v
                break
        if matched is None:
            continue
        if "enable" in matched and "disable" not in matched:
            actions.append(f"{matched.get('key', key)}: уже выключен")
            continue
        disable_url = matched.get("disable")
        if disable_url:
            actions.append(f"DISABLE {matched.get('key', key)}")
            if not dry_run:
                status, _, _ = auth.request(disable_url, method="GET")
                _delay(delay)
                if status >= 400:
                    return StepResult(step_id, title, False, f"disable {key}: HTTP {status}")

    changed = any(a.startswith("ENABLE") or a.startswith("DISABLE") for a in actions)
    prefix = "dry-run: " if dry_run and changed else ""
    return StepResult(
        step_id,
        title,
        True,
        prefix + "; ".join(actions) if actions else "нечего менять",
        changed=changed,
        dry_run=dry_run,
    )


def step_pubids(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    return _toggle_plugins(
        auth,
        page_url=journal_url(base_url, journal, "manager/plugins/pubIds"),
        to_enable=PUBIDS_ENABLE,
        dry_run=dry_run,
        delay=delay,
        step_id="pubids",
        title="DOI и EDN",
    )


def step_metrics(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    return _toggle_plugins(
        auth,
        page_url=journal_url(base_url, journal, "manager/plugins/metrics"),
        to_enable=METRICS_ENABLE,
        to_disable=METRICS_DISABLE,
        dry_run=dry_run,
        delay=delay,
        step_id="metrics",
        title="Метрики",
    )


def step_generic(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    # whitelist из чек-листа БАЗА
    return _toggle_plugins(
        auth,
        page_url=journal_url(base_url, journal, "manager/plugins/generic"),
        to_enable=ALLOWED_ENABLED_GENERIC,
        dry_run=dry_run,
        delay=delay,
        step_id="generic",
        title="Основные модули (whitelist)",
    )


def step_browse_settings(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    """Настройки плагина «Браузер»: просмотр по разделам (БАЗА)."""
    url = journal_url(
        base_url, journal, "manager/plugin/generic/browseplugin/settings"
    )
    html = auth.get_text(url)
    if "signinForm" in html or "/browseplugin/enable" in html.casefold():
        if dry_run:
            return StepResult(
                "browse",
                "Браузер: просмотр по разделам",
                True,
                "dry-run: enableBrowseBySections=1 (после включения плагина)",
                changed=True,
                dry_run=True,
            )
        return StepResult(
            "browse",
            "Браузер: просмотр по разделам",
            False,
            "Страница настроек недоступна — сначала включите browseplugin",
        )

    if 'name="enableBrowseBySections"' not in html:
        return StepResult(
            "browse",
            "Браузер: просмотр по разделам",
            False,
            "Поле enableBrowseBySections не найдено на странице настроек",
        )

    already = bool(
        re.search(
            r'name="enableBrowseBySections"[^>]*checked|checked[^>]*name="enableBrowseBySections"',
            html,
            re.I,
        )
    )
    if already:
        return StepResult(
            "browse",
            "Браузер: просмотр по разделам",
            True,
            "enableBrowseBySections уже включён",
            changed=False,
            dry_run=dry_run,
        )
    if dry_run:
        return StepResult(
            "browse",
            "Браузер: просмотр по разделам",
            True,
            "dry-run: enableBrowseBySections=1",
            changed=True,
            dry_run=True,
        )

    # OJS сохраняет только при submit name=save (кнопка «Сохранить»)
    form = find_post_form(html, url, action_contains="browseplugin")
    action = (form.action if form else "") or url
    payload: list[tuple[str, str]] = [
        ("enableBrowseBySections", "1"),
        ("excludedSections[]", ""),
        ("excludedIdentifyTypes[]", ""),
        ("save", "Сохранить"),
    ]
    status, _, _ = auth.request(
        action,
        data=payload,
        headers={"Referer": url, "Origin": base_url.rstrip("/")},
    )
    _delay(delay)
    if status >= 400:
        return StepResult(
            "browse",
            "Браузер: просмотр по разделам",
            False,
            f"HTTP {status}",
        )

    verify = auth.get_text(url)
    ok = bool(
        re.search(
            r'name="enableBrowseBySections"[^>]*checked|checked[^>]*name="enableBrowseBySections"',
            verify,
            re.I,
        )
    )
    if ok:
        return StepResult(
            "browse",
            "Браузер: просмотр по разделам",
            True,
            "enableBrowseBySections включён",
            changed=True,
            dry_run=False,
        )
    return StepResult(
        "browse",
        "Браузер: просмотр по разделам",
        False,
        "POST выполнен, но enableBrowseBySections не отмечен — проверьте вручную",
    )


def _checklist_indexes(form_fields: list[tuple[str, str]]) -> list[int]:
    return sorted(
        {
            int(m.group(1))
            for name, _ in form_fields
            for m in [re.match(r"submissionChecklist\[[^\]]+]\[(\d+)]", name)]
            if m
        }
    )


def _payload_without_checklist(
    fields: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Сохранение без полей checklist очищает требования к статьям в OJS 2.4."""
    return [(n, v) for n, v in fields if not n.startswith("submissionChecklist")]


def step_setup3(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    url = journal_url(base_url, journal, "manager/setup/3")
    notes: list[str] = []

    html = auth.get_text(url)
    form = find_post_form(html, url, action_contains="saveSetup/3")
    if form is None:
        return StepResult("setup3", "Шаг 3", False, "Форма saveSetup/3 не найдена")

    before = _checklist_indexes(form.fields)
    if before:
        notes.append(f"очистить требования к статьям ({len(before)} пункт(ов))")
        if not dry_run:
            # Надёжный способ: saveSetup без submissionChecklist[] (не delChecklist по одному)
            payload = _payload_without_checklist(form.fields)
            status, _, _ = auth.request(form.action, data=payload)
            _delay(delay)
            if status >= 400:
                return StepResult(
                    "setup3",
                    "Шаг 3",
                    False,
                    f"Не удалось очистить требования к статьям: HTTP {status}",
                )
            html_after = auth.get_text(url)
            form_after = find_post_form(html_after, url, action_contains="saveSetup/3")
            if form_after is None:
                return StepResult(
                    "setup3",
                    "Шаг 3",
                    False,
                    "После очистки требований форма saveSetup/3 не найдена",
                )
            left = _checklist_indexes(form_after.fields)
            if left:
                return StepResult(
                    "setup3",
                    "Шаг 3",
                    False,
                    (
                        "Не удалось очистить требования к статьям: "
                        f"было {len(before)}, осталось {len(left)}. "
                        "Проверьте шаг 3 вручную (manager/setup/3)."
                    ),
                )
            form = form_after
            notes.append("требования очищены")
    else:
        notes.append("требования к статьям уже пусты")

    # metaSubject + Vancouver + сохранить (без повторного добавления checklist)
    form.set_checkbox("metaSubject", True, "1")
    form.set_value("metaCitationOutputFilterId", VANCOUVER_OUTPUT_ID)
    notes.append("metaSubject=1")
    notes.append("metaCitationOutputFilterId=Vancouver(22)")

    if not dry_run:
        payload = _payload_without_checklist(form.fields)
        status, _, _ = auth.request(form.action, data=payload)
        _delay(delay)
        if status >= 400:
            return StepResult("setup3", "Шаг 3", False, f"saveSetup/3 HTTP {status}")

    parser_notes = _ensure_citation_parsers(
        auth, base_url=base_url, journal=journal, dry_run=dry_run, delay=delay
    )
    notes.extend(parser_notes)

    prefix = "dry-run: " if dry_run else ""
    return StepResult(
        "setup3",
        "Шаг 3 (checklist / keywords / citations)",
        True,
        prefix + "; ".join(notes),
        changed=True,
        dry_run=dry_run,
    )


def _ensure_citation_parsers(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> list[str]:
    grid_url = journal_url(
        base_url,
        journal,
        "$$$call$$$/grid/filter/parser-filter-grid/fetch-grid",
    )
    update_url = journal_url(
        base_url,
        journal,
        "$$$call$$$/grid/filter/parser-filter-grid/update-filter",
    )
    notes: list[str] = []
    try:
        status, _, body = auth.request(grid_url, method="GET")
        text = body.decode("utf-8", "replace")
        existing = set(re.findall(r"FreeCite|ParsCit|ParaCite|RegEx", text))
    except Exception as exc:  # noqa: BLE001
        return [f"parser grid: {exc}"]

    wanted = {
        "26": "FreeCite",
        "27": "ParsCit",
        "28": "ParaCite",
        "29": "RegEx",
    }
    for tid, label in wanted.items():
        # ParaCite в HTML может быть "ParaCite ()"
        present = any(label.casefold() in e.casefold() for e in existing) or label in existing
        if label == "ParaCite":
            present = present or "ParaCite" in text
        if present:
            notes.append(f"{label}: уже есть")
            continue
        notes.append(f"add {label}({tid})")
        if dry_run:
            continue
        status, _, body = auth.request(update_url, data={"filterTemplateId": tid})
        _delay(delay)
        if status >= 400:
            notes.append(f"{label}: HTTP {status}")
            continue
        # refresh existing names
        try:
            resp = json.loads(body.decode("utf-8", "replace"))
            if not resp.get("status"):
                notes.append(f"{label}: status=false")
        except json.JSONDecodeError:
            pass
    return notes


def step_setup4(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    url = journal_url(base_url, journal, "manager/setup/4")
    html = auth.get_text(url)
    form = find_post_form(html, url, action_contains="saveSetup/4")
    if form is None:
        return StepResult("setup4", "Шаг 4 пагинация", False, "Форма saveSetup/4 не найдена")
    already = bool(form.values_for("enablePageNumber"))
    form.set_checkbox("enablePageNumber", True, "1")
    if already:
        return StepResult(
            "setup4",
            "Шаг 4 пагинация",
            True,
            "enablePageNumber уже включён",
            changed=False,
            dry_run=dry_run,
        )
    if dry_run:
        return StepResult(
            "setup4",
            "Шаг 4 пагинация",
            True,
            "dry-run: enablePageNumber=1",
            changed=True,
            dry_run=True,
        )
    status, _, _ = auth.request(form.action, data=form.fields)
    _delay(delay)
    if status >= 400:
        return StepResult("setup4", "Шаг 4 пагинация", False, f"HTTP {status}")
    return StepResult(
        "setup4",
        "Шаг 4 пагинация",
        True,
        "enablePageNumber включён",
        changed=True,
        dry_run=False,
    )


def step_setup5_description(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    dry_run: bool,
    delay: float,
) -> StepResult:
    """Шаг 5: шаблон «Содержание главной» (description) RU+EN без конкретных данных."""
    from ipsas.modules.sandbox_journal_setup.homepage_template import (
        HOMEPAGE_DESCRIPTION_EN,
        HOMEPAGE_DESCRIPTION_RU,
        is_blank_description,
    )

    url = journal_url(base_url, journal, "manager/setup/5")
    html = auth.get_text(url)
    form = find_post_form(html, url, action_contains="saveSetup/5")
    if form is None:
        return StepResult(
            "setup5",
            "Шаг 5 содержание главной",
            False,
            "Форма saveSetup/5 не найдена",
        )

    current_ru = ""
    current_en = ""
    for name, value in form.fields:
        if name == "description[ru_RU]":
            current_ru = value
        elif name == "description[en_US]":
            current_en = value

    if not current_en:
        m = re.search(
            r'name="description\[en_US\]"[^>]*>(.*?)</textarea>',
            html,
            re.I | re.S,
        )
        if m:
            current_en = m.group(1)

    need_ru = is_blank_description(current_ru)
    need_en = is_blank_description(current_en)
    if not need_ru and not need_en:
        return StepResult(
            "setup5",
            "Шаг 5 содержание главной",
            True,
            "description RU/EN уже заполнены — не перезаписываем",
            changed=False,
            dry_run=dry_run,
        )

    parts: list[str] = []
    if need_ru:
        parts.append("description[ru_RU]=шаблон")
        form.set_value("description[ru_RU]", HOMEPAGE_DESCRIPTION_RU.strip())
    else:
        form.set_value("description[ru_RU]", current_ru)
    if need_en:
        parts.append("description[en_US]=шаблон")
        form.set_value("description[en_US]", HOMEPAGE_DESCRIPTION_EN.strip())
    elif current_en:
        form.set_value("description[en_US]", current_en)

    detail = ", ".join(parts)
    if dry_run:
        return StepResult(
            "setup5",
            "Шаг 5 содержание главной",
            True,
            f"dry-run: вставить шаблон ({detail})",
            changed=True,
            dry_run=True,
        )

    status, _, _ = auth.request(form.action, data=form.fields)
    _delay(delay)
    if status >= 400:
        return StepResult(
            "setup5",
            "Шаг 5 содержание главной",
            False,
            f"HTTP {status}",
        )

    if need_en:
        verify = auth.get_text(url)
        if "[specify]" not in verify:
            form2 = find_post_form(verify, url, action_contains="saveSetup/5")
            if form2 is None:
                return StepResult(
                    "setup5",
                    "Шаг 5 содержание главной",
                    False,
                    "EN шаблон не сохранился, форма не найдена",
                )
            form2.set_value("formLocale", "en_US")
            form2.set_value(
                "description[en_US]", HOMEPAGE_DESCRIPTION_EN.strip()
            )
            if need_ru or current_ru:
                form2.set_value(
                    "description[ru_RU]",
                    HOMEPAGE_DESCRIPTION_RU.strip() if need_ru else current_ru,
                )
            status2, _, _ = auth.request(form2.action, data=form2.fields)
            _delay(delay)
            if status2 >= 400:
                return StepResult(
                    "setup5",
                    "Шаг 5 содержание главной",
                    False,
                    f"EN locale HTTP {status2}",
                )
            verify2 = auth.get_text(url)
            if "[specify]" not in verify2 and "Founder:" not in verify2:
                return StepResult(
                    "setup5",
                    "Шаг 5 содержание главной",
                    False,
                    "RU сохранён, EN шаблон не подтверждён — проверьте вручную",
                )

    return StepResult(
        "setup5",
        "Шаг 5 содержание главной",
        True,
        f"Вставлен шаблон ({detail})",
        changed=True,
        dry_run=False,
    )


ALL_STEPS: list[Callable[..., StepResult]] = [
    step_languages,
    step_section_articles,
    step_reader_tools,
    step_pubids,
    step_metrics,
    step_generic,
    step_browse_settings,
    step_setup3,
    step_setup4,
    step_setup5_description,
]
