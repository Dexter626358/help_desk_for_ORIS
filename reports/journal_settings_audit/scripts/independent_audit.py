"""
Независимый аудит проверки настроек журнала (.data / OJS export).

Не использует ipsas.modules.journal_site.data_check.evaluate_checklist_item —
только парсер JSON и собственную логику правил (validation_rules.json).
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Literal, Optional

from ipsas.modules.journal_site.data_export import (
    JournalDataExport,
    PluginInfo,
    html_to_text,
    parse_journal_data,
)
from ipsas.modules.journal_site.default_setup_checklist import (
    ALLOWED_ENABLED_GENERIC,
    DEFAULT_SETUP_CHECKLIST,
    ChecklistItem,
)

Status = Literal["pass", "fail", "partial", "not_applicable", "cannot_determine"]
Severity = Literal["critical", "error", "warning", "info"]
Confidence = Literal["high", "medium", "low"]

_ISSN_RE = re.compile(r"^\d{4}-?\d{3}[\dXx]$")
_EMPTY_MARKERS = {"", "-", "—", "n/a", "na", "none", "null", "undefined"}

# Маппинг severity чек-листа → уровни аудита
_SEV_MAP = {
    "required": "error",
    "recommended": "warning",
    "info": "info",
}

# Критичные для «готовности к работе» (P0 readiness)
_CRITICAL_IDS = frozenset(
    {
        "pages.lang.ru",
        "pages.lang.en",
        "step1.title",
        "step1.issn",
        "step1.publisher",
        "step2.focus",
        "step2.review",
        "step3.guidelines",
        "step3.copyright",
    }
)


@dataclass
class RuleResult:
    rule_id: str
    expected_status: Status
    actual_value: str
    json_path: str
    evidence: str
    confidence: Confidence
    explanation: str
    severity: Severity
    service_status: str = ""
    compare: str = "cannot_compare"


def _is_empty_text(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return False
    text = html_to_text(str(value)).strip().lower()
    return text in _EMPTY_MARKERS or len(text) == 0


def _text_len(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return len(str(value))
    return len(html_to_text(str(value)).strip())


def _find_plugin(export: JournalDataExport, *names: str) -> Optional[PluginInfo]:
    lower = {k.lower(): v for k, v in export.plugins.items()}
    for name in names:
        hit = export.plugins.get(name) or lower.get(name.lower())
        if hit is not None:
            return hit
    return None


def _plugin_state(export: JournalDataExport, *names: str) -> tuple[str, str]:
    info = _find_plugin(export, *names)
    if info is None:
        return "absent", ""
    if info.enabled:
        return "enabled", info.name
    return "disabled", info.name


def _locale_list_has(export: JournalDataExport, key: str, prefix: str) -> bool:
    raw = export.get_raw_setting(key)
    if not isinstance(raw, list):
        return False
    return any(str(x).lower().startswith(prefix) for x in raw)


def _has_doi(export: JournalDataExport) -> bool:
    info = _find_plugin(export, "DOIPubIdPlugin")
    if not info or not info.enabled:
        return False
    prefix = str((info.settings or {}).get("doiPrefix") or "").strip()
    return bool(prefix)


def _truthy(value: Any) -> bool:
    if value is True or value == 1:
        return True
    if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}:
        return True
    return False


def build_validation_rules() -> list[dict[str, Any]]:
    """Формализация правил из чек-листа сервиса + независимые уточнения."""
    rules: list[dict[str, Any]] = []
    for item in DEFAULT_SETUP_CHECKLIST:
        sev: Severity = "critical" if item.id in _CRITICAL_IDS else _SEV_MAP.get(item.severity, "error")  # type: ignore[assignment]
        if item.severity == "info":
            sev = "info"
        elif item.severity == "recommended" and item.id not in _CRITICAL_IDS:
            sev = "warning"
        elif item.id in _CRITICAL_IDS:
            sev = "critical"
        else:
            sev = "error"

        json_paths: list[str] = []
        if item.setting_keys:
            json_paths.extend([f"settings.{k}" for k in item.setting_keys])
        if item.plugin_keys:
            json_paths.extend([f"plugins.{k}" for k in item.plugin_keys])
        if item.kind.startswith("sections"):
            json_paths.append("sections[]")
        if not json_paths:
            json_paths.append(f"kind:{item.kind}")

        applies_all = item.condition is None and item.id != "modules.doi"
        exceptions = ""
        if item.condition == "has_doi":
            exceptions = "Только если DOI включён и указан doiPrefix"
            applies_all = False
        if item.condition == "in_scopus":
            exceptions = "Только если журнал в Scopus; признак в .data часто отсутствует → cannot_determine"
            applies_all = False
        if item.id == "modules.doi":
            exceptions = "Если журнал не присваивает DOI — правило not_applicable (по JSON нельзя доказать намерение)"
            applies_all = False
        if item.id == "modules.fundref":
            exceptions = "Обновление базы организаций по .data проверить нельзя → отдельный cannot_determine не штрафует enabled"

        pass_cond = {
            "locale_flags": "Локаль lang есть в supportedLocales, supportedSubmissionLocales, supportedFormLocales",
            "setting_text": f"Непустой текст (после очистки HTML) по ключам {item.setting_keys} для локалей {item.locales or 'scalar'}",
            "plugin": f"Плагин {'включён' if item.expect_enabled else 'выключен/отсутствует'}: {item.plugin_keys}",
            "plugin_conditional": f"При выполнении условия {item.condition}: плагин включён",
            "board": "boardCustomText RU и EN ≥ 40 символов текста",
            "custom_about": "focusAndScope или focusScopeDesc или customAbout с тематикой ≥ 40 символов",
            "copyright_license": "copyrightNotice RU+EN, holder, licenseURL/лицензия",
            "submission_checklist": "submissionChecklist с пунктами на RU и EN",
            "sections_articles": "Раздел Статьи: title/abbrev RU+EN, hideAbout, editorRestriction",
            "map_address": "mailingAddress RU+EN непустой",
            "dates_display": "Хотя бы один display*Date включён",
        }.get(item.kind, f"Выполнено по kind={item.kind}")

        rules.append(
            {
                "rule_id": item.id.upper().replace(".", "_"),
                "checklist_id": item.id,
                "name": item.title,
                "json_paths": json_paths,
                "pass_condition": pass_cond,
                "severity": sev,
                "checklist_severity": item.severity,
                "applies_to_all_journals": applies_all,
                "exceptions": exceptions,
                "editorial_message": f"Исправить: {item.title}",
                "kind": item.kind,
                "setting_keys": list(item.setting_keys),
                "plugin_keys": list(item.plugin_keys),
                "locales": list(item.locales),
                "expect_enabled": item.expect_enabled,
                "condition": item.condition,
                "min_chars": item.min_chars,
                "computation": "independent_eval.eval_item",
            }
        )
    return rules


def eval_item(export: JournalDataExport, item: ChecklistItem) -> RuleResult:
    """Независимая оценка одного пункта чек-листа."""
    rule_id = item.id.upper().replace(".", "_")
    sev: Severity = "critical" if item.id in _CRITICAL_IDS else (
        "info" if item.severity == "info" else ("warning" if item.severity == "recommended" else "error")
    )
    kind = item.kind

    if kind == "locale_flags":
        lang = item.locales[0] if item.locales else "ru"
        prefix = "ru" if lang.startswith("ru") else "en"
        flags = {
            "supportedLocales": _locale_list_has(export, "supportedLocales", prefix),
            "supportedSubmissionLocales": _locale_list_has(export, "supportedSubmissionLocales", prefix),
            "supportedFormLocales": _locale_list_has(export, "supportedFormLocales", prefix),
        }
        ok = all(flags.values())
        return RuleResult(
            rule_id=rule_id,
            expected_status="pass" if ok else "fail",
            actual_value=str(flags),
            json_path="settings.supportedLocales|supportedSubmissionLocales|supportedFormLocales",
            evidence=json.dumps(flags, ensure_ascii=False),
            confidence="high",
            explanation="Все три флага локали включены" if ok else "Не все флаги локали включены",
            severity=sev,
        )

    if kind == "setting_text":
        if item.id == "step1.issn":
            print_v = str(export.get_raw_setting("printIssn") or "").strip()
            online_v = str(export.get_raw_setting("onlineIssn") or "").strip()
            print_ok = bool(print_v and _ISSN_RE.search(print_v))
            online_ok = bool(online_v and _ISSN_RE.search(online_v))
            actual = f"printIssn={print_v!r}; onlineIssn={online_v!r}"
            if print_ok and online_ok:
                st: Status = "pass"
                expl = "Оба ISSN указаны"
            elif print_ok or online_ok:
                st = "partial"
                expl = "Указан только один ISSN"
            else:
                st = "fail"
                expl = "ISSN отсутствует или некорректен"
            return RuleResult(
                rule_id=rule_id,
                expected_status=st,
                actual_value=actual,
                json_path="settings.printIssn|onlineIssn",
                evidence=actual,
                confidence="high",
                explanation=expl,
                severity=sev,
            )

        missing: list[str] = []
        present: list[str] = []
        locales = item.locales or ()
        for key in item.setting_keys:
            if not locales:
                raw = export.get_raw_setting(key)
                if _is_empty_text(raw) and not (
                    isinstance(raw, (int, float)) and not isinstance(raw, bool)
                ):
                    missing.append(key)
                else:
                    present.append(f"{key}={html_to_text(str(raw))[:40]}")
                continue
            for lang in locales:
                text = export.get_setting_text(key, lang)
                if key == "supportEmail" and not text:
                    text = str(export.get_raw_setting("supportEmail") or "")
                if len(html_to_text(text)) < item.min_chars:
                    missing.append(f"{key}/{lang}")
                else:
                    present.append(f"{key}/{lang}")
        if not missing:
            st = "pass"
            expl = "Поля заполнены"
        elif present:
            st = "partial"
            expl = f"Частично; нет: {', '.join(missing)}"
        else:
            st = "fail"
            expl = f"Пусто: {', '.join(missing)}"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value="; ".join(present) if present else "empty",
            json_path="|".join(f"settings.{k}" for k in item.setting_keys),
            evidence=expl,
            confidence="high",
            explanation=expl,
            severity=sev,
        )

    if kind == "setting_present_or_empty_ok":
        return RuleResult(
            rule_id=rule_id,
            expected_status="pass",
            actual_value="optional",
            json_path="|".join(f"settings.{k}" for k in item.setting_keys),
            evidence="Опциональное поле: пусто или заполнено — ок",
            confidence="high",
            explanation="Info/optional — не штрафуем",
            severity="info",
        )

    if kind == "plugin":
        # DOI: условное требование
        if item.id == "modules.doi":
            state, name = _plugin_state(export, *item.plugin_keys)
            info = _find_plugin(export, *item.plugin_keys)
            prefix = str((info.settings or {}).get("doiPrefix") or "").strip() if info else ""
            if state == "enabled" and prefix:
                return RuleResult(
                    rule_id=rule_id,
                    expected_status="pass",
                    actual_value=f"{name}; doiPrefix={prefix}",
                    json_path="plugins.DOIPubIdPlugin",
                    evidence=f"enabled + prefix={prefix}",
                    confidence="high",
                    explanation="DOI включён и префикс указан",
                    severity=sev,
                )
            if state == "enabled" and not prefix:
                return RuleResult(
                    rule_id=rule_id,
                    expected_status="fail",
                    actual_value=f"{name}; doiPrefix empty",
                    json_path="plugins.DOIPubIdPlugin.settings.doiPrefix",
                    evidence="enabled без doiPrefix",
                    confidence="high",
                    explanation="DOI включён, но префикс пуст",
                    severity=sev,
                )
            return RuleResult(
                rule_id=rule_id,
                expected_status="not_applicable",
                actual_value=f"state={state}",
                json_path="plugins.DOIPubIdPlugin",
                evidence="Плагин выключен/отсутствует; по .data нельзя доказать, что журнал обязан присваивать DOI",
                confidence="medium",
                explanation="Правило применимо только если журнал присваивает DOI",
                severity=sev,
            )

        if item.id == "modules.acron":
            info = _find_plugin(export, *item.plugin_keys)
            if info is not None:
                return RuleResult(
                    rule_id=rule_id,
                    expected_status="pass",
                    actual_value=info.name,
                    json_path="plugins.acronPlugin",
                    evidence="Плагин присутствует в экспорте",
                    confidence="medium",
                    explanation="ACRON на уровне сайта — достаточно наличия",
                    severity=sev,
                )
            return RuleResult(
                rule_id=rule_id,
                expected_status="partial",
                actual_value="absent",
                json_path="plugins.acronPlugin",
                evidence="ACRON не найден",
                confidence="low",
                explanation="Нужна проверка администратором сайта",
                severity=sev,
            )

        if item.id == "modules.fundref":
            state, name = _plugin_state(export, *item.plugin_keys)
            if state == "enabled":
                return RuleResult(
                    rule_id=rule_id,
                    expected_status="pass",
                    actual_value=f"{name} enabled; org_db=cannot_determine",
                    json_path="plugins.fundrefplugin",
                    evidence="Плагин включён; обновление базы организаций по JSON не подтверждается",
                    confidence="medium",
                    explanation="Enabled ok; факт обновления базы — cannot_determine (не штрафуем)",
                    severity=sev,
                )
            return RuleResult(
                rule_id=rule_id,
                expected_status="fail",
                actual_value=state,
                json_path="plugins.fundrefplugin",
                evidence=state,
                confidence="high",
                explanation="FundRef выключен или отсутствует",
                severity=sev,
            )

        state, name = _plugin_state(export, *item.plugin_keys)
        expect = item.expect_enabled
        if expect is True:
            if state == "enabled":
                st = "pass"
                expl = f"Плагин {name} включён"
            elif state == "disabled":
                st = "fail"
                expl = f"Плагин {name} найден, выключен"
            else:
                st = "fail"
                expl = "Плагин отсутствует"
            return RuleResult(
                rule_id=rule_id,
                expected_status=st,
                actual_value=f"{name or item.plugin_keys[0]}:{state}",
                json_path="|".join(f"plugins.{k}" for k in item.plugin_keys),
                evidence=expl,
                confidence="high",
                explanation=expl,
                severity=sev,
            )
        if expect is False:
            ok = state != "enabled"
            return RuleResult(
                rule_id=rule_id,
                expected_status="pass" if ok else "fail",
                actual_value=f"{name or item.plugin_keys[0]}:{state}",
                json_path="|".join(f"plugins.{k}" for k in item.plugin_keys),
                evidence="должен быть выключен",
                confidence="high",
                explanation="Выключен/отсутствует" if ok else f"Включён вопреки требованию: {name}",
                severity=sev,
            )

    if kind == "plugin_conditional":
        cond = item.condition or ""
        if cond == "has_doi":
            if not _has_doi(export):
                state, name = _plugin_state(export, *item.plugin_keys)
                if state == "enabled":
                    return RuleResult(
                        rule_id=rule_id,
                        expected_status="partial",
                        actual_value=f"no_doi but {name} enabled",
                        json_path="|".join(f"plugins.{k}" for k in item.plugin_keys),
                        evidence="DOI нет, метрика включена",
                        confidence="high",
                        explanation="Метрика без DOI обычно лишняя",
                        severity="warning",
                    )
                return RuleResult(
                    rule_id=rule_id,
                    expected_status="not_applicable",
                    actual_value="no_doi",
                    json_path="plugins.DOIPubIdPlugin",
                    evidence="DOI не настроен",
                    confidence="high",
                    explanation="Правило не применяется без DOI",
                    severity=sev,
                )
            state, name = _plugin_state(export, *item.plugin_keys)
            ok = state == "enabled"
            return RuleResult(
                rule_id=rule_id,
                expected_status="pass" if ok else "fail",
                actual_value=f"{name}:{state}",
                json_path="|".join(f"plugins.{k}" for k in item.plugin_keys),
                evidence="при наличии DOI плагин должен быть включён",
                confidence="high",
                explanation="OK" if ok else "При наличии DOI плагин выключен",
                severity=sev,
            )
        if cond == "in_scopus":
            state, name = _plugin_state(export, *item.plugin_keys)
            return RuleResult(
                rule_id=rule_id,
                expected_status="cannot_determine",
                actual_value=f"{name}:{state}",
                json_path="|".join(f"plugins.{k}" for k in item.plugin_keys),
                evidence="Признак индексации в Scopus в .data не надёжен",
                confidence="low",
                explanation="Нельзя автоматически решить, нужен ли PlumX",
                severity=sev,
            )

    if kind == "custom_about":
        texts: list[str] = []
        paths_hit: list[str] = []
        for key in ("focusAndScope", "focusScopeDesc"):
            for lang in ("ru", "en"):
                t = export.get_setting_text(key, lang)
                if len(t) >= 40:
                    texts.append(t)
                    paths_hit.append(f"settings.{key}/{lang}")
        for lang in ("ru", "en"):
            t = export.find_custom_about(
                lang, "focus and scope", "aims and scope", "тематика", "цели и задачи", "предметн"
            )
            if len(t) >= 40:
                texts.append(t)
                paths_hit.append(f"settings.customAboutItems/{lang}")
        body = " ".join(texts)
        ok = len(html_to_text(body)) >= 40
        return RuleResult(
            rule_id=rule_id,
            expected_status="pass" if ok else "fail",
            actual_value=(html_to_text(body)[:120] if ok else "empty"),
            json_path="settings.focusAndScope|focusScopeDesc|customAboutItems",
            evidence="; ".join(paths_hit) if paths_hit else "не найдено",
            confidence="high",
            explanation="Предметная область найдена" if ok else "Предметная область отсутствует",
            severity=sev,
        )

    if kind == "board":
        ru = export.get_setting_text("boardCustomText", "ru")
        en = export.get_setting_text("boardCustomText", "en")
        ru_ok, en_ok = len(ru) >= 40, len(en) >= 40
        if ru_ok and en_ok:
            st = "pass"
        elif ru_ok or en_ok:
            st = "partial"
        else:
            st = "fail"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"ru={len(ru)} en={len(en)}",
            json_path="settings.boardCustomText",
            evidence=f"ru_chars={len(ru)}; en_chars={len(en)}",
            confidence="high",
            explanation="Редакция заполнена" if st == "pass" else "Редакция неполная/пуста",
            severity=sev,
        )

    if kind == "map_address":
        ru = export.get_setting_text("mailingAddress", "ru")
        en = export.get_setting_text("mailingAddress", "en")
        if ru and en:
            st = "pass"
        elif ru or en:
            st = "partial"
        else:
            st = "fail"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"ru={bool(ru)} en={bool(en)}",
            json_path="settings.mailingAddress",
            evidence=f"ru_len={len(ru)}; en_len={len(en)}",
            confidence="high",
            explanation="Адрес заполнен" if st == "pass" else "Адрес неполный/пуст",
            severity=sev,
        )

    if kind == "copyright_license":
        notice_ru = export.get_setting_text("copyrightNotice", "ru")
        notice_en = export.get_setting_text("copyrightNotice", "en")
        holder = str(export.get_raw_setting("copyrightHolderType") or "").strip()
        license_url = str(export.get_raw_setting("licenseURL") or "").strip()
        deficits = []
        if len(notice_ru) < 20:
            deficits.append("notice_ru")
        if len(notice_en) < 20:
            deficits.append("notice_en")
        if not holder:
            deficits.append("holder")
        if not license_url and "creative" not in (notice_ru + notice_en).lower() and "cc-" not in (
            notice_ru + notice_en
        ).lower():
            deficits.append("license")
        if not deficits:
            st = "pass"
        elif len(deficits) <= 2 and (notice_ru or notice_en):
            st = "partial" if "license" in deficits or len(deficits) == 1 else "fail"
            if "notice_ru" in deficits and "notice_en" in deficits and "license" in deficits:
                st = "fail"
        else:
            st = "fail"
        # refine: only license missing with notices present → partial
        if deficits == ["license"] and notice_ru and notice_en and holder:
            st = "partial"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"holder={holder!r}; licenseURL={license_url!r}; notice_ru={len(notice_ru)}; notice_en={len(notice_en)}",
            json_path="settings.copyrightNotice|copyrightHolderType|licenseURL",
            evidence=f"deficits={deficits}",
            confidence="high",
            explanation="OK" if st == "pass" else f"Не хватает: {', '.join(deficits) or 'данные'}",
            severity=sev,
        )

    if kind == "submission_checklist":
        raw = export.get_raw_setting("submissionChecklist")
        def count_items(lang: str) -> int:
            # localized dict of lists or list
            if raw is None:
                return 0
            if isinstance(raw, dict):
                # try locale keys
                for k, v in raw.items():
                    if str(k).lower().startswith(lang):
                        if isinstance(v, list):
                            return sum(1 for x in v if isinstance(x, dict) and html_to_text(str(x.get("content") or x.get("title") or "")))
                        return 0
                return 0
            if isinstance(raw, list):
                return len(raw)
            return 0
        ru_n, en_n = count_items("ru"), count_items("en")
        if ru_n > 0 and en_n > 0:
            st = "pass"
        elif ru_n > 0 or en_n > 0:
            st = "partial"
        else:
            st = "fail"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"ru={ru_n} en={en_n}",
            json_path="settings.submissionChecklist",
            evidence=f"ru={ru_n}; en={en_n}",
            confidence="high",
            explanation="Чек-лист есть" if st == "pass" else "Чек-лист отсутствует/неполный",
            severity=sev,
        )

    if kind == "sections_articles":
        articles = None
        for sec in export.sections:
            title_ru = html_to_text(str((sec.get("title") or {}).get("ru_RU") or (sec.get("title") or {}).get("ru") or ""))
            title_en = html_to_text(str((sec.get("title") or {}).get("en_US") or (sec.get("title") or {}).get("en") or ""))
            if "стат" in title_ru.lower() or title_en.lower() == "articles" or "article" in title_en.lower():
                articles = sec
                break
        if articles is None:
            return RuleResult(
                rule_id=rule_id,
                expected_status="fail",
                actual_value="section not found",
                json_path="sections[]",
                evidence="Раздел «Статьи» не найден",
                confidence="medium",
                explanation="Нет раздела Статьи",
                severity=sev,
            )
        title = articles.get("title") or {}
        abbrev = articles.get("abbrev") or {}
        def loc(d, lang):
            if not isinstance(d, dict):
                return ""
            for k, v in d.items():
                if str(k).lower().startswith(lang):
                    return html_to_text(str(v))
            return ""
        deficits = []
        if not loc(title, "ru"):
            deficits.append("title_ru")
        if not loc(title, "en"):
            deficits.append("title_en")
        if not loc(abbrev, "ru"):
            deficits.append("abbrev_ru")
        if not loc(abbrev, "en"):
            deficits.append("abbrev_en")
        hide = _truthy(articles.get("hideAbout"))
        editor = _truthy(articles.get("editorRestriction"))
        if not hide:
            deficits.append("hideAbout")
        if not editor:
            deficits.append("editorRestriction")
        if not deficits:
            st = "pass"
        elif len(deficits) <= 2 and loc(title, "ru") and loc(title, "en"):
            st = "partial"
        else:
            st = "fail"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st if deficits else "pass",
            actual_value=f"hide={hide}; editor={editor}; deficits={deficits}",
            json_path="sections[Articles]",
            evidence=str(deficits),
            confidence="high",
            explanation="OK" if not deficits else f"Проблемы: {', '.join(deficits)}",
            severity=sev,
        )

    if kind == "sections_other":
        n = len(export.sections)
        if n >= 2:
            st = "pass"
        elif n == 1:
            st = "partial"
        else:
            st = "fail"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"sections={n}",
            json_path="sections[]",
            evidence=f"count={n}",
            confidence="medium",
            explanation="Есть дополнительные разделы" if st == "pass" else "Мало разделов",
            severity=sev,
        )

    if kind == "dates_display":
        keys = [
            "displayIssuePublishDate",
            "displaySubmissionPublishDate",
            "displaySubmissionAcceptDate",
            "displaySubmissionSubmitDate",
        ]
        on = [k for k in keys if _truthy(export.get_raw_setting(k))]
        return RuleResult(
            rule_id=rule_id,
            expected_status="pass" if on else "fail",
            actual_value=",".join(on) if on else "none",
            json_path="settings.display*Date",
            evidence=str(on),
            confidence="high",
            explanation="Даты выбраны" if on else "Даты не выбраны",
            severity=sev,
        )

    if kind == "reader_tools":
        # reading tools often via plugin blocks; check readingTools / enableReadingTools-like
        candidates = ["readingTools", "enableReadingTools", "readerTools"]
        found = {k: export.get_raw_setting(k) for k in candidates if k in export.settings}
        # also readingtoolsblockplugin
        state, name = _plugin_state(export, "readingtoolsblockplugin", "ReadingToolsPlugin")
        if state == "enabled" or any(_truthy(v) for v in found.values()):
            st = "pass"
            conf: Confidence = "medium"
        elif found or state == "disabled":
            st = "fail"
            conf = "medium"
        else:
            st = "cannot_determine"
            conf = "low"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"plugin={state}; settings={found}",
            json_path="plugins.readingtoolsblockplugin|settings.readingTools",
            evidence=str({"plugin": state, "settings": found}),
            confidence=conf,
            explanation="Инструменты читателя" if st == "pass" else "Не подтверждены / выключены",
            severity=sev,
        )

    if kind == "webfeed_plugin":
        state, name = _plugin_state(export, *item.plugin_keys)
        if state != "enabled":
            return RuleResult(
                rule_id=rule_id,
                expected_status="fail",
                actual_value=state,
                json_path="plugins.webfeedplugin",
                evidence=state,
                confidence="high",
                explanation="Новостная лента выключена",
                severity=sev,
            )
        # display settings often not fully in export → cannot auto-confirm pages
        return RuleResult(
            rule_id=rule_id,
            expected_status="cannot_determine",
            actual_value=f"{name} enabled; page flags unknown",
            json_path="plugins.webfeedplugin",
            evidence="Плагин включён; отображение на страницах выпуска по .data часто не видно",
            confidence="low",
            explanation="Нужна ручная проверка отображения",
            severity=sev,
        )

    if kind == "browse_plugin":
        state, name = _plugin_state(export, *item.plugin_keys)
        info = _find_plugin(export, *item.plugin_keys)
        if state != "enabled":
            return RuleResult(
                rule_id=rule_id,
                expected_status="fail",
                actual_value=f"{name}:{state}",
                json_path="plugins.browseplugin",
                evidence=state,
                confidence="high",
                explanation="Браузер выключен",
                severity=sev,
            )
        settings = (info.settings if info else {}) or {}
        by_sections = bool(settings.get("enableBrowseBySections") in (True, 1, "1", "true"))
        if by_sections:
            return RuleResult(
                rule_id=rule_id,
                expected_status="pass",
                actual_value="enableBrowseBySections=true",
                json_path="plugins.browseplugin.settings.enableBrowseBySections",
                evidence=str(settings.get("enableBrowseBySections")),
                confidence="high",
                explanation="Браузер включён, просмотр по разделам",
                severity=sev,
            )
        if "enableBrowseBySections" in settings:
            return RuleResult(
                rule_id=rule_id,
                expected_status="fail",
                actual_value=f"enableBrowseBySections={settings.get('enableBrowseBySections')}",
                json_path="plugins.browseplugin.settings.enableBrowseBySections",
                evidence=str(settings.get("enableBrowseBySections")),
                confidence="high",
                explanation="Просмотр по разделам выключен",
                severity=sev,
            )
        return RuleResult(
            rule_id=rule_id,
            expected_status="cannot_determine",
            actual_value="enabled; enableBrowseBySections missing",
            json_path="plugins.browseplugin.settings",
            evidence=str(settings),
            confidence="low",
            explanation="Нет флага enableBrowseBySections в экспорте",
            severity=sev,
        )

    if kind == "email_outgoing":
        email = str(export.get_raw_setting("envelopeSender") or export.get_raw_setting("contactEmail") or "").strip()
        if "@" in email:
            st = "pass"
        else:
            st = "partial"  # может быть глобальная настройка сайта
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=email or "empty",
            json_path="settings.envelopeSender|contactEmail",
            evidence=email or "пусто; возможны глобальные SMTP сайта",
            confidence="medium",
            explanation="Исходящая почта журнала" if st == "pass" else "Локально пусто — проверьте глобальные настройки",
            severity=sev,
        )

    if kind == "library_mode":
        # publishingMode / disableSubmissions
        mode = export.get_raw_setting("publishingMode")
        return RuleResult(
            rule_id=rule_id,
            expected_status="cannot_determine",
            actual_value=str(mode),
            json_path="settings.publishingMode",
            evidence="Интерпретация режима библиотеки требует уточнения правила",
            confidence="low",
            explanation="requires_rule_clarification",
            severity="info",
        )

    if kind == "issue_identification":
        return RuleResult(
            rule_id=rule_id,
            expected_status="cannot_determine",
            actual_value=str(export.get_raw_setting("publicationFormatVolume") or ""),
            json_path="settings.publicationFormat*",
            evidence="Формат идентификации выпусков — requires_rule_clarification по допустимым комбинациям",
            confidence="low",
            explanation="requires_rule_clarification",
            severity=sev,
        )

    if kind == "extra_generic_plugins":
        extras = []
        for name, info in export.plugins.items():
            if not info.enabled:
                continue
            if name.lower() in ALLOWED_ENABLED_GENERIC:
                continue
            if info.category and "generic" not in str(info.category).lower() and "block" in name.lower():
                continue
            # ignore known metrics/pubids
            low = name.lower()
            if any(x in low for x in ("doi", "edn", "dimension", "plum", "cited", "alm", "altmetric", "publons", "usage", "piwik", "yandex", "jivo", "dataverse", "sword", "pln", "backup", "antiplag", "domate", "xml", "announcement", "externalfeed", "agreement", "layout", "import", "translator", "download", "userblock", "notification", "metricsblock", "navigation", "keyword", "currentissue", "roleblock", "submit", "customlocale")):
                continue
            extras.append(name)
        # Явно отмечаем лишние модули (по согласованию — не игнорировать)
        if not extras:
            st = "pass"
            expl = "Лишних generic-модулей сверх эвристики не найдено"
        else:
            st = "partial"
            expl = f"Отмечено включённых вне эталона: {len(extras)} (whitelist эвристический)"
        return RuleResult(
            rule_id=rule_id,
            expected_status=st,
            actual_value=f"extra_enabled={len(extras)}",
            json_path="plugins.*",
            evidence=",".join(extras[:20]) or "none",
            confidence="medium",
            explanation=expl,
            severity="warning",
        )

    if kind == "manual":
        return RuleResult(
            rule_id=rule_id,
            expected_status="cannot_determine",
            actual_value="",
            json_path="manual",
            evidence="Ручная проверка",
            confidence="low",
            explanation="Не оценивается автоматически",
            severity=sev,
        )

    if kind == "setting_bool":
        # step4.access — информационная подсказка (кроме полного disableUserReg)
        if item.id == "step4.access":
            disable = _truthy(export.get_raw_setting("disableUserReg"))
            author = _truthy(export.get_raw_setting("allowRegAuthor"))
            reader = _truthy(export.get_raw_setting("allowRegReader"))
            reviewer = _truthy(export.get_raw_setting("allowRegReviewer"))
            actual = (
                f"disableUserReg={disable}; author={author}; reader={reader}; reviewer={reviewer}"
            )
            if disable:
                return RuleResult(
                    rule_id=rule_id,
                    expected_status="fail",
                    actual_value=actual,
                    json_path="settings.disableUserReg|allowReg*",
                    evidence=actual,
                    confidence="high",
                    explanation="Регистрация пользователей отключена глобально",
                    severity="error",
                )
            if author and reader and reviewer:
                st: Status = "pass"
                expl = "Роли регистрации открыты"
            else:
                st = "cannot_determine"
                expl = "Подсказка: сверьте роли регистрации вручную (не обязательный fail)"
            return RuleResult(
                rule_id=rule_id,
                expected_status=st,
                actual_value=actual,
                json_path="settings.allowRegAuthor|allowRegReader|allowRegReviewer",
                evidence=actual,
                confidence="medium",
                explanation=expl,
                severity="info",
            )
        key = item.setting_keys[0] if item.setting_keys else ""
        raw = export.get_raw_setting(key)
        ok = _truthy(raw) if item.expect_enabled is not False else (not _truthy(raw))
        # default: expect true-like if expect_enabled True
        if item.expect_enabled is True:
            ok = _truthy(raw)
        elif item.expect_enabled is False:
            ok = not _truthy(raw)
        return RuleResult(
            rule_id=rule_id,
            expected_status="pass" if ok else "fail",
            actual_value=str(raw),
            json_path=f"settings.{key}",
            evidence=str(raw),
            confidence="high",
            explanation="Флаг в ожидаемом состоянии" if ok else "Флаг не в ожидаемом состоянии",
            severity=sev,
        )

    return RuleResult(
        rule_id=rule_id,
        expected_status="cannot_determine",
        actual_value="",
        json_path=f"kind:{kind}",
        evidence="Неизвестный kind",
        confidence="low",
        explanation=f"requires_rule_clarification: kind={kind}",
        severity="info",
    )


def map_service_status(status: str, ui_status: str = "", bucket: str = "") -> Status:
    s = (ui_status or status or "").lower()
    if s in {"ok", "pass"}:
        return "pass"
    if s in {"fail", "error", "missing", "empty"}:
        return "fail"
    if s in {"warn", "partial", "weak", "formal"}:
        return "partial"
    if s in {"na", "not_applicable", "n/a"}:
        return "not_applicable"
    if s in {"manual"}:
        return "cannot_determine"
    if bucket == "fix":
        return "fail"
    if bucket == "partial":
        return "partial"
    if bucket == "manual":
        return "cannot_determine"
    return "cannot_determine"


def compare_status(indep: Status, service: Status) -> str:
    """Классификация сравнения относительно проблемы (fail/partial как проблема)."""
    indep_prob = indep in {"fail", "partial"}
    svc_prob = service in {"fail", "partial"}
    # treat cannot_determine/manual specially
    if indep == "cannot_determine" or service == "cannot_determine":
        if indep == service:
            return "true_negative" if indep == "pass" else "cannot_compare"
        return "cannot_compare"
    if indep == "not_applicable":
        if service == "not_applicable" or service == "pass":
            return "true_negative"
        if svc_prob:
            return "false_positive"
        return "cannot_compare"
    if indep == "pass" and service == "pass":
        return "true_negative"
    if indep_prob and svc_prob:
        if indep != service:
            return "severity_mismatch"
        return "true_positive"
    if indep_prob and not svc_prob:
        return "false_negative"
    if not indep_prob and svc_prob:
        return "false_positive"
    if indep == "pass" and service == "not_applicable":
        return "severity_mismatch"
    return "cannot_compare"


def score_journal(results: list[RuleResult]) -> dict[str, Any]:
    applicable = [r for r in results if r.expected_status not in {"not_applicable"}]
    # cannot_determine исключаем из completeness denominator optionally
    scored = [r for r in applicable if r.expected_status != "cannot_determine"]
    weights = {"critical": 5, "error": 3, "warning": 1, "info": 0}
    total_w = 0.0
    got_w = 0.0
    for r in scored:
        w = weights.get(r.severity, 1)
        if r.severity == "info":
            continue
        total_w += w
        if r.expected_status == "pass":
            got_w += w
        elif r.expected_status == "partial":
            got_w += w * 0.5
    completeness = round(100.0 * got_w / total_w, 1) if total_w else 0.0
    # correctness: among non-empty attempts, share without fail
    checked = [r for r in scored if r.severity != "info"]
    correct = sum(1 for r in checked if r.expected_status in {"pass", "partial"})
    correctness = round(100.0 * correct / len(checked), 1) if checked else 0.0
    critical_fail = sum(1 for r in results if r.severity == "critical" and r.expected_status == "fail")
    failed = sum(1 for r in results if r.expected_status == "fail")
    if critical_fail >= 2 or completeness < 55:
        readiness = "not_ready"
    elif critical_fail == 1 or completeness < 70:
        readiness = "needs_revision"
    elif failed <= 2 and completeness >= 85:
        readiness = "ready"
    elif completeness >= 75:
        readiness = "mostly_ready"
    else:
        readiness = "needs_revision"
    if sum(1 for r in results if r.expected_status == "cannot_determine") >= 8:
        if readiness == "ready":
            readiness = "manual_review"
    return {
        "applicable_checks": len(applicable),
        "passed": sum(1 for r in results if r.expected_status == "pass"),
        "failed": sum(1 for r in results if r.expected_status == "fail"),
        "partial": sum(1 for r in results if r.expected_status == "partial"),
        "cannot_determine": sum(1 for r in results if r.expected_status == "cannot_determine"),
        "not_applicable": sum(1 for r in results if r.expected_status == "not_applicable"),
        "critical_errors": critical_fail,
        "completeness_score": completeness,
        "correctness_score": correctness,
        "expected_readiness": readiness,
    }


def audit_file(path: Path) -> dict[str, Any]:
    export = parse_journal_data(path.read_bytes())
    results = [eval_item(export, item) for item in DEFAULT_SETUP_CHECKLIST]
    summary = score_journal(results)
    return {
        "filename": path.name,
        "journal_id": export.journal_id,
        "journal_path": export.path,
        "journal_name": export.journal_title("ru") or export.journal_title("en") or export.path,
        "results": [asdict(r) for r in results],
        **summary,
    }


def attach_service_comparison(indep: dict[str, Any], service_report: dict[str, Any]) -> dict[str, Any]:
    by_id = {str(f.get("id")): f for f in (service_report.get("fields") or [])}
    mismatches = 0
    fps = 0
    fns = 0
    for r in indep["results"]:
        cid = r["rule_id"].lower().replace("_", ".")
        # rule_id STEP2_FOCUS -> step2.focus
        parts = r["rule_id"].lower().split("_")
        # better: use checklist mapping
        pass
    # rebuild with checklist ids
    for item, r in zip(DEFAULT_SETUP_CHECKLIST, indep["results"]):
        sf = by_id.get(item.id, {})
        svc = map_service_status(str(sf.get("status") or ""), str(sf.get("ui_status") or ""), str(sf.get("bucket") or ""))
        r["service_status"] = svc
        r["service_raw"] = str(sf.get("status") or "")
        r["compare"] = compare_status(r["expected_status"], svc)  # type: ignore[arg-type]
        if r["compare"] == "false_positive":
            fps += 1
            mismatches += 1
        elif r["compare"] == "false_negative":
            fns += 1
            mismatches += 1
        elif r["compare"] in {"severity_mismatch", "message_mismatch", "score_mismatch"}:
            mismatches += 1
    indep["service_score"] = service_report.get("mandatory_percent") or service_report.get("completeness_percent")
    # map service readiness roughly
    pct = float(indep["service_score"] or 0)
    if pct >= 90:
        indep["service_readiness"] = "ready"
    elif pct >= 80:
        indep["service_readiness"] = "mostly_ready"
    elif pct >= 65:
        indep["service_readiness"] = "needs_revision"
    else:
        indep["service_readiness"] = "not_ready"
    indep["mismatches"] = mismatches
    indep["false_positives"] = fps
    indep["false_negatives"] = fns
    indep["review_required"] = bool(fps or fns or indep["expected_readiness"] != indep["service_readiness"])
    return indep
