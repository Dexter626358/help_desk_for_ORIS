"""Парсер экспорта настроек журнала OJS (.data / JSON)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from html import unescape
from typing import Any, Literal, Mapping, Optional

from ipsas.modules.journal_site.parser import norm_space

PluginEnabled = Literal[True, False]

_TAG_RE = re.compile(r"<[^>]+>", re.DOTALL)
_PLACEHOLDER_RE = re.compile(r"##[^#]+##")
_LOCALE_ALIAS = {
    "ru": "ru",
    "ru_ru": "ru",
    "en": "en",
    "en_us": "en",
    "en_gb": "en",
}


def html_to_text(value: str) -> str:
    """Убрать HTML-теги и placeholder'ы OJS, нормализовать пробелы."""
    raw = unescape(str(value or ""))
    raw = _PLACEHOLDER_RE.sub(" ", raw)
    raw = _TAG_RE.sub(" ", raw)
    return norm_space(raw)


def normalize_locale_key(locale: str) -> str:
    key = (locale or "").strip().replace("-", "_").lower()
    if key in _LOCALE_ALIAS:
        return _LOCALE_ALIAS[key]
    if "_" in key:
        return key.split("_", 1)[0]
    return key


@dataclass(slots=True)
class PluginInfo:
    name: str
    category: str
    enabled: bool
    settings: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "enabled": self.enabled,
            "settings": dict(self.settings),
        }


@dataclass(slots=True)
class JournalDataExport:
    journal_id: str
    path: str
    settings: dict[str, Any]
    plugins: dict[str, PluginInfo]
    sections: list[dict[str, Any]] = field(default_factory=list)
    static_pages: list[dict[str, Any]] = field(default_factory=list)
    supported_locales: list[str] = field(default_factory=list)
    raw_journal: dict[str, Any] = field(default_factory=dict)

    def get_raw_setting(self, name: str) -> Any:
        return self.settings.get(name)

    def get_setting_text(self, name: str, lang: str) -> str:
        """Текст настройки для локали (пустая строка, если нет)."""
        value = self.settings.get(name)
        return _value_to_text(value, lang)

    def setting_present(self, name: str, lang: str) -> bool:
        return bool(self.get_setting_text(name, lang))

    def custom_about_by_title(self, lang: str) -> dict[str, str]:
        """title(lower) → content text из customAboutItems."""
        value = self.settings.get("customAboutItems")
        items = _localized_list(value, lang)
        out: dict[str, str] = {}
        for item in items:
            if not isinstance(item, Mapping):
                continue
            title = html_to_text(str(item.get("title") or ""))
            content = html_to_text(str(item.get("content") or ""))
            if title:
                out[title.lower()] = content
        return out

    def find_custom_about(self, lang: str, *title_needles: str) -> str:
        items = self.custom_about_by_title(lang)
        if not items:
            return ""
        for needle in title_needles:
            n = needle.lower()
            for title, content in items.items():
                if n in title:
                    return content
        return ""

    def journal_title(self, lang: str = "ru") -> str:
        return self.get_setting_text("title", lang) or self.path or self.journal_id

    def to_summary(self) -> dict[str, Any]:
        return {
            "journal_id": self.journal_id,
            "path": self.path,
            "supported_locales": list(self.supported_locales),
            "settings_count": len(self.settings),
            "plugins_count": len(self.plugins),
            "sections_count": len(self.sections),
            "static_pages_count": len(self.static_pages),
        }


def parse_journal_data(
    payload: bytes | str | Mapping[str, Any],
    *,
    max_bytes: int = 10_485_760,
) -> JournalDataExport:
    """Разобрать JSON-экспорт OJS (.data)."""
    if isinstance(payload, Mapping):
        root = dict(payload)
    else:
        if isinstance(payload, bytes):
            if len(payload) > max_bytes:
                raise ValueError(
                    f"Файл слишком большой. Максимальный размер: {max_bytes / (1024 * 1024):.1f} MB"
                )
            if not payload.strip():
                raise ValueError("Файл пустой")
            text = payload.decode("utf-8-sig")
        else:
            text = str(payload)
            if len(text.encode("utf-8")) > max_bytes:
                raise ValueError(
                    f"Файл слишком большой. Максимальный размер: {max_bytes / (1024 * 1024):.1f} MB"
                )
            if not text.strip():
                raise ValueError("Файл пустой")
        try:
            root = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Некорректный JSON в файле .data: {exc}") from exc

    if not isinstance(root, dict) or not root:
        raise ValueError("В файле .data нет данных журнала")

    journal_id, block = _pick_journal_block(root)
    if not isinstance(block, dict):
        raise ValueError("Блок журнала в .data должен быть объектом")

    settings_raw = block.get("settings")
    if not isinstance(settings_raw, dict):
        raise ValueError("В .data отсутствует объект settings")

    plugins_raw = block.get("plugins") if isinstance(block.get("plugins"), dict) else {}
    plugins = {
        name: _parse_plugin(name, info)
        for name, info in plugins_raw.items()
        if isinstance(info, dict)
    }

    journal_meta = block.get("journal") if isinstance(block.get("journal"), dict) else {}
    path = str(journal_meta.get("path") or "").strip()

    locales_raw = settings_raw.get("supportedLocales")
    supported: list[str] = []
    if isinstance(locales_raw, list):
        for item in locales_raw:
            norm = normalize_locale_key(str(item))
            if norm and norm not in supported:
                supported.append(norm)

    sections = [x for x in (block.get("sections") or []) if isinstance(x, dict)]
    static_pages = [x for x in (block.get("staticPages") or []) if isinstance(x, dict)]

    return JournalDataExport(
        journal_id=str(journal_id),
        path=path,
        settings=dict(settings_raw),
        plugins=plugins,
        sections=sections,
        static_pages=static_pages,
        supported_locales=supported,
        raw_journal=dict(journal_meta),
    )


def _pick_journal_block(root: Mapping[str, Any]) -> tuple[str, Any]:
    # типичный вид: {"308": {...}} или уже сам блок журнала
    if "settings" in root and isinstance(root.get("settings"), dict):
        return str(root.get("journal_id") or root.get("id") or "journal"), root

    # предпочесть числовой id
    items = [(k, v) for k, v in root.items() if isinstance(v, dict)]
    if not items:
        raise ValueError("Не найден блок журнала в .data")
    for key, value in items:
        if isinstance(value.get("settings"), dict):
            return str(key), value
    return str(items[0][0]), items[0][1]


def _parse_plugin(name: str, info: Mapping[str, Any]) -> PluginInfo:
    category = str(info.get("category") or "").strip()
    settings = info.get("settings")
    enabled = False
    sett_dict: dict[str, Any] = {}
    if isinstance(settings, dict):
        sett_dict = dict(settings)
        raw_en = sett_dict.get("enabled")
        if raw_en is True or raw_en == 1 or str(raw_en).lower() in {"1", "true"}:
            enabled = True
        elif raw_en is False or raw_en == 0 or str(raw_en).lower() in {"0", "false"}:
            enabled = False
        else:
            # dict без явного enabled — считаем выключенным по чек-листу Sergio
            enabled = False
    # list / None / прочее → выключен
    return PluginInfo(name=name, category=category, enabled=enabled, settings=sett_dict)


def _locale_candidates(lang: str) -> list[str]:
    lang_n = normalize_locale_key(lang)
    if lang_n == "ru":
        return ["ru_RU", "ru", "ru_ru"]
    if lang_n == "en":
        return ["en_US", "en", "en_GB", "en_us", "en_gb"]
    return [lang, lang_n]


def _pick_localized(value: Mapping[str, Any], lang: str) -> Any:
    candidates = _locale_candidates(lang)
    lower_map = {str(k).lower(): v for k, v in value.items()}
    for cand in candidates:
        if cand in value:
            return value[cand]
        hit = lower_map.get(cand.lower())
        if hit is not None:
            return hit
    # fallback: любой ключ с тем же языковым префиксом
    prefix = normalize_locale_key(lang)
    for key, val in value.items():
        if normalize_locale_key(str(key)) == prefix:
            return val
    return None


def _localized_list(value: Any, lang: str) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, Mapping):
        picked = _pick_localized(value, lang)
        if isinstance(picked, list):
            return picked
    return []


def _value_to_text(value: Any, lang: str) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return html_to_text(value)
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                parts.append(
                    html_to_text(
                        " ".join(
                            str(item.get(k) or "")
                            for k in ("title", "content", "name", "label")
                            if item.get(k)
                        )
                    )
                )
            else:
                parts.append(html_to_text(str(item)))
        return norm_space(" ".join(p for p in parts if p))
    if isinstance(value, Mapping):
        # изображение / файл
        if "uploadName" in value or "name" in value and "width" in value:
            name = str(value.get("uploadName") or value.get("name") or "").strip()
            return name
        picked = _pick_localized(value, lang)
        if picked is None:
            # возможно это не локализованный dict, а структура файла без локалей
            if any(k in value for k in ("uploadName", "name", "url")):
                return str(value.get("uploadName") or value.get("name") or value.get("url") or "")
            return ""
        return _value_to_text(picked, lang)
    return html_to_text(str(value))
