"""Парсинг HTML-форм OJS 2.4 (в т.ч. checkbox[])."""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urljoin

from lxml import html as lxml_html


@dataclass
class HtmlForm:
    action: str
    method: str
    form_id: str
    # список пар — сохраняет несколько checkbox с одним name
    fields: list[tuple[str, str]] = field(default_factory=list)

    def as_dict_first(self) -> dict[str, str]:
        """Первое значение каждого имени (для простых полей)."""
        out: dict[str, str] = {}
        for name, value in self.fields:
            if name not in out:
                out[name] = value
        return out

    def values_for(self, name: str) -> list[str]:
        return [v for n, v in self.fields if n == name]

    def set_value(self, name: str, value: str) -> None:
        """Заменить все значения name одним value (или добавить)."""
        remaining = [(n, v) for n, v in self.fields if n != name]
        remaining.append((name, value))
        self.fields = remaining

    def set_checkbox(self, name: str, checked: bool, value: str = "1") -> None:
        remaining = [(n, v) for n, v in self.fields if n != name]
        if checked:
            remaining.append((name, value))
        self.fields = remaining

    def ensure_multi_values(self, name: str, required: set[str], *, keep_others: bool = True) -> None:
        """Гарантировать наличие required среди значений name[]."""
        current = set(self.values_for(name))
        if keep_others:
            wanted = current | required
        else:
            wanted = set(required)
        remaining = [(n, v) for n, v in self.fields if n != name]
        for val in sorted(wanted):
            remaining.append((name, val))
        self.fields = remaining


def parse_forms(html: str, base_url: str = "") -> list[HtmlForm]:
    """Разобрать все формы; checkbox/radio — только отмеченные."""
    root = lxml_html.fromstring(html)
    forms: list[HtmlForm] = []
    nodes = root.xpath("self::form | .//form")
    for form in nodes:
        fields: list[tuple[str, str]] = []
        for el in form.xpath(".//input|.//textarea|.//select"):
            name = el.get("name")
            if not name:
                continue
            tag = (el.tag or "").lower()
            if tag == "input":
                typ = (el.get("type") or "text").lower()
                if typ in {"submit", "button", "image", "file"}:
                    continue
                if typ in {"checkbox", "radio"} and el.get("checked") is None:
                    continue
                fields.append((name, el.get("value") or ""))
            elif tag == "textarea":
                fields.append((name, "".join(el.itertext()) or ""))
            elif tag == "select":
                options = el.xpath(".//option")
                selected = None
                for opt in options:
                    if opt.get("selected") is not None:
                        selected = opt
                        break
                if selected is None and options:
                    selected = options[0]
                fields.append(
                    (name, (selected.get("value") if selected is not None else "") or "")
                )
        action = form.get("action") or ""
        if base_url:
            action = urljoin(base_url, action)
        forms.append(
            HtmlForm(
                action=action,
                method=(form.get("method") or "get").lower(),
                form_id=form.get("id") or "",
                fields=fields,
            )
        )
    return forms


def find_post_form(
    html: str,
    base_url: str,
    *,
    action_contains: str | None = None,
    form_id: str | None = None,
) -> HtmlForm | None:
    for form in parse_forms(html, base_url):
        if form.method != "post":
            continue
        if form_id and form.form_id != form_id:
            continue
        if action_contains and action_contains.casefold() not in form.action.casefold():
            continue
        return form
    return None


def plugin_action_links(html: str) -> dict[str, dict[str, str]]:
    """pluginKey(lower) -> {enable|disable|settings: url}."""
    root = lxml_html.fromstring(html)
    result: dict[str, dict[str, str]] = {}
    for a in root.xpath(".//a[@href]"):
        href = a.get("href") or ""
        # .../manager/plugin/{category}/{PluginKey}/enable
        parts = href.rstrip("/").split("/")
        if len(parts) < 3:
            continue
        action = parts[-1].lower()
        if action not in {"enable", "disable", "settings"}:
            continue
        if "plugin" not in href.casefold():
            continue
        key = parts[-2]
        bucket = result.setdefault(key.lower(), {})
        bucket[action] = href
        bucket["key"] = key
    return result
