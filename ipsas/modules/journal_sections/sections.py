"""Список и создание разделов на manager/sections / createSection."""

from __future__ import annotations

import logging
import time
from typing import Protocol
from urllib.parse import urlencode, urljoin

from ipsas.modules.journal_sections.models import ExistingSection, SectionSpec
from ipsas.modules.sandbox_journal_setup.forms import find_post_form
from ipsas.modules.sandbox_journal_setup.urls import journal_url

logger = logging.getLogger(__name__)


class PlatformHttpClient(Protocol):
    base_url: str

    def get_text(self, url: str, encoding: str = "utf-8") -> str: ...

    def request(
        self,
        url: str,
        *,
        data: dict[str, str] | list[tuple[str, str]] | None = None,
        method: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes]: ...


def normalize_title(value: str) -> str:
    return " ".join((value or "").split()).casefold()


def list_sections(html: str, page_url: str = "") -> list[ExistingSection]:
    """Разобрать таблицу разделов ``tr#section-*``."""
    from lxml import html as lxml_html

    root = lxml_html.fromstring(html)
    rows = root.xpath(".//tr[starts-with(@id,'section-')]")
    result: list[ExistingSection] = []
    for row in rows:
        sid = (row.get("id") or "").replace("section-", "").strip()
        cells = ["".join(td.itertext()).strip() for td in row.xpath("./td")]
        title = cells[0] if cells else ""
        abbrev = cells[1] if len(cells) > 1 else ""
        hrefs = row.xpath(".//a[contains(@href,'editSection')]/@href")
        if not hrefs:
            continue
        edit_url = urljoin(page_url, hrefs[0]) if page_url else hrefs[0]
        result.append(
            ExistingSection(
                section_id=sid,
                title=title,
                abbrev=abbrev,
                edit_url=edit_url,
            )
        )
    return result


def find_by_title_ru(
    existing: list[ExistingSection],
    title_ru: str,
) -> ExistingSection | None:
    needle = normalize_title(title_ru)
    if not needle:
        return None
    for section in existing:
        if normalize_title(section.title) == needle:
            return section
    return None


def create_section(
    auth: PlatformHttpClient,
    *,
    base_url: str,
    journal: str,
    spec: SectionSpec,
    dry_run: bool = False,
    delay: float = 0.35,
) -> tuple[str, str]:
    """Создать раздел. Returns ``(status, message)`` где status: created|dry_run|error."""
    create_url = journal_url(base_url, journal, "manager/createSection")
    summary = (
        f"«{spec.title_ru}» / «{spec.title_en}», "
        f"сокр. «{spec.short_title_ru}» / «{spec.short_title_en}»"
    )
    if dry_run:
        return "dry_run", f"будет создан: {summary}"

    html = auth.get_text(create_url)
    form = find_post_form(html, create_url, action_contains="updateSection")
    if form is None:
        form = find_post_form(html, create_url)
    if form is None:
        return "error", "форма создания раздела не найдена"

    form.set_value("title[ru_RU]", spec.title_ru)
    form.set_value("abbrev[ru_RU]", spec.short_title_ru)
    form.set_value("policy[ru_RU]", "")
    form.set_value("title[en_US]", spec.title_en)
    form.set_value("abbrev[en_US]", spec.short_title_en)
    form.set_value("policy[en_US]", "")
    form.set_checkbox("hideAbout", True, "1")
    form.set_checkbox("editorRestriction", True, "1")

    status, final_url, body = auth.request(form.action, data=form.fields)
    if delay > 0:
        time.sleep(delay)
    if status >= 400:
        return "error", f"HTTP {status} при сохранении раздела"
    text = body.decode("utf-8", "replace")
    if "signinForm" in text or "loginUsername" in text:
        return "error", "сессия потеряна при создании раздела"
    if "error" in text.casefold() and "createSection" in (final_url or form.action):
        logger.warning(
            "createSection возможно вернул ошибку: url=%s title=%r",
            final_url,
            spec.title_ru,
        )
    return "created", f"создан: {summary}"


def sorted_by_title_ru(sections: list[ExistingSection]) -> list[ExistingSection]:
    """Алфавитный порядок по русскому названию (как в таблице разделов)."""
    return sorted(sections, key=lambda s: (normalize_title(s.title), s.section_id))


def is_articles_section(title: str, abbrev: str = "") -> bool:
    """Раздел «Статьи» / Articles — не двигаем при алфавитной сортировке."""
    t = normalize_title(title)
    a = normalize_title(abbrev)
    if t in {"статьи", "articles"}:
        return True
    if a in {"стат", "art", "статьи"}:
        return True
    return False


def order_titles_alpha_keep_articles(titles: list[str]) -> list[str]:
    """A→Я по названиям, «Статьи» остаётся на своём индексе."""
    pinned_idx: int | None = None
    pinned: str | None = None
    others: list[str] = []
    for i, title in enumerate(titles):
        if pinned is None and is_articles_section(title):
            pinned = title
            pinned_idx = i
        else:
            others.append(title)
    others_sorted = sorted(others, key=normalize_title)
    if pinned is None or pinned_idx is None:
        return others_sorted
    idx = min(max(pinned_idx, 0), len(others_sorted))
    return others_sorted[:idx] + [pinned] + others_sorted[idx:]


def order_sections_alpha_keep_articles(
    sections: list[ExistingSection],
) -> list[ExistingSection]:
    """A→Я, раздел «Статьи» остаётся на текущей позиции."""
    pinned_idx: int | None = None
    pinned: ExistingSection | None = None
    others: list[ExistingSection] = []
    for i, section in enumerate(sections):
        if pinned is None and is_articles_section(section.title, section.abbrev):
            pinned = section
            pinned_idx = i
        else:
            others.append(section)
    others_sorted = sorted_by_title_ru(others)
    if pinned is None or pinned_idx is None:
        return others_sorted
    idx = min(max(pinned_idx, 0), len(others_sorted))
    return others_sorted[:idx] + [pinned] + others_sorted[idx:]


def sort_sections_alphabetically(
    auth: PlatformHttpClient,
    *,
    base_url: str,
    journal: str,
    sections: list[ExistingSection] | None = None,
    dry_run: bool = False,
    delay: float = 0.35,
) -> tuple[str, str]:
    """Выстроить разделы A→Я через ``manager/moveSection`` (prevId).

    Раздел «Статьи» / Articles не перемещается — сохраняет текущий индекс.

    Returns:
        ``(status, message)`` — ``sorted`` | ``unchanged`` | ``dry_run`` | ``error``.
    """
    list_url = journal_url(base_url, journal, "manager/sections")
    if sections is None:
        html = auth.get_text(list_url)
        if "signinForm" in html or "loginUsername" in html:
            return "error", "нет доступа к списку разделов для сортировки"
        sections = list_sections(html, list_url)

    if len(sections) < 2:
        return "unchanged", "меньше двух разделов — сортировка не нужна"

    # только с id (нужны для moveSection)
    movable = [s for s in sections if s.section_id]
    if len(movable) < 2:
        return "error", "не удалось определить id разделов для сортировки"

    desired = order_sections_alpha_keep_articles(movable)
    current_ids = [s.section_id for s in movable]
    desired_ids = [s.section_id for s in desired]
    order_preview = " → ".join(s.title for s in desired)
    pinned_note = ""
    if any(is_articles_section(s.title, s.abbrev) for s in movable):
        pinned_note = " («Статьи» на месте)"

    if current_ids == desired_ids:
        return "unchanged", f"уже по алфавиту{pinned_note}: {order_preview}"

    if dry_run:
        return "dry_run", f"будет порядок{pinned_note}: {order_preview}"

    move_base = journal_url(base_url, journal, "manager/moveSection")
    for index, section in enumerate(desired):
        params: dict[str, str] = {"id": section.section_id}
        if index > 0:
            params["prevId"] = desired[index - 1].section_id
        move_url = f"{move_base}?{urlencode(params)}"
        status, _, body = auth.request(move_url, method="GET")
        if delay > 0:
            time.sleep(delay)
        if status >= 400:
            return (
                "error",
                f"moveSection HTTP {status} для «{section.title}» (id={section.section_id})",
            )
        text = body.decode("utf-8", "replace")
        if "signinForm" in text or "loginUsername" in text:
            return "error", "сессия потеряна при сортировке разделов"

    # проверка
    verify_html = auth.get_text(list_url)
    after = [s.section_id for s in list_sections(verify_html, list_url) if s.section_id]
    if after[: len(desired_ids)] != desired_ids and after != desired_ids:
        logger.warning(
            "После сортировки порядок отличается: got=%s want=%s",
            after,
            desired_ids,
        )
        return (
            "error",
            f"порядок после moveSection не совпал с ожидаемым ({order_preview})",
        )
    return "sorted", f"отсортировано{pinned_note}: {order_preview}"
