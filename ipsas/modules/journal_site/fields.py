"""Каталог проверяемых полей сайта журнала (OJS / RCSI)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Status = Literal["filled", "weak", "empty", "missing", "error"]

CATEGORY_HOME = "homepage"
CATEGORY_PEOPLE = "people"
CATEGORY_POLICIES = "policies"
CATEGORY_SUBMISSIONS = "submissions"
CATEGORY_OTHER = "other"

# Совместимость со старыми именами в письмах/тестах
CATEGORY_ABOUT = CATEGORY_PEOPLE
CATEGORY_CONTACT = CATEGORY_PEOPLE


@dataclass(frozen=True, slots=True)
class FieldSpec:
    id: str
    title: str
    category: str
    weight: float = 1.0
    min_chars_filled: int = 8
    min_chars_weak: int = 1


@dataclass(frozen=True, slots=True)
class AboutMenuItem:
    """Пункт меню страницы /about."""

    id: str
    title: str
    category: str
    page_url: str
    fragment: str = ""
    weight: float = 1.0
    min_chars_filled: int = 40
    min_chars_weak: int = 10


# Только фиксированные поля главной (см. критерии journal.* на /index).
HOME_FIELD_SPECS: tuple[FieldSpec, ...] = (
    FieldSpec("journal_title", "Название журнала", CATEGORY_HOME, weight=1.3, min_chars_filled=3),
    FieldSpec("issn", "ISSN", CATEGORY_HOME, weight=1.3, min_chars_filled=8),
    FieldSpec(
        "description",
        "Описание журнала на главной",
        CATEGORY_HOME,
        weight=1.2,
        min_chars_filled=80,
        min_chars_weak=20,
    ),
    FieldSpec("homepage_image", "Обложка на главной", CATEGORY_HOME, weight=1.0, min_chars_filled=3),
    FieldSpec("current_issue", "Текущий выпуск на главной", CATEGORY_HOME, weight=1.0, min_chars_filled=3),
)

# Обратная совместимость: раньше экспортировали FIELD_SPECS
FIELD_SPECS: tuple[FieldSpec, ...] = HOME_FIELD_SPECS

CATEGORY_TITLES: dict[str, str] = {
    CATEGORY_HOME: "Главная страница",
    CATEGORY_PEOPLE: "О журнале · People",
    CATEGORY_POLICIES: "О журнале · Policies",
    CATEGORY_SUBMISSIONS: "О журнале · Submissions",
    CATEGORY_OTHER: "О журнале · Other",
}

# Заголовки секций на /about (EN + RU): точное совпадение или вхождение.
ABOUT_SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    CATEGORY_PEOPLE: (
        "people",
        "люди",
        "редакция",  # часто вместо People
    ),
    CATEGORY_POLICIES: (
        "policies",
        "политики",
        "политика",
        "политика редакции",
    ),
    CATEGORY_SUBMISSIONS: (
        "submissions",
        "отправка материалов",
        "подача рукописей",
        "прием статей",
        "приём статей",
        "рукописи",
    ),
    CATEGORY_OTHER: (
        "other",
        "другое",
        "прочее",
    ),
}

# устаревший плоский словарь — для совместимости импортов
ABOUT_SECTION_CATEGORIES: dict[str, str] = {
    alias: cat
    for cat, aliases in ABOUT_SECTION_ALIASES.items()
    for alias in aliases
}


def resolve_about_section_category(section_title: str) -> str | None:
    """Определить категорию секции /about по заголовку H2."""
    key = (section_title or "").strip().lower()
    if not key:
        return None
    # Сначала более длинные алиасы, чтобы «политика редакции» не ушла в «редакция».
    ranked: list[tuple[str, str]] = []
    for cat, aliases in ABOUT_SECTION_ALIASES.items():
        for alias in aliases:
            ranked.append((alias, cat))
    ranked.sort(key=lambda x: len(x[0]), reverse=True)

    for alias, cat in ranked:
        if key == alias:
            return cat
        if key.startswith(alias + " ") or key.startswith(alias + "·") or key.startswith(alias + ":"):
            return cat
        # «Политика редакции» и т.п. без пробела после точного длинного алиаса уже покрыто ==
        if len(alias) >= 8 and alias in key:
            return cat
    return None

# Суффиксы путей OJS, которые нужно срезать до корня журнала
_JOURNAL_PATH_SUFFIXES: tuple[str, ...] = tuple(
    sorted(
        (
            "/index",
            "/about/editorialTeam",
            "/about/editorialPolicies",
            "/about/submissions",
            "/about/contact",
            "/about/history",
            "/about/subscriptions",
            "/about",
            "/search",
            "/login",
            "/user/register",
            "/gateway",
        ),
        key=len,
        reverse=True,
    )
)


def about_item_to_spec(item: AboutMenuItem) -> FieldSpec:
    return FieldSpec(
        id=item.id,
        title=item.title,
        category=item.category,
        weight=item.weight,
        min_chars_filled=item.min_chars_filled,
        min_chars_weak=item.min_chars_weak,
    )


def thresholds_for_about_item(title: str, fragment: str, path: str) -> tuple[int, int, float]:
    """min_filled, min_weak, weight по типу пункта about."""
    low = f"{title} {fragment} {path}".lower()
    if "editorialteam" in path.lower() or "editorial team" in low or "редколлег" in low:
        return 80, 30, 1.5
    if "authorguidelines" in low or "author guidelines" in low or "руководств" in low:
        return 80, 30, 1.3
    if fragment in {"focusAndScope", "peerReviewProcess"} or "aims" in low or "peer review" in low:
        return 60, 20, 1.3
    if "contact" in path.lower() and not fragment:
        return 40, 15, 1.2
    if fragment.startswith("custom-") or "plagiarism" in low or "ethic" in low or "этик" in low:
        return 40, 15, 1.2
    return 40, 10, 1.0
