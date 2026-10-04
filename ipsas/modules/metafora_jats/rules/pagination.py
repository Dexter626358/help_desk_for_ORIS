"""M004 / M004A / M004B — страницы или elocation-id."""

from __future__ import annotations

import re

from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)

_INT = re.compile(r"^\d+$")

# Длинные тире / минусы, которые Метафора не принимает в диапазоне страниц.
_BAD_DASHES = (
    "\u2014",  # —
    "\u2013",  # –
    "\u2012",  # ‒
    "\u2015",  # ―
    "\u2212",  # −
)


def _has_bad_dash(value: str) -> bool:
    return any(ch in value for ch in _BAD_DASHES)


def check_pagination(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    fpage = (pub.fpage or "").strip()
    lpage = (pub.lpage or "").strip()
    page_range = (pub.page_range or "").strip()
    eloc = (pub.elocation_id or "").strip()
    xpath = pub.pagination_xpath or "/article/front/article-meta"

    has_pages = bool(fpage and lpage)
    has_range = bool(page_range)
    has_eloc = bool(eloc)

    if not has_pages and not has_range and not has_eloc:
        return (
            [
                issue(
                    "M004",
                    "error",
                    "Не указаны первая и последняя страницы или номер "
                    "электронной статьи (elocation-id).",
                    field="pagination",
                    xpath=xpath,
                )
            ],
            [],
        )

    issues: list[ValidationIssue] = []

    # Метафора: диапазон только с обычным дефисом, напр. 234-456
    dash_candidates: list[tuple[str, str]] = []
    if page_range:
        dash_candidates.append((page_range, "/article/front/article-meta/page-range"))
    if fpage:
        dash_candidates.append((fpage, "/article/front/article-meta/fpage"))
    if lpage:
        dash_candidates.append((lpage, "/article/front/article-meta/lpage"))
    if has_pages:
        dash_candidates.append(
            (f"{fpage}-{lpage}", "/article/front/article-meta/fpage|lpage")
        )

    for value, dash_xpath in dash_candidates:
        if _has_bad_dash(value):
            issues.append(
                issue(
                    "M004B",
                    "error",
                    "В диапазоне страниц использовано длинное тире. "
                    "Для Метафоры нужен обычный дефис, например 234-456 "
                    "(неверно: 234—456).",
                    field="pagination",
                    value=value,
                    xpath=dash_xpath,
                )
            )
            break

    if has_pages:
        if _INT.match(fpage) and _INT.match(lpage):
            if int(fpage) > int(lpage):
                issues.append(
                    issue(
                        "M004A",
                        "error",
                        "Первая страница публикации больше последней.",
                        field="pagination",
                        value=f"{fpage}-{lpage}",
                        xpath=xpath,
                    )
                )
        elif not any(i.code == "M004B" for i in issues):
            issues.append(
                issue(
                    "M004W",
                    "warning",
                    "Пагинация в нестандартном формате — сравнение "
                    "первой и последней страницы не выполнено.",
                    field="pagination",
                    value=f"{fpage}-{lpage}",
                    xpath=xpath,
                )
            )

    if any(i.severity == "error" for i in issues):
        return issues, []

    if has_pages:
        pages_label = f"Страницы: {fpage}-{lpage}"
        if page_range and page_range != f"{fpage}-{lpage}":
            pages_label += f" (page-range: {page_range})"
    elif has_range:
        pages_label = f"Страницы (page-range): {page_range}"
    else:
        pages_label = f"elocation-id: {eloc}"
    return issues, [PassedCheck(code="M004", label=pages_label)]
