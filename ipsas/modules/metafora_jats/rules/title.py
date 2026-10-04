"""M002 — название публикации."""

from __future__ import annotations

from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)


def check_title(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    titles = [t for t in pub.titles if (t.text or "").strip()]
    if not titles:
        return (
            [
                issue(
                    "M002",
                    "error",
                    "Не указано название публикации.",
                    field="titles",
                    xpath="/article/front/article-meta/title-group",
                )
            ],
            [],
        )
    text = titles[0].text.strip()
    if len(text) > 120:
        text = text[:117] + "…"
    return ([], [PassedCheck(code="M002", label=f"Название: {text}")])
