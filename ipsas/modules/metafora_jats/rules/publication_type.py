"""M001 — тип публикации."""

from __future__ import annotations

from ipsas.modules.metafora_jats.article_types import label_for_article_type
from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)


def check_publication_type(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    value = (pub.publication_type or "").strip()
    if not value:
        return (
            [
                issue(
                    "M001",
                    "error",
                    "Не указан тип публикации.",
                    field="publication_type",
                    xpath=pub.publication_type_xpath,
                )
            ],
            [],
        )
    label = label_for_article_type(value)
    return (
        [],
        [
            PassedCheck(
                code="M001",
                label=f"Тип публикации: {label} ({value})",
            )
        ],
    )
