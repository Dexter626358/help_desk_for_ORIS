"""M009 — список литературы для научных статей."""

from __future__ import annotations

from ipsas.modules.metafora_jats.article_types import references_required_for_type
from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)


def check_references(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    if not references_required_for_type(pub.publication_type):
        return (
            [],
            [
                PassedCheck(
                    code="M009",
                    label="Список литературы (для данного типа не обязателен)",
                )
            ],
        )

    if pub.reference_count <= 0:
        return (
            [
                issue(
                    "M009",
                    "error",
                    "У научной статьи отсутствует список литературы "
                    "(нужен непустой ref-list с элементами ref).",
                    field="references",
                    xpath=pub.references_xpath or "/article/back/ref-list",
                )
            ],
            [],
        )

    return (
        [],
        [
            PassedCheck(
                code="M009",
                label=f"Список литературы ({pub.reference_count})",
            )
        ],
    )
