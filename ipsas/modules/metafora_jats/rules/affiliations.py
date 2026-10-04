"""M010 — аффилиации авторов (справочно, не блокирует Метафору)."""

from __future__ import annotations

from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
)


def check_affiliations(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    authors = list(pub.authors)
    if not authors:
        return (
            [],
            [
                PassedCheck(
                    code="M010",
                    label="Аффилиации (нет авторов — не применимо)",
                    category="metadata_quality",
                )
            ],
        )

    missing = [
        (idx, author)
        for idx, author in enumerate(authors, start=1)
        if not author.has_affiliation
    ]
    if not missing:
        return (
            [],
            [
                PassedCheck(
                    code="M010",
                    label="Аффилиации авторов",
                    category="metadata_quality",
                )
            ],
        )

    issues: list[ValidationIssue] = []
    for idx, author in missing:
        label = author.display_name or f"author#{idx}"
        issues.append(
            ValidationIssue(
                code="M010",
                severity="info",
                message=(
                    f"У автора не указана аффилиация (справочно, "
                    f"на отправку в Метафору не влияет): {label}."
                ),
                field="affiliations",
                value=label,
                xpath=author.xpath
                or "/article/front/article-meta//contrib[@contrib-type='author']",
                category="metadata_quality",
            )
        )
    return issues, []
