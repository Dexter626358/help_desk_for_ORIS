"""M007 / M008 — авторы."""

from __future__ import annotations

from ipsas.modules.metafora_jats.article_types import authors_required_for_type
from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)


def check_authors(
    pub: Publication,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    required = authors_required_for_type(pub.publication_type)
    authors = list(pub.authors)

    def _authors_label(prefix: str) -> str:
        names = [a.display_name for a in authors if a.display_name]
        if not names:
            return prefix
        shown = "; ".join(names[:8])
        if len(names) > 8:
            shown += f" … (+{len(names) - 8})"
        return f"{prefix}: {shown}"

    if not required:
        return (
            [],
            [
                PassedCheck(
                    code="M007",
                    label=_authors_label("Авторы (для данного типа не обязательны)"),
                )
            ],
        )

    if not authors:
        return (
            [
                issue(
                    "M007",
                    "error",
                    "Не указан ни один автор публикации.",
                    field="authors",
                    xpath=(
                        "/article/front/article-meta"
                        "//contrib[@contrib-type='author']"
                    ),
                )
            ],
            [],
        )

    issues: list[ValidationIssue] = []
    for idx, author in enumerate(authors, start=1):
        if not author.has_name:
            issues.append(
                issue(
                    "M008",
                    "error",
                    "У автора не указаны фамилия или имя.",
                    field="authors",
                    value=f"author#{idx}",
                    xpath=author.xpath or ".//contrib[@contrib-type='author']",
                )
            )

    if issues:
        return issues, []
    return ([], [PassedCheck(code="M007", label=_authors_label("Авторы"))])
