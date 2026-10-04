"""M003 / M003A / M003B — дата публикации."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta

from ipsas.modules.metafora_jats.models import (
    PassedCheck,
    Publication,
    ValidationIssue,
    issue,
)

_ISO_FULL = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_ISO_YM = re.compile(r"^(\d{4})-(\d{2})$")
_ISO_Y = re.compile(r"^(\d{4})$")
_DOT = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")


def parse_publication_date(raw: str) -> date | None:
    """Разобрать дату; ``None`` если формат недопустим."""
    text = (raw or "").strip()
    if not text:
        return None
    m = _ISO_FULL.match(text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = _DOT.match(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    m = _ISO_YM.match(text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    m = _ISO_Y.match(text)
    if m:
        try:
            return date(int(m.group(1)), 1, 1)
        except ValueError:
            return None
    # fallback ISO
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def check_publication_date(
    pub: Publication,
    *,
    today: date | None = None,
) -> tuple[list[ValidationIssue], list[PassedCheck]]:
    raw = (pub.publication_date_raw or "").strip()
    xpath = pub.publication_date_xpath or "/article/front/article-meta"
    if not raw:
        return (
            [
                issue(
                    "M003",
                    "error",
                    "Не указана дата публикации.",
                    field="publication_date",
                    xpath=xpath,
                )
            ],
            [],
        )

    parsed = parse_publication_date(raw)
    if parsed is None:
        return (
            [
                issue(
                    "M003A",
                    "error",
                    "Дата публикации имеет неправильный формат. "
                    "Допустимы: ГГГГ-ММ-ДД, ДД.ММ.ГГГГ или ГГГГ.",
                    field="publication_date",
                    value=raw,
                    xpath=xpath,
                )
            ],
            [],
        )

    ref = today or date.today()
    max_allowed = ref + timedelta(days=2)
    if parsed > max_allowed:
        return (
            [
                issue(
                    "M003B",
                    "error",
                    "Дата публикации позже допустимой "
                    f"(не позднее {max_allowed.isoformat()}).",
                    field="publication_date",
                    value=raw,
                    xpath=xpath,
                )
            ],
            [],
        )

    return (
        [],
        [PassedCheck(code="M003", label=f"Дата публикации: {parsed.isoformat()}")],
    )
