"""Шаблоны содержания главной (шаг 5 setup) для песочницы."""

from __future__ import annotations

import re

# Плейсхолдеры без конкретных ISSN / ФИО / периодичности — редакция заполняет вручную.

HOMEPAGE_DESCRIPTION_RU = """\
<p>ISSN (print):&nbsp;[указать],&nbsp;ISSN (online):&nbsp;[указать]</p>
<p>Учредитель: [указать]</p>
<p>Главный редактор: [указать]</p>
<p>Периодичность / доступ: [указать] / [указать]</p>
<p>Входит в: [указать]</p>
"""

HOMEPAGE_DESCRIPTION_EN = """\
<p>ISSN (print):&nbsp;[specify],&nbsp;ISSN (online):&nbsp;[specify]</p>
<p>Founder: [specify]</p>
<p>Editor-in-Chief: [specify]</p>
<p>Frequency / Access: [specify] / [specify]</p>
<p>Included in: [specify]</p>
"""

_TAG_RE = re.compile(r"<[^>]+>")


def is_blank_description(html: str) -> bool:
    """True, если поле пустое (нет текста для читателя)."""
    text = (html or "").strip()
    if not text:
        return True
    plain = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("<br>", " ")
        .replace("<br/>", " ")
        .replace("<br />", " ")
    )
    plain = _TAG_RE.sub(" ", plain)
    return not plain.split()
