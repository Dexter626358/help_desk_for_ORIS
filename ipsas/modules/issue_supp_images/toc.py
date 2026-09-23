"""Парсинг editor/issueToc: статьи и интервалы страниц."""

from __future__ import annotations

import re
from html import unescape
from urllib.parse import urlparse

from ipsas.modules.issue_supp_images.archive import parse_page_range_token
from ipsas.modules.issue_supp_images.models import IssueArticle, PageRange

PAGES_INPUT_RE = re.compile(
    r'name="pages\[(\d+)\]"[^>]*value="([^"]*)"'
    r'|value="([^"]*)"[^>]*name="pages\[(\d+)\]"',
    re.I,
)
TITLE_LINK_RE = re.compile(
    r'/editor/submission/(\d+)"[^>]*>([^<]+)',
    re.I,
)
ISSUE_TOC_RE = re.compile(
    r"/([^/]+)/editor/issueToc/(\d+)",
    re.I,
)
ISSUE_URL_RE = re.compile(
    r"/([^/]+)/(?:issue/view|editor/issueToc)/(\d+)",
    re.I,
)


def parse_issue_ref(url: str) -> tuple[str, str, int]:
    """Вернуть (base_url, journal_path, issue_id) из ссылки выпуска/TOC."""
    raw = (url or "").strip()
    if not raw:
        raise ValueError("Укажите ссылку на выпуск (issueToc / issue/view)")
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    if not parsed.netloc:
        raise ValueError(f"Некорректный URL: {url}")
    base = f"{parsed.scheme}://{parsed.netloc}"
    m = ISSUE_URL_RE.search(parsed.path or "")
    if not m:
        raise ValueError(
            "Ожидается ссылка вида …/JOURNAL/editor/issueToc/ID "
            "или …/JOURNAL/issue/view/ID"
        )
    journal = m.group(1)
    issue_id = int(m.group(2))
    return base.rstrip("/"), journal, issue_id


def issue_toc_url(base_url: str, journal: str, issue_id: int) -> str:
    return f"{base_url.rstrip('/')}/{journal}/editor/issueToc/{issue_id}"


def submission_editing_url(base_url: str, journal: str, article_id: int) -> str:
    return (
        f"{base_url.rstrip('/')}/{journal}/editor/submissionEditing/{article_id}"
    )


def parse_issue_toc(html: str) -> list[IssueArticle]:
    """Извлечь статьи с pages[articleId] из HTML issueToc."""
    titles: dict[int, str] = {}
    for m in TITLE_LINK_RE.finditer(html):
        aid = int(m.group(1))
        titles[aid] = unescape(m.group(2)).strip()

    articles: list[IssueArticle] = []
    seen: set[int] = set()
    for m in PAGES_INPUT_RE.finditer(html):
        if m.group(1):
            aid_s, pages_s = m.group(1), m.group(2)
        else:
            pages_s, aid_s = m.group(3), m.group(4)
        aid = int(aid_s)
        if aid in seen:
            continue
        page_range = parse_page_range_token(pages_s)
        if page_range is None:
            continue
        seen.add(aid)
        articles.append(
            IssueArticle(
                article_id=aid,
                pages=page_range,
                title=titles.get(aid, ""),
            )
        )
    articles.sort(key=lambda a: (a.pages.start, a.pages.end, a.article_id))
    return articles


def match_article(
    articles: list[IssueArticle],
    page_range: PageRange,
) -> IssueArticle | None:
    for article in articles:
        if article.pages.matches(page_range):
            return article
    return None
