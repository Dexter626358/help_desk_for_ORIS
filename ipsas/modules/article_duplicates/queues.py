"""Выгрузка рукописей из активных очередей редактора OJS (без Архива)."""

from __future__ import annotations

import logging
import re
import time
import urllib.parse
from typing import Protocol

from ipsas.modules.archive_by_sender.parser import (
    extract_page_numbers,
    extract_search_params,
    is_login_page,
    parse_result_range,
)
from ipsas.modules.article_duplicates.models import QueueSubmission
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError

logger = logging.getLogger(__name__)

# Архив не включаем: там уже отработанные статьи.
QUEUE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("unassigned", "submissionsUnassigned", "Новые"),
    ("in_review", "submissionsInReview", "На рецензии"),
    ("in_editing", "submissionsInEditing", "В редакции"),
)

# id | date | section | authors | [пустые td] | title-link | optional status
# В очереди «В редакции» перед ссылкой бывает пустая ячейка (иконки/статус).
_SUBMISSION_ROW_RE = re.compile(
    r"<td>(\d+)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>"
    r"(?:\s*<td[^>]*>\s*</td>)*"
    r'\s*<td[^>]*>\s*<a href="([^"]+)"([^>]*)>(.*?)</a>\s*</td>'
    r"(?:\s*<td>([^<]*)</td>)?",
    re.I | re.S,
)
_TITLE_ATTR_RE = re.compile(r'\btitle="([^"]*)"', re.I)
_EDITOR_SUBMISSION_PATH_RE = re.compile(
    r"/(?:editor/)?(?:submission(?:Editing|Review)?)/(\d+)\b",
    re.I,
)


class PlatformHttpClient(Protocol):
    base_url: str

    def get_text(self, url: str, encoding: str = "utf-8") -> str: ...


def queue_list_url(base_url: str, journal: str, queue_path: str) -> str:
    return f"{base_url.rstrip('/')}/{journal}/editor/submissions/{queue_path}"


def queue_page_url(
    base_url: str,
    journal: str,
    queue_path: str,
    page: int,
    params: dict[str, str] | None = None,
) -> str:
    query = dict(params or {})
    if page > 1:
        query["submissionsPage"] = str(page)
    else:
        query.pop("submissionsPage", None)
    base = queue_list_url(base_url, journal, queue_path)
    if not query:
        return base
    return f"{base}?{urllib.parse.urlencode(query)}"


def _clean_text(value: str) -> str:
    from html import unescape

    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).replace("\xa0", " ").strip()


def _normalize_submission_url(raw_url: str, article_id: int) -> str:
    """Привести submissionEditing/Review → editor/submission/{id}."""
    url = (raw_url or "").strip()
    if not url:
        return url
    match = _EDITOR_SUBMISSION_PATH_RE.search(url)
    if not match:
        return url
    head = url[: match.start()]
    # сохранить origin+journal: .../0044-4502
    return f"{head}/editor/submission/{article_id}"


def parse_queue_submissions(
    html: str,
    *,
    queue: str,
    queue_label: str,
) -> list[QueueSubmission]:
    items: list[QueueSubmission] = []
    for match in _SUBMISSION_ROW_RE.finditer(html):
        article_id = int(match.group(1))
        link_attrs = match.group(6) or ""
        link_text = _clean_text(match.group(7))
        attr_title = ""
        attr_match = _TITLE_ATTR_RE.search(link_attrs)
        if attr_match:
            attr_title = _clean_text(attr_match.group(1))
        title = attr_title if len(attr_title) > len(link_text) else link_text
        raw_url = _clean_text(match.group(5))
        items.append(
            QueueSubmission(
                article_id=article_id,
                submit_date=_clean_text(match.group(2)),
                section=_clean_text(match.group(3)),
                authors=_clean_text(match.group(4)),
                url=_normalize_submission_url(raw_url, article_id),
                title=title,
                title_from_list=link_text,
                queue=queue,
                queue_label=queue_label,
                ojs_status=_clean_text(match.group(8) or ""),
            )
        )
    return items


def fetch_queue_submissions(
    auth: PlatformHttpClient,
    journal: str,
    queue: str,
    queue_path: str,
    queue_label: str,
    *,
    delay: float = 0.0,
) -> list[QueueSubmission]:
    """Загрузить все страницы одной очереди редактора."""
    all_items: list[QueueSubmission] = []
    seen: set[int] = set()
    base_params: dict[str, str] = {}
    page = 1
    max_pages = 1
    base = auth.base_url.rstrip("/")

    while page <= max_pages:
        url = queue_page_url(base, journal, queue_path, page, base_params)
        logger.info("Очередь %s, страница %s: %s", queue_label, page, url)
        html = auth.get_text(url)
        if is_login_page(html):
            raise PlatformAuthError(f"Нет доступа к {url}")

        if page == 1:
            base_params = extract_search_params(html)

        batch = parse_queue_submissions(
            html, queue=queue, queue_label=queue_label
        )
        new_count = 0
        for item in batch:
            if item.article_id not in seen:
                seen.add(item.article_id)
                all_items.append(item)
                new_count += 1

        range_info = parse_result_range(html)
        link_pages = extract_page_numbers(html)
        if link_pages:
            max_pages = max(max_pages, max(link_pages))

        if range_info:
            start, end, total = range_info
            page_size = max(end - start + 1, 1)
            max_pages = max(max_pages, (total + page_size - 1) // page_size)
            if end >= total:
                break
        elif not batch:
            break

        if new_count == 0 and page > 1:
            logger.warning(
                "Страница %s очереди %s без новых id — остановка",
                page,
                queue_label,
            )
            break

        page += 1
        if delay > 0:
            time.sleep(delay)

    return all_items


def fetch_all_journal_submissions(
    auth: PlatformHttpClient,
    journal: str,
    *,
    delay: float = 0.0,
) -> list[QueueSubmission]:
    """
    Выгрузить рукописи из активных очередей редактора (без Архива).

    При дубликате id оставляем первое появление
    (приоритет: Новые → рецензия → редакция).
    """
    merged: list[QueueSubmission] = []
    seen: set[int] = set()
    for queue, path, label in QUEUE_SPECS:
        batch = fetch_queue_submissions(
            auth,
            journal,
            queue,
            path,
            label,
            delay=delay,
        )
        logger.info("Очередь %s: %s рукописей", label, len(batch))
        for item in batch:
            if item.article_id in seen:
                continue
            seen.add(item.article_id)
            merged.append(item)
    logger.info("Всего уникальных рукописей журнала: %s", len(merged))
    return merged
