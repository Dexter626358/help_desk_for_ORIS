"""Архивация рукописей из «Новые» при совпадении отправителя."""

from __future__ import annotations

import logging
import time
from typing import Protocol

from ipsas.modules.archive_by_sender.models import ArchiveResult, SubmissionInfo
from ipsas.modules.archive_by_sender.parser import (
    build_skip_payload,
    extract_page_numbers,
    extract_search_params,
    extract_sender,
    is_login_page,
    parse_email_form,
    parse_result_range,
    parse_submissions,
    sender_matches,
    submission_in_unassigned,
    submission_url,
    unassigned_page_url,
    unassigned_url,
    unsuitable_url,
)
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError

logger = logging.getLogger(__name__)


class PlatformHttpClient(Protocol):
    """Минимальный HTTP-клиент платформы (PlatformAuthClient)."""

    base_url: str

    def get_text(self, url: str, encoding: str = "utf-8") -> str: ...

    def request(
        self,
        url: str,
        *,
        data: dict[str, str] | None = None,
        method: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes]: ...


def fetch_all_unassigned_submissions(
    auth: PlatformHttpClient,
    journal: str,
    *,
    delay: float = 0.0,
) -> list[SubmissionInfo]:
    """Загружает все страницы списка «Новые»."""
    all_items: list[SubmissionInfo] = []
    seen: set[int] = set()
    base_params: dict[str, str] = {}
    page = 1
    max_pages = 1
    base = auth.base_url.rstrip("/")

    while page <= max_pages:
        url = unassigned_page_url(base, journal, page, base_params)
        logger.info("Список «Новые», страница %s: %s", page, url)
        html = auth.get_text(url)
        if is_login_page(html):
            raise PlatformAuthError(f"Нет доступа к {url}")

        if page == 1:
            base_params = extract_search_params(html)

        batch = parse_submissions(html)
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
            logger.info(
                "  диапазон %s-%s из %s, на странице %s, новых %s",
                start,
                end,
                total,
                len(batch),
                new_count,
            )
            if end >= total:
                break
        else:
            logger.info("  на странице %s, новых %s", len(batch), new_count)
            if not batch:
                break

        if new_count == 0 and page > 1:
            logger.warning("Страница %s не добавила новых рукописей — остановка", page)
            break

        page += 1
        if delay > 0:
            time.sleep(delay)

    return all_items


def archive_submission(
    auth: PlatformHttpClient,
    journal: str,
    submission: SubmissionInfo,
    *,
    sender_names: list[str],
    dry_run: bool = False,
    verify: bool = True,
) -> ArchiveResult:
    base = auth.base_url.rstrip("/")
    detail_url = submission.url
    if detail_url.startswith("/"):
        detail_url = f"{base}{detail_url}"
    detail_html = auth.get_text(detail_url)
    if is_login_page(detail_html):
        raise PlatformAuthError(f"Нет доступа к {detail_url}")

    sender = extract_sender(detail_html)
    result = ArchiveResult(
        journal=journal,
        article_id=submission.article_id,
        title=submission.title,
        sender=sender,
        status="pending",
        message="",
    )

    if not sender_matches(sender, sender_names):
        result.status = "skipped_sender"
        result.message = (
            f"отправитель не совпадает: {sender!r} "
            f"(ожидается один из списка сотрудников)"
        )
        return result

    reject_url = unsuitable_url(base, journal, submission.article_id)
    reject_html = auth.get_text(reject_url)
    if is_login_page(reject_html):
        raise PlatformAuthError(f"Нет доступа к {reject_url}")

    form = parse_email_form(reject_html)
    payload = build_skip_payload(form)
    action = form.form_action
    if action.startswith("/"):
        action = f"{base}{action}"

    if dry_run:
        result.status = "dry_run"
        result.message = (
            f"будет отклонена без письма через {action} "
            f"({form.skip_field}={form.skip_value!r})"
        )
        return result

    status, final_url, content = auth.request(
        action,
        data=payload,
        headers={
            "Referer": reject_url,
            "Origin": base,
        },
    )
    body = content.decode("utf-8", "replace")
    if status >= 400:
        result.status = "error"
        result.message = f"HTTP {status}"
        return result
    if is_login_page(body):
        result.status = "error"
        result.message = "после POST снова страница логина"
        return result

    if verify:
        list_html = auth.get_text(unassigned_url(base, journal))
        if submission_in_unassigned(list_html, submission.article_id):
            result.status = "verify_failed"
            result.message = "POST выполнен, но статья всё ещё в «Новые»"
            return result

    result.status = "archived"
    result.message = f"отклонена и заархивирована без письма ({final_url})"
    return result


def process_journal(
    auth: PlatformHttpClient,
    journal: str,
    *,
    sender_names: list[str],
    article_id: int | None = None,
    limit: int | None = None,
    delay: float = 0.35,
    dry_run: bool = False,
    verify: bool = True,
) -> list[ArchiveResult]:
    base = auth.base_url.rstrip("/")
    if article_id is not None:
        submissions = [
            SubmissionInfo(
                article_id=article_id,
                submit_date="",
                section="",
                authors="",
                title=f"article {article_id}",
                url=submission_url(base, journal, article_id),
            )
        ]
    else:
        logger.info("Загрузка всех страниц «Новые» для %s", journal)
        submissions = fetch_all_unassigned_submissions(
            auth,
            journal,
            delay=delay,
        )
        logger.info(
            "Найдено рукописей в «Новые» (все страницы): %s",
            len(submissions),
        )

    if limit is not None:
        submissions = submissions[: max(0, limit)]
    if not submissions:
        logger.info("Нет рукописей для обработки в %s", journal)
        return []

    results: list[ArchiveResult] = []
    for idx, submission in enumerate(submissions, start=1):
        logger.info(
            "[%s/%s] %s id=%s | %s",
            idx,
            len(submissions),
            journal,
            submission.article_id,
            submission.title[:80],
        )
        try:
            result = archive_submission(
                auth,
                journal,
                submission,
                sender_names=sender_names,
                dry_run=dry_run,
                verify=verify,
            )
        except Exception as exc:  # noqa: BLE001
            result = ArchiveResult(
                journal=journal,
                article_id=submission.article_id,
                title=submission.title,
                sender="",
                status="error",
                message=str(exc),
            )
            logger.error("Ошибка: %s", exc)
        results.append(result)
        logger.info(
            "  -> %s | sender=%r | %s",
            result.status,
            result.sender,
            result.message,
        )
        if delay > 0:
            time.sleep(delay)
    return results
