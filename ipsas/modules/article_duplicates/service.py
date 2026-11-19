"""Поиск дублей XML↔OJS и архивация подтверждённых неопубликованных."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Protocol

from ipsas.modules.archive_by_sender.parser import (
    build_skip_payload,
    is_login_page,
    parse_email_form,
    submission_url,
    unsuitable_url,
)
from ipsas.modules.article_duplicates.models import (
    ArchiveActionResult,
    ArticleMatchGroup,
    QueueSubmission,
)
from ipsas.modules.article_duplicates.details import enrich_candidates
from ipsas.modules.article_duplicates.models import XmlArticle
from ipsas.modules.article_duplicates.queues import fetch_all_journal_submissions
from ipsas.modules.article_duplicates.similarity import (
    DEFAULT_SCORE_FLOOR,
    DEFAULT_TOP_K,
    annotate_uniqueness,
    rank_candidates,
    score_pair,
)
from ipsas.modules.article_duplicates.xml_source import load_xml_articles
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError

logger = logging.getLogger(__name__)


class PlatformHttpClient(Protocol):
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


def find_duplicates(
    auth: PlatformHttpClient,
    journal: str,
    xml_path: Path | str | None = None,
    *,
    articles: list[XmlArticle] | None = None,
    delay: float = 0.0,
    top_k: int = DEFAULT_TOP_K,
    score_floor: float = DEFAULT_SCORE_FLOOR,
    submissions: list[QueueSubmission] | None = None,
) -> tuple[list[ArticleMatchGroup], list[QueueSubmission]]:
    """
    Для каждой эталонной статьи — до top_k похожих рукописей журнала.

    Эталоны: ``articles`` или загрузка из ``xml_path``.

    Returns:
        (группы совпадений, корпус рукописей)
    """
    if articles is not None:
        xml_articles = list(articles)
    elif xml_path is not None:
        xml_articles = load_xml_articles(xml_path)
    else:
        raise ValueError("Нужен XML-файл или список статей выпуска")
    if not xml_articles:
        raise ValueError("Нет эталонных статей для сравнения")

    corpus = submissions
    if corpus is None:
        corpus = fetch_all_journal_submissions(auth, journal, delay=delay)

    base = auth.base_url.rstrip("/")
    for item in corpus:
        if item.url.startswith("/"):
            item.url = f"{base}{item.url}"

    groups: list[ArticleMatchGroup] = []
    for article in xml_articles:
        candidates = rank_candidates(
            article,
            corpus,
            top_k=top_k,
            score_floor=score_floor,
        )
        groups.append(ArticleMatchGroup(xml=article, candidates=candidates))
        logger.info(
            "XML #%s «%s»: кандидатов %s",
            article.index,
            article.title[:60],
            len(candidates),
        )

    # Полные названия и поля — только для отобранных кандидатов.
    to_enrich = [c.submission for g in groups for c in g.candidates]
    if to_enrich:
        try:
            enrich_candidates(auth, to_enrich, delay=delay)
        except PlatformAuthError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("Догрузка деталей кандидатов: %s", exc)

        # Пересчитать score по полному названию со страницы рукописи.
        for group in groups:
            rescored = [score_pair(group.xml, c.submission) for c in group.candidates]
            rescored.sort(
                key=lambda c: (c.score, c.title_score, c.author_score),
                reverse=True,
            )
            annotate_uniqueness(rescored)
            group.candidates = rescored

    return groups, corpus


def archive_submission_skip_email(
    auth: PlatformHttpClient,
    journal: str,
    submission: QueueSubmission,
    *,
    dry_run: bool = True,
) -> ArchiveActionResult:
    """Отклонить рукопись без письма (как archive_by_sender, без фильтра отправителя)."""
    base = auth.base_url.rstrip("/")
    result = ArchiveActionResult(
        journal=journal,
        article_id=submission.article_id,
        title=submission.title,
        status="pending",
        message="",
    )

    if submission.is_published:
        result.status = "skipped_published"
        result.message = "опубликованная статья — архивация недоступна"
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
        headers={"Referer": reject_url, "Origin": base},
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

    result.status = "archived"
    result.message = f"отклонена и заархивирована без письма ({final_url})"
    return result


def archive_confirmed(
    auth: PlatformHttpClient,
    journal: str,
    article_ids: list[int],
    *,
    corpus: list[QueueSubmission] | None = None,
    dry_run: bool = True,
    delay: float = 0.0,
) -> list[ArchiveActionResult]:
    """Архивировать подтверждённые id (только неопубликованные)."""
    by_id: dict[int, QueueSubmission] = {}
    if corpus:
        by_id = {s.article_id: s for s in corpus}

    base = auth.base_url.rstrip("/")
    results: list[ArchiveActionResult] = []
    for article_id in article_ids:
        submission = by_id.get(article_id)
        if submission is None:
            submission = QueueSubmission(
                article_id=article_id,
                submit_date="",
                section="",
                authors="",
                title=f"article {article_id}",
                url=submission_url(base, journal, article_id),
                queue="unknown",
                queue_label="—",
                ojs_status="",
            )
        try:
            result = archive_submission_skip_email(
                auth,
                journal,
                submission,
                dry_run=dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            result = ArchiveActionResult(
                journal=journal,
                article_id=article_id,
                title=submission.title,
                status="error",
                message=str(exc),
            )
            logger.error("archive id=%s: %s", article_id, exc)
        results.append(result)
        if delay > 0:
            time.sleep(delay)
    return results
