"""Оркестрация: XML или URL выпуска + журнал → кандидаты-дубли / архивация."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from ipsas.config.settings import get_settings
from ipsas.modules.archive_by_sender.parser import parse_journal_ref
from ipsas.modules.article_duplicates.issue_source import load_issue_articles
from ipsas.modules.article_duplicates.service import (
    archive_confirmed,
    find_duplicates,
)
from ipsas.modules.article_duplicates.similarity import (
    DEFAULT_SCORE_FLOOR,
    DEFAULT_TOP_K,
)
from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthError,
    ensure_platform_auth,
)
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


def _auth_client():
    settings = get_settings()
    if not settings.platform_username or not settings.platform_password:
        raise ValueError(
            "Задайте PLATFORM_USERNAME / PLATFORM_PASSWORD "
            "(или RCSI_USERNAME / RCSI_PASSWORD) в .env"
        )
    return ensure_platform_auth(
        username=settings.platform_username,
        password=settings.platform_password,
        base_url=settings.platform_base_url,
        cookie_file=settings.platform_cookie_file,
        timeout=float(settings.request_timeout),
        ssl_verify=bool(settings.platform_ssl_verify),
    )


def execute_find(
    *,
    journal_url: str = "",
    xml_path: Path | str | None = None,
    issue_url: str | None = None,
    top_k: int = DEFAULT_TOP_K,
    score_floor: float = DEFAULT_SCORE_FLOOR,
) -> dict[str, Any]:
    """Найти кандидатов-дублей (эталон: XML или опубликованный выпуск)."""
    settings = get_settings()
    issue = (issue_url or "").strip()
    path = Path(xml_path) if xml_path else None
    has_xml = path is not None and path.is_file()
    has_issue = bool(issue)

    if has_xml and has_issue:
        raise ValueError("Укажите либо XML, либо ссылку на выпуск — не оба сразу")
    if not has_xml and not has_issue:
        raise ValueError("Загрузите journal XML или укажите ссылку на выпуск")

    source = "xml"
    source_label = ""
    articles = None
    journal = ""
    journal_url_out = (journal_url or "").strip()

    if has_issue:
        source = "issue"
        try:
            articles, journal = load_issue_articles(issue)
        except ValueError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"Не удалось разобрать выпуск: {exc}") from exc
        source_label = issue
        if not journal_url_out:
            journal_url_out = issue
    else:
        assert path is not None
        if not journal_url_out:
            raise ValueError("Укажите ссылку на журнал или ISSN")
        journal = parse_journal_ref(journal_url_out)
        source_label = path.name

    auth = _auth_client()
    try:
        groups, corpus = find_duplicates(
            auth,
            journal,
            xml_path=path if has_xml else None,
            articles=articles,
            delay=float(settings.platform_request_delay),
            top_k=top_k,
            score_floor=score_floor,
        )
    except PlatformAuthError as exc:
        raise ValueError(f"Ошибка доступа к платформе: {exc}") from exc

    with_hits = sum(1 for g in groups if g.candidates)
    summary = {
        "mode": "find",
        "source": source,
        "source_label": source_label,
        "issue_url": issue if has_issue else "",
        "journal": journal,
        "journal_url": journal_url_out,
        "xml_name": path.name if has_xml and path is not None else "",
        "xml_articles": len(groups),
        "corpus_size": len(corpus),
        "groups_with_candidates": with_hits,
        "top_k": top_k,
        "score_floor": score_floor,
        "groups": [g.to_dict() for g in groups],
    }
    logger.info(
        "article_duplicates find source=%s journal=%s articles=%s corpus=%s with_hits=%s",
        source,
        journal,
        summary["xml_articles"],
        summary["corpus_size"],
        with_hits,
    )
    return summary


def execute_archive(
    *,
    journal_url: str,
    article_ids: list[int],
    dry_run: bool = True,
) -> dict[str, Any]:
    """Архивировать подтверждённые рукописи без письма (не Published)."""
    settings = get_settings()
    journal = parse_journal_ref(journal_url)
    ids = [int(x) for x in article_ids if int(x) > 0]
    if not ids:
        raise ValueError("Не указаны id рукописей для архивации")

    auth = _auth_client()
    try:
        results = archive_confirmed(
            auth,
            journal,
            ids,
            dry_run=dry_run,
            delay=float(settings.platform_request_delay),
        )
    except PlatformAuthError as exc:
        raise ValueError(f"Ошибка доступа к платформе: {exc}") from exc

    counts = Counter(r.status for r in results)
    summary = {
        "mode": "archive",
        "journal": journal,
        "journal_url": journal_url.strip(),
        "dry_run": dry_run,
        "total": len(results),
        "counts": dict(counts),
        "archived": counts.get("archived", 0),
        "dry_run_hits": counts.get("dry_run", 0),
        "skipped_published": counts.get("skipped_published", 0),
        "errors": counts.get("error", 0),
        "results": [r.to_dict() for r in results],
    }
    logger.info(
        "article_duplicates archive journal=%s dry_run=%s total=%s archived=%s errors=%s",
        journal,
        dry_run,
        summary["total"],
        summary["archived"],
        summary["errors"],
    )
    return summary
