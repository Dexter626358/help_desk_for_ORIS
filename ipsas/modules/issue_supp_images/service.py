"""Оркестрация: issueToc + images.zip → загрузка рисунков в доп. файлы."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthClient,
    PlatformAuthError,
)
from ipsas.modules.issue_supp_images.archive import parse_images_archive
from ipsas.modules.issue_supp_images.models import UploadResult
from ipsas.modules.issue_supp_images.toc import (
    issue_toc_url,
    match_article,
    parse_issue_ref,
    parse_issue_toc,
)
from ipsas.modules.issue_supp_images.upload import upload_image_to_article

logger = logging.getLogger(__name__)


def process_issue_images(
    auth: PlatformAuthClient,
    *,
    issue_url: str,
    zip_path: Path,
    extract_dir: Path,
    dry_run: bool = True,
    delay: float = 0.35,
) -> dict[str, Any]:
    base, journal, issue_id = parse_issue_ref(issue_url)
    # клиент мог быть создан с другим base_url — используем из URL
    if auth.base_url.rstrip("/") != base:
        logger.info("base_url клиента %s, из ссылки %s — берём из ссылки", auth.base_url, base)
        auth.base_url = base

    toc = issue_toc_url(base, journal, issue_id)
    logger.info("Чтение оглавления: %s", toc)
    toc_html = auth.get_text(toc)
    if "signinForm" in toc_html or "loginUsername" in toc_html:
        raise PlatformAuthError(f"Нет доступа к {toc}")

    articles = parse_issue_toc(toc_html)
    if not articles:
        raise ValueError("В issueToc не найдены статьи с интервалами страниц")

    bundles = parse_images_archive(Path(zip_path), Path(extract_dir))
    if not bundles:
        raise ValueError(
            "В архиве нет папок вида «47-67_images» с рисунками "
            "(pdf/jpeg/png/…)"
        )

    results: list[UploadResult] = []
    unmatched_folders: list[str] = []
    matched_article_ids: set[int] = set()

    for bundle in bundles:
        article = match_article(articles, bundle.page_range)
        if article is None:
            unmatched_folders.append(bundle.folder_name)
            logger.warning(
                "Нет статьи для интервала %s (%s)",
                bundle.page_range.key,
                bundle.folder_name,
            )
            continue
        matched_article_ids.add(article.article_id)
        logger.info(
            "Статья %s (%s) ← %s (%s файл(ов))",
            article.article_id,
            article.pages.key,
            bundle.folder_name,
            len(bundle.files),
        )
        for image in bundle.files:
            result = upload_image_to_article(
                auth,
                journal=journal,
                article_id=article.article_id,
                pages_label=article.pages.key,
                image=image,
                dry_run=dry_run,
                delay=delay,
            )
            results.append(result)
            logger.info(
                "  %s → %s | %s",
                image.original_name,
                result.status,
                result.message,
            )
            if delay > 0 and not dry_run:
                time.sleep(delay)

    uploaded = sum(1 for r in results if r.status == "uploaded")
    dry_hits = sum(1 for r in results if r.status == "dry_run")
    errors = sum(1 for r in results if r.status == "error")

    return {
        "journal": journal,
        "issue_id": issue_id,
        "issue_url": issue_url.strip(),
        "toc_url": toc,
        "dry_run": dry_run,
        "articles_in_toc": len(articles),
        "bundles": len(bundles),
        "matched_articles": len(matched_article_ids),
        "unmatched_folders": unmatched_folders,
        "total_files": len(results),
        "uploaded": uploaded,
        "dry_run_hits": dry_hits,
        "errors": errors,
        "results": [r.to_dict() for r in results],
        "toc_articles": [
            {
                "article_id": a.article_id,
                "pages": a.pages.key,
                "title": a.title,
            }
            for a in articles
        ],
    }
