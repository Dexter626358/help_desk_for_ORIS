"""Оркестрация: CSV разделов → createSection; опционально/отдельно — сортировка."""

from __future__ import annotations

import logging
from pathlib import Path

from ipsas.modules.eng_metadata.platform_auth import PlatformAuthClient, PlatformAuthError
from ipsas.modules.journal_sections.csv_io import load_sections_csv
from ipsas.modules.journal_sections.models import SectionSyncResult, SyncReport
from ipsas.modules.journal_sections.sections import (
    create_section,
    find_by_title_ru,
    is_articles_section,
    list_sections,
    order_titles_alpha_keep_articles,
    sort_sections_alphabetically,
)
from ipsas.modules.sandbox_journal_setup.urls import journal_url, parse_journal_url

logger = logging.getLogger(__name__)


def _prepare_report(
    auth: PlatformAuthClient,
    *,
    journal_url_raw: str,
    dry_run: bool,
    sort_alpha: bool,
) -> tuple[SyncReport, str, str]:
    base_url, journal = parse_journal_url(journal_url_raw)
    report = SyncReport(
        journal_url=journal_url_raw.strip(),
        base_url=base_url,
        journal=journal,
        dry_run=dry_run,
        sort_alpha=sort_alpha,
    )
    if auth.base_url.rstrip("/") != base_url:
        logger.info(
            "base_url клиента %s, из ссылки %s — берём из ссылки",
            auth.base_url,
            base_url,
        )
        auth.base_url = base_url
    return report, base_url, journal


def _load_existing(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    report: SyncReport,
) -> list | None:
    list_url = journal_url(base_url, journal, "manager/sections")
    logger.info("Чтение разделов: %s", list_url)
    try:
        html = auth.get_text(list_url)
    except PlatformAuthError as exc:
        report.error = str(exc)
        return None
    if "signinForm" in html or "loginUsername" in html:
        report.error = f"Нет доступа к {list_url} (нужен вход менеджера)"
        return None
    existing = list_sections(html, list_url)
    report.existing_count = len(existing)
    return existing


def _apply_sort_alpha(
    auth: PlatformAuthClient,
    *,
    base_url: str,
    journal: str,
    report: SyncReport,
    existing: list | None,
    dry_run: bool,
    delay: float,
) -> None:
    try:
        if dry_run:
            titles = [s.title for s in (existing or [])]
            for row in report.results:
                if row.status == "dry_run" and row.title_ru not in titles:
                    titles.append(row.title_ru)
            desired = order_titles_alpha_keep_articles(titles)
            current = [s.title for s in (existing or [])]
            pinned_note = (
                " («Статьи» на месте)"
                if any(is_articles_section(t) for t in titles)
                else ""
            )
            if not desired:
                sort_status, sort_message = "unchanged", "нет разделов для сортировки"
            elif current == desired:
                sort_status, sort_message = (
                    "unchanged",
                    f"уже по алфавиту{pinned_note}: {' → '.join(desired)}",
                )
            else:
                sort_status, sort_message = (
                    "dry_run",
                    f"будет порядок{pinned_note}: {' → '.join(desired)}",
                )
        else:
            sort_status, sort_message = sort_sections_alphabetically(
                auth,
                base_url=base_url,
                journal=journal,
                sections=None,
                dry_run=False,
                delay=delay,
            )
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка сортировки разделов: %s", exc)
        sort_status, sort_message = "error", str(exc)
    report.sort_status = sort_status
    report.sort_message = sort_message
    logger.info("  sort_alpha → %s | %s", sort_status, sort_message)


def sync_journal_sections(
    auth: PlatformAuthClient,
    *,
    journal_url_raw: str,
    csv_path: Path | None = None,
    dry_run: bool = True,
    sort_alpha: bool = False,
    delay: float = 0.35,
) -> SyncReport:
    """Сверить CSV с manager/sections и/или отсортировать разделы.

    - с CSV: создать отсутствующие (по ``title_ru``);
    - ``sort_alpha``: выстроить все разделы A→Я (можно без CSV).
    """
    if csv_path is None and not sort_alpha:
        report = SyncReport(
            journal_url=journal_url_raw.strip(),
            base_url="",
            journal="",
            dry_run=dry_run,
            sort_alpha=False,
            error="Нужен CSV и/или опция сортировки (sort_alpha)",
        )
        return report

    report, base_url, journal = _prepare_report(
        auth,
        journal_url_raw=journal_url_raw,
        dry_run=dry_run,
        sort_alpha=sort_alpha,
    )

    existing = _load_existing(
        auth, base_url=base_url, journal=journal, report=report
    )
    if existing is None:
        return report
    logger.info(
        "На платформе разделов: %s; CSV: %s",
        len(existing),
        "да" if csv_path else "нет",
    )

    if csv_path is not None:
        try:
            specs = load_sections_csv(Path(csv_path))
        except (OSError, ValueError) as exc:
            report.error = str(exc)
            return report

        for spec in specs:
            found = find_by_title_ru(existing, spec.title_ru)
            if found is not None:
                result = SectionSyncResult(
                    number=spec.number,
                    title_ru=spec.title_ru,
                    status="exists",
                    message=(
                        f"уже есть (id={found.section_id}, abbrev={found.abbrev!r})"
                    ),
                    section_id=found.section_id,
                )
                report.results.append(result)
                logger.info("  #%s %s → exists", spec.number, spec.title_ru)
                continue

            try:
                status, message = create_section(
                    auth,
                    base_url=base_url,
                    journal=journal,
                    spec=spec,
                    dry_run=dry_run,
                    delay=delay,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Ошибка создания «%s»: %s", spec.title_ru, exc)
                status, message = "error", str(exc)

            report.results.append(
                SectionSyncResult(
                    number=spec.number,
                    title_ru=spec.title_ru,
                    status=status,
                    message=message,
                )
            )
            logger.info(
                "  #%s %s → %s | %s",
                spec.number,
                spec.title_ru,
                status,
                message,
            )

            if status == "created":
                existing_titles = {s.title for s in existing}
                if spec.title_ru not in existing_titles:
                    from ipsas.modules.journal_sections.models import ExistingSection

                    existing.append(
                        ExistingSection(
                            section_id="",
                            title=spec.title_ru,
                            abbrev=spec.short_title_ru,
                            edit_url="",
                        )
                    )

    if sort_alpha:
        _apply_sort_alpha(
            auth,
            base_url=base_url,
            journal=journal,
            report=report,
            existing=existing,
            dry_run=dry_run,
            delay=delay,
        )

    return report
