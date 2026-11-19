"""
Поиск дублей: эталон (journal XML или URL выпуска) ↔ активные очереди редактора OJS.

Примеры:
  python find_article_duplicates.py ^
    --journal-url https://journals.rcsi.science/0320-7919 ^
    --xml issue.xml

  python find_article_duplicates.py ^
    --issue-url https://journals.rcsi.science/0044-4502/issue/view/30862

  python find_article_duplicates.py ^
    --journal-url https://journals.rcsi.science/0320-7919 ^
    --xml issue.xml --archive-ids 125,231 --dry-run

  python find_article_duplicates.py ^
    --journal-url https://journals.rcsi.science/0320-7919 ^
    --xml issue.xml --archive-ids 125,231 --apply-archive
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from ipsas.services.article_duplicates import execute_archive, execute_find


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Поиск дублей: XML или опубликованный выпуск "
            "↔ активные очереди редактора OJS"
        )
    )
    parser.add_argument(
        "--journal-url",
        default="",
        help="Ссылка на журнал или ISSN (обязательно для XML; "
        "для выпуска можно не указывать — берётся из URL)",
    )
    parser.add_argument("--xml", default="", help="Путь к journal XML")
    parser.add_argument(
        "--issue-url",
        default="",
        help="Ссылка на опубликованный выпуск "
        "(…/ISSN/issue/view/…) вместо XML",
    )
    parser.add_argument(
        "--archive-ids",
        default="",
        help="Через запятую id рукописей для архивации после поиска "
        "(или только архивация, если указаны)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Архивация только проверка (по умолчанию)",
    )
    mode.add_argument(
        "--apply-archive",
        action="store_true",
        help="Записать архивацию без письма",
    )
    parser.add_argument("--json", action="store_true", help="Вывести JSON summary")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    xml_arg = (args.xml or "").strip()
    issue_url = (args.issue_url or "").strip()
    journal_url = (args.journal_url or "").strip()

    if xml_arg and issue_url:
        logging.error("Укажите либо --xml, либо --issue-url — не оба сразу")
        return 2
    if not xml_arg and not issue_url:
        logging.error("Нужен --xml или --issue-url")
        return 2

    xml_path: Path | None = None
    if xml_arg:
        xml_path = Path(xml_arg)
        if not xml_path.is_file():
            logging.error("XML не найден: %s", xml_path)
            return 2
        if not journal_url:
            logging.error("Для --xml укажите --journal-url")
            return 2

    archive_ids: list[int] = []
    for part in str(args.archive_ids or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            archive_ids.append(int(part))
        except ValueError:
            logging.error("Некорректный id: %s", part)
            return 2

    dry_run = not args.apply_archive

    try:
        find_summary = execute_find(
            journal_url=journal_url,
            xml_path=xml_path,
            issue_url=issue_url or None,
        )
        if args.json:
            print(json.dumps(find_summary, ensure_ascii=False, indent=2))
        else:
            source = find_summary.get("source") or "xml"
            src_label = (
                f"выпуск {find_summary.get('source_label')}"
                if source == "issue"
                else f"XML {find_summary.get('xml_name') or find_summary.get('source_label')}"
            )
            print(
                f"Журнал {find_summary['journal']} ({src_label}): "
                f"статей {find_summary['xml_articles']}, "
                f"корпус {find_summary['corpus_size']}, "
                f"с кандидатами {find_summary['groups_with_candidates']}"
            )
            for group in find_summary.get("groups") or []:
                print(f"\n#{group['index']} {group['title']}")
                print(f"  авторы эталона: {group['authors'] or '—'}")
                for cand in group.get("candidates") or []:
                    mark = " [published]" if cand.get("is_published") else ""
                    full = " authors*" if cand.get("authors_full_match") else ""
                    print(
                        f"  - {cand['score']*100:.0f}% id={cand['article_id']} "
                        f"[{cand['queue_label']}] {cand['title']}{full}{mark}"
                    )

        if archive_ids:
            arch_journal = journal_url or str(find_summary.get("journal_url") or "")
            arch = execute_archive(
                journal_url=arch_journal,
                article_ids=archive_ids,
                dry_run=dry_run,
            )
            if args.json:
                print(json.dumps(arch, ensure_ascii=False, indent=2))
            else:
                print(
                    f"\nАрхивация dry_run={dry_run}: "
                    f"archived={arch['archived']} dry_run_hits={arch['dry_run_hits']} "
                    f"skipped_published={arch['skipped_published']} errors={arch['errors']}"
                )
    except ValueError as exc:
        logging.error("%s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        logging.exception("Сбой: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
