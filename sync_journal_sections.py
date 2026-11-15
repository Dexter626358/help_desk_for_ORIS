"""
Синхронизация разделов журнала из CSV и/или сортировка A→Я.

CSV (разделитель «;», необязателен при --sort-alpha):
  number;title_ru;short_title_ru;title_en;short_title_en

Примеры:
  python sync_journal_sections.py ^
    --journal-url https://f23g45.rcsi.science/2079-7877 ^
    --csv journal_sections.csv --dry-run

  python sync_journal_sections.py ^
    --journal-url https://f23g45.rcsi.science/2079-7877 ^
    --csv journal_sections.csv --apply --sort-alpha

  # только сортировка в песочнице
  python sync_journal_sections.py ^
    --journal-url https://f23g45.rcsi.science/2079-7877/manager/sections ^
    --sort-alpha --apply

  # то же на платформе
  python sync_journal_sections.py ^
    --journal-url https://journals.rcsi.science/2079-7877/manager/sections ^
    --sort-alpha --apply
"""

from __future__ import annotations

import argparse
import logging
import sys

from dotenv import load_dotenv

load_dotenv()

from ipsas.services.journal_sections import execute


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Создать недостающие разделы и/или отсортировать их по алфавиту"
    )
    parser.add_argument(
        "--journal-url",
        required=True,
        help=(
            "Ссылка на журнал/manager/sections в песочнице (f23g45) "
            "или на платформе (journals.rcsi.science)"
        ),
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="CSV с разделами (необязателен, если указан только --sort-alpha)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Только проверка (по умолчанию)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Записать изменения на платформу",
    )
    parser.add_argument(
        "--sort-alpha",
        action="store_true",
        help="Выстроить разделы по алфавиту (RU); можно без --csv",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    if not args.csv and not args.sort_alpha:
        parser.error("Укажите --csv и/или --sort-alpha")

    dry_run = not bool(args.apply)
    try:
        summary = execute(
            journal_url=args.journal_url,
            csv_path=args.csv,
            dry_run=dry_run,
            sort_alpha=bool(args.sort_alpha),
        )
    except Exception as exc:  # noqa: BLE001
        logging.error("%s", exc)
        return 1

    print(
        f"journal={summary.get('journal')} dry_run={dry_run} "
        f"sort_alpha={bool(args.sort_alpha)} "
        f"created={summary.get('created')} exists={summary.get('exists')} "
        f"dry_run_hits={summary.get('dry_run_hits')} errors={summary.get('errors')} "
        f"sort={summary.get('sort_status')}"
    )
    if summary.get("sort_message"):
        print(f"  sort: {summary.get('sort_message')}")
    for row in summary.get("results") or []:
        print(
            f"  #{row.get('number')} {row.get('title_ru')} → "
            f"{row.get('status')}: {row.get('message')}"
        )
    return 0 if summary.get("ok") else 2


if __name__ == "__main__":
    sys.exit(main())
