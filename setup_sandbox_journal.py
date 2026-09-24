"""
Базовая настройка ветки журнала в песочнице (f23g45).

Вход: ссылка на журнал, например:
  https://f23g45.rcsi.science/257/index

По умолчанию — dry-run (только отчёт). Для записи: --apply

Учётки из .env:
  SANDBOX_GATE_* / SANDBOX_USER1_* — HTTP Basic (ворота)
  SANDBOX_OJS_*  / SANDBOX_USER2_* — логин OJS
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from ipsas.config.settings import get_settings, reset_settings
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError
from ipsas.modules.sandbox_journal_setup.service import run_sandbox_setup
from ipsas.modules.sandbox_journal_setup.urls import parse_journal_url

logger = logging.getLogger(__name__)


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
            "Базовая настройка журнала в песочнице. "
            "Передайте ссылку на журнал: https://f23g45.rcsi.science/257/index"
        )
    )
    parser.add_argument(
        "journal_url",
        help="Ссылка на журнал (…/257/index или …/257/manager/…)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Записать изменения (без флага — только dry-run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Явный dry-run (по умолчанию и так dry-run)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Печать отчёта в JSON",
    )
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    dry_run = not args.apply
    if args.dry_run:
        dry_run = True

    reset_settings()
    settings = get_settings()

    try:
        base, journal = parse_journal_url(args.journal_url)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2

    logger.info(
        "Журнал %s на %s (%s)",
        journal,
        base,
        "dry-run" if dry_run else "APPLY",
    )

    try:
        report = run_sandbox_setup(
            args.journal_url,
            gate_username=settings.sandbox_gate_username,
            gate_password=settings.sandbox_gate_password,
            ojs_username=settings.sandbox_ojs_username,
            ojs_password=settings.sandbox_ojs_password,
            cookie_file=settings.sandbox_cookie_file,
            dry_run=dry_run,
            delay=settings.sandbox_request_delay,
            timeout=settings.request_timeout,
        )
    except PlatformAuthError as exc:
        logger.error("Ошибка входа: %s", exc)
        return 1

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        for step in report.steps:
            mark = "OK" if step.ok else "FAIL"
            ch = "changed" if step.changed else "skip"
            print(f"[{mark}] {step.title} ({ch}): {step.message}")
        if report.error:
            print(f"ERROR: {report.error}")
        print(
            f"Итого: {'успех' if report.ok else 'есть ошибки'} "
            f"({'dry-run' if report.dry_run else 'apply'})"
        )

    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
