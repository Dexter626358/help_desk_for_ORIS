"""
Загрузка рисунков из images.zip в доп. файлы статей выпуска (режим редактора).

Структура архива:
  47-67_images/Fig. 1.jpeg
  68-82_images/Fig. 1.jpeg
  83-103_images/Fig. 1.pdf
  …

Скрипт:
  1) открывает editor/issueToc/{issueId}
  2) сопоставляет папки с интервалами pages статей
  3) для каждой статьи: submissionEditing → Доп. файлы → upload
  4) title = имя файла, type = «Рисунок (материалы исследования)»
  5) включает показ для скрытых языков (кнопка «Показывать»)

Примеры:
  python upload_issue_images.py ^
    --issue-url https://journals.rcsi.science/2782-2168/editor/issueToc/31488 ^
    --zip images.zip --dry-run

  python upload_issue_images.py ^
    --issue-url https://journals.rcsi.science/2782-2168/editor/issueToc/31488 ^
    --zip images.zip
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from ipsas.config.settings import get_settings, reset_settings
from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthError,
    ensure_platform_auth,
)
from ipsas.modules.issue_supp_images.service import process_issue_images

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
            "Загрузить рисунки из ZIP в доп. файлы статей выпуска "
            "(сопоставление по интервалам страниц)"
        )
    )
    parser.add_argument(
        "--issue-url",
        required=True,
        help="Ссылка на editor/issueToc/… или issue/view/…",
    )
    parser.add_argument(
        "--zip",
        dest="zip_path",
        required=True,
        type=Path,
        help="Архив images.zip с папками {start}-{end}_images",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только сопоставить и показать план без загрузки",
    )
    parser.add_argument(
        "--extract-dir",
        type=Path,
        default=None,
        help="Каталог распаковки (по умолчанию временный)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="CSV-отчёт о результатах",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    reset_settings()
    settings = get_settings()
    if not settings.platform_username or not settings.platform_password:
        logger.error(
            "Задайте PLATFORM_USERNAME / PLATFORM_PASSWORD "
            "(или RCSI_USERNAME / RCSI_PASSWORD) в .env"
        )
        return 2

    zip_path = args.zip_path
    if not zip_path.is_file():
        logger.error("Архив не найден: %s", zip_path)
        return 2

    try:
        auth = ensure_platform_auth(
            username=settings.platform_username,
            password=settings.platform_password,
            base_url=settings.platform_base_url or "https://journals.rcsi.science",
            cookie_file=Path(
                settings.platform_cookie_file or "temp/platform_cookies.txt"
            ),
            timeout=float(settings.request_timeout or 60),
        )
    except PlatformAuthError as exc:
        logger.error("Авторизация: %s", exc)
        return 2

    def _run(extract_dir: Path) -> dict:
        return process_issue_images(
            auth,
            issue_url=args.issue_url,
            zip_path=zip_path,
            extract_dir=extract_dir,
            dry_run=bool(args.dry_run),
            delay=float(settings.platform_request_delay or 0.35),
        )

    try:
        if args.extract_dir is not None:
            summary = _run(args.extract_dir)
        else:
            with tempfile.TemporaryDirectory(prefix="ipsas_images_") as tmp:
                summary = _run(Path(tmp))
    except (PlatformAuthError, ValueError, RuntimeError) as exc:
        logger.error("%s", exc)
        return 1

    logger.info(
        "Готово: journal=%s issue=%s files=%s uploaded=%s dry_run=%s errors=%s",
        summary["journal"],
        summary["issue_id"],
        summary["total_files"],
        summary["uploaded"],
        summary["dry_run_hits"],
        summary["errors"],
    )
    if summary["unmatched_folders"]:
        logger.warning(
            "Папки без статьи: %s",
            ", ".join(summary["unmatched_folders"]),
        )

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with args.report.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "article_id",
                    "pages",
                    "filename",
                    "status",
                    "supp_file_id",
                    "message",
                ]
            )
            for row in summary["results"]:
                writer.writerow(
                    [
                        row.get("article_id", ""),
                        row.get("pages", ""),
                        row.get("filename", ""),
                        row.get("status", ""),
                        row.get("supp_file_id", ""),
                        row.get("message", ""),
                    ]
                )
        logger.info("Отчёт: %s", args.report)

    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
