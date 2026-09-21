"""Веб-сервис: архивация «Новые» по отправителю (сотрудники ОРИС)."""

from __future__ import annotations

import csv
import re
import time
from datetime import datetime
from pathlib import Path

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from ipsas.config.settings import get_settings
from ipsas.modules.archive_by_sender.constants import DEFAULT_SENDERS
from ipsas.services.archive_by_sender import execute as archive_by_sender_execute
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import safe_temp_path

logger = get_logger(__name__)

archive_by_sender_bp = Blueprint(
    "archive_by_sender", __name__, template_folder="templates"
)

_SAFE_REPORT_RE = re.compile(r"^archive_by_sender_[A-Za-z0-9._\-]+_\d{8}_\d{6}\.csv$")


def _write_report_csv(path: Path, results: list[dict]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["journal", "article_id", "title", "sender", "status", "message"]
        )
        for row in results:
            writer.writerow(
                [
                    row.get("journal", ""),
                    row.get("article_id", ""),
                    row.get("title", ""),
                    row.get("sender", ""),
                    row.get("status", ""),
                    row.get("message", ""),
                ]
            )


@archive_by_sender_bp.route("/archive-by-sender")
def archive_page():
    return render_template(
        "archive_by_sender.html",
        default_senders="\n".join(DEFAULT_SENDERS),
    )


@archive_by_sender_bp.route("/archive-by-sender/process", methods=["POST"])
def process_archive():
    journal_url = (request.form.get("journal_url") or "").strip()
    senders_text = request.form.get("senders_text")
    dry_run = (request.form.get("dry_run") or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }
    verify = (request.form.get("verify") or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }

    if not journal_url:
        flash("Укажите ссылку на журнал или ISSN", "error")
        return redirect(url_for("archive_by_sender.archive_page"))

    t0 = time.perf_counter()
    try:
        summary = archive_by_sender_execute(
            journal_url=journal_url,
            senders_text=senders_text,
            dry_run=dry_run,
            verify=verify,
        )
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("archive_by_sender.archive_page"))
    except Exception as exc:  # noqa: BLE001
        logger.error("archive_by_sender failed: %s", exc, exc_info=True)
        flash(
            "Не удалось выполнить обработку. Подробности в журнале сервера.",
            "error",
        )
        return redirect(url_for("archive_by_sender.archive_page"))

    elapsed = time.perf_counter() - t0
    settings = get_settings()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    journal = str(summary.get("journal") or "journal")
    safe_journal = re.sub(r"[^A-Za-z0-9._\-]+", "_", journal).strip("._-")[:40] or "journal"
    report_name = f"archive_by_sender_{safe_journal}_{stamp}.csv"
    report_path = safe_temp_path(settings.temp_dir, report_name)
    csv_name = None
    if report_path is not None:
        try:
            _write_report_csv(report_path, list(summary.get("results") or []))
            csv_name = report_name
        except OSError as exc:
            logger.warning("Не удалось записать CSV-отчёт: %s", exc)

    status = "ok" if summary.get("errors", 0) == 0 else "warning"
    detail = (
        f"{summary.get('journal')}: всего {summary.get('total')}, "
        f"архив {summary.get('archived')}, dry-run {summary.get('dry_run_hits')}, "
        f"пропуск {summary.get('skipped_sender')}, ошибки {summary.get('errors')} "
        f"({elapsed:.1f} с)"
    )
    record_operation(
        tool="archive_by_sender",
        title="Архивация «Новые» по отправителю",
        status=status,
        detail=detail,
        url=url_for("archive_by_sender.archive_page"),
    )

    if dry_run:
        flash(
            f"Проверка без изменений: к архивации {summary.get('dry_run_hits')}, "
            f"пропущено по отправителю {summary.get('skipped_sender')}, "
            f"ошибок {summary.get('errors')}.",
            "success" if summary.get("errors", 0) == 0 else "warning",
        )
    else:
        flash(
            f"Готово: заархивировано {summary.get('archived')}, "
            f"пропущено {summary.get('skipped_sender')}, "
            f"ошибок {summary.get('errors')}.",
            "success" if summary.get("errors", 0) == 0 else "warning",
        )

    return render_template(
        "archive_by_sender_result.html",
        summary=summary,
        elapsed=elapsed,
        csv_name=csv_name,
    )


@archive_by_sender_bp.route("/archive-by-sender/download/<filename>")
def download_report(filename: str):
    if not _SAFE_REPORT_RE.fullmatch(filename):
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("archive_by_sender.archive_page"))
    settings = get_settings()
    path = safe_temp_path(settings.temp_dir, filename)
    if path is None or not path.is_file():
        flash("Файл отчёта не найден или устарел", "error")
        return redirect(url_for("archive_by_sender.archive_page"))
    return send_file(
        path,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=filename,
    )
