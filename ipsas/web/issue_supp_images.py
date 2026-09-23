"""Веб-сервис: загрузка рисунков выпуска в доп. файлы статей."""

from __future__ import annotations

import csv
import re
import shutil
import time
import uuid
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
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.services.issue_supp_images import execute as issue_supp_images_execute
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import safe_temp_path

logger = get_logger(__name__)

issue_supp_images_bp = Blueprint(
    "issue_supp_images", __name__, template_folder="templates"
)

_SAFE_REPORT_RE = re.compile(
    r"^issue_supp_images_[A-Za-z0-9._\-]+_\d{8}_\d{6}\.csv$"
)


def _unlink_quiet(*paths: Path) -> None:
    for path in paths:
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
        except OSError as exc:
            logger.warning("Не удалось удалить %s: %s", path, exc)


def _write_report_csv(path: Path, results: list[dict]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
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
        for row in results:
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


@issue_supp_images_bp.route("/issue-supp-images")
def upload_page():
    return render_template("issue_supp_images.html")


@issue_supp_images_bp.route("/issue-supp-images/process", methods=["POST"])
def process_upload():
    issue_url = (request.form.get("issue_url") or "").strip()
    dry_run = (request.form.get("dry_run") or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }

    if not issue_url:
        flash("Укажите ссылку на выпуск (issueToc или issue/view)", "error")
        return redirect(url_for("issue_supp_images.upload_page"))

    if "zip_file" not in request.files:
        flash("ZIP-архив не был загружен", "error")
        return redirect(url_for("issue_supp_images.upload_page"))

    file = request.files["zip_file"]
    if not file.filename:
        flash("Файл не выбран", "error")
        return redirect(url_for("issue_supp_images.upload_page"))
    if not file.filename.lower().endswith(".zip"):
        flash("Поддерживаются только ZIP-архивы", "error")
        return redirect(url_for("issue_supp_images.upload_page"))

    settings = get_settings()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    safe_name = secure_filename(file.filename) or "images.zip"
    zip_path = settings.temp_dir / f"{stamp}_{uid}_{safe_name}"
    extract_dir = settings.temp_dir / f"{stamp}_{uid}_issue_images_extract"
    settings.temp_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    try:
        file.save(str(zip_path))
        summary = issue_supp_images_execute(
            issue_url=issue_url,
            zip_path=zip_path,
            extract_dir=extract_dir,
            dry_run=dry_run,
        )
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("issue_supp_images.upload_page"))
    except Exception as exc:  # noqa: BLE001
        logger.error("issue_supp_images failed: %s", exc, exc_info=True)
        flash(
            "Не удалось выполнить загрузку. Подробности в журнале сервера.",
            "error",
        )
        return redirect(url_for("issue_supp_images.upload_page"))
    finally:
        _unlink_quiet(zip_path, extract_dir)

    elapsed = time.perf_counter() - t0
    journal = str(summary.get("journal") or "journal")
    safe_journal = (
        re.sub(r"[^A-Za-z0-9._\-]+", "_", journal).strip("._-")[:40] or "journal"
    )
    report_name = f"issue_supp_images_{safe_journal}_{stamp}.csv"
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
        f"{summary.get('journal')}/{summary.get('issue_id')}: "
        f"файлов {summary.get('total_files')}, "
        f"загружено {summary.get('uploaded')}, "
        f"dry-run {summary.get('dry_run_hits')}, "
        f"ошибок {summary.get('errors')} ({elapsed:.1f} с)"
    )
    record_operation(
        tool="issue_supp_images",
        title="Рисунки выпуска → доп. файлы",
        status=status,
        detail=detail,
        url=url_for("issue_supp_images.upload_page"),
    )

    if dry_run:
        flash(
            f"Проверка без изменений: к загрузке {summary.get('dry_run_hits')}, "
            f"ошибок {summary.get('errors')}, "
            f"папок без статьи: {len(summary.get('unmatched_folders') or [])}.",
            "success" if summary.get("errors", 0) == 0 else "warning",
        )
    else:
        flash(
            f"Готово: загружено {summary.get('uploaded')}, "
            f"ошибок {summary.get('errors')}.",
            "success" if summary.get("errors", 0) == 0 else "warning",
        )

    return render_template(
        "issue_supp_images_result.html",
        summary=summary,
        elapsed=elapsed,
        csv_name=csv_name,
    )


@issue_supp_images_bp.route("/issue-supp-images/download/<filename>")
def download_report(filename: str):
    if not _SAFE_REPORT_RE.fullmatch(filename):
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_supp_images.upload_page"))
    settings = get_settings()
    path = safe_temp_path(settings.temp_dir, filename)
    if path is None or not path.is_file():
        flash("Файл отчёта не найден или устарел", "error")
        return redirect(url_for("issue_supp_images.upload_page"))
    return send_file(
        path,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=filename,
    )
