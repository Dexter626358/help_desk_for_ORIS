"""Веб-сервис: синхронизация разделов журнала из CSV / сортировка."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.modules.sandbox_journal_setup.urls import parse_journal_url
from ipsas.services.journal_sections import execute as journal_sections_execute
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import safe_temp_path

logger = get_logger(__name__)

journal_sections_bp = Blueprint(
    "journal_sections", __name__, template_folder="templates"
)


def _unlink_quiet(path: Path) -> None:
    try:
        if path.is_file():
            path.unlink()
    except OSError as exc:
        logger.warning("Не удалось удалить %s: %s", path, exc)


def _flag(name: str) -> bool:
    return (request.form.get(name) or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }


@journal_sections_bp.route("/journal-sections")
def sync_page():
    settings = get_settings()
    return render_template(
        "journal_sections.html",
        sandbox_base_url=settings.sandbox_base_url,
        platform_base_url=settings.platform_base_url,
    )


@journal_sections_bp.route("/journal-sections/process", methods=["POST"])
def process_sync():
    journal_url = (request.form.get("journal_url") or "").strip()
    dry_run = _flag("dry_run")
    sort_alpha = _flag("sort_alpha")

    if not journal_url:
        flash("Укажите ссылку на журнал", "error")
        return redirect(url_for("journal_sections.sync_page"))

    try:
        parse_journal_url(journal_url)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("journal_sections.sync_page"))

    upload = request.files.get("csv_file")
    has_csv = bool(upload and (upload.filename or "").strip())
    if not has_csv and not sort_alpha:
        flash("Загрузите CSV и/или включите сортировку разделов", "error")
        return redirect(url_for("journal_sections.sync_page"))

    csv_path: Path | None = None
    if has_csv:
        assert upload is not None
        filename = secure_filename(upload.filename or "sections.csv")
        if not filename.lower().endswith(".csv"):
            flash("Нужен файл с расширением .csv", "error")
            return redirect(url_for("journal_sections.sync_page"))
        settings = get_settings()
        temp_dir = Path(settings.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        csv_path = safe_temp_path(
            temp_dir, f"journal_sections_{uuid.uuid4().hex}_{filename}"
        )
        if csv_path is None:
            flash("Некорректное имя загруженного файла", "error")
            return redirect(url_for("journal_sections.sync_page"))
        upload.save(str(csv_path))

    t0 = time.perf_counter()
    try:
        summary = journal_sections_execute(
            journal_url=journal_url,
            csv_path=csv_path,
            dry_run=dry_run,
            sort_alpha=sort_alpha,
        )
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("journal_sections.sync_page"))
    except Exception as exc:  # noqa: BLE001
        logger.error("journal_sections failed: %s", exc, exc_info=True)
        flash(
            "Не удалось выполнить операцию. Подробности в журнале сервера.",
            "error",
        )
        return redirect(url_for("journal_sections.sync_page"))
    finally:
        if csv_path is not None:
            _unlink_quiet(csv_path)

    elapsed = time.perf_counter() - t0
    mode = "dry-run" if dry_run else "apply"
    sort_bit = ""
    if summary.get("sort_alpha"):
        sort_bit = f", sort={summary.get('sort_status')}"
    detail = (
        f"{summary.get('journal')} @ {summary.get('base_url')}: {mode}, "
        f"создано {summary.get('created')}, уже есть {summary.get('exists')}, "
        f"ошибок {summary.get('errors')}{sort_bit} ({elapsed:.1f} с)"
    )
    status = "ok" if summary.get("ok") else "warning"
    record_operation(
        tool="journal_sections",
        title="Разделы журнала из CSV",
        status=status,
        detail=detail,
        url=url_for("journal_sections.sync_page"),
    )

    if not has_csv and sort_alpha:
        flash(
            f"Сортировка: {summary.get('sort_status')} — "
            f"{summary.get('sort_message') or '—'}",
            "success" if summary.get("ok") else "warning",
        )
    elif dry_run:
        flash(
            f"Проверка: к созданию {summary.get('dry_run_hits')}, "
            f"уже есть {summary.get('exists')}, ошибок {summary.get('errors')}.",
            "success" if summary.get("ok") else "warning",
        )
    else:
        flash(
            f"Готово: создано {summary.get('created')}, "
            f"уже есть {summary.get('exists')}, ошибок {summary.get('errors')}.",
            "success" if summary.get("ok") else "warning",
        )

    return render_template(
        "journal_sections_result.html",
        summary=summary,
        dry_run=dry_run,
    )
