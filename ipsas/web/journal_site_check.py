"""Роуты: проверка заполненности сайта журнала."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for

from ipsas.config.settings import get_settings
from ipsas.modules.validator import Validator
from ipsas.services.check_journal_site import build_editorial_letter_text
from ipsas.services.check_journal_site import execute as check_journal_site
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import cleanup_temp_dir, safe_temp_path

logger = get_logger(__name__)

journal_site_check_bp = Blueprint("journal_site_check", __name__, template_folder="templates")

_SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9._\-]+")


def _unlink_quiet(*paths: Path) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError as e:
            logger.warning("Не удалось удалить временный файл %s: %s", path.name, e)


def _letter_basename(report: dict) -> str:
    title = str(report.get("journal_title") or "journal").strip()
    stem = _SAFE_STEM_RE.sub("_", title).strip("._-")[:80] or "journal"
    return f"{stem}_site_report"


@journal_site_check_bp.route("/journal-site-check")
def journal_site_check_page():
    """Форма проверки сайта журнала."""
    settings = get_settings()
    external = settings.journal_site_check_url
    if external:
        return redirect(external)
    return render_template("journal_site_check.html")


@journal_site_check_bp.route("/journal-site-check/process", methods=["POST"])
def process_journal_site_check():
    """Запуск проверки по URL журнала."""
    settings = get_settings()
    cleanup_temp_dir(
        settings.temp_dir,
        ttl_seconds=6 * 60 * 60,
        suffixes=(".xml", ".html", ".json", ".csv", ".zip", ".txt"),
    )

    validator = Validator()
    journal_url = (request.form.get("journal_url") or "").strip()
    if not journal_url:
        flash("Ссылка на журнал не указана", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))
    if not validator.validate_url(journal_url):
        flash("Некорректная или небезопасная ссылка на журнал", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    try:
        report = check_journal_site(journal_url)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))
    except Exception as e:
        logger.error("Ошибка проверки сайта журнала: %s", e, exc_info=True)
        flash(f"Ошибка проверки сайта: {e}", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    pct = report.get("completeness_percent") or 0
    ru = report.get("completeness_ru") or 0
    en = report.get("completeness_en") or 0
    title = report.get("journal_title") or journal_url
    status = "ok" if pct >= 70 else ("warning" if pct >= 40 else "error")
    record_operation(
        tool="journal_site_check",
        title="Проверить сайт журнала",
        status=status,
        detail=f"{title}; RU {ru}% · EN {en}% · итого {pct}%",
        url=url_for("journal_site_check.journal_site_check_page"),
    )

    generated_at = str(report.get("generated_at") or datetime.now().strftime("%d.%m.%Y %H:%M"))
    letter_text = build_editorial_letter_text(
        report,
        journal_url=journal_url,
        generated_at=generated_at,
    )
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    letter_base = _letter_basename(report)
    letter_temp_name = f"{timestamp}_{unique_id}_{letter_base}_editorial.txt"
    letter_temp_path = settings.temp_dir / letter_temp_name
    letter_temp_path.write_text(letter_text, encoding="utf-8")

    return render_template(
        "journal_site_check_result.html",
        journal_url=journal_url,
        report=report,
        letter_filename=letter_temp_path.name,
    )


@journal_site_check_bp.route("/journal-site-check/download-letter/<filename>")
def download_editorial_letter(filename: str):
    """Скачать текст письма для редакции (.txt)."""
    settings = get_settings()
    file_path = safe_temp_path(settings.temp_dir, filename)
    if (
        file_path is None
        or not file_path.exists()
        or file_path.suffix.lower() != ".txt"
    ):
        flash("Файл письма не найден", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    from ipsas.utils.download_names import content_disposition_attachment, strip_temp_prefix

    download_name = strip_temp_prefix(filename)
    if not download_name.lower().endswith(".txt"):
        download_name = f"{download_name}.txt"

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            _unlink_quiet(file_path)

    return Response(
        generate(),
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )
