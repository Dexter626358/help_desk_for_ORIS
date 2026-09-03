"""Роуты: проверка заполненности сайта журнала и/или OJS .data."""

from __future__ import annotations

import re
import time
import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.modules.validator import Validator
from ipsas.services.check_journal_site import (
    build_editorial_letter_html,
    build_editorial_letter_html_document,
    build_editorial_letter_text,
)
from ipsas.services.check_journal_site import execute as check_journal_site
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import safe_temp_path

logger = get_logger(__name__)

journal_site_check_bp = Blueprint("journal_site_check", __name__, template_folder="templates")

_SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9._\-]+")
_DATA_SUFFIXES = (".data", ".json")


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
    """Запуск проверки по URL журнала либо по файлу .data (взаимоисключающе)."""
    settings = get_settings()
    t0 = time.perf_counter()

    validator = Validator()
    journal_url = (request.form.get("journal_url") or "").strip()
    upload = request.files.get("data_file")
    has_upload = bool(upload and upload.filename)

    if not journal_url and not has_upload:
        flash("Укажите ссылку на журнал или загрузите файл .data", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    if journal_url and has_upload:
        flash("Выберите один источник: либо ссылку на журнал, либо файл .data", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    if journal_url and not validator.validate_url(journal_url):
        flash("Некорректная или небезопасная ссылка на журнал", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    data_bytes: bytes | None = None
    data_filename = ""

    if has_upload:
        assert upload is not None
        original_name = upload.filename or "journal.data"
        original = secure_filename(original_name) or "journal.data"
        lower = original.lower()
        raw_name = original_name.lower()
        if not lower.endswith(_DATA_SUFFIXES) and not raw_name.endswith(_DATA_SUFFIXES):
            flash("Поддерживаются файлы .data или .json (экспорт настроек OJS)", "error")
            return redirect(url_for("journal_site_check.journal_site_check_page"))
        if not lower.endswith(_DATA_SUFFIXES):
            original = Path(original_name).name

        # Читаем в память — без записи в OneDrive/temp (это часто тормозит)
        data_bytes = upload.read()
        if not data_bytes:
            flash("Файл пустой", "error")
            return redirect(url_for("journal_site_check.journal_site_check_page"))
        if len(data_bytes) > settings.max_file_size:
            max_mb = settings.max_file_size / (1024 * 1024)
            flash(f"Файл слишком большой. Максимальный размер: {max_mb:.1f} MB", "error")
            return redirect(url_for("journal_site_check.journal_site_check_page"))
        data_filename = original
        journal_url = ""  # на всякий случай не смешиваем с сайтом

    try:
        report = check_journal_site(
            journal_url or None,
            data_file=data_bytes,
            data_filename=data_filename,
        )
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))
    except Exception as e:
        logger.error("Ошибка проверки сайта журнала: %s", e, exc_info=True)
        flash(f"Ошибка проверки сайта: {e}", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    elapsed = time.perf_counter() - t0
    logger.info(
        "journal_site_check done mode=%s in %.2fs title=%s",
        report.get("check_mode"),
        elapsed,
        report.get("journal_title") or data_filename or journal_url,
    )

    pct = report.get("completeness_percent") or 0
    ru = report.get("completeness_ru") or 0
    en = report.get("completeness_en") or 0
    title = report.get("journal_title") or journal_url or data_filename
    plugin_issues = len(report.get("plugins_must_fix") or [])
    status = "ok" if pct >= 70 and plugin_issues == 0 else ("warning" if pct >= 40 else "error")
    mode = report.get("check_mode") or "site"
    detail = f"{title}; RU {ru}% · EN {en}% · итого {pct}% · {elapsed:.1f}с"
    if mode != "site":
        detail += f" · плагины: {plugin_issues} замечаний"
    record_operation(
        tool="journal_site_check",
        title="Проверить сайт журнала",
        status=status,
        detail=detail,
        url=url_for("journal_site_check.journal_site_check_page"),
    )

    generated_at = str(report.get("generated_at") or datetime.now().strftime("%d.%m.%Y %H:%M"))
    letter_text = build_editorial_letter_text(
        report,
        journal_url=journal_url,
        generated_at=generated_at,
    )
    letter_html = build_editorial_letter_html(
        report,
        journal_url=journal_url,
        generated_at=generated_at,
    )
    letter_html_doc = build_editorial_letter_html_document(
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
    html_temp_name = f"{timestamp}_{unique_id}_{letter_base}_editorial.html"
    html_temp_path = settings.temp_dir / html_temp_name
    html_temp_path.write_text(letter_html_doc, encoding="utf-8")

    return render_template(
        "journal_site_check_result.html",
        journal_url=journal_url,
        report=report,
        letter_text=letter_text,
        letter_html=letter_html,
        letter_filename=letter_temp_path.name,
        letter_html_filename=html_temp_path.name,
    )


@journal_site_check_bp.route("/journal-site-check/download-letter/<filename>")
def download_editorial_letter(filename: str):
    """Скачать письмо для редакции (.txt или .html)."""
    settings = get_settings()
    file_path = safe_temp_path(settings.temp_dir, filename)
    suffix = file_path.suffix.lower() if file_path is not None else ""
    if (
        file_path is None
        or not file_path.exists()
        or suffix not in {".txt", ".html"}
    ):
        flash("Файл письма не найден", "error")
        return redirect(url_for("journal_site_check.journal_site_check_page"))

    from ipsas.utils.download_names import content_disposition_attachment, strip_temp_prefix

    download_name = strip_temp_prefix(filename)
    if suffix == ".html":
        if not download_name.lower().endswith(".html"):
            download_name = f"{download_name}.html"
        mimetype = "text/html; charset=utf-8"
    else:
        if not download_name.lower().endswith(".txt"):
            download_name = f"{download_name}.txt"
        mimetype = "text/plain; charset=utf-8"

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            _unlink_quiet(file_path)

    return Response(
        generate(),
        mimetype=mimetype,
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )
