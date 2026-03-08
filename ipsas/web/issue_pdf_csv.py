"""Routes for building issue CSV file for PDF attachment."""

import shutil
import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, url_for, Response
from flask_login import login_required
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.modules.issue_pdf_csv_builder import IssuePdfCsvBuilder
from ipsas.modules.validator import Validator
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

issue_pdf_csv_bp = Blueprint("issue_pdf_csv", __name__, template_folder="templates")


@issue_pdf_csv_bp.route("/issue-pdf-csv")
@login_required
def issue_pdf_csv_page():
    """Service page."""
    return render_template("issue_pdf_csv.html")


@issue_pdf_csv_bp.route("/issue-pdf-csv/process", methods=["POST"])
@login_required
def process_issue_pdf_csv():
    """Build CSV for issue + ZIP with PDFs."""
    settings = get_settings()
    validator = Validator()

    issue_url = (request.form.get("issue_url") or "").strip()
    if not issue_url:
        flash("Ссылка на выпуск не указана", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))
    if not validator.validate_url(issue_url):
        flash("Некорректная ссылка на выпуск", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))

    if "zip_file" not in request.files:
        flash("ZIP архив не был загружен", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))

    file = request.files["zip_file"]
    if not file.filename:
        flash("Файл не выбран", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))
    if not file.filename.lower().endswith(".zip"):
        flash("Поддерживаются только ZIP архивы", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))

    original_zip_name = secure_filename(file.filename)
    unique_id = uuid.uuid4().hex[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    zip_name = f"{timestamp}_{unique_id}_{original_zip_name}"
    zip_path = settings.temp_dir / zip_name
    extract_dir = settings.temp_dir / f"{timestamp}_{unique_id}_issue_pdf_extract"
    csv_filename = f"{timestamp}_{unique_id}_issue_pdf.csv"
    csv_path = settings.temp_dir / csv_filename

    try:
        file.save(str(zip_path))
        builder = IssuePdfCsvBuilder(max_download_size=settings.max_file_size)
        result = builder.build_csv(
            issue_url=issue_url,
            zip_path=zip_path,
            output_csv_path=csv_path,
            extract_dir=extract_dir,
        )

        return render_template(
            "issue_pdf_csv_result.html",
            issue_url=issue_url,
            result=result,
            output_csv_filename=csv_filename,
        )
    except Exception as exc:
        logger.error("Ошибка формирования CSV выпуска: %s", exc, exc_info=True)
        flash(f"Ошибка формирования CSV: {exc}", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))
    finally:
        try:
            if zip_path.exists():
                zip_path.unlink()
        except Exception as exc:
            logger.warning("Не удалось удалить временный ZIP %s: %s", zip_path, exc)
        try:
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
        except Exception as exc:
            logger.warning("Не удалось удалить временную директорию %s: %s", extract_dir, exc)


@issue_pdf_csv_bp.route("/issue-pdf-csv/manual-assign/<filename>", methods=["POST"])
@login_required
def manual_assign_issue_pdf_csv(filename: str):
    """Apply manual PDF assignment in CSV rows and download updated CSV."""
    settings = get_settings()
    file_path: Path | None = None
    for path in settings.temp_dir.rglob(filename):
        if path.is_file() and path.suffix.lower() == ".csv":
            file_path = path
            break

    if not file_path or not file_path.exists():
        flash("CSV файл не найден", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))

    try:
        row_count = int((request.form.get("row_count") or "0").strip())
    except ValueError:
        row_count = 0

    if row_count <= 0:
        flash("Нет данных для ручной корректировки", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))

    rows = []
    changed = 0
    for i in range(row_count):
        doi = (request.form.get(f"doi_{i}") or "").strip()
        surnames = (request.form.get(f"surnames_{i}") or "").strip()
        current_pdf = (request.form.get(f"pdf_{i}") or "").strip()
        lang = (
            request.form.get(f"assign_lang_{i}")
            or request.form.get(f"lang_{i}")
            or "en_US"
        ).strip() or "en_US"
        if lang not in {"en_US", "ru_RUS"}:
            lang = "en_US"
        manual_pdf = (request.form.get(f"assign_row_{i}") or "").strip()

        if not doi:
            continue

        final_pdf = current_pdf
        if manual_pdf:
            final_pdf = manual_pdf
            if manual_pdf != current_pdf:
                changed += 1

        rows.append([doi, surnames, final_pdf, lang])

    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        import csv
        writer = csv.writer(f, delimiter=";", lineterminator="\n")
        for row in rows:
            writer.writerow([row[0], row[2], row[3]])

    if changed > 0:
        flash(f"Ручные изменения применены: {changed}", "success")

    return redirect(url_for("issue_pdf_csv.download_issue_pdf_csv", filename=filename))


@issue_pdf_csv_bp.route("/issue-pdf-csv/download/<filename>")
@login_required
def download_issue_pdf_csv(filename: str):
    """Download generated CSV and remove it after download."""
    settings = get_settings()
    file_path: Path | None = None
    for path in settings.temp_dir.rglob(filename):
        if path.is_file() and path.suffix.lower() == ".csv":
            file_path = path
            break

    if not file_path or not file_path.exists():
        flash("CSV файл не найден", "error")
        return redirect(url_for("issue_pdf_csv.issue_pdf_csv_page"))

    download_name = "issue_pdf.csv"

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            try:
                if file_path.exists():
                    file_path.unlink()
            except Exception as exc:
                logger.warning("Не удалось удалить временный CSV %s: %s", file_path, exc)

    return Response(
        generate(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )
