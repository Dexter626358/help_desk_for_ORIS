"""Роуты для парсинга метаданных выпуска по ссылке."""

import uuid
from datetime import datetime
from pathlib import Path

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required

from ipsas.config.settings import get_settings
from ipsas.modules.issue_metadata_parser import IssueMetadataParser
from ipsas.modules.validator import Validator
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

issue_metadata_bp = Blueprint("issue_metadata", __name__, template_folder="templates")


@issue_metadata_bp.route("/issue-metadata-parser")
@login_required
def issue_metadata_page():
    """Страница сервиса парсинга метаданных выпуска."""
    return render_template("issue_metadata_parser.html")


@issue_metadata_bp.route("/issue-metadata-parser/process", methods=["POST"])
@login_required
def process_issue_metadata():
    """Обработка ссылки на выпуск."""
    issue_url = request.form.get("issue_url", "").strip()
    if not issue_url:
        flash("Ссылка на выпуск не указана", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    validator = Validator()
    if not validator.validate_url(issue_url):
        flash("Некорректная ссылка", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    settings = get_settings()
    parser = IssueMetadataParser(max_download_size=settings.max_file_size)

    lower_url = issue_url.lower()
    try:
        if lower_url.endswith(".xml") or lower_url.endswith(".zip"):
            original_name = issue_url.split("/")[-1]
            unique_id = uuid.uuid4().hex[:8]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{unique_id}_{original_name}"
            temp_path = settings.temp_dir / filename

            download = parser.download(issue_url, temp_path)
            issue_metadata = parser.parse_issue_metadata(download.path)
            result = {
                "issue": issue_metadata,
                "articles": [],
                "notice": "Ссылка указывает на файл. Для анализа статей используйте ссылку на страницу выпуска.",
            }

            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception as exc:
                logger.warning("Не удалось удалить временный файл %s: %s", temp_path, exc)
        else:
            result = parser.parse_issue_url(issue_url)
            result["notice"] = None

        return render_template(
            "issue_metadata_result.html",
            result=result,
            issue_url=issue_url,
        )
    except Exception as exc:
        logger.error("Ошибка парсинга выпуска: %s", exc, exc_info=True)
        flash(f"Ошибка при парсинге: {exc}", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))
