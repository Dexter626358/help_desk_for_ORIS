"""HTTP-слой аудита опубликованного выпуска."""

from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from flask import (
    Blueprint,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from ipsas.common.validation import Validator
from ipsas.config.settings import get_settings
from ipsas.jobs.issue_metadata import (
    cleanup_expired_tasks,
    task_get,
    task_pop,
    task_set,
)
from ipsas.services.audit_published_issue import (
    build_editorial_letter_text,
    run_parse_task,
    summarize_findings,
)
from ipsas.utils.logger import get_logger
from ipsas.utils.temp_files import cleanup_temp_dir
from ipsas.web.issue_metadata.inflight import (
    inflight_age_s,
    release_inflight,
    try_acquire_inflight,
)

logger = get_logger(__name__)

issue_metadata_bp = Blueprint("issue_metadata", __name__, template_folder="templates")


def _safe_temp_artifact(filename: str, *, allowed_suffixes: tuple[str, ...] = (".html", ".txt")) -> Path:
    settings = get_settings()
    name = (filename or "").strip()
    lower = name.lower()
    if (
        not name
        or "/" in name
        or "\\" in name
        or ".." in name
        or not any(lower.endswith(suf) for suf in allowed_suffixes)
    ):
        raise ValueError("Некорректное имя файла")
    return settings.temp_dir / name


def _safe_report_path(filename: str) -> Path:
    return _safe_temp_artifact(filename, allowed_suffixes=(".html",))


@issue_metadata_bp.route("/issue-metadata-parser")
def issue_metadata_page():
    return render_template("issue_metadata_parser.html")


@issue_metadata_bp.route("/issue-metadata-parser/process", methods=["GET", "POST"])
def process_issue_metadata():
    issue_url = ""
    if request.method == "POST":
        issue_url = request.form.get("issue_url", "").strip()
    else:
        issue_url = request.args.get("issue_url", "").strip()
    logger.info("Issue metadata request: method=%s issue_url=%s", request.method, issue_url)
    if not issue_url:
        flash("Ссылка на выпуск не указана", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    user_key = "anonymous"
    cleanup_temp_dir(get_settings().temp_dir)
    cleanup_expired_tasks()
    try:
        inflight_ttl_s = int(os.getenv("ISSUE_PARSER_INFLIGHT_TTL_S", str(15 * 60)))
    except ValueError:
        inflight_ttl_s = 15 * 60

    force_unlock = (request.args.get("force_unlock") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }
    acquired = try_acquire_inflight(
        user_key=user_key, issue_url=issue_url, ttl_s=inflight_ttl_s
    )
    logger.info(
        "Issue metadata inflight: user=%s acquired=%s url=%s",
        user_key,
        acquired,
        issue_url,
    )
    if not acquired:
        age_s = inflight_age_s(user_key=user_key, issue_url=issue_url)
        age_msg = f" (идёт ~{int(age_s)}с)" if age_s is not None else ""
        if force_unlock or (age_s is not None and age_s >= float(inflight_ttl_s)):
            logger.warning(
                "Releasing stale inflight lock: user=%s url=%s age_s=%s ttl_s=%s force=%s",
                user_key,
                issue_url,
                age_s,
                inflight_ttl_s,
                force_unlock,
            )
            release_inflight(user_key=user_key, issue_url=issue_url)
            acquired = try_acquire_inflight(
                user_key=user_key, issue_url=issue_url, ttl_s=inflight_ttl_s
            )
            if not acquired:
                age_s = inflight_age_s(user_key=user_key, issue_url=issue_url)
                age_msg = f" (идёт ~{int(age_s)}с)" if age_s is not None else ""

        if not acquired:
            ttl_msg = f" TTL={inflight_ttl_s}с."
            extra = (
                " Можно снять блокировку параметром force_unlock=1."
                if not force_unlock
                else ""
            )
            flash(
                "Парсинг этого выпуска уже запущен." f"{age_msg}{ttl_msg}{extra}",
                "warning",
            )
            return redirect(url_for("issue_metadata.issue_metadata_page"))

    if not Validator().validate_url(issue_url):
        shown = issue_url if len(issue_url) <= 200 else (issue_url[:200] + "…")
        flash(f"Некорректная ссылка: {shown}", "error")
        release_inflight(user_key=user_key, issue_url=issue_url)
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    try:
        task_id = uuid.uuid4().hex
        task_set(
            task_id,
            status="running",
            started_at=time.time(),
            issue_url=issue_url,
            lower_url=issue_url.lower(),
            user_id=user_key,
            result=None,
            error=None,
        )

        threading.Thread(
            target=run_parse_task,
            kwargs={
                "task_id": task_id,
                "issue_url": issue_url,
                "user_key": user_key,
                "task_set": task_set,
                "release_inflight": release_inflight,
            },
            daemon=True,
            name=f"issue-parser-{task_id[:8]}",
        ).start()

        return render_template(
            "issue_metadata_waiting.html", task_id=task_id, issue_url=issue_url
        )
    except Exception as exc:
        logger.error("Ошибка парсинга выпуска: %s", exc, exc_info=True)
        flash(f"Ошибка при парсинге: {exc}", "error")
        release_inflight(user_key=user_key, issue_url=issue_url)
        return redirect(url_for("issue_metadata.issue_metadata_page"))


@issue_metadata_bp.route("/issue-metadata-parser/status/<task_id>")
def issue_metadata_task_status(task_id: str):
    task = task_get(task_id)
    if not task:
        return jsonify({"status": "not_found"}), 404

    status = task.get("status", "running")
    started_at = float(task.get("started_at") or time.time())
    elapsed_s = int(max(0.0, time.time() - started_at))

    if status == "done":
        return jsonify(
            {
                "status": "done",
                "redirect": url_for(
                    "issue_metadata.issue_metadata_task_result", task_id=task_id
                ),
            }
        )
    if status == "error":
        return jsonify(
            {"status": "error", "error": task.get("error") or "Неизвестная ошибка"}
        )
    return jsonify({"status": "running", "elapsed_s": elapsed_s})


@issue_metadata_bp.route("/issue-metadata-parser/task/<task_id>")
def issue_metadata_task_result(task_id: str):
    task = task_get(task_id)
    if not task:
        flash("Задача не найдена или уже завершена", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    status = task.get("status")
    if status == "running":
        return render_template(
            "issue_metadata_waiting.html",
            task_id=task_id,
            issue_url=task.get("issue_url", ""),
        )
    if status == "error":
        err = task.get("error") or "Неизвестная ошибка"
        task_pop(task_id)
        flash(f"Ошибка при парсинге: {err}", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    result = task.get("result")
    if not isinstance(result, dict):
        task_pop(task_id)
        flash("Некорректный результат задачи", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    issue_url = str(task.get("issue_url") or "")
    settings = get_settings()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    issue_data = result.get("issue") if isinstance(result.get("issue"), dict) else {}
    generated_at = datetime.now().strftime("%d.%m.%Y %H:%M")
    findings = summarize_findings(result)
    report_html = render_template(
        "issue_metadata_report.html",
        result=result,
        issue_url=issue_url,
        generated_at=generated_at,
        findings=findings,
    )
    from ipsas.utils.download_names import basename_from_issue_meta, with_report_stem

    issue_base = with_report_stem(
        basename_from_issue_meta(issue_data), fallback="issue"
    )
    report_temp_name = f"{timestamp}_{task_id[:8]}_{issue_base}.html"
    report_temp_path = settings.temp_dir / report_temp_name
    report_temp_path.write_text(report_html, encoding="utf-8")

    letter_text = build_editorial_letter_text(
        result=result,
        findings=findings,
        issue_url=issue_url,
        generated_at=generated_at,
    )
    letter_temp_name = f"{timestamp}_{task_id[:8]}_{issue_base}_editorial.txt"
    letter_temp_path = settings.temp_dir / letter_temp_name
    letter_temp_path.write_text(letter_text, encoding="utf-8")

    task_pop(task_id)
    return render_template(
        "issue_metadata_result.html",
        result=result,
        issue_url=issue_url,
        report_filename=report_temp_path.name,
        letter_filename=letter_temp_path.name,
    )


@issue_metadata_bp.route("/issue-metadata-parser/report/<filename>")
def view_issue_report(filename: str):
    try:
        file_path = _safe_report_path(filename)
    except ValueError:
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    if not file_path.exists():
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    return Response(file_path.read_text(encoding="utf-8"), mimetype="text/html")


@issue_metadata_bp.route("/issue-metadata-parser/download-letter/<filename>")
def download_editorial_letter(filename: str):
    """Скачать текст письма для редакции (.txt)."""
    try:
        file_path = _safe_temp_artifact(filename, allowed_suffixes=(".txt",))
    except ValueError:
        flash("Файл письма не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    if not file_path.exists():
        flash("Файл письма не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    from ipsas.utils.download_names import content_disposition_attachment, strip_temp_prefix

    download_name = strip_temp_prefix(filename)
    if not download_name.lower().endswith(".txt"):
        download_name = f"{download_name}.txt"

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info("Удалено письмо редакции после скачивания: %s", file_path.name)
            except OSError as e:
                logger.warning(
                    "Не удалось удалить письмо %s: %s", file_path.name, e
                )

    return Response(
        generate(),
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )


@issue_metadata_bp.route("/issue-metadata-parser/download/<filename>")
def download_issue_report(filename: str):
    """Скачать HTML-отчёт (attachment)."""
    try:
        file_path = _safe_report_path(filename)
    except ValueError:
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    if not file_path.exists():
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    from ipsas.utils.download_names import content_disposition_attachment, strip_temp_prefix

    download_name = strip_temp_prefix(filename)
    if not download_name.lower().endswith(".html"):
        download_name = f"{download_name}.html"

    def generate():
        try:
            with open(file_path, "rb") as f:
                yield f.read()
        finally:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info("Удалён HTML-отчёт после скачивания: %s", file_path.name)
            except OSError as e:
                logger.warning(
                    "Не удалось удалить HTML-отчёт %s: %s", file_path.name, e
                )

    return Response(
        generate(),
        mimetype="text/html",
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )
