"""Роуты для парсинга метаданных выпуска по ссылке."""

import uuid
from datetime import datetime
from pathlib import Path
import hashlib
import os
import threading
import time
from typing import Any

from flask import Blueprint, Response, render_template, request, redirect, url_for, flash, jsonify

from ipsas.config.settings import get_settings
from ipsas.modules.issue_metadata_parser import IssueMetadataParser
from ipsas.modules.issue_metadata.report_summary import group_findings
from ipsas.modules.validator import Validator
from ipsas.utils.logger import get_logger
from ipsas.utils.temp_files import cleanup_temp_dir
from ipsas.web.issue_metadata_tasks import (
    cleanup_expired_tasks,
    task_get as _task_get,
    task_pop as _task_pop,
    task_set as _task_set,
)

logger = get_logger(__name__)

issue_metadata_bp = Blueprint("issue_metadata", __name__, template_folder="templates")

_inflight_lock = threading.Lock()


def _inflight_lock_path(*, user_key: str, issue_url: str) -> Path:
    settings = get_settings()
    lock_dir = settings.temp_dir / "issue_metadata_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(f"{user_key}\n{issue_url}".encode("utf-8")).hexdigest()[:32]
    return lock_dir / f"{digest}.lock"


def _try_acquire_inflight(*, user_key: str, issue_url: str, ttl_s: int = 15 * 60) -> bool:
    """Защита от двойного запуска (межпроцессная).

    In-memory lock не работает, если запросы попали в разные процессы/воркеры.
    Поэтому используем lockfile с атомарным созданием.
    """
    lock_path = _inflight_lock_path(user_key=user_key, issue_url=issue_url)
    now = time.time()
    # чистка протухшего (под локом, чтобы не было гонок)
    with _inflight_lock:
        if lock_path.exists():
            try:
                age = now - lock_path.stat().st_mtime
                if age > ttl_s:
                    lock_path.unlink(missing_ok=True)  # type: ignore[call-arg]
            except Exception:
                # Если не можем прочитать/удалить — считаем, что лок активен.
                return False
        try:
            # Atomic create: fails if exists
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(f"started_at={now}\nuser={user_key}\nurl={issue_url}\n")
            return True
        except FileExistsError:
            return False
        except Exception:
            return False


def _inflight_age_s(*, user_key: str, issue_url: str) -> float | None:
    """Возраст текущей блокировки (сек), если существует."""
    lock_path = _inflight_lock_path(user_key=user_key, issue_url=issue_url)
    if not lock_path.exists():
        return None
    try:
        return max(0.0, time.time() - lock_path.stat().st_mtime)
    except Exception:
        return None


def _release_inflight(*, user_key: str, issue_url: str) -> None:
    lock_path = _inflight_lock_path(user_key=user_key, issue_url=issue_url)
    with _inflight_lock:
        try:
            lock_path.unlink(missing_ok=True)  # type: ignore[call-arg]
        except Exception:
            pass


def _safe_report_path(filename: str) -> Path:
    settings = get_settings()
    name = (filename or "").strip()
    if not name or "/" in name or "\\" in name or ".." in name or not name.lower().endswith(".html"):
        raise ValueError("Некорректное имя файла отчёта")
    return settings.temp_dir / name


@issue_metadata_bp.route("/issue-metadata-parser")
def issue_metadata_page():
    """Страница сервиса парсинга метаданных выпуска."""
    return render_template("issue_metadata_parser.html")


@issue_metadata_bp.route("/issue-metadata-parser/process", methods=["GET", "POST"])
def process_issue_metadata():
    """Обработка ссылки на выпуск.

    Поддерживает POST (форма) и GET (для диагностик/прямой ссылки).
    """
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

    force_unlock = (request.args.get("force_unlock") or "").strip().lower() in {"1", "true", "yes", "y"}
    acquired = _try_acquire_inflight(user_key=user_key, issue_url=issue_url, ttl_s=inflight_ttl_s)
    logger.info("Issue metadata inflight: user=%s acquired=%s url=%s", user_key, acquired, issue_url)
    if not acquired:
        age_s = _inflight_age_s(user_key=user_key, issue_url=issue_url)
        age_msg = f" (идёт ~{int(age_s)}с)" if age_s is not None else ""
        # Если блокировка протухла (или пользователь явно попросил снять) — снимаем и пробуем ещё раз.
        if force_unlock or (age_s is not None and age_s >= float(inflight_ttl_s)):
            logger.warning(
                "Releasing stale inflight lock: user=%s url=%s age_s=%s ttl_s=%s force=%s",
                user_key,
                issue_url,
                age_s,
                inflight_ttl_s,
                force_unlock,
            )
            _release_inflight(user_key=user_key, issue_url=issue_url)
            acquired2 = _try_acquire_inflight(user_key=user_key, issue_url=issue_url, ttl_s=inflight_ttl_s)
            logger.info("Issue metadata inflight retry: user=%s acquired=%s url=%s", user_key, acquired2, issue_url)
            if acquired2:
                acquired = True
            else:
                age_s = _inflight_age_s(user_key=user_key, issue_url=issue_url)
                age_msg = f" (идёт ~{int(age_s)}с)" if age_s is not None else ""

        if not acquired:
            ttl_msg = f" TTL={inflight_ttl_s}с."
            extra = " Можно снять блокировку параметром force_unlock=1." if not force_unlock else ""
            flash(
                "Парсинг этого выпуска уже запущен."
                f"{age_msg}{ttl_msg}{extra}",
                "warning",
            )
            return redirect(url_for("issue_metadata.issue_metadata_page"))

    validator = Validator()
    if not validator.validate_url(issue_url):
        shown = issue_url if len(issue_url) <= 200 else (issue_url[:200] + "…")
        flash(f"Некорректная ссылка: {shown}", "error")
        _release_inflight(user_key=user_key, issue_url=issue_url)
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    lower_url = issue_url.lower()
    try:
        task_id = uuid.uuid4().hex
        _task_set(
            task_id,
            status="running",
            started_at=time.time(),
            issue_url=issue_url,
            lower_url=lower_url,
            user_id=user_key,
            result=None,
            error=None,
        )

        def _run() -> None:
            settings = get_settings()
            parser = IssueMetadataParser(max_download_size=settings.max_file_size)
            try:
                unique_id = task_id[:8]
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                if lower_url.endswith(".xml") or lower_url.endswith(".zip"):
                    original_name = issue_url.split("/")[-1]
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

                _task_set(task_id, status="done", result=result, finished_at=time.time())
            except Exception as exc:
                logger.error("Фоновый парсинг выпуска упал: %s", exc, exc_info=True)
                _task_set(task_id, status="error", error=str(exc), finished_at=time.time())
            finally:
                _release_inflight(user_key=user_key, issue_url=issue_url)

        threading.Thread(target=_run, daemon=True, name=f"issue-parser-{task_id[:8]}").start()

        return render_template("issue_metadata_waiting.html", task_id=task_id, issue_url=issue_url)
    except Exception as exc:
        logger.error("Ошибка парсинга выпуска: %s", exc, exc_info=True)
        flash(f"Ошибка при парсинге: {exc}", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))


@issue_metadata_bp.route("/issue-metadata-parser/status/<task_id>")
def issue_metadata_task_status(task_id: str):
    task = _task_get(task_id)
    if not task:
        return jsonify({"status": "not_found"}), 404

    status = task.get("status", "running")
    started_at = float(task.get("started_at") or time.time())
    elapsed_s = int(max(0.0, time.time() - started_at))

    if status == "done":
        return jsonify({"status": "done", "redirect": url_for("issue_metadata.issue_metadata_task_result", task_id=task_id)})
    if status == "error":
        return jsonify({"status": "error", "error": task.get("error") or "Неизвестная ошибка"})
    return jsonify({"status": "running", "elapsed_s": elapsed_s})


@issue_metadata_bp.route("/issue-metadata-parser/task/<task_id>")
def issue_metadata_task_result(task_id: str):
    task = _task_get(task_id)
    if not task:
        flash("Задача не найдена или уже завершена", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    status = task.get("status")
    if status == "running":
        return render_template("issue_metadata_waiting.html", task_id=task_id, issue_url=task.get("issue_url", ""))
    if status == "error":
        err = task.get("error") or "Неизвестная ошибка"
        _task_pop(task_id)
        flash(f"Ошибка при парсинге: {err}", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    result = task.get("result")
    if not isinstance(result, dict):
        _task_pop(task_id)
        flash("Некорректный результат задачи", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    issue_url = str(task.get("issue_url") or "")
    settings = get_settings()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    issue_data = result.get("issue") if isinstance(result.get("issue"), dict) else {}
    articles_data = result.get("articles") if isinstance(result.get("articles"), list) else []
    findings = group_findings(issue_data.get("warnings") or [], articles_data)
    report_html = render_template(
        "issue_metadata_report.html",
        result=result,
        issue_url=issue_url,
        generated_at=datetime.now().strftime("%d.%m.%Y %H:%M"),
        findings=findings,
    )
    from ipsas.utils.download_names import basename_from_issue_meta, with_report_stem

    issue_base = with_report_stem(basename_from_issue_meta(issue_data), fallback="issue")
    report_temp_name = f"{timestamp}_{task_id[:8]}_{issue_base}.html"
    report_temp_path = settings.temp_dir / report_temp_name
    report_temp_path.write_text(report_html, encoding="utf-8")

    _task_pop(task_id)
    return render_template(
        "issue_metadata_result.html",
        result=result,
        issue_url=issue_url,
        report_filename=report_temp_path.name,
    )


@issue_metadata_bp.route("/issue-metadata-parser/report/<filename>")
def view_issue_report(filename: str):
    """Показать HTML-отчёт в браузере (inline)."""
    try:
        file_path = _safe_report_path(filename)
    except ValueError:
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    if not file_path.exists():
        flash("Файл отчёта не найден", "error")
        return redirect(url_for("issue_metadata.issue_metadata_page"))

    return Response(file_path.read_text(encoding="utf-8"), mimetype="text/html")


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
            except Exception as e:
                logger.warning("Не удалось удалить HTML-отчёт %s: %s", file_path.name, e)

    return Response(
        generate(),
        mimetype="text/html",
        headers={"Content-Disposition": content_disposition_attachment(download_name)},
    )
