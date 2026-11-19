"""Веб-сервис: поиск дублей статей (XML или выпуск ↔ рукописи журнала)."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from flask import (
    Blueprint,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.services.article_duplicates import execute_archive, execute_find
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation
from ipsas.utils.temp_files import safe_temp_path

logger = get_logger(__name__)

article_duplicates_bp = Blueprint(
    "article_duplicates", __name__, template_folder="templates"
)


def _flag(name: str) -> bool:
    return (request.form.get(name) or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }


def _unlink_quiet(path: Path) -> None:
    try:
        if path.is_file():
            path.unlink()
    except OSError as exc:
        logger.warning("Не удалось удалить %s: %s", path, exc)


@article_duplicates_bp.route("/article-duplicates")
def duplicates_page():
    return render_template("article_duplicates.html")


@article_duplicates_bp.route("/article-duplicates/process", methods=["POST"])
def process_find():
    journal_url = (request.form.get("journal_url") or "").strip()
    issue_url = (request.form.get("issue_url") or "").strip()

    upload = request.files.get("xml_file")
    has_xml = bool(upload and (upload.filename or "").strip())
    has_issue = bool(issue_url)

    if has_xml and has_issue:
        flash("Укажите либо XML, либо ссылку на выпуск — не оба сразу", "error")
        return redirect(url_for("article_duplicates.duplicates_page"))
    if not has_xml and not has_issue:
        flash("Загрузите journal XML или укажите ссылку на выпуск", "error")
        return redirect(url_for("article_duplicates.duplicates_page"))
    if has_xml and not journal_url:
        flash("Для XML укажите ссылку на журнал или ISSN", "error")
        return redirect(url_for("article_duplicates.duplicates_page"))

    settings = get_settings()
    xml_path: Path | None = None
    if has_xml:
        assert upload is not None
        filename = secure_filename(upload.filename or "issue.xml")
        if not filename.lower().endswith(".xml"):
            flash("Нужен файл с расширением .xml", "error")
            return redirect(url_for("article_duplicates.duplicates_page"))
        temp_dir = Path(settings.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        xml_path = safe_temp_path(
            temp_dir, f"article_duplicates_{uuid.uuid4().hex}_{filename}"
        )
        if xml_path is None:
            flash("Некорректное имя загруженного файла", "error")
            return redirect(url_for("article_duplicates.duplicates_page"))
        upload.save(str(xml_path))

    t0 = time.perf_counter()
    try:
        summary = execute_find(
            journal_url=journal_url,
            xml_path=xml_path,
            issue_url=issue_url or None,
        )
    except ValueError as exc:
        flash(str(exc), "error")
        if xml_path is not None:
            _unlink_quiet(xml_path)
        return redirect(url_for("article_duplicates.duplicates_page"))
    except Exception as exc:  # noqa: BLE001
        logger.error("article_duplicates find failed: %s", exc, exc_info=True)
        flash(
            "Не удалось выполнить поиск. Подробности в журнале сервера.",
            "error",
        )
        if xml_path is not None:
            _unlink_quiet(xml_path)
        return redirect(url_for("article_duplicates.duplicates_page"))
    finally:
        if xml_path is not None:
            _unlink_quiet(xml_path)

    elapsed = time.perf_counter() - t0
    source = summary.get("source") or "xml"
    source_bit = (
        f"выпуск {summary.get('source_label')}"
        if source == "issue"
        else f"XML {summary.get('xml_name') or summary.get('source_label')}"
    )
    detail = (
        f"{summary.get('journal')}: {source_bit}, "
        f"статей {summary.get('xml_articles')}, "
        f"корпус {summary.get('corpus_size')}, "
        f"с кандидатами {summary.get('groups_with_candidates')} "
        f"({elapsed:.1f} с)"
    )
    record_operation(
        tool="article_duplicates",
        title="Поиск дублей статей",
        status="ok",
        detail=detail,
        url=url_for("article_duplicates.duplicates_page"),
    )
    flash(
        f"Поиск ({source}): групп с кандидатами "
        f"{summary.get('groups_with_candidates')} из {summary.get('xml_articles')} "
        f"(корпус {summary.get('corpus_size')}).",
        "success",
    )
    return render_template(
        "article_duplicates_result.html",
        summary=summary,
        elapsed=elapsed,
    )

def _wants_json() -> bool:
    if (request.form.get("ajax") or "").strip() in {"1", "true", "yes"}:
        return True
    best = (request.accept_mimetypes.best or "").casefold()
    return "application/json" in best


@article_duplicates_bp.route("/article-duplicates/archive", methods=["POST"])
def process_archive():
    journal_url = (request.form.get("journal_url") or "").strip()
    dry_run = _flag("dry_run")
    wants_json = _wants_json()
    raw_ids = request.form.getlist("match_ids")
    article_ids: list[int] = []
    for raw in raw_ids:
        try:
            article_ids.append(int(str(raw).strip()))
        except ValueError:
            continue

    if not journal_url:
        if wants_json:
            return jsonify({"ok": False, "error": "Укажите ссылку на журнал"}), 400
        flash("Укажите ссылку на журнал", "error")
        return redirect(url_for("article_duplicates.duplicates_page"))
    if not article_ids:
        if wants_json:
            return jsonify(
                {"ok": False, "error": "Не указан id рукописи для архивации"}
            ), 400
        flash("Не указан id рукописи для архивации", "error")
        return redirect(url_for("article_duplicates.duplicates_page"))

    t0 = time.perf_counter()
    try:
        summary = execute_archive(
            journal_url=journal_url,
            article_ids=article_ids,
            dry_run=dry_run,
        )
    except ValueError as exc:
        if wants_json:
            return jsonify({"ok": False, "error": str(exc)}), 400
        flash(str(exc), "error")
        return redirect(url_for("article_duplicates.duplicates_page"))
    except Exception as exc:  # noqa: BLE001
        logger.error("article_duplicates archive failed: %s", exc, exc_info=True)
        if wants_json:
            return jsonify(
                {
                    "ok": False,
                    "error": "Не удалось выполнить архивацию. "
                    "Подробности в журнале сервера.",
                }
            ), 500
        flash(
            "Не удалось выполнить архивацию. Подробности в журнале сервера.",
            "error",
        )
        return redirect(url_for("article_duplicates.duplicates_page"))

    elapsed = time.perf_counter() - t0
    status = "ok" if summary.get("errors", 0) == 0 else "warning"
    record_operation(
        tool="article_duplicates",
        title="Архивация дублей статей",
        status=status,
        detail=(
            f"{summary.get('journal')}: всего {summary.get('total')}, "
            f"архив {summary.get('archived')}, dry-run {summary.get('dry_run_hits')}, "
            f"published {summary.get('skipped_published')}, "
            f"ошибки {summary.get('errors')} ({elapsed:.1f} с)"
        ),
        url=url_for("article_duplicates.duplicates_page"),
    )

    if wants_json:
        results = list(summary.get("results") or [])
        first = results[0] if results else {}
        return jsonify(
            {
                "ok": summary.get("errors", 0) == 0,
                "dry_run": dry_run,
                "elapsed": round(elapsed, 2),
                "archived": summary.get("archived", 0),
                "errors": summary.get("errors", 0),
                "result": first,
                "results": results,
            }
        )

    if dry_run:
        flash(
            f"Проверка архивации: к записи {summary.get('dry_run_hits')}, "
            f"опубликованные пропущены {summary.get('skipped_published')}, "
            f"ошибок {summary.get('errors')}.",
            "success" if summary.get("errors", 0) == 0 else "warning",
        )
    else:
        flash(
            f"Заархивировано {summary.get('archived')}, "
            f"пропущено published {summary.get('skipped_published')}, "
            f"ошибок {summary.get('errors')}.",
            "success" if summary.get("errors", 0) == 0 else "warning",
        )
    return render_template(
        "article_duplicates_archive_result.html",
        summary=summary,
        elapsed=elapsed,
    )
