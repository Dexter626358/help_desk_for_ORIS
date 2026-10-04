"""Веб-сервис: обновление англоязычных метаданных (ZIP PDF+JSON)."""

from __future__ import annotations

import uuid
from datetime import datetime

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

from ipsas.common.zip_safe import UnsafeZipError
from ipsas.config.settings import get_settings
from ipsas.modules.eng_metadata.forms import metadata_to_form_defaults
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError
from ipsas.modules.eng_metadata.session import (
    get_article_dir,
    is_safe_article_id,
    is_safe_session_id,
    load_article_json,
    load_manifest,
)
from ipsas.services.eng_metadata_review import (
    apply_platform_update,
    execute_upload,
    prepare_platform_dry_run,
    save_article_edits,
)
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation

logger = get_logger(__name__)

eng_metadata_bp = Blueprint("eng_metadata", __name__, template_folder="templates")


@eng_metadata_bp.route("/eng-metadata")
def upload_page():
    settings = get_settings()
    return render_template(
        "eng_metadata_upload.html",
        platform_apply_enabled=settings.platform_apply_enabled,
    )


@eng_metadata_bp.route("/eng-metadata/upload", methods=["POST"])
def upload_archive():
    settings = get_settings()
    if "zip_file" not in request.files:
        flash("Файл не был загружен", "error")
        return redirect(url_for("eng_metadata.upload_page"))

    file = request.files["zip_file"]
    if not file or not file.filename:
        flash("Файл не выбран", "error")
        return redirect(url_for("eng_metadata.upload_page"))

    if not file.filename.lower().endswith(".zip"):
        flash("Поддерживаются только ZIP-архивы", "error")
        return redirect(url_for("eng_metadata.upload_page"))

    original = secure_filename(file.filename) or "archive.zip"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    temp_zip = settings.temp_dir / f"{stamp}_{uid}_{original}"

    try:
        file.save(str(temp_zip))
        manifest = execute_upload(temp_zip, source_name=original)
        record_operation(
            tool="eng_metadata",
            title="Загрузка ENG ZIP",
            status="ok",
            detail=f"{manifest.get('ok_count')}/{manifest.get('total')} статей OK",
            url=url_for(
                "eng_metadata.session_list",
                session_id=manifest["session_id"],
            ),
        )
        flash(
            f"Архив разобран: {manifest.get('ok_count')} из {manifest.get('total')} "
            "статей без ошибок комплекта.",
            "success" if manifest.get("ok_count") else "warning",
        )
        return redirect(
            url_for("eng_metadata.session_list", session_id=manifest["session_id"])
        )
    except UnsafeZipError as exc:
        logger.warning("Небезопасный ZIP ENG metadata: %s", exc)
        flash("Архив отклонён политикой безопасности.", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    except ValueError as exc:
        logger.warning("ENG ZIP: %s", exc)
        flash(str(exc) or "Не удалось обработать архив.", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    except OSError as exc:
        logger.error("Ошибка загрузки ENG ZIP: %s", exc, exc_info=True)
        flash("Не удалось обработать архив.", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    finally:
        try:
            temp_zip.unlink(missing_ok=True)
        except OSError:
            pass


@eng_metadata_bp.route("/eng-metadata/session/<session_id>")
def session_list(session_id: str):
    if not is_safe_session_id(session_id):
        flash("Сессия не найдена", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    try:
        manifest = load_manifest(session_id)
    except (FileNotFoundError, ValueError, OSError):
        flash("Сессия не найдена или устарела", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    return render_template(
        "eng_metadata_list.html",
        manifest=manifest,
        session_id=session_id,
    )


@eng_metadata_bp.route(
    "/eng-metadata/session/<session_id>/article/<article_id>"
)
def review_article(session_id: str, article_id: str):
    if not is_safe_session_id(session_id) or not is_safe_article_id(article_id):
        flash("Статья не найдена", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    try:
        manifest = load_manifest(session_id)
        data = load_article_json(session_id, article_id)
        article_dir = get_article_dir(session_id, article_id)
    except (FileNotFoundError, ValueError, OSError):
        flash("Статья не найдена в сессии", "error")
        return redirect(url_for("eng_metadata.upload_page"))

    entry = next(
        (
            a
            for a in (manifest.get("articles") or [])
            if str(a.get("article_id")) == str(article_id)
        ),
        {},
    )
    pdf_ok = (article_dir / "article.pdf").is_file()
    form = metadata_to_form_defaults(data)
    settings = get_settings()
    return render_template(
        "eng_metadata_review.html",
        session_id=session_id,
        article_id=article_id,
        entry=entry,
        form=form,
        pdf_ok=pdf_ok,
        dry_run=None,
        platform_apply_enabled=settings.platform_apply_enabled,
    )


@eng_metadata_bp.route(
    "/eng-metadata/session/<session_id>/article/<article_id>/pdf"
)
def article_pdf(session_id: str, article_id: str):
    if not is_safe_session_id(session_id) or not is_safe_article_id(article_id):
        flash("PDF не найден", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    try:
        pdf_path = get_article_dir(session_id, article_id) / "article.pdf"
        if not pdf_path.is_file():
            flash("PDF не найден", "error")
            return redirect(
                url_for(
                    "eng_metadata.review_article",
                    session_id=session_id,
                    article_id=article_id,
                )
            )
        return send_file(
            pdf_path,
            mimetype="application/pdf",
            download_name=f"article_{article_id}.pdf",
            as_attachment=False,
        )
    except (FileNotFoundError, ValueError, OSError):
        flash("PDF не найден", "error")
        return redirect(url_for("eng_metadata.upload_page"))


@eng_metadata_bp.route(
    "/eng-metadata/session/<session_id>/article/<article_id>/save",
    methods=["POST"],
)
def save_article(session_id: str, article_id: str):
    if not is_safe_session_id(session_id) or not is_safe_article_id(article_id):
        flash("Статья не найдена", "error")
        return redirect(url_for("eng_metadata.upload_page"))

    intent = (request.form.get("form_action") or "save").strip().lower()
    review_url = url_for(
        "eng_metadata.review_article",
        session_id=session_id,
        article_id=article_id,
    )

    try:
        if "title_eng" not in request.form and "abstract_eng" not in request.form:
            logger.error(
                "eng_metadata empty form session=%s article=%s keys=%s",
                session_id,
                article_id,
                list(request.form.keys())[:40],
            )
            flash(
                "Форма не передала поля редактирования. "
                "Обновите страницу (Ctrl+F5) и повторите.",
                "error",
            )
            return redirect(review_url)

        saved = save_article_edits(session_id, article_id, request.form)
        logger.info(
            "eng_metadata saved session=%s article=%s intent=%s title=%r refs=%s",
            session_id,
            article_id,
            intent,
            (saved.get("title_eng") or "")[:80],
            len(saved.get("references_eng") or []),
        )
    except ValueError as exc:
        flash(f"Не удалось сохранить: {exc}", "error")
        return redirect(review_url)
    except (FileNotFoundError, OSError) as exc:
        logger.error("save eng metadata: %s", exc, exc_info=True)
        flash("Не удалось сохранить метаданные", "error")
        return redirect(review_url)

    if intent == "prepare":
        return _run_prepare_send(session_id, article_id)

    flash("Метаданные сохранены", "success")
    return redirect(review_url)


@eng_metadata_bp.route(
    "/eng-metadata/session/<session_id>/article/<article_id>/prepare",
    methods=["POST"],
)
def prepare_send(session_id: str, article_id: str):
    """Совместимость: прямой POST /prepare тоже сначала сохраняет форму."""
    if not is_safe_session_id(session_id) or not is_safe_article_id(article_id):
        flash("Статья не найдена", "error")
        return redirect(url_for("eng_metadata.upload_page"))
    review_url = url_for(
        "eng_metadata.review_article",
        session_id=session_id,
        article_id=article_id,
    )
    try:
        if "title_eng" in request.form or "abstract_eng" in request.form:
            saved = save_article_edits(session_id, article_id, request.form)
            logger.info(
                "eng_metadata saved(via prepare) session=%s article=%s title=%r",
                session_id,
                article_id,
                (saved.get("title_eng") or "")[:80],
            )
        else:
            logger.warning(
                "prepare without editable fields session=%s article=%s keys=%s",
                session_id,
                article_id,
                list(request.form.keys())[:40],
            )
        return _run_prepare_send(session_id, article_id)
    except ValueError as exc:
        flash(f"Не удалось подготовить/отправить: {exc}", "error")
        return redirect(review_url)
    except PlatformAuthError as exc:
        flash(
            f"Не удалось войти на платформу: {exc}. "
            "Проверьте PLATFORM_USERNAME / PLATFORM_PASSWORD.",
            "error",
        )
        return redirect(review_url)
    except (FileNotFoundError, OSError) as exc:
        logger.error("prepare/apply eng metadata: %s", exc, exc_info=True)
        flash("Не удалось подготовить отправку", "error")
        return redirect(url_for("eng_metadata.upload_page"))


def _run_prepare_send(session_id: str, article_id: str):
    settings = get_settings()
    review_url = url_for(
        "eng_metadata.review_article",
        session_id=session_id,
        article_id=article_id,
    )
    try:
        summary = (
            apply_platform_update(session_id, article_id)
            if settings.platform_apply_enabled
            else prepare_platform_dry_run(session_id, article_id)
        )
        manifest = load_manifest(session_id)
        data = load_article_json(session_id, article_id)
        article_dir = get_article_dir(session_id, article_id)
        entry = next(
            (
                a
                for a in (manifest.get("articles") or [])
                if str(a.get("article_id")) == str(article_id)
            ),
            {},
        )
        apply_info = summary.get("apply") if isinstance(summary, dict) else None
        if settings.platform_apply_enabled:
            errs = (apply_info or {}).get("errors") or []
            if apply_info and apply_info.get("ok"):
                base = (
                    f"Отправлено на платформу: {apply_info.get('message') or 'OK'}. "
                    f"Полей: {len(apply_info.get('applied_fields') or [])}."
                )
                if errs:
                    flash(
                        base + " Внимание: " + "; ".join(str(e) for e in errs[:3]),
                        "warning",
                    )
                else:
                    flash(base, "success")
            else:
                flash(
                    "Отправка завершилась с ошибками: "
                    + ("; ".join(str(e) for e in errs[:5]) or "см. лог"),
                    "error",
                )
        else:
            flash(
                "Данные подготовлены локально. На платформу ничего не отправлялось "
                "(PLATFORM_APPLY_ENABLED=0).",
                "success",
            )
        return render_template(
            "eng_metadata_review.html",
            session_id=session_id,
            article_id=article_id,
            entry=entry,
            form=metadata_to_form_defaults(data),
            pdf_ok=(article_dir / "article.pdf").is_file(),
            dry_run=summary,
            platform_apply_enabled=settings.platform_apply_enabled,
        )
    except ValueError as exc:
        flash(f"Не удалось подготовить/отправить: {exc}", "error")
        return redirect(review_url)
    except PlatformAuthError as exc:
        flash(
            f"Не удалось войти на платформу: {exc}. "
            "Проверьте PLATFORM_USERNAME / PLATFORM_PASSWORD.",
            "error",
        )
        return redirect(review_url)
    except (FileNotFoundError, OSError) as exc:
        logger.error("prepare/apply eng metadata: %s", exc, exc_info=True)
        flash("Не удалось подготовить отправку", "error")
        return redirect(url_for("eng_metadata.upload_page"))
