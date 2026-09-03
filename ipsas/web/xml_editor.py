"""Маршруты Flask для редактора XML."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.utils import secure_filename

from ipsas.config.settings import get_settings
from ipsas.modules.xml_editor.editor import (
    add_author,
    delete_author,
    move_author,
    restore_all_from_original,
    restore_article_from_original,
    update_article_from_form,
)
from ipsas.modules.xml_editor.parser import get_issue_summary, list_articles, parse_article
from ipsas.modules.xml_editor.utils import (
    cleanup_old_uploads,
    edited_path,
    ensure_uploads_dir,
    is_safe_article_id,
    is_safe_session_id,
    meta_path,
    new_session_id,
    original_path,
    parse_xml_bytes,
    parse_xml_file,
    save_tree,
    session_dir,
)
from ipsas.modules.xml_editor.validator import (
    summarize_issues,
    validate_all_articles,
    validate_article_data,
    validate_tree_has_articles,
)

xml_editor_bp = Blueprint("xml_editor", __name__, template_folder="templates")


def _load_meta(session_id: str) -> dict[str, Any]:
    path = meta_path(session_id)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_meta(session_id: str, meta: dict[str, Any]) -> None:
    meta_path(session_id).write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _require_session(session_id: str) -> Path:
    if not is_safe_session_id(session_id):
        raise PermissionError("Некорректный идентификатор сессии")
    path = session_dir(session_id)
    if not path.is_dir() or not edited_path(session_id).exists():
        raise FileNotFoundError("Сессия не найдена или истекла")
    return path


def _load_edited(session_id: str):
    return parse_xml_file(edited_path(session_id))


def _articles_with_issues(tree) -> list[dict[str, Any]]:
    items = list_articles(tree)
    all_issues = validate_all_articles(tree)
    for item in items:
        issues = all_issues.get(item["id"], [])
        summary = summarize_issues(issues)
        item["issues"] = issues
        item["issue_summary"] = summary
        item["has_errors"] = summary["errors"] > 0
        item["has_warnings"] = summary["warnings"] > 0
    return items


def _parse_authors_from_request() -> list[dict[str, Any]]:
    """Собирает авторов из полей authors-<i>-RUS-surname и authors-<i>-orcid."""
    indices: set[int] = set()
    for key in request.form:
        if not key.startswith("authors-"):
            continue
        parts = key.split("-")
        if len(parts) < 2:
            continue
        try:
            indices.add(int(parts[1]))
        except ValueError:
            continue
    authors: list[dict[str, Any]] = []
    for i in sorted(indices):
        block: dict[str, Any] = {
            "id": str(i),
            "orcid": request.form.get(f"authors-{i}-orcid", ""),
            "RUS": {},
            "ENG": {},
        }
        for lang in ("RUS", "ENG"):
            for field in (
                "surname",
                "initials",
                "first_name",
                "middle_name",
                "org_name",
                "address",
                "email",
            ):
                block[lang][field] = request.form.get(f"authors-{i}-{lang}-{field}", "")
        authors.append(block)
    return authors


def _parse_references_from_request() -> list[dict[str, Any]]:
    indices: set[int] = set()
    for key in request.form:
        if key.startswith("ref-") and key.endswith("-text"):
            try:
                indices.add(int(key.split("-")[1]))
            except ValueError:
                continue
    refs: list[dict[str, Any]] = []
    for i in sorted(indices):
        text = request.form.get(f"ref-{i}-text", "")
        lang = request.form.get(f"ref-{i}-lang", "UNK")
        # Пустые пропуская только если явно удалены — оставляем все ключи формы
        refs.append({"id": str(i), "text": text, "lang": lang})
    return refs


def _form_to_article_dict() -> dict[str, Any]:
    return {
        "title_rus": request.form.get("title_rus", ""),
        "title_eng": request.form.get("title_eng", ""),
        "doi": request.form.get("doi", ""),
        "lang": request.form.get("lang", ""),
        "art_type": request.form.get("art_type", ""),
        "section": request.form.get("section", ""),
        "udk": request.form.get("udk", ""),
        "page_first": request.form.get("page_first", ""),
        "page_last": request.form.get("page_last", ""),
        "abstract_rus": request.form.get("abstract_rus", ""),
        "abstract_eng": request.form.get("abstract_eng", ""),
        "keywords_rus": request.form.get("keywords_rus", ""),
        "keywords_eng": request.form.get("keywords_eng", ""),
        "authors": _parse_authors_from_request(),
        "references": _parse_references_from_request(),
    }


def _field_errors(issues: list[dict[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for issue in issues:
        field = issue.get("field")
        if field and field not in out:
            out[field] = issue.get("text") or ""
    return out


@xml_editor_bp.get("/")
def index():
    cleanup_old_uploads()
    max_mb = get_settings().max_content_length // (1024 * 1024)
    return render_template("xml_editor_upload.html", max_mb=max_mb)


@xml_editor_bp.post("/upload")
def upload():
    cleanup_old_uploads()
    file = request.files.get("xml_file")
    if file is None or not file.filename:
        flash("Выберите XML-файл", "danger")
        return redirect(url_for("xml_editor.index"))

    filename = secure_filename(file.filename)
    if not filename.lower().endswith(".xml"):
        flash("Разрешены только файлы с расширением .xml", "danger")
        return redirect(url_for("xml_editor.index"))

    data = file.read()
    if not data:
        flash("Файл пуст", "danger")
        return redirect(url_for("xml_editor.index"))

    try:
        tree = parse_xml_bytes(data)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))

    structural = validate_tree_has_articles(tree)
    if structural:
        flash(structural[0]["text"], "danger")
        return redirect(url_for("xml_editor.index"))

    ensure_uploads_dir()
    sid = new_session_id()
    sdir = session_dir(sid)
    sdir.mkdir(parents=True, exist_ok=True)
    original_path(sid).write_bytes(data)
    save_tree(tree, edited_path(sid))
    _save_meta(
        sid,
        {
            "original_filename": filename,
            "created": True,
        },
    )
    session["xml_editor_session_id"] = sid
    flash("Файл загружен", "success")
    return redirect(url_for("xml_editor.editor", session_id=sid, article_id=0))


@xml_editor_bp.get("/editor/<session_id>")
@xml_editor_bp.get("/editor/<session_id>/<article_id>")
def editor(session_id: str, article_id: str = "0"):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))

    if not is_safe_article_id(article_id):
        flash("Некорректный идентификатор статьи", "danger")
        return redirect(url_for("xml_editor.editor", session_id=session_id, article_id=0))

    tree = _load_edited(session_id)
    articles = _articles_with_issues(tree)
    if not articles:
        flash("В файле нет статей", "danger")
        return redirect(url_for("xml_editor.index"))

    idx = int(article_id)
    if idx < 0 or idx >= len(articles):
        return redirect(url_for("xml_editor.editor", session_id=session_id, article_id=0))

    article = parse_article(tree, idx)
    issues = validate_article_data(article)
    meta = _load_meta(session_id)
    issue_info = get_issue_summary(tree)

    return render_template(
        "xml_editor_editor.html",
        session_id=session_id,
        meta=meta,
        issue_info=issue_info,
        articles=articles,
        article=article,
        article_id=str(idx),
        issues=issues,
        field_errors=_field_errors(issues),
        issue_summary=summarize_issues(issues),
        active_tab=request.args.get("tab", "main"),
    )


@xml_editor_bp.post("/editor/<session_id>/<article_id>/save")
def save_article(session_id: str, article_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    if not is_safe_article_id(article_id):
        flash("Некорректный идентификатор статьи", "danger")
        return redirect(url_for("xml_editor.index"))

    form_data = _form_to_article_dict()
    issues = validate_article_data(form_data)
    # Сохраняем даже при ошибках валидации (чтобы можно было править), но предупреждаем
    tree = _load_edited(session_id)
    update_article_from_form(tree, article_id, form_data)
    save_tree(tree, edited_path(session_id))

    err_n = summarize_issues(issues)["errors"]
    if err_n:
        flash(f"Изменения сохранены, но есть ошибки валидации: {err_n}", "warning")
    else:
        flash("Изменения сохранены", "success")
    tab = request.form.get("active_tab", "main")
    return redirect(
        url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab=tab)
    )


@xml_editor_bp.post("/editor/<session_id>/<article_id>/authors/add")
def authors_add(session_id: str, article_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    if not is_safe_article_id(article_id):
        flash("Некорректный идентификатор статьи", "danger")
        return redirect(url_for("xml_editor.index"))

    # Сначала сохраняем текущую форму, затем добавляем автора
    tree = _load_edited(session_id)
    update_article_from_form(tree, article_id, _form_to_article_dict())
    add_author(tree, article_id)
    save_tree(tree, edited_path(session_id))
    flash("Автор добавлен", "success")
    return redirect(
        url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab="authors")
    )


@xml_editor_bp.post("/editor/<session_id>/<article_id>/authors/<author_id>/delete")
def authors_delete(session_id: str, article_id: str, author_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    if not is_safe_article_id(article_id) or not is_safe_article_id(author_id):
        flash("Некорректный идентификатор", "danger")
        return redirect(url_for("xml_editor.index"))

    tree = _load_edited(session_id)
    # Сохраняем форму без удаляемого автора: проще удалить из дерева после apply
    form_data = _form_to_article_dict()
    update_article_from_form(tree, article_id, form_data)
    try:
        delete_author(tree, article_id, author_id)
    except IndexError:
        flash("Автор не найден", "danger")
        return redirect(
            url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab="authors")
        )
    save_tree(tree, edited_path(session_id))
    flash("Автор удалён", "success")
    return redirect(
        url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab="authors")
    )


@xml_editor_bp.post("/editor/<session_id>/<article_id>/authors/<author_id>/move")
def authors_move(session_id: str, article_id: str, author_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    direction = request.form.get("direction", "")
    if direction not in {"up", "down"}:
        flash("Некорректное направление", "danger")
        return redirect(
            url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab="authors")
        )
    tree = _load_edited(session_id)
    update_article_from_form(tree, article_id, _form_to_article_dict())
    try:
        move_author(tree, article_id, author_id, direction)
    except IndexError:
        flash("Автор не найден", "danger")
    else:
        save_tree(tree, edited_path(session_id))
        flash("Порядок авторов обновлён", "success")
    return redirect(
        url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab="authors")
    )


@xml_editor_bp.post("/editor/<session_id>/<article_id>/restore")
def restore_article(session_id: str, article_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    if not is_safe_article_id(article_id):
        flash("Некорректный идентификатор статьи", "danger")
        return redirect(url_for("xml_editor.index"))

    edited = _load_edited(session_id)
    original = parse_xml_file(original_path(session_id))
    try:
        restore_article_from_original(edited, original, article_id)
    except IndexError:
        flash("Статья не найдена", "danger")
    else:
        save_tree(edited, edited_path(session_id))
        flash("Статья восстановлена из исходного XML", "success")
    return redirect(url_for("xml_editor.editor", session_id=session_id, article_id=article_id))


@xml_editor_bp.post("/editor/<session_id>/restore-all")
def restore_all(session_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    restore_all_from_original(edited_path(session_id), original_path(session_id))
    flash("Весь файл возвращён к исходному состоянию", "success")
    return redirect(url_for("xml_editor.editor", session_id=session_id, article_id=0))


@xml_editor_bp.get("/download/<session_id>/original")
def download_original(session_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    meta = _load_meta(session_id)
    name = meta.get("original_filename") or "original.xml"
    return send_file(
        original_path(session_id),
        as_attachment=True,
        download_name=f"original_{name}",
        mimetype="application/xml",
    )


@xml_editor_bp.get("/download/<session_id>/edited")
def download_edited(session_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    meta = _load_meta(session_id)
    name = meta.get("original_filename") or "edited.xml"
    stem = Path(name).stem
    return send_file(
        edited_path(session_id),
        as_attachment=True,
        download_name=f"{stem}_edited.xml",
        mimetype="application/xml",
    )


@xml_editor_bp.post("/validate/<session_id>")
def validate_session(session_id: str):
    try:
        _require_session(session_id)
    except (PermissionError, FileNotFoundError, ValueError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("xml_editor.index"))
    tree = _load_edited(session_id)
    all_issues = validate_all_articles(tree)
    total_err = sum(summarize_issues(v)["errors"] for v in all_issues.values())
    total_warn = sum(summarize_issues(v)["warnings"] for v in all_issues.values())
    if total_err or total_warn:
        flash(f"Проверка завершена: ошибок {total_err}, предупреждений {total_warn}", "warning")
    else:
        flash("Проверка завершена: критических замечаний нет", "success")
    article_id = request.form.get("article_id", "0")
    if not is_safe_article_id(article_id):
        article_id = "0"
    return redirect(
        url_for("xml_editor.editor", session_id=session_id, article_id=article_id, tab="validation")
    )


@xml_editor_bp.post("/close/<session_id>")
def close_session(session_id: str):
    if is_safe_session_id(session_id):
        try:
            sdir = session_dir(session_id)
            if sdir.exists():
                shutil.rmtree(sdir, ignore_errors=True)
        except ValueError:
            pass
    session.pop("xml_editor_session_id", None)
    flash("Сессия закрыта, временные файлы удалены", "success")
    return redirect(url_for("xml_editor.index"))
