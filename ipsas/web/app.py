"""Создание Flask приложения."""

from __future__ import annotations

import os
import uuid

from flask import Flask, flash, g, jsonify, redirect, request, url_for
from ipsas.common.exceptions import IpsasError, InvalidUploadError, UnsafeRemoteUrlError
from ipsas.common.rate_limit import RequestGuard
from ipsas.config.settings import get_settings
from ipsas.utils.logger import setup_logger


def create_app(*, testing: bool = False) -> Flask:
    """Создание и настройка Flask приложения."""
    app = Flask(
        __name__,
        static_folder="static",
        template_folder="templates",
    )
    if testing:
        app.config["TESTING"] = True
        os.environ.setdefault("IPSAS_ENV", "development")

    settings = get_settings()

    app.config["SECRET_KEY"] = settings.secret_key
    app.config["MAX_CONTENT_LENGTH"] = settings.max_content_length
    app.config["SESSION_COOKIE_SECURE"] = settings.session_cookie_secure
    app.config["SESSION_COOKIE_HTTPONLY"] = settings.session_cookie_httponly
    app.config["SESSION_COOKIE_SAMESITE"] = settings.session_cookie_samesite

    csrf_disabled_env = os.getenv("IPSAS_DISABLE_CSRF", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if settings.is_production and csrf_disabled_env and not testing:
        raise RuntimeError("IPSAS_DISABLE_CSRF is not allowed in production")

    app.config["WTF_CSRF_ENABLED"] = not (
        testing
        or app.config.get("TESTING")
        or csrf_disabled_env
    )
    app.config["WTF_CSRF_TIME_LIMIT"] = None

    logger = setup_logger(
        log_file=settings.log_file,
        log_level=settings.log_level,
    )
    app.logger = logger

    try:
        from flask_wtf.csrf import CSRFProtect

        csrf = CSRFProtect(app)
        app.extensions["csrf"] = csrf
    except ImportError:
        if settings.is_production and not testing:
            raise RuntimeError("Flask-WTF is required in production for CSRF protection")
        logger.warning("Flask-WTF not installed; CSRF protection disabled")

    from ipsas.web.routes import main_bp
    from ipsas.web.xml_validation import xml_validation_bp
    from ipsas.web.xml_report import xml_report_bp
    from ipsas.web.reference_processing import reference_processing_bp
    from ipsas.web.reference_cleaning import reference_cleaning_bp
    from ipsas.web.pdf_matching import pdf_matching_bp
    from ipsas.web.reference_formatting import reference_formatting_bp
    from ipsas.web.issue_metadata import issue_metadata_bp
    from ipsas.web.issue_pdf_csv import issue_pdf_csv_bp
    from ipsas.web.journal_site_check import journal_site_check_bp
    from ipsas.web.xml_editor import xml_editor_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(xml_validation_bp, url_prefix="/services")
    app.register_blueprint(xml_report_bp, url_prefix="/services")
    app.register_blueprint(reference_processing_bp, url_prefix="/services")
    app.register_blueprint(reference_cleaning_bp, url_prefix="/services")
    app.register_blueprint(pdf_matching_bp, url_prefix="/services")
    app.register_blueprint(reference_formatting_bp, url_prefix="/services")
    app.register_blueprint(issue_metadata_bp, url_prefix="/services")
    app.register_blueprint(issue_pdf_csv_bp, url_prefix="/services")
    app.register_blueprint(journal_site_check_bp, url_prefix="/services")
    app.register_blueprint(xml_editor_bp, url_prefix="/services/xml-editor")

    guard = RequestGuard(
        max_concurrent=settings.max_concurrent_jobs,
        rate_limit=settings.rate_limit_per_minute,
    )
    app.extensions["request_guard"] = guard

    if not testing:
        from ipsas.jobs.issue_metadata import interrupt_stale_running_tasks

        interrupt_stale_running_tasks()

    @app.before_request
    def _guard_service_posts():
        g.request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
        g._guard_held = False
        if request.method != "POST":
            return None
        path = request.path or ""
        if not path.startswith("/services/"):
            return None
        # Лёгкие API статуса не считаем «тяжёлой» операцией
        if path.rstrip("/").endswith("/status") or "/download/" in path:
            return None
        if settings.trust_proxy_headers:
            client = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
            client_key = client.split(",")[0].strip()
        else:
            client_key = request.remote_addr or "unknown"
        decision = guard.try_acquire(client_key)
        if not decision.allowed:
            logger.warning(
                "request_id=%s blocked: %s", g.request_id, decision.reason
            )
            if request.accept_mimetypes.best == "application/json" or path.endswith(".json"):
                return (
                    jsonify({"error": decision.reason, "request_id": g.request_id}),
                    429,
                    {"Retry-After": str(decision.retry_after_s or 30)},
                )
            flash(decision.reason, "error")
            return redirect(request.referrer or url_for("main.dashboard"), code=429)
        g._guard_held = True
        return None

    @app.teardown_request
    def _release_guard(_exc=None):
        if getattr(g, "_guard_held", False):
            guard.release()
            g._guard_held = False

    @app.after_request
    def _security_headers(response):
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        response.headers.setdefault(
            "Permissions-Policy", "geolocation=(), microphone=(), camera=()"
        )
        return response

    @app.get("/health/live")
    def health_live():
        return jsonify({"status": "ok", "service": "ipsas"}), 200

    @app.get("/health/ready")
    def health_ready():
        temp_ok = False
        try:
            settings.temp_dir.mkdir(parents=True, exist_ok=True)
            probe = settings.temp_dir / f".ready-{uuid.uuid4().hex}"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            temp_ok = True
        except OSError:
            temp_ok = False
        xsd_ok = (settings.schemas_dir / "journal3.xsd").is_file()
        ready = temp_ok and xsd_ok
        code = 200 if ready else 503
        return jsonify(
            {
                "status": "ok" if ready else "not_ready",
                "service": "ipsas",
                "temp_ok": temp_ok,
                "schemas_ok": xsd_ok,
            }
        ), code

    @app.errorhandler(InvalidUploadError)
    def handle_invalid_upload(error: InvalidUploadError):
        flash("Некорректный файл для загрузки.", "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=400)

    @app.errorhandler(UnsafeRemoteUrlError)
    def handle_unsafe_url(error: UnsafeRemoteUrlError):
        flash("Указанный URL отклонён политикой безопасности.", "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=400)

    @app.errorhandler(IpsasError)
    def handle_ipsas_error(error: IpsasError):
        rid = getattr(g, "request_id", "-")
        logger.error("request_id=%s ipsas_error=%s", rid, error)
        flash("Ошибка обработки запроса. Повторите попытку или обратитесь к администратору.", "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=400)

    @app.errorhandler(413)
    def handle_too_large(_error):
        flash("Файл слишком большой для загрузки.", "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=413)

    @app.errorhandler(400)
    def handle_bad_request(error):
        # CSRF failures arrive as 400 from Flask-WTF
        description = getattr(error, "description", "") or ""
        if "CSRF" in str(description) or "csrf" in str(description).lower():
            flash("Сессия формы устарела. Обновите страницу и повторите действие.", "error")
            return redirect(request.referrer or url_for("main.dashboard"), code=400)
        return error

    @app.errorhandler(403)
    def handle_forbidden(_error):
        flash("Доступ запрещён.", "error")
        return redirect(url_for("main.dashboard"), code=403)

    @app.errorhandler(404)
    def handle_not_found(_error):
        flash("Страница не найдена.", "error")
        return redirect(url_for("main.dashboard"), code=404)

    @app.errorhandler(Exception)
    def handle_unexpected(error: Exception):
        rid = getattr(g, "request_id", uuid.uuid4().hex[:12])
        from werkzeug.exceptions import HTTPException

        if isinstance(error, HTTPException):
            return error
        logger.exception("request_id=%s unexpected_error=%s", rid, error)
        flash(f"Внутренняя ошибка. Код обращения: {rid}", "error")
        return redirect(url_for("main.dashboard"), code=500)

    @app.context_processor
    def inject_ui_globals():
        from markupsafe import Markup
        from ipsas.modules.journal_xml.report.standalone import load_standalone_css

        def csrf_field() -> Markup:
            if not app.config.get("WTF_CSRF_ENABLED"):
                return Markup("")
            try:
                from flask_wtf.csrf import generate_csrf

                token = generate_csrf()
            except Exception:
                return Markup("")
            return Markup(
                f'<input type="hidden" name="csrf_token" value="{token}">'
            )

        return {
            "max_file_size": settings.max_file_size,
            "app_version": "0.1.0",
            "editorial_board_url": settings.editorial_board_url,
            "app_name": "IPSAS",
            "request_id": getattr(g, "request_id", None),
            "standalone_css": Markup(load_standalone_css()),
            "csrf_field": csrf_field,
        }

    return app
