"""Создание Flask приложения."""

from __future__ import annotations

import uuid

from flask import Flask, flash, g, jsonify, redirect, request, url_for
from ipsas.common.exceptions import IpsasError, InvalidUploadError, UnsafeRemoteUrlError
from ipsas.common.rate_limit import RequestGuard
from ipsas.config.settings import get_settings
from ipsas.utils.logger import setup_logger


def create_app() -> Flask:
    """Создание и настройка Flask приложения."""
    app = Flask(
        __name__,
        static_folder="static",
        template_folder="templates",
    )
    settings = get_settings()

    app.config["SECRET_KEY"] = settings.secret_key
    app.config["MAX_CONTENT_LENGTH"] = settings.max_content_length

    logger = setup_logger(
        log_file=settings.log_file,
        log_level=settings.log_level,
    )
    app.logger = logger

    from ipsas.web.routes import main_bp
    from ipsas.web.xml_validation import xml_validation_bp
    from ipsas.web.xml_report import xml_report_bp
    from ipsas.web.reference_processing import reference_processing_bp
    from ipsas.web.reference_cleaning import reference_cleaning_bp
    from ipsas.web.pdf_matching import pdf_matching_bp
    from ipsas.web.reference_formatting import reference_formatting_bp
    from ipsas.web.issue_metadata import issue_metadata_bp
    from ipsas.web.issue_pdf_csv import issue_pdf_csv_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(xml_validation_bp, url_prefix="/services")
    app.register_blueprint(xml_report_bp, url_prefix="/services")
    app.register_blueprint(reference_processing_bp, url_prefix="/services")
    app.register_blueprint(reference_cleaning_bp, url_prefix="/services")
    app.register_blueprint(pdf_matching_bp, url_prefix="/services")
    app.register_blueprint(reference_formatting_bp, url_prefix="/services")
    app.register_blueprint(issue_metadata_bp, url_prefix="/services")
    app.register_blueprint(issue_pdf_csv_bp, url_prefix="/services")

    guard = RequestGuard(
        max_concurrent=settings.max_concurrent_jobs,
        rate_limit=settings.rate_limit_per_minute,
    )
    app.extensions["request_guard"] = guard

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
        client = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
        client_key = client.split(",")[0].strip()
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

    @app.get("/health/live")
    def health_live():
        return jsonify({"status": "ok"}), 200

    @app.get("/health/ready")
    def health_ready():
        ready = settings.temp_dir.exists() and settings.schemas_dir.exists()
        code = 200 if ready else 503
        return jsonify(
            {
                "status": "ok" if ready else "not_ready",
                "temp_dir": str(settings.temp_dir),
                "schemas_dir": str(settings.schemas_dir),
                "environment": settings.environment,
            }
        ), code

    @app.errorhandler(InvalidUploadError)
    def handle_invalid_upload(error: InvalidUploadError):
        flash(str(error), "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=400)

    @app.errorhandler(UnsafeRemoteUrlError)
    def handle_unsafe_url(error: UnsafeRemoteUrlError):
        flash(str(error), "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=400)

    @app.errorhandler(IpsasError)
    def handle_ipsas_error(error: IpsasError):
        rid = getattr(g, "request_id", "-")
        logger.error("request_id=%s ipsas_error=%s", rid, error)
        flash(str(error), "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=400)

    @app.errorhandler(413)
    def handle_too_large(_error):
        flash("Файл слишком большой для загрузки.", "error")
        return redirect(request.referrer or url_for("main.dashboard"), code=413)

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

        return {
            "max_file_size": settings.max_file_size,
            "app_version": "0.1.0",
            "editorial_board_url": settings.editorial_board_url,
            "app_name": "IPSAS",
            "request_id": getattr(g, "request_id", None),
            "standalone_css": Markup(load_standalone_css()),
        }

    return app
