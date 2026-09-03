"""Точка входа Flask-приложения «Редактор XML»."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from flask import Flask, render_template
from flask_wtf import CSRFProtect
from flask_wtf.csrf import generate_csrf

# Корень сервиса в PYTHONPATH
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from xml_editor.utils import cleanup_old_uploads, ensure_uploads_dir


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(ROOT / "templates"),
        static_folder=str(ROOT / "static"),
    )
    app.config["SECRET_KEY"] = config.SECRET_KEY
    app.config["MAX_CONTENT_LENGTH"] = config.MAX_CONTENT_LENGTH
    app.config["WTF_CSRF_TIME_LIMIT"] = None

    csrf = CSRFProtect(app)

    from xml_editor.routes import bp

    app.register_blueprint(bp)

    @app.context_processor
    def inject_csrf():
        return {"csrf_token": generate_csrf}

    @app.errorhandler(413)
    def too_large(_err):
        max_mb = app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024)
        return (
            render_template(
                "error.html",
                title="Файл слишком большой",
                message=f"Максимальный размер загрузки — {max_mb} МБ.",
            ),
            413,
        )

    @app.errorhandler(404)
    def not_found(_err):
        return (
            render_template(
                "error.html",
                title="Страница не найдена",
                message="Запрошенный адрес не существует.",
            ),
            404,
        )

    @app.cli.command("cleanup-uploads")
    def cleanup_uploads_cmd():
        """Удалить временные сессии старше TTL."""
        n = cleanup_old_uploads()
        print(f"Удалено каталогов: {n}")

    ensure_uploads_dir()
    cleanup_old_uploads()
    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5055"))
    app.run(host="127.0.0.1", port=port, debug=True)
