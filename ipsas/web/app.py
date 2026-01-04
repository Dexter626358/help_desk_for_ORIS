"""Создание Flask приложения."""

from flask import Flask
from ipsas.config.settings import get_settings
from ipsas.database import db
from ipsas.utils.logger import setup_logger
from ipsas.web.auth import login_manager


def create_app() -> Flask:
    """
    Создание и настройка Flask приложения.

    Returns:
        Настроенное Flask приложение
    """
    app = Flask(__name__)
    settings = get_settings()

    # Конфигурация
    app.config["SECRET_KEY"] = settings.secret_key
    app.config["SQLALCHEMY_DATABASE_URI"] = settings.database_uri
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # Инициализация расширений
    db.init_app(app)
    login_manager.init_app(app)

    # Настройка логирования
    logger = setup_logger(
        log_file=settings.log_file,
        log_level=settings.log_level
    )
    app.logger = logger

    # Регистрация роутов
    from ipsas.web.routes import main_bp, admin_bp
    from ipsas.web.auth import auth_bp
    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(admin_bp, url_prefix="/admin")

    # Инициализация базы данных
    from ipsas.models.user import init_db
    init_db(app)

    return app

