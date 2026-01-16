"""Модель пользователя."""

from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from ipsas.database import db


class User(UserMixin, db.Model):
    """Модель пользователя системы."""

    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=True)
    password_hash = db.Column(db.String(255), nullable=False)
    last_name = db.Column(db.String(100), nullable=True)  # Фамилия
    first_name = db.Column(db.String(100), nullable=True)  # Имя
    middle_name = db.Column(db.String(100), nullable=True)  # Отчество
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    must_change_password = db.Column(db.Boolean, default=False, nullable=False)  # Требуется смена пароля
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    last_login = db.Column(db.DateTime, nullable=True)

    def set_password(self, password: str) -> None:
        """
        Установить пароль пользователя.

        Args:
            password: Пароль в открытом виде
        """
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        """
        Проверить пароль пользователя.

        Args:
            password: Пароль для проверки

        Returns:
            True если пароль верный
        """
        return check_password_hash(self.password_hash, password)

    def update_last_login(self) -> None:
        """Обновить время последнего входа."""
        self.last_login = datetime.utcnow()
        db.session.commit()

    def get_full_name(self) -> str:
        """
        Получить полное ФИО пользователя.

        Returns:
            Полное имя в формате "Фамилия Имя Отчество" или логин, если ФИО не указано
        """
        parts = []
        if self.last_name:
            parts.append(self.last_name)
        if self.first_name:
            parts.append(self.first_name)
        if self.middle_name:
            parts.append(self.middle_name)
        
        if parts:
            return " ".join(parts)
        return self.username

    def __repr__(self) -> str:
        return f"<User {self.username}>"


def init_db(app):
    """
    Инициализация базы данных.

    Args:
        app: Flask приложение
    """
    with app.app_context():
        # Создаем таблицы, если их нет (не удаляем существующие!)
        # Это безопасно для production - не удаляет существующие данные
        db.create_all()

        # Создание администратора по умолчанию, если его нет
        try:
            admin = User.query.filter_by(username="admin").first()
            if not admin:
                admin = User(
                    username="admin",
                    email="admin@ipsas.local",
                    is_admin=True,
                    is_active=True
                )
                admin.set_password("admin123")  # Пароль по умолчанию - нужно изменить!
                db.session.add(admin)
                db.session.commit()
                app.logger.info("Создан администратор по умолчанию: admin/admin123")
            else:
                app.logger.info("Администратор уже существует в базе данных")
        except Exception as e:
            # Если произошла ошибка (например, пользователь уже существует),
            # откатываем транзакцию и логируем
            db.session.rollback()
            app.logger.warning(f"Не удалось создать администратора (возможно, уже существует): {e}")
            # Проверяем еще раз после rollback
            try:
                admin = User.query.filter_by(username="admin").first()
                if admin:
                    app.logger.info("Администратор существует в базе данных")
            except Exception:
                pass

