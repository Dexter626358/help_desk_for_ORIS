"""Модуль аутентификации."""

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user, LoginManager
from ipsas.models.user import User
from ipsas.database import db
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

# Инициализация LoginManager
login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = "Пожалуйста, войдите в систему для доступа к этой странице."
login_manager.login_message_category = "info"


@login_manager.user_loader
def load_user(user_id: str):
    """
    Загрузка пользователя для Flask-Login.

    Args:
        user_id: ID пользователя

    Returns:
        Объект пользователя или None
    """
    return User.query.get(int(user_id))


# Создание Blueprint для аутентификации
auth_bp = Blueprint("auth", __name__, template_folder="templates")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """Страница входа в систему."""
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("Пожалуйста, введите логин и пароль", "error")
            return render_template("login.html")

        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            if not user.is_active:
                flash("Ваш аккаунт деактивирован. Обратитесь к администратору.", "error")
                return render_template("login.html")

            login_user(user, remember=True)
            user.update_last_login()
            logger.info(f"Пользователь {username} вошел в систему")
            
            # Проверка необходимости смены пароля
            if user.must_change_password:
                flash("Требуется смена пароля", "info")
                return redirect(url_for("auth.change_password"))
            
            flash("Вы успешно вошли в систему", "success")
            next_page = request.args.get("next")
            return redirect(next_page or url_for("main.dashboard"))
        else:
            logger.warning(f"Неудачная попытка входа для пользователя: {username}")
            flash("Неверный логин или пароль", "error")

    return render_template("login.html")


@auth_bp.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    """Смена пароля пользователя."""
    if request.method == "POST":
        current_password = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        # Проверка текущего пароля (если требуется)
        if not current_user.must_change_password:
            if not current_user.check_password(current_password):
                flash("Неверный текущий пароль", "error")
                return render_template("change_password.html", must_change=current_user.must_change_password)

        # Валидация нового пароля
        if not new_password:
            flash("Новый пароль не может быть пустым", "error")
            return render_template("change_password.html", must_change=current_user.must_change_password)

        if new_password != confirm_password:
            flash("Пароли не совпадают", "error")
            return render_template("change_password.html", must_change=current_user.must_change_password)

        # Установка нового пароля
        current_user.set_password(new_password)
        current_user.must_change_password = False  # Сбрасываем флаг
        db.session.commit()

        logger.info(f"Пользователь {current_user.username} сменил пароль")
        flash("Пароль успешно изменен", "success")
        return redirect(url_for("main.dashboard"))

    return render_template("change_password.html", must_change=current_user.must_change_password)


@auth_bp.route("/logout")
@login_required
def logout():
    """Выход из системы."""
    username = current_user.username
    logout_user()
    logger.info(f"Пользователь {username} вышел из системы")
    flash("Вы вышли из системы", "info")
    return redirect(url_for("auth.login"))

