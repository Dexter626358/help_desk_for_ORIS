"""Основные роуты приложения."""

from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from ipsas.models.user import User
from ipsas.database import db
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

# Основной Blueprint
main_bp = Blueprint("main", __name__, template_folder="templates")

# Админ Blueprint
admin_bp = Blueprint("admin", __name__, template_folder="templates")


@main_bp.route("/")
def index():
    """Главная страница - редирект на вход или дашборд."""
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))
    return redirect(url_for("auth.login"))


@main_bp.route("/dashboard")
@login_required
def dashboard():
    """Дашборд пользователя."""
    return render_template("dashboard.html", user=current_user)


@admin_bp.route("/")
@login_required
def admin_panel():
    """Админ-панель с поиском и фильтрами."""
    if not current_user.is_admin:
        flash("У вас нет прав доступа к этой странице", "error")
        return redirect(url_for("main.dashboard"))

    # Получение параметров фильтрации
    search_query = request.args.get("search", "").strip()
    role_filter = request.args.get("role", "")  # "admin", "user", или ""
    status_filter = request.args.get("status", "")  # "active", "inactive", или ""

    # Начальный запрос
    query = User.query

    # Поиск по логину, email или ФИО
    if search_query:
        search_pattern = f"%{search_query}%"
        query = query.filter(
            db.or_(
                User.username.like(search_pattern),
                User.email.like(search_pattern),
                User.last_name.like(search_pattern),
                User.first_name.like(search_pattern),
                User.middle_name.like(search_pattern)
            )
        )

    # Фильтр по роли
    if role_filter == "admin":
        query = query.filter(User.is_admin == True)
    elif role_filter == "user":
        query = query.filter(User.is_admin == False)

    # Фильтр по статусу
    if status_filter == "active":
        query = query.filter(User.is_active == True)
    elif status_filter == "inactive":
        query = query.filter(User.is_active == False)

    users = query.order_by(User.created_at.desc()).all()

    return render_template(
        "admin_panel.html",
        users=users,
        search_query=search_query,
        role_filter=role_filter,
        status_filter=status_filter
    )


@admin_bp.route("/users/create", methods=["GET", "POST"])
@login_required
def create_user():
    """Создание нового пользователя."""
    if not current_user.is_admin:
        flash("У вас нет прав доступа к этой странице", "error")
        return redirect(url_for("main.dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        email = request.form.get("email", "").strip() or None
        last_name = request.form.get("last_name", "").strip() or None
        first_name = request.form.get("first_name", "").strip() or None
        middle_name = request.form.get("middle_name", "").strip() or None
        is_admin = request.form.get("is_admin") == "on"

        # Валидация
        if not username or not password:
            flash("Логин и пароль обязательны", "error")
            return render_template("create_user.html")

        # Проверка существования пользователя
        if User.query.filter_by(username=username).first():
            flash("Пользователь с таким логином уже существует", "error")
            return render_template("create_user.html")

        # Создание пользователя
        user = User(
            username=username,
            email=email,
            last_name=last_name,
            first_name=first_name,
            middle_name=middle_name,
            is_admin=is_admin,
            is_active=True
        )
        user.set_password(password)
        db.session.add(user)
        db.session.commit()

        logger.info(f"Администратор {current_user.username} создал пользователя {username}")
        flash(f"Пользователь {username} успешно создан", "success")
        return redirect(url_for("admin.admin_panel"))

    return render_template("create_user.html")


@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
def delete_user(user_id: int):
    """Удаление пользователя."""
    if not current_user.is_admin:
        flash("У вас нет прав доступа к этой странице", "error")
        return redirect(url_for("main.dashboard"))

    user = User.query.get_or_404(user_id)

    # Нельзя удалить самого себя
    if user.id == current_user.id:
        flash("Вы не можете удалить свой собственный аккаунт", "error")
        return redirect(url_for("admin.admin_panel"))

    username = user.username
    db.session.delete(user)
    db.session.commit()

    logger.info(f"Администратор {current_user.username} удалил пользователя {username}")
    flash(f"Пользователь {username} удален", "success")
    return redirect(url_for("admin.admin_panel"))


@admin_bp.route("/users/<int:user_id>/toggle_active", methods=["POST"])
@login_required
def toggle_user_active(user_id: int):
    """Активация/деактивация пользователя."""
    if not current_user.is_admin:
        flash("У вас нет прав доступа к этой странице", "error")
        return redirect(url_for("main.dashboard"))

    user = User.query.get_or_404(user_id)

    # Нельзя деактивировать самого себя
    if user.id == current_user.id:
        flash("Вы не можете деактивировать свой собственный аккаунт", "error")
        return redirect(url_for("admin.admin_panel"))

    user.is_active = not user.is_active
    db.session.commit()

    status = "активирован" if user.is_active else "деактивирован"
    logger.info(f"Администратор {current_user.username} {status} пользователя {user.username}")
    flash(f"Пользователь {user.username} {status}", "success")
    return redirect(url_for("admin.admin_panel"))


@admin_bp.route("/users/<int:user_id>/reset_password", methods=["GET", "POST"])
@login_required
def reset_password(user_id: int):
    """Сброс пароля пользователя."""
    if not current_user.is_admin:
        flash("У вас нет прав доступа к этой странице", "error")
        return redirect(url_for("main.dashboard"))

    user = User.query.get_or_404(user_id)

    if request.method == "POST":
        new_password = request.form.get("new_password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        if not new_password:
            flash("Пароль не может быть пустым", "error")
            return render_template("reset_password.html", user=user)

        if new_password != confirm_password:
            flash("Пароли не совпадают", "error")
            return render_template("reset_password.html", user=user)

        user.set_password(new_password)
        user.must_change_password = False  # Сбрасываем флаг при ручном сбросе
        db.session.commit()

        logger.info(f"Администратор {current_user.username} сбросил пароль для пользователя {user.username}")
        flash(f"Пароль для пользователя {user.username} успешно изменен", "success")
        return redirect(url_for("admin.admin_panel"))

    return render_template("reset_password.html", user=user)


@admin_bp.route("/users/<int:user_id>/force_password_change", methods=["POST"])
@login_required
def force_password_change(user_id: int):
    """Установить флаг принудительной смены пароля."""
    if not current_user.is_admin:
        flash("У вас нет прав доступа к этой странице", "error")
        return redirect(url_for("main.dashboard"))

    user = User.query.get_or_404(user_id)

    # Нельзя установить для самого себя
    if user.id == current_user.id:
        flash("Вы не можете установить принудительную смену пароля для себя", "error")
        return redirect(url_for("admin.admin_panel"))

    user.must_change_password = True
    db.session.commit()

    logger.info(f"Администратор {current_user.username} установил принудительную смену пароля для {user.username}")
    flash(f"Пользователь {user.username} будет обязан сменить пароль при следующем входе", "success")
    return redirect(url_for("admin.admin_panel"))

