"""Основные роуты приложения."""

from flask import Blueprint, render_template, redirect, url_for

main_bp = Blueprint("main", __name__, template_folder="templates")


@main_bp.route("/")
def index():
    """Главная страница — сразу дашборд с сервисами."""
    return redirect(url_for("main.dashboard"))


@main_bp.route("/dashboard")
def dashboard():
    """Дашборд с сервисами."""
    return render_template("dashboard.html")
