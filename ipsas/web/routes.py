"""Основные роуты приложения."""

from flask import Blueprint, jsonify, render_template, redirect, url_for

main_bp = Blueprint("main", __name__, template_folder="templates")


@main_bp.route("/health")
def health():
    """Проверка готовности для Railway и балансировщика."""
    return jsonify(status="ok"), 200


@main_bp.route("/")
def index():
    """Главная страница — сразу дашборд с сервисами."""
    return redirect(url_for("main.dashboard"))


@main_bp.route("/dashboard")
def dashboard():
    """Дашборд с сервисами."""
    return render_template("dashboard.html")
