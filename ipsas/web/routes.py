"""Основные роуты приложения."""

from flask import Blueprint, jsonify, redirect, render_template, url_for

from ipsas.config.settings import get_settings

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
    from ipsas.utils.operation_history import list_operations

    settings = get_settings()
    return render_template(
        "dashboard.html",
        recent_operations=list_operations(limit=8),
        editorial_board_url=settings.editorial_board_url,
    )


@main_bp.route("/services/bibliography")
def bibliography_hub():
    """Каталог инструментов обработки списка литературы."""
    return render_template("bibliography_hub.html")


@main_bp.route("/services/xml-typo-fixes")
def xml_typo_fixes_page():
    """Исправить типовые ошибки XML (отдельный сервис)."""
    settings = get_settings()
    external = settings.xml_typo_fixes_url
    if external:
        return redirect(external)
    return render_template(
        "service_placeholder.html",
        service_title="Исправить типовые ошибки XML",
        service_section="Исправление XML",
        service_lead=(
            "Автоматическое исправление частых ошибок в journal XML "
            "(типографика, служебные поля, повторяющиеся дефекты разметки)."
        ),
        service_io="XML журнала → исправленный XML",
        in_development=True,
    )


@main_bp.route("/services/journal-site-check")
def journal_site_check_page():
    """Проверить сайт журнала (отдельный сервис)."""
    settings = get_settings()
    external = settings.journal_site_check_url
    if external:
        return redirect(external)
    return render_template(
        "service_placeholder.html",
        service_title="Проверить сайт журнала",
        service_section="Метаданные журнала",
        service_lead=(
            "Проверка публичного сайта журнала: разделы, метаданные, "
            "доступность страниц и типовые проблемы отображения."
        ),
        service_io="URL сайта журнала → отчёт",
        in_development=True,
    )
