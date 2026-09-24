"""Веб-сервис: базовая настройка журнала в песочнице."""

from __future__ import annotations

import time

from flask import Blueprint, flash, redirect, render_template, request, url_for

from ipsas.config.settings import get_settings
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError
from ipsas.modules.sandbox_journal_setup.service import run_sandbox_setup
from ipsas.modules.sandbox_journal_setup.urls import parse_journal_url
from ipsas.utils.logger import get_logger
from ipsas.utils.operation_history import record_operation

logger = get_logger(__name__)

sandbox_journal_setup_bp = Blueprint(
    "sandbox_journal_setup", __name__, template_folder="templates"
)


@sandbox_journal_setup_bp.route("/sandbox-journal-setup")
def setup_page():
    settings = get_settings()
    return render_template(
        "sandbox_journal_setup.html",
        default_journal_url="",
        sandbox_base_url=settings.sandbox_base_url,
        has_gate_creds=bool(settings.sandbox_gate_username and settings.sandbox_gate_password),
        has_ojs_creds=bool(settings.sandbox_ojs_username and settings.sandbox_ojs_password),
    )


@sandbox_journal_setup_bp.route("/sandbox-journal-setup/process", methods=["POST"])
def process_setup():
    journal_url = (request.form.get("journal_url") or "").strip()
    dry_run = (request.form.get("dry_run") or "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }

    if not journal_url:
        flash("Укажите ссылку на журнал в песочнице", "error")
        return redirect(url_for("sandbox_journal_setup.setup_page"))

    try:
        parse_journal_url(journal_url)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("sandbox_journal_setup.setup_page"))

    settings = get_settings()
    if not (settings.sandbox_gate_username and settings.sandbox_gate_password):
        flash("В .env не заданы SANDBOX_GATE_* / SANDBOX_USER1_* (ворота)", "error")
        return redirect(url_for("sandbox_journal_setup.setup_page"))
    if not (settings.sandbox_ojs_username and settings.sandbox_ojs_password):
        flash("В .env не заданы SANDBOX_OJS_* / SANDBOX_USER2_* (OJS)", "error")
        return redirect(url_for("sandbox_journal_setup.setup_page"))

    t0 = time.perf_counter()
    try:
        report = run_sandbox_setup(
            journal_url,
            gate_username=settings.sandbox_gate_username,
            gate_password=settings.sandbox_gate_password,
            ojs_username=settings.sandbox_ojs_username,
            ojs_password=settings.sandbox_ojs_password,
            cookie_file=settings.sandbox_cookie_file,
            dry_run=dry_run,
            delay=settings.sandbox_request_delay,
            timeout=settings.request_timeout,
        )
    except PlatformAuthError as exc:
        flash(f"Ошибка входа: {exc}", "error")
        return redirect(url_for("sandbox_journal_setup.setup_page"))
    except Exception as exc:  # noqa: BLE001
        logger.error("sandbox_journal_setup failed: %s", exc, exc_info=True)
        flash(
            "Не удалось выполнить настройку. Подробности в журнале сервера.",
            "error",
        )
        return redirect(url_for("sandbox_journal_setup.setup_page"))

    elapsed = time.perf_counter() - t0
    data = report.to_dict()
    changed = sum(1 for s in report.steps if s.changed)
    failed = sum(1 for s in report.steps if not s.ok)

    status = "ok" if report.ok else "warning"
    mode = "dry-run" if dry_run else "apply"
    detail = (
        f"{report.journal} @ {report.base_url}: {mode}, "
        f"шагов {len(report.steps)}, изменений {changed}, ошибок {failed} "
        f"({elapsed:.1f} с)"
    )
    if report.error:
        detail = f"{detail}; {report.error}"
    record_operation(
        tool="sandbox_journal_setup",
        title="Настройка журнала в песочнице",
        status=status,
        detail=detail,
        url=url_for("sandbox_journal_setup.setup_page"),
    )

    if report.error:
        flash(report.error, "error")
    elif dry_run:
        flash(
            f"Проверка без изменений: шагов {len(report.steps)}, "
            f"к изменению {changed}, ошибок {failed}.",
            "success" if report.ok else "warning",
        )
    else:
        flash(
            f"Готово: шагов {len(report.steps)}, изменено {changed}, "
            f"ошибок {failed}.",
            "success" if report.ok else "warning",
        )

    return render_template(
        "sandbox_journal_setup_result.html",
        report=data,
        elapsed=elapsed,
    )
