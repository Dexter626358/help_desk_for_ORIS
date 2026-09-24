"""Оркестрация базовой настройки журнала в песочнице."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ipsas.modules.eng_metadata.platform_auth import PlatformAuthClient, PlatformAuthError
from ipsas.modules.sandbox_journal_setup.auth import ensure_sandbox_auth
from ipsas.modules.sandbox_journal_setup.steps import ALL_STEPS, StepResult
from ipsas.modules.sandbox_journal_setup.urls import parse_journal_url

logger = logging.getLogger(__name__)


@dataclass
class SetupReport:
    journal_url: str
    base_url: str
    journal: str
    dry_run: bool
    steps: list[StepResult] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        if self.error:
            return False
        return all(s.ok for s in self.steps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "journal_url": self.journal_url,
            "base_url": self.base_url,
            "journal": self.journal,
            "dry_run": self.dry_run,
            "ok": self.ok,
            "error": self.error,
            "steps": [
                {
                    "id": s.id,
                    "title": s.title,
                    "ok": s.ok,
                    "changed": s.changed,
                    "dry_run": s.dry_run,
                    "message": s.message,
                }
                for s in self.steps
            ],
        }


def run_sandbox_setup(
    journal_url: str,
    *,
    gate_username: str,
    gate_password: str,
    ojs_username: str,
    ojs_password: str,
    cookie_file: Path,
    dry_run: bool = True,
    delay: float = 0.35,
    timeout: float = 60.0,
    auth: PlatformAuthClient | None = None,
) -> SetupReport:
    """Выполнить шаги настройки для URL вида …/257/index."""
    base_url, journal = parse_journal_url(journal_url)
    report = SetupReport(
        journal_url=journal_url.strip(),
        base_url=base_url,
        journal=journal,
        dry_run=dry_run,
    )
    try:
        client = auth or ensure_sandbox_auth(
            base_url=base_url,
            gate_username=gate_username,
            gate_password=gate_password,
            ojs_username=ojs_username,
            ojs_password=ojs_password,
            cookie_file=Path(cookie_file),
            timeout=timeout,
        )
        if client.base_url.rstrip("/") != base_url:
            client.base_url = base_url
    except PlatformAuthError as exc:
        report.error = str(exc)
        logger.error("Auth: %s", exc)
        return report

    for step_fn in ALL_STEPS:
        result = step_fn(
            client,
            base_url=base_url,
            journal=journal,
            dry_run=dry_run,
            delay=delay,
        )
        report.steps.append(result)
        level = logging.INFO if result.ok else logging.ERROR
        logger.log(
            level,
            "[%s] %s — %s",
            result.id,
            "OK" if result.ok else "FAIL",
            result.message,
        )
        if not result.ok:
            break
    return report
