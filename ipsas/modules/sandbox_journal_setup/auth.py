"""Двухшаговый вход: HTTP Basic (ворота) → OJS signIn."""

from __future__ import annotations

import logging
from pathlib import Path

from ipsas.modules.eng_metadata.platform_auth import (
    PlatformAuthClient,
    PlatformAuthError,
)

logger = logging.getLogger(__name__)


def ensure_sandbox_auth(
    *,
    base_url: str,
    gate_username: str,
    gate_password: str,
    ojs_username: str,
    ojs_password: str,
    cookie_file: Path,
    timeout: float = 60.0,
) -> PlatformAuthClient:
    """Ворота (Basic Auth) + логин OJS; cookies переиспользуются при валидности."""
    if not gate_username or not gate_password:
        raise PlatformAuthError("Нужны SANDBOX_GATE_* / SANDBOX_USER1_* (ворота Basic Auth)")
    if not ojs_username or not ojs_password:
        raise PlatformAuthError("Нужны SANDBOX_OJS_* / SANDBOX_USER2_* (логин OJS)")

    auth = PlatformAuthClient(
        base_url=base_url,
        timeout=timeout,
        basic_auth=(gate_username, gate_password),
    )

    # проверка ворот
    status, _, body = auth.request(f"{base_url.rstrip('/')}/", method="GET")
    if status == 401:
        raise PlatformAuthError("Ворота песочницы: 401 Unauthorized (проверьте GATE-учётку)")
    if status >= 400:
        raise PlatformAuthError(f"Ворота песочницы: HTTP {status}")

    if cookie_file.is_file():
        try:
            auth.load_cookies(cookie_file)
            if auth.is_logged_in():
                logger.info("Песочница: сессия OJS из cookies")
                return auth
        except (OSError, PlatformAuthError) as exc:
            logger.info("Песочница: cookies недействительны, повторный вход: %s", exc)

    auth.login(ojs_username, ojs_password)
    try:
        auth.save_cookies(cookie_file)
    except OSError as exc:
        logger.warning("Не удалось сохранить sandbox cookies: %s", exc)
    return auth
