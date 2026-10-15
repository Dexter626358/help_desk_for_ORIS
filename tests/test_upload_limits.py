"""Согласованность лимитов загрузки между кодом, шаблонами env и nginx.

Причина теста: app-лимит однажды подняли, а `client_max_body_size` в nginx
остался прежним, поэтому файлы больше старого значения отбивались на прокси.
Здесь фиксируется инвариант: nginx >= MAX_CONTENT_LENGTH > MAX_FILE_SIZE.
"""

from __future__ import annotations

import re
from pathlib import Path

from ipsas.config.settings import get_settings, reset_settings

ROOT = Path(__file__).resolve().parents[1]

PRODUCTION_ENV = ROOT / "deploy" / "ipsas.env.production.example"
DEV_ENV = ROOT / ".env.example"
NGINX_CONF = ROOT / "deploy" / "nginx.example.conf"

MB = 1024 * 1024
EXPECTED_MAX_FILE_SIZE = 50 * MB


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = value.strip()
    return values


def _client_max_body_size_bytes(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"client_max_body_size\s+(\d+)([kKmM]?)\s*;", text)
    assert match, f"{path.name}: не найден client_max_body_size"
    value = int(match.group(1))
    unit = match.group(2).lower()
    multiplier = {"": 1, "k": 1024, "m": MB}[unit]
    return value * multiplier


def test_upload_limit_present_in_env_templates():
    for path in (PRODUCTION_ENV, DEV_ENV):
        env = _read_env(path)
        assert "MAX_FILE_SIZE" in env, f"{path.name}: нет MAX_FILE_SIZE"
        assert "MAX_CONTENT_LENGTH" in env, f"{path.name}: нет MAX_CONTENT_LENGTH"


def test_env_templates_agree():
    production = _read_env(PRODUCTION_ENV)
    dev = _read_env(DEV_ENV)
    assert production["MAX_FILE_SIZE"] == dev["MAX_FILE_SIZE"], (
        f"{PRODUCTION_ENV.name} и {DEV_ENV.name} расходятся по MAX_FILE_SIZE: "
        f"{production['MAX_FILE_SIZE']} != {dev['MAX_FILE_SIZE']}"
    )
    assert production["MAX_CONTENT_LENGTH"] == dev["MAX_CONTENT_LENGTH"], (
        f"{PRODUCTION_ENV.name} и {DEV_ENV.name} расходятся по MAX_CONTENT_LENGTH: "
        f"{production['MAX_CONTENT_LENGTH']} != {dev['MAX_CONTENT_LENGTH']}"
    )


def test_file_size_default_is_50_mb():
    env = _read_env(PRODUCTION_ENV)
    assert int(env["MAX_FILE_SIZE"]) == EXPECTED_MAX_FILE_SIZE, (
        f"{PRODUCTION_ENV.name}: MAX_FILE_SIZE={env['MAX_FILE_SIZE']}, "
        f"ожидается {EXPECTED_MAX_FILE_SIZE} (50 МБ)"
    )


def test_content_length_exceeds_file_size():
    env = _read_env(PRODUCTION_ENV)
    file_size = int(env["MAX_FILE_SIZE"])
    content_length = int(env["MAX_CONTENT_LENGTH"])
    assert content_length > file_size, (
        f"{PRODUCTION_ENV.name}: MAX_CONTENT_LENGTH={content_length} должен быть "
        f"больше MAX_FILE_SIZE={file_size} (запас под multipart)"
    )


def test_nginx_limit_not_below_content_length():
    env = _read_env(PRODUCTION_ENV)
    content_length = int(env["MAX_CONTENT_LENGTH"])
    nginx_limit = _client_max_body_size_bytes(NGINX_CONF)
    assert nginx_limit >= content_length, (
        f"{NGINX_CONF.name}: client_max_body_size={nginx_limit} байт меньше "
        f"MAX_CONTENT_LENGTH={content_length} байт из {PRODUCTION_ENV.name}; "
        "nginx отобьёт запрос раньше приложения"
    )


def test_settings_default_keeps_multipart_headroom(monkeypatch):
    # Импорт settings на уровне модуля уже подхватил корневой .env; здесь
    # переменные снимаются, чтобы проверить именно дефолты кода.
    for name in (
        "MAX_FILE_SIZE",
        "MAX_CONTENT_LENGTH",
        "IPSAS_ENV",
        "SECRET_KEY",
        "ISSUE_FETCH_ALLOWED_HOSTS",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("IPSAS_ENV", "development")

    reset_settings()
    try:
        settings = get_settings()
        assert settings.max_file_size == EXPECTED_MAX_FILE_SIZE
        assert settings.max_content_length > settings.max_file_size, (
            "дефолт MAX_CONTENT_LENGTH должен быть больше MAX_FILE_SIZE"
        )
    finally:
        reset_settings()
