"""Production-hardening: health, CSRF, SSRF redirects, config."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from urllib.error import URLError

import pytest

from ipsas.common.ssrf import UnsafeUrlError, assert_safe_fetch_url, urlopen_safe
from ipsas.config.settings import get_settings, reset_settings
from ipsas.modules.issue_metadata.http_client import HttpClient
from ipsas.web.app import create_app


@pytest.fixture()
def app(monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "development")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    reset_settings()
    application = create_app(testing=True)
    yield application
    reset_settings()


@pytest.fixture()
def client(app):
    return app.test_client()


def test_create_app_and_health_shape(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data == {"status": "ok", "service": "ipsas"}
    live = client.get("/health/live")
    assert live.status_code == 200
    assert live.get_json()["service"] == "ipsas"
    ready = client.get("/health/ready")
    assert ready.status_code in (200, 503)
    body = ready.get_json()
    assert body["service"] == "ipsas"
    assert "temp_dir" not in body
    assert "schemas_dir" not in body


def test_production_requires_secret_key(monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "production")
    monkeypatch.setenv("ISSUE_FETCH_ALLOWED_HOSTS", "journals.example.org")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    reset_settings()
    with pytest.raises(RuntimeError, match="SECRET_KEY"):
        get_settings()
    reset_settings()


def test_production_requires_allowed_hosts(monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "prod-secret-key-for-tests-32chars")
    monkeypatch.setenv("ISSUE_FETCH_ALLOWED_HOSTS", "")
    reset_settings()
    with pytest.raises(RuntimeError, match="ISSUE_FETCH_ALLOWED_HOSTS"):
        get_settings()
    reset_settings()


def test_production_forbids_disable_csrf(monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "prod-secret-key-for-tests-32chars")
    monkeypatch.setenv("ISSUE_FETCH_ALLOWED_HOSTS", "journals.example.org")
    monkeypatch.setenv("IPSAS_DISABLE_CSRF", "1")
    reset_settings()
    with pytest.raises(RuntimeError, match="IPSAS_DISABLE_CSRF"):
        create_app(testing=False)
    reset_settings()
    monkeypatch.delenv("IPSAS_DISABLE_CSRF", raising=False)


def test_csrf_blocks_post_without_token(monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "development")
    monkeypatch.setenv("SECRET_KEY", "test-secret-for-csrf")
    monkeypatch.delenv("IPSAS_DISABLE_CSRF", raising=False)
    reset_settings()
    app = create_app(testing=False)
    app.config["WTF_CSRF_ENABLED"] = True
    client = app.test_client()
    resp = client.post(
        "/services/xml-validator/validate",
        data={},
        content_type="multipart/form-data",
    )
    assert resp.status_code in {400, 302}
    reset_settings()


def test_ssrf_redirect_to_metadata_blocked(monkeypatch):
    """Публичный URL → 302 на link-local metadata должен быть отвергнут."""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.end_headers()

        def log_message(self, *_args):  # silence
            return

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        # Прямой localhost блокируется до запроса — проверяем handler отдельно:
        # разрешаем исходный URL только для unit-теста SafeRedirect через подмену assert.
        from ipsas.common import ssrf as ssrf_mod

        calls: list[str] = []

        def _track(url, **kwargs):
            calls.append(url)
            if "169.254" in url:
                raise UnsafeUrlError("blocked metadata")
            return url

        monkeypatch.setattr(ssrf_mod, "assert_safe_fetch_url", _track)

        with pytest.raises((UnsafeUrlError, URLError, ValueError)):
            urlopen_safe(f"http://127.0.0.1:{port}/start", timeout=2)
        assert any("169.254" in u for u in calls)
    finally:
        server.shutdown()


def test_http_client_uses_safe_redirects(monkeypatch):
    client = HttpClient(timeout_s=2, retries=1, issue_retries=1)
    monkeypatch.setattr(
        "ipsas.common.ssrf.assert_safe_fetch_url",
        lambda url, **kw: (_ for _ in ()).throw(UnsafeUrlError("blocked")),
    )
    with pytest.raises(ValueError, match="blocked"):
        client.fetch_bytes("https://example.com/x")


def test_health_ready_no_path_leak(client):
    body = client.get("/health/ready").get_json()
    dumped = str(body)
    assert "C:\\" not in dumped
    assert "/Users/" not in dumped
    assert "temp_ok" in body
    assert "schemas_ok" in body
    assert body["schemas_ok"] is True
    assert body["temp_ok"] is True
    assert body["status"] == "ok"
