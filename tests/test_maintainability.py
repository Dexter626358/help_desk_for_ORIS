"""Тесты rate limit, Finding, CLI, standalone CSS."""

from __future__ import annotations

from ipsas.common.models import Finding, Severity
from ipsas.common.rate_limit import RequestGuard
from ipsas.config.settings import reset_settings
from ipsas.modules.journal_xml.report.standalone import load_standalone_css, render_web_style_html_report


def test_finding_roundtrip():
    f = Finding(
        code="doi.missing",
        severity=Severity.ERROR,
        message="Нет DOI",
        field="doi",
        article_id="12",
    )
    d = f.to_dict()
    assert d["severity"] == "error"
    back = Finding.from_mapping({"rule_id": "x", "level": "warn", "text": "t", "field": "f"})
    assert back.severity == Severity.WARNING
    assert back.message == "t"


def test_request_guard_rate_and_concurrency():
    guard = RequestGuard(max_concurrent=10, rate_limit=2, rate_window_s=60)
    assert guard.try_acquire("a").allowed
    assert guard.try_acquire("a").allowed
    assert guard.try_acquire("a").allowed is False
    guard.release()
    guard.release()

    guard2 = RequestGuard(max_concurrent=1, rate_limit=100, rate_window_s=60)
    assert guard2.try_acquire("x").allowed
    assert guard2.try_acquire("y").allowed is False
    guard2.release()
    assert guard2.try_acquire("y").allowed


def test_standalone_css_includes_theme_tokens():
    css = load_standalone_css()
    assert "--primary" in css or "--color-text" in css
    assert ".xr-card" in css


def test_standalone_html_uses_loaded_css():
    report = {
        "source_file": "x.xml",
        "generated_at": "2026-01-01",
        "root_tag": "journal",
        "journal": {
            "titleid": "",
            "issn": "",
            "eissn": "",
            "title_ru": "Журнал",
            "title_en": "Journal",
            "titles": {},
        },
        "issue": {"volume": "1", "number": "1", "date_uni": "2026", "pages": ""},
        "summary": {
            "articles_total": 0,
            "articles_ok": 0,
            "articles_with_errors": 0,
            "articles_with_warnings": 0,
            "missing_eng_title": 0,
            "missing_eng_abstract": 0,
            "missing_eng_keywords": 0,
        },
        "articles": [],
    }
    html = render_web_style_html_report(report, filename="x.xml")
    assert "<style>" in html
    assert ".xr-card" in html


def test_cli_version():
    from ipsas.cli import main

    assert main(["version"]) == 0


def test_rate_limit_blocks_service_post(monkeypatch):
    monkeypatch.setenv("MAX_CONCURRENT_JOBS", "4")
    monkeypatch.setenv("RATE_LIMIT_PER_MINUTE", "1")
    reset_settings()
    from ipsas.web.app import create_app

    app = create_app()
    client = app.test_client()
    r1 = client.post("/services/xml-validator/validate", data={})
    assert r1.status_code in (302, 400, 429)
    r2 = client.post("/services/xml-validator/validate", data={})
    assert r2.status_code == 429
