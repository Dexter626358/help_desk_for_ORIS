"""Тесты безопасности: XML / ZIP / SSRF / XSS экранирование."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest
from lxml import etree

from ipsas.common.ssrf import UnsafeUrlError, assert_safe_fetch_url, is_safe_fetch_url
from ipsas.common.xml_secure import create_secure_parser
from ipsas.common.zip_safe import UnsafeZipError, ZipLimits, extract_zip_safely, safe_member_path
from ipsas.modules.journal_xml.report.standalone import render_web_style_html_report


def test_xml_parser_rejects_xxe(tmp_path: Path):
    evil = tmp_path / "secret.txt"
    evil.write_text("SECRET", encoding="utf-8")
    xml = f"""<?xml version="1.0"?>
<!DOCTYPE foo [
  <!ENTITY xxe SYSTEM "{evil.resolve().as_uri()}">
]>
<root>&xxe;</root>
"""
    parser = create_secure_parser()
    # Сущности не резолвятся: либо ошибка, либо текст без утечки файла.
    try:
        root = etree.fromstring(xml.encode("utf-8"), parser=parser)
        text = "".join(root.itertext())
        assert "SECRET" not in text
    except etree.XMLSyntaxError:
        pass


def test_xml_parser_rejects_billion_laughs():
    xml = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [
 <!ENTITY lol "lol">
 <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
 <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
]>
<root>&lol2;</root>
"""
    parser = create_secure_parser()
    try:
        root = etree.fromstring(xml, parser=parser)
        text = "".join(root.itertext())
        # Не должно развернуться в огромную строку
        assert len(text) < 1000
    except etree.XMLSyntaxError:
        pass


def test_zip_rejects_path_traversal(tmp_path: Path):
    zpath = tmp_path / "evil.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("../escape.xml", b"<root/>")
        zf.writestr("ok.pdf", b"%PDF-1.4")
    out = tmp_path / "out"
    with pytest.raises(UnsafeZipError):
        extract_zip_safely(zpath, out, limits=ZipLimits(allowed_suffixes=(".xml", ".pdf")))


def test_zip_safe_member_blocks_absolute():
    with pytest.raises(UnsafeZipError):
        safe_member_path(Path("/tmp/out"), "/etc/passwd")
    with pytest.raises(UnsafeZipError):
        safe_member_path(Path("C:/tmp/out"), "C:/Windows/file.txt")


def test_zip_allows_normal_pdf_xml(tmp_path: Path):
    zpath = tmp_path / "ok.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("issue/data.xml", b"<journal/>")
        zf.writestr("issue/a.pdf", b"%PDF-1.4")
    out = tmp_path / "out"
    written = extract_zip_safely(zpath, out)
    names = {p.name for p, _ in written}
    assert "data.xml" in names
    assert "a.pdf" in names


def test_ssrf_blocks_localhost_and_private():
    assert is_safe_fetch_url("http://127.0.0.1/admin") is False
    assert is_safe_fetch_url("http://localhost/x") is False
    assert is_safe_fetch_url("http://10.0.0.5/x") is False
    assert is_safe_fetch_url("http://192.168.1.1/x") is False
    assert is_safe_fetch_url("http://169.254.169.254/latest") is False
    assert is_safe_fetch_url("file:///etc/passwd") is False
    assert is_safe_fetch_url("ftp://example.com/a") is False
    with pytest.raises(UnsafeUrlError):
        assert_safe_fetch_url("http://127.0.0.1/", resolve_dns=False)
    assert is_safe_fetch_url("https://journals.example.org/issue/1") is True


def test_ssrf_allowlist(monkeypatch):
    monkeypatch.setenv("ISSUE_FETCH_ALLOWED_HOSTS", "journals.rcsi.science")
    assert is_safe_fetch_url("https://journals.rcsi.science/x") is True
    assert is_safe_fetch_url("https://evil.example/x") is False


def test_standalone_html_escapes_xss():
    report = {
        "source_file": "x.xml",
        "generated_at": "2026-01-01",
        "root_tag": "journal",
        "journal": {
            "titleid": "",
            "issn": "",
            "eissn": "",
            "title_ru": "<script>alert(1)</script>",
            "title_en": "<img src=x onerror=alert(1)>",
            "titles": {},
        },
        "issue": {"volume": "1", "number": "2", "date_uni": "2026", "pages": ""},
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
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<img src=x onerror=alert(1)>" not in html


def test_health_endpoints():
    from ipsas.config.settings import reset_settings
    from ipsas.web.app import create_app

    reset_settings()
    app = create_app(testing=True)
    client = app.test_client()
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "service": "ipsas"}
    assert client.get("/health/live").status_code == 200
    ready = client.get("/health/ready")
    assert ready.status_code in (200, 503)
    assert "temp_dir" not in (ready.get_json() or {})
