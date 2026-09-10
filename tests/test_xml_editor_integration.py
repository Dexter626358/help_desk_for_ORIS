"""Интеграция редактора XML в IPSAS."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest

from ipsas.web.app import create_app

SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "journal_sample.xml"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from ipsas.config import settings as settings_mod

    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path / "temp")
    (tmp_path / "temp").mkdir(parents=True, exist_ok=True)
    app = create_app(testing=True)
    app.config["TESTING"] = True
    return app.test_client()


def test_xml_editor_in_menu(client) -> None:
    resp = client.get("/dashboard")
    assert resp.status_code == 200
    assert b"xml-editor" in resp.data or "Редактор XML".encode("utf-8") in resp.data
    assert b"/services/xml-editor" in resp.data


def test_xml_editor_upload_and_edit(client) -> None:
    data = SAMPLE.read_bytes()
    resp = client.post(
        "/services/xml-editor/upload",
        data={"xml_file": (BytesIO(data), "journal_sample.xml")},
        content_type="multipart/form-data",
    )
    assert resp.status_code in (302, 303)
    loc = resp.headers["Location"]
    assert "/services/xml-editor/editor/" in loc
    page = client.get(loc)
    assert page.status_code == 200
    assert "Моделирование".encode("utf-8") in page.data
