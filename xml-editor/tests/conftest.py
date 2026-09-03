"""Фикстуры pytest для xml-editor."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from app import create_app
import config


SAMPLES = Path(__file__).resolve().parents[1] / "samples" / "sample.xml"


@pytest.fixture()
def app(tmp_path, monkeypatch):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(config, "UPLOADS_DIR", uploads)
    monkeypatch.setattr("xml_editor.utils.UPLOADS_DIR", uploads)
    application = create_app()
    application.config.update(
        {
            "TESTING": True,
            "WTF_CSRF_ENABLED": False,
            "SECRET_KEY": "test-secret",
        }
    )
    yield application
    if uploads.exists():
        shutil.rmtree(uploads, ignore_errors=True)


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def sample_xml_bytes() -> bytes:
    return SAMPLES.read_bytes()


@pytest.fixture()
def sample_path() -> Path:
    return SAMPLES
