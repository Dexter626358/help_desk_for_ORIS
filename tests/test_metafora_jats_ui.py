"""UI smoke для валидатора JATS Метафоры."""

from __future__ import annotations

import pytest

from ipsas.config.settings import reset_settings
from ipsas.web.app import create_app


@pytest.fixture()
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "development")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    reset_settings()
    from ipsas.config import settings as settings_mod

    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path / "temp")
    (tmp_path / "temp").mkdir(parents=True, exist_ok=True)
    application = create_app(testing=True)
    yield application
    reset_settings()


@pytest.fixture()
def client(app):
    return app.test_client()


def test_metafora_jats_page_renders(client):
    resp = client.get("/services/metafora-jats")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Метафор" in body
    assert "xml_file" in body
    assert ".zip" in body.lower() or "ZIP" in body


def test_metafora_jats_process_valid(client):
    from io import BytesIO

    xml = b"""<?xml version="1.0"?>
<article article-type="research-article">
  <front><article-meta>
    <title-group><article-title>Title</article-title></title-group>
    <pub-date date-type="pub" iso-8601-date="2025-01-01"><year>2025</year></pub-date>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
  <back><ref-list><ref id="R1"><mixed-citation>Ref</mixed-citation></ref></ref-list></back>
</article>"""
    resp = client.post(
        "/services/metafora-jats/process",
        data={"xml_file": (BytesIO(xml), "sample.xml")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "обязательные требования" in body.lower() or "Пройдено" in body


def test_metafora_jats_process_zip(client):
    import zipfile
    from io import BytesIO

    xml = b"""<?xml version="1.0"?>
<article article-type="research-article">
  <front><article-meta>
    <title-group><article-title>Title</article-title></title-group>
    <pub-date date-type="pub" iso-8601-date="2025-01-01"><year>2025</year></pub-date>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
  <back><ref-list><ref id="R1"><mixed-citation>Ref</mixed-citation></ref></ref-list></back>
</article>"""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a1.xml", xml)
        zf.writestr("a2.xml", xml)
    buf.seek(0)
    resp = client.post(
        "/services/metafora-jats/process",
        data={"xml_file": (buf, "issue.zip")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "статей в архиве" in body.lower() or "a1.xml" in body
    assert "a2.xml" in body
