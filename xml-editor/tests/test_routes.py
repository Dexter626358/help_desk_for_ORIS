"""Интеграционные тесты маршрутов."""

from __future__ import annotations

from io import BytesIO

from xml_editor.utils import edited_path, parse_xml_file


def _upload(client, sample_xml_bytes: bytes, filename: str = "sample.xml"):
    return client.post(
        "/upload",
        data={"xml_file": (BytesIO(sample_xml_bytes), filename)},
        content_type="multipart/form-data",
    )


def test_upload_ok(client, sample_xml_bytes: bytes) -> None:
    resp = _upload(client, sample_xml_bytes)
    assert resp.status_code in (302, 303)
    assert "/editor/" in resp.headers["Location"]


def test_upload_rejects_non_xml(client) -> None:
    resp = _upload(client, b"not xml", filename="note.txt")
    assert resp.status_code in (302, 303)
    follow = client.get(resp.headers["Location"])
    assert b".xml" in follow.data or "xml".encode() in follow.data


def test_upload_rejects_bad_xml(client) -> None:
    resp = _upload(client, b"<journal><broken>", filename="bad.xml")
    assert resp.status_code in (302, 303)


def test_upload_rejects_xxe(client) -> None:
    xxe = b"""<?xml version="1.0"?>
    <!DOCTYPE journal [
      <!ENTITY xxe SYSTEM "file:///etc/passwd">
    ]>
    <journal>
      <issue><articles><article>
        <pages>&xxe;</pages>
        <artTitles><artTitle lang="RUS">T</artTitle></artTitles>
        <authors><author><individInfo lang="RUS"><surname>A</surname></individInfo></author></authors>
      </article></articles></issue>
    </journal>
    """
    resp = _upload(client, xxe, filename="xxe.xml")
    # Должен отклонить или не раскрыть сущность: в любом случае не открыть редактор с /etc/passwd
    if resp.status_code in (302, 303):
        loc = resp.headers.get("Location", "")
        if "/editor/" in loc:
            page = client.get(loc)
            assert b"/etc/passwd" not in page.data
            assert b"root:" not in page.data


def test_path_traversal_session_id(client) -> None:
    resp = client.get("/editor/../uploads")
    assert resp.status_code in (302, 303, 404)
    resp2 = client.get("/download/../../etc/passwd/edited")
    assert resp2.status_code in (302, 303, 404)


def test_integration_edit_download(client, sample_xml_bytes: bytes) -> None:
    resp = _upload(client, sample_xml_bytes)
    assert resp.status_code in (302, 303)
    loc = resp.headers["Location"]
    # /editor/<sid>/0
    parts = loc.rstrip("/").split("/")
    session_id = parts[-2] if parts[-1].isdigit() else parts[-1]
    article_id = parts[-1] if parts[-1].isdigit() else "0"

    page = client.get(f"/editor/{session_id}/{article_id}")
    assert page.status_code == 200
    assert "Моделирование".encode("utf-8") in page.data

    save = client.post(
        f"/editor/{session_id}/{article_id}/save",
        data={
            "title_rus": "Изменённое название",
            "title_eng": "Changed title",
            "doi": "10.1234/sample.2025.1",
            "lang": "RUS",
            "art_type": "RAR",
            "section": "Информатика",
            "udk": "004.8",
            "page_first": "1",
            "page_last": "10",
            "abstract_rus": "Аннотация",
            "abstract_eng": "Abstract",
            "keywords_rus": "а; б",
            "keywords_eng": "a; b",
            "authors-0-RUS-surname": "Иванов",
            "authors-0-RUS-initials": "И.И.",
            "authors-0-ENG-surname": "Ivanov",
            "authors-0-ENG-initials": "I.I.",
            "authors-1-RUS-surname": "Петров",
            "authors-1-RUS-initials": "П.П.",
            "authors-1-ENG-surname": "Petrov",
            "authors-1-ENG-initials": "P.P.",
            "ref-0-lang": "RUS",
            "ref-0-text": "Источник 1",
            "ref-1-lang": "ENG",
            "ref-1-text": "Reference 1",
            "active_tab": "main",
        },
    )
    assert save.status_code in (302, 303)

    dl = client.get(f"/download/{session_id}/edited")
    assert dl.status_code == 200
    body = dl.data.decode("utf-8")
    assert "Изменённое название" in body
    assert 'customAttr="keep-me"' in body
    assert "неизвестный служебный блок" in body
    # Вторая статья не должна потерять название
    assert "Вторая статья выпуска" in body


def test_add_author_route(client, sample_xml_bytes: bytes) -> None:
    resp = _upload(client, sample_xml_bytes)
    loc = resp.headers["Location"]
    parts = loc.rstrip("/").split("/")
    session_id = parts[-2] if parts[-1].isdigit() else parts[-1]
    article_id = "0"
    before = client.get(f"/editor/{session_id}/{article_id}?tab=authors")
    assert before.status_code == 200
    add = client.post(
        f"/editor/{session_id}/{article_id}/authors/add",
        data={
            "title_rus": "Моделирование систем управления",
            "title_eng": "Modeling of control systems",
            "authors-0-RUS-surname": "Иванов",
            "authors-0-ENG-surname": "Ivanov",
            "authors-1-RUS-surname": "Петров",
            "authors-1-ENG-surname": "Petrov",
        },
    )
    assert add.status_code in (302, 303)
    tree = parse_xml_file(edited_path(session_id))
    authors = tree.xpath(".//issue/articles/article[1]/authors/author")
    assert len(authors) == 3
