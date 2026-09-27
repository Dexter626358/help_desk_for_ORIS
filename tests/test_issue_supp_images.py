"""Тесты загрузки рисунков выпуска в доп. файлы."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from ipsas.config.settings import reset_settings
from ipsas.modules.eng_metadata.platform_auth import _ascii_upload_filename
from ipsas.modules.issue_supp_images.archive import (
    parse_folder_name,
    parse_images_archive,
    parse_page_range_token,
)
from ipsas.modules.issue_supp_images.toc import (
    match_article,
    parse_issue_ref,
    parse_issue_toc,
)
from ipsas.modules.issue_supp_images.upload import (
    discover_hidden_locales,
    parse_edit_supp_redirect,
)
from ipsas.web.app import create_app

def test_parse_folder_and_pages():
    pr = parse_folder_name("47-67_images")
    assert pr is not None
    assert pr.key == "47-67"
    assert parse_folder_name("83-103_Images").key == "83-103"
    assert parse_page_range_token("68–82").key == "68-82"
    assert parse_folder_name("readme") is None


def test_parse_issue_ref():
    base, journal, issue_id = parse_issue_ref(
        "https://journals.rcsi.science/2782-2168/editor/issueToc/31488"
    )
    assert base == "https://journals.rcsi.science"
    assert journal == "2782-2168"
    assert issue_id == 31488
    _, j2, i2 = parse_issue_ref(
        "https://journals.rcsi.science/2782-2168/issue/view/31488"
    )
    assert (j2, i2) == ("2782-2168", 31488)


def test_parse_issue_toc_pages():
    html = """
    <a href="/2782-2168/editor/submission/466706">Языковая компетентность</a>
    <input type="text" name="pages[466706]" value="47-67" size="7" />
    <a href="/2782-2168/editor/submission/466707">Маркеры</a>
    <input type="text" name="pages[466707]" value="68-82" size="7" />
    """
    articles = parse_issue_toc(html)
    assert [a.article_id for a in articles] == [466706, 466707]
    assert articles[0].pages.key == "47-67"
    assert "Языковая" in articles[0].title


def test_parse_images_archive_and_match(tmp_path: Path):
    zpath = tmp_path / "images.zip"
    captions = (
        '{\n'
        '  "Fig. 1": "Корреляция",\n'
        '  "Fig. 2": "Второй рисунок"\n'
        '}\n'
    ).encode("utf-8")
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("47-67_images/Fig. 1.jpeg", b"fakejpeg")
        zf.writestr("47-67_images/Fig. 2.jpeg", b"fakejpeg2")
        zf.writestr("47-67_images/figure_captions.json", captions)
        zf.writestr("68-82_images/Fig. 1.jpeg", b"x")
        zf.writestr("ignore.txt", b"no")
    bundles = parse_images_archive(zpath, tmp_path / "ex")
    assert [b.page_range.key for b in bundles] == ["47-67", "68-82"]
    assert len(bundles[0].files) == 2
    by_name = {f.original_name: f for f in bundles[0].files}
    assert by_name["Fig. 1.jpeg"].display_title == "Корреляция"
    assert by_name["Fig. 2.jpeg"].display_title == "Второй рисунок"
    # Без JSON — fallback на имя файла
    assert bundles[1].files[0].display_title == "Fig. 1.jpeg"

    toc_html = """
    <input name="pages[1]" value="47-67" />
    <a href="/j/editor/submission/1">A</a>
    <input name="pages[2]" value="68-82" />
    <a href="/j/editor/submission/2">B</a>
    """
    articles = parse_issue_toc(toc_html)
    assert match_article(articles, bundles[0].page_range).article_id == 1
    assert match_article(articles, bundles[1].page_range).article_id == 2


def test_caption_lookup_helpers():
    from ipsas.modules.issue_supp_images.archive import (
        _caption_for_filename,
        _load_captions_json,
    )

    captions = _load_captions_json(
        '{"Fig. 1": "Реальное название", "Fig. 2": "  "}'.encode("utf-8")
    )
    assert captions == {"Fig. 1": "Реальное название"}
    assert _caption_for_filename(captions, "Fig. 1.jpeg") == "Реальное название"
    assert _caption_for_filename(captions, "Fig. 2.jpeg") == ""
    assert _caption_for_filename({}, "Fig. 1.jpeg") == ""


def test_ascii_upload_filename_and_redirect():
    assert _ascii_upload_filename("Fig. 1.jpeg") == "Fig._1.jpeg"
    aid, fid = parse_edit_supp_redirect(
        "https://journals.rcsi.science/2782-2168/editor/editSuppFile/466706/334759",
        "",
    )
    assert (aid, fid) == (466706, 334759)


def test_discover_hidden_locales():
    html = """
    <div id="suppFileDisplay_334757" class="suppFileDisplay">
      <div class="suppFIleDisplayLanguage en_US">
        <img src="/img/style/hidden.png">
        <a onclick="suppFileDisplayChange(466704, 334757, 'en_US');" class="action">Показывать</a>
      </div>
      <div class="suppFIleDisplayLanguage ru_RU">
        <img src="/img/style/visible.png">
        <a onclick="suppFileDisplayChange(466704, 334757, 'ru_RU');" class="action">Скрыть</a>
      </div>
    </div>
    """
    locales = discover_hidden_locales(html, 466704, 334757)
    assert "en_US" in locales
    assert "ru_RU" not in locales


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


def test_issue_supp_images_page_renders(client):
    resp = client.get("/services/issue-supp-images")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Загрузить рисунки" in body
    assert "issue_url" in body
    assert "zip_file" in body
