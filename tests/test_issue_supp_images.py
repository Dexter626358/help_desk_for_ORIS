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
    # Legacy: строка → RU
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
    assert by_name["Fig. 1.jpeg"].resolved_title_ru == "Корреляция"
    assert by_name["Fig. 1.jpeg"].resolved_title_en == ""
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


def test_parse_images_archive_bilingual_captions(tmp_path: Path):
    zpath = tmp_path / "images.zip"
    captions = (
        "{\n"
        '  "Fig. 1": {\n'
        '    "ru": "Рис. 1. Типы взаимодействий",\n'
        '    "en": "Fig. 1. Types of interactions"\n'
        "  },\n"
        '  "Fig. 2": {\n'
        '    "ru": "Рис. 2. Только русский"\n'
        "  },\n"
        '  "Fig. 3": {\n'
        '    "en": "Fig. 3. English only"\n'
        "  }\n"
        "}\n"
    ).encode("utf-8")
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("10-20_images/Fig. 1.jpeg", b"a")
        zf.writestr("10-20_images/Fig. 2.jpeg", b"b")
        zf.writestr("10-20_images/Fig. 3.jpeg", b"c")
        zf.writestr("10-20_images/figure_captions.json", captions)
    bundles = parse_images_archive(zpath, tmp_path / "ex")
    assert len(bundles) == 1
    by_name = {f.original_name: f for f in bundles[0].files}
    f1 = by_name["Fig. 1.jpeg"]
    assert f1.resolved_title_ru == "Рис. 1. Типы взаимодействий"
    assert f1.resolved_title_en == "Fig. 1. Types of interactions"
    assert f1.display_title == "Рис. 1. Типы взаимодействий"
    f2 = by_name["Fig. 2.jpeg"]
    assert f2.resolved_title_ru == "Рис. 2. Только русский"
    assert f2.resolved_title_en == ""
    f3 = by_name["Fig. 3.jpeg"]
    assert f3.resolved_title_ru == ""
    assert f3.resolved_title_en == "Fig. 3. English only"
    assert f3.display_title == "Fig. 3. English only"


def test_caption_lookup_helpers():
    from ipsas.modules.issue_supp_images.archive import (
        FigureCaption,
        _caption_for_filename,
        _load_captions_json,
    )

    legacy = _load_captions_json(
        '{"Fig. 1": "Реальное название", "Fig. 2": "  "}'.encode("utf-8")
    )
    assert legacy == {"Fig. 1": FigureCaption(ru="Реальное название")}
    assert _caption_for_filename(legacy, "Fig. 1.jpeg") == FigureCaption(
        ru="Реальное название"
    )
    assert _caption_for_filename(legacy, "Fig. 2.jpeg") == FigureCaption()
    assert _caption_for_filename({}, "Fig. 1.jpeg") == FigureCaption()

    bilingual = _load_captions_json(
        (
            '{"Fig. 1": {"ru": "RU", "en": "EN"}, '
            '"Fig. 2": {"en": "Only EN"}, '
            '"Fig. 3": {}}'
        ).encode("utf-8")
    )
    assert bilingual["Fig. 1"] == FigureCaption(ru="RU", en="EN")
    assert bilingual["Fig. 2"] == FigureCaption(en="Only EN")
    assert "Fig. 3" not in bilingual


def test_titles_for_image_and_save_payload():
    from ipsas.modules.issue_supp_images.models import ImageFile
    from ipsas.modules.issue_supp_images.upload import (
        _titles_for_image,
        save_supp_metadata,
    )

    both = ImageFile(
        path=Path("x.jpeg"),
        original_name="Fig. 1.jpeg",
        folder_key="1-2",
        title_ru="RU cap",
        title_en="EN cap",
    )
    assert _titles_for_image(both) == ("RU cap", "EN cap")

    only_en = ImageFile(
        path=Path("x.jpeg"),
        original_name="Fig. 2.jpeg",
        folder_key="1-2",
        title_en="EN only",
    )
    assert _titles_for_image(only_en) == ("", "EN only")

    none = ImageFile(
        path=Path("x.jpeg"),
        original_name="Fig. 3.jpeg",
        folder_key="1-2",
    )
    assert _titles_for_image(none) == ("Fig. 3.jpeg", "")

    saved: dict[str, str] = {}

    class FakeAuth:
        base_url = "https://example.test"

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            return '<input name="dateCreated" value="2024-01-01" />'

        def request(
            self,
            url: str,
            *,
            data: dict[str, str] | None = None,
            method: str | None = None,
            headers: dict[str, str] | None = None,
        ) -> tuple[int, str, bytes]:
            saved.update(data or {})
            return 200, url.replace("saveSuppFile", "submissionEditing"), b"ok"

    save_supp_metadata(
        FakeAuth(),  # type: ignore[arg-type]
        journal="j",
        article_id=1,
        file_id=2,
        title_ru="RU cap",
        title_en="EN cap",
        delay=0,
    )
    assert saved["title[ru_RU]"] == "RU cap"
    assert saved["title[en_US]"] == "EN cap"
    assert saved["formLocale"] == "ru_RU"

    saved.clear()
    save_supp_metadata(
        FakeAuth(),  # type: ignore[arg-type]
        journal="j",
        article_id=1,
        file_id=2,
        title_en="EN only",
        delay=0,
    )
    assert "title[ru_RU]" not in saved
    assert saved["title[en_US]"] == "EN only"
    assert saved["formLocale"] == "en_US"


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
