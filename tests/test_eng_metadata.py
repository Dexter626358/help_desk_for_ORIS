"""Тесты модуля обновления ENG-метаданных."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from ipsas.config.settings import reset_settings
from ipsas.modules.eng_metadata.archive import (
    build_article_pairs,
    parse_pages_from_stem,
    unpack_eng_archive,
)
from ipsas.modules.eng_metadata.payload import build_update_payload
from ipsas.modules.eng_metadata.session import create_session_from_zip, load_article_json
from ipsas.modules.eng_metadata.validate import validate_article_json
from ipsas.web.app import create_app

# Минимальный PDF
_MIN_PDF = b"""%PDF-1.1
1 0 obj<<>>endobj
2 0 obj<< /Length 44 >>stream
BT /F1 12 Tf 100 700 Td (Hello) Tj ET
endstream
endobj
3 0 obj<< /Type /Page /Parent 4 0 R /Contents 2 0 R >>endobj
4 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj
5 0 obj<< /Type /Catalog /Pages 4 0 R >>endobj
xref
0 6
0000000000 65535 f 
0000000009 00000 n 
0000000024 00000 n 
0000000110 00000 n 
0000000179 00000 n 
0000000240 00000 n 
trailer<< /Size 6 /Root 5 0 R >>
startxref
299
%%EOF
"""

_SAMPLE_JSON = {
    "article_url": "https://journals.rcsi.science/0005-2310/article/view/288752",
    "title_eng": "Sample Title",
    "title_ru": "Пример заголовка",
    "authors": [
        {
            "full_name_en": "A. Author",
            "full_name_ru": "А. Автор",
            "given_en": "A.",
            "given_ru": "А.",
            "surname_en": "Author",
            "surname_ru": "Автор",
            "email": "a@example.com",
            "orcid": None,
            "affiliations": [
                {
                    "organization_en": "Univ",
                    "organization_ru": "Университет",
                    "address_en": "City",
                }
            ],
        }
    ],
    "dates": {"received": "2024-01-01", "revised": None, "accepted": "2024-02-01"},
    "abstract_eng": "Abstract text.",
    "abstract_ru": "Текст аннотации.",
    "keywords_eng": ["kw1", "kw2"],
    "references_eng": ["Ref one.", "Ref two."],
}


def _make_zip(path: Path, *, orphan_pdf: bool = False) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("3-21__article_288752.pdf", _MIN_PDF)
        if not orphan_pdf:
            zf.writestr(
                "3-21__article_288752.json",
                json.dumps(_SAMPLE_JSON, ensure_ascii=False),
            )
        else:
            zf.writestr("9-10__article_999.json", json.dumps(_SAMPLE_JSON))
    return path


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


def test_validate_article_json_ok():
    assert validate_article_json(_SAMPLE_JSON) == []


def test_validate_article_json_missing_fields():
    errs = validate_article_json({"title_eng": "x"})
    assert any("authors" in e for e in errs)


def test_build_pairs_and_orphan(tmp_path):
    zpath = _make_zip(tmp_path / "o.zip", orphan_pdf=True)
    extract = tmp_path / "ex"
    unpack_eng_archive(zpath, extract)
    pairs = build_article_pairs(extract)
    assert len(pairs) == 2
    by_stem = {p.stem: p for p in pairs}
    assert by_stem["3-21__article_288752"].json_name is None
    assert by_stem["9-10__article_999"].pdf_name is None
    assert by_stem["3-21__article_288752"].pages_label == "3-21"
    assert by_stem["9-10__article_999"].page_start == 9


def test_parse_pages_from_stem():
    assert parse_pages_from_stem("3-21__article_288752") == (3, 21)
    assert parse_pages_from_stem("100–110__article_1") == (100, 110)
    assert parse_pages_from_stem("article_288752") is None


def test_articles_sorted_by_page_start(tmp_path):
    """Числовая сортировка по страницам (не лексикографическая по имени)."""
    zpath = tmp_path / "pages.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        for stem in (
            "100-110__article_3",
            "20-30__article_2",
            "3-21__article_1",
            "nopages__article_9",
        ):
            zf.writestr(f"{stem}.pdf", _MIN_PDF)
            zf.writestr(f"{stem}.json", json.dumps(_SAMPLE_JSON))
    extract = tmp_path / "ex"
    unpack_eng_archive(zpath, extract)
    pairs = build_article_pairs(extract)
    assert [p.pages_label or "—" for p in pairs] == [
        "3-21",
        "20-30",
        "100-110",
        "—",
    ]
    assert [p.article_id for p in pairs] == ["1", "2", "3", "9"]


def test_create_session_from_zip(tmp_path, monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "development")
    reset_settings()
    from ipsas.config import settings as settings_mod

    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path / "temp")
    (tmp_path / "temp").mkdir(parents=True, exist_ok=True)

    zpath = _make_zip(tmp_path / "ok.zip")
    manifest = create_session_from_zip(zpath, source_name="ok.zip")
    assert manifest["total"] == 1
    assert manifest["ok_count"] == 1
    sid = manifest["session_id"]
    data = load_article_json(sid, "288752")
    assert data["title_eng"] == "Sample Title"
    reset_settings()


def test_build_update_payload(monkeypatch):
    from ipsas.config import settings as settings_mod

    reset_settings()
    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "platform_apply_enabled", False)
    payload = build_update_payload(article_id="288752", data=_SAMPLE_JSON)
    assert payload["dry_run"] is True
    assert payload["approved"] is True
    fields = {c["field"] for c in payload["changes"]}
    assert "article.titles.en" in fields
    assert "authors[0].surname_en" in fields
    assert "authors[0].orcid" not in fields
    assert "dates.received" in fields
    assert "dates.accepted" in fields
    assert "dates.revised" not in fields
    reset_settings()


def test_issn_from_article_url():
    from ipsas.services.eng_metadata_review import issn_from_article_url

    assert (
        issn_from_article_url(
            "https://journals.rcsi.science/0005-2310/article/view/288752"
        )
        == "0005-2310"
    )
    assert issn_from_article_url(None) is None


def test_apply_platform_update_dry_when_disabled(tmp_path, monkeypatch):
    from ipsas.config import settings as settings_mod
    from ipsas.services.eng_metadata_review import apply_platform_update

    monkeypatch.setenv("IPSAS_ENV", "development")
    monkeypatch.setenv("PLATFORM_APPLY_ENABLED", "0")
    reset_settings()
    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path / "temp")
    monkeypatch.setattr(settings, "platform_apply_enabled", False)
    (tmp_path / "temp").mkdir(parents=True, exist_ok=True)

    zpath = _make_zip(tmp_path / "ok.zip")
    manifest = create_session_from_zip(zpath, source_name="ok.zip")
    sid = manifest["session_id"]
    summary = apply_platform_update(sid, "288752")
    assert summary["apply"]["dry_run"] is True
    assert summary["approved"] is True
    reset_settings()


def test_parse_forms_and_patch_title():
    from ipsas.modules.eng_metadata.platform_update import Change, UpdateService

    html = """
    <html><body>
    <form method="post" action="/0005-2310/editor/saveMetadata">
      <input type="hidden" name="articleId" value="1" />
      <input type="text" name="title[en_US]" value="Old" />
      <textarea name="abstract[en_US]">Abs</textarea>
      <input type="text" name="subject[en_US]" value="kw" />
      <textarea name="citations">ref</textarea>
    </form>
    </body></html>
    """
    forms = UpdateService._parse_forms(html, "https://journals.rcsi.science/x")
    assert len(forms) == 1
    fields = dict(forms[0]["fields"])
    svc = UpdateService.__new__(UpdateService)
    applied, missing = svc._patch_main_fields(
        fields, [Change(field="article.titles.en", value="New Title")]
    )
    assert applied == ["article.titles.en"]
    assert missing == []
    assert fields["title[en_US]"] == "New Title"


def test_patch_citations_moves_cyrillic_en_to_ru():
    """Русский список из EN-поля → RU, затем в EN пишется английский."""
    from ipsas.modules.eng_metadata.platform_update import Change, UpdateService

    svc = UpdateService.__new__(UpdateService)
    ru_list = (
        "Хаджиев С.Н. Синтез и свойства наноразмерных систем // Нефтехимия. 2014."
    )
    fields = {
        "citations": ru_list,
        "localeCitations[ru_RU]": "",
    }
    applied, missing, skip = svc._patch_citations_en(
        fields,
        [
            Change(field="references[0].text.en", value="Smith J. Catalysis. 2020."),
            Change(field="references[1].text.en", value="Jones A. Oil Chem. 2021."),
        ],
    )
    assert missing == []
    assert skip is None
    assert fields["localeCitations[ru_RU]"] == ru_list
    assert fields["citations"] == "Smith J. Catalysis. 2020.\r\nJones A. Oil Chem. 2021."
    assert "citations→localeCitations[ru_RU]" in applied
    assert "references[0].text.en" in applied


def test_patch_citations_skips_when_both_en_cyrillic_and_ru_filled():
    """Спорный случай: не трогаем citations, просим проверить вручную."""
    from ipsas.modules.eng_metadata.platform_update import Change, UpdateService

    svc = UpdateService.__new__(UpdateService)
    fields = {
        "citations": "Хаджиев С.Н. Старый русский в EN.",
        "localeCitations[ru_RU]": "Уже правильный русский список.",
    }
    applied, missing, skip = svc._patch_citations_en(
        fields,
        [Change(field="references[0].text.en", value="English Ref.")],
    )
    assert applied == []
    assert missing == []
    assert skip is not None
    assert "не отправлен" in skip
    assert "Проверьте" in skip
    assert fields["citations"] == "Хаджиев С.Н. Старый русский в EN."
    assert fields["localeCitations[ru_RU]"] == "Уже правильный русский список."


def test_patch_citations_latin_en_moves_to_empty_ru():
    """EN без кириллицы при пустом RU тоже переносим — список мог быть англ."""
    from ipsas.modules.eng_metadata.platform_update import Change, UpdateService

    svc = UpdateService.__new__(UpdateService)
    old_en = "Smith J. Already English."
    fields = {
        "citations": old_en,
        "localeCitations[ru_RU]": "",
    }
    applied, missing, skip = svc._patch_citations_en(
        fields,
        [Change(field="references[0].text.en", value="New English.")],
    )
    assert skip is None
    assert missing == []
    assert "references[0].text.en" in applied
    assert "citations→localeCitations[ru_RU]" in applied
    assert fields["localeCitations[ru_RU]"] == old_en
    assert fields["citations"] == "New English."


def test_parse_select_options_inside_optgroup():
    """issueId на RCSI лежит в <optgroup>; ./option ломал назначение в выпуск."""
    from ipsas.modules.eng_metadata.platform_update import UpdateService

    html = """
    <html><body>
    <form id="schedulingForm" method="post" action="/editor/updateScheduling/1">
      <select name="issueId">
        <option value="">Будет назначено</option>
        <optgroup label="Старые">
          <option value="19024" selected="selected">№ 4 (2025)</option>
        </optgroup>
      </select>
      <input name="dateSubmitted" value="2024-11-29" />
    </form>
    </body></html>
    """
    forms = UpdateService._parse_forms(html, "https://journals.rcsi.science/x")
    assert forms[0]["fields"]["issueId"] == "19024"


def test_apply_skips_scheduling_dates(monkeypatch):
    """updateScheduling на RCSI снимает с выпуска — даты не POST'им."""
    from ipsas.modules.eng_metadata.platform_update import Change, UpdateService

    class _Client:
        def get_text(self, url: str) -> str:
            if "viewMetadata" in url:
                return """
                <html><body>
                <form method="post" action="/0005-2310/editor/saveMetadata">
                  <input name="title[en_US]" value="Old" />
                </form>
                </body></html>
                """
            raise AssertionError(f"unexpected GET {url}")

        def post_form(self, url, fields, referer=None):
            assert "updateScheduling" not in url
            return 200, url, b"ok"

        settings = type("S", (), {"base_url": "https://journals.rcsi.science"})()

    svc = UpdateService(_Client(), issn="0005-2310")  # type: ignore[arg-type]
    result = svc.apply_payload(
        {
            "article_id": "1",
            "approved": True,
            "changes": [
                {"field": "article.titles.en", "value": "New"},
                {"field": "dates.received", "value": "2024-11-29"},
                {"field": "dates.accepted", "value": "2025-01-14"},
            ],
        },
        allow_apply=True,
    )
    assert "dates.received" not in result.applied_fields
    assert "dates.accepted" not in result.applied_fields
    assert not any("updateScheduling" in e or "Даты не отправлены" in e for e in result.errors)
    assert "article.titles.en" in result.applied_fields
    assert result.ok


def test_update_service_dry_run_path():
    from ipsas.modules.eng_metadata.platform_update import UpdateService

    class _Dummy:
        settings = type("S", (), {"base_url": "https://journals.rcsi.science"})()

    svc = UpdateService(_Dummy(), issn="0005-2310")  # type: ignore[arg-type]
    result = svc.apply_payload(
        {"article_id": "288752", "approved": True, "changes": []},
        allow_apply=False,
    )
    assert result.dry_run is True
    assert result.ok is True


def test_form_references_as_separate_fields():
    from werkzeug.datastructures import MultiDict

    from ipsas.modules.eng_metadata.forms import (
        apply_form_to_metadata,
        metadata_to_form_defaults,
    )

    defaults = metadata_to_form_defaults(_SAMPLE_JSON)
    assert defaults["references_eng"] == ["Ref one.", "Ref two."]

    form = MultiDict(
        [
            ("title_eng", "T"),
            ("abstract_eng", "A"),
            ("keywords_eng", "kw"),
            ("references_count", "2"),
            ("ref_value_0", " First ref "),
            ("ref_value_1", "Second ref"),
            ("authors_count", "0"),
        ]
    )
    out = apply_form_to_metadata({"authors": [], "references_eng": ["a", "b"]}, form)
    assert out["references_eng"] == ["First ref", "Second ref"]


def test_form_references_can_append_extra_fields():
    """Поля, добавленные в UI сверх исходного списка, сохраняются."""
    from werkzeug.datastructures import MultiDict

    from ipsas.modules.eng_metadata.forms import apply_form_to_metadata

    form = MultiDict(
        [
            ("title_eng", "T"),
            ("abstract_eng", "A"),
            ("keywords_eng", "kw"),
            ("references_count", "3"),
            ("ref_value_0", "First"),
            ("ref_value_1", "Second"),
            ("ref_value_2", "Missed third"),
            ("authors_count", "0"),
        ]
    )
    out = apply_form_to_metadata(
        {"authors": [], "references_eng": ["a", "b"]},
        form,
    )
    assert out["references_eng"] == ["First", "Second", "Missed third"]


def test_form_references_preserve_middle_insert_order():
    """Вставка в середину: индексы 0..n сохраняют порядок при сохранении."""
    from werkzeug.datastructures import MultiDict

    from ipsas.modules.eng_metadata.forms import apply_form_to_metadata

    form = MultiDict(
        [
            ("title_eng", "T"),
            ("abstract_eng", "A"),
            ("keywords_eng", "kw"),
            ("references_count", "3"),
            ("ref_value_0", "First"),
            ("ref_value_1", "Inserted middle"),
            ("ref_value_2", "Was second"),
            ("authors_count", "0"),
        ]
    )
    out = apply_form_to_metadata(
        {"authors": [], "references_eng": ["First", "Was second"]},
        form,
    )
    assert out["references_eng"] == ["First", "Inserted middle", "Was second"]


def test_form_preserves_ru_on_save():
    from werkzeug.datastructures import MultiDict

    from ipsas.modules.eng_metadata.forms import apply_form_to_metadata

    form = MultiDict(
        [
            ("title_eng", "Updated EN"),
            ("abstract_eng", "Updated abstract"),
            ("keywords_eng", "kw1"),
            ("references_count", "1"),
            ("ref_value_0", "Ref one."),
            ("authors_count", "1"),
            ("author_0_given_en", "A."),
            ("author_0_surname_en", "Author"),
            ("author_0_full_name_en", "A. Author"),
            ("author_0_email", "a@example.com"),
            ("author_0_orcid", ""),
            ("author_0_aff_count", "1"),
            ("author_0_aff_0_org_en", "Univ"),
            ("author_0_aff_0_address_en", "City"),
        ]
    )
    out = apply_form_to_metadata(_SAMPLE_JSON, form)
    assert out["title_ru"] == "Пример заголовка"
    assert out["abstract_ru"] == "Текст аннотации."
    assert out["authors"][0]["full_name_ru"] == "А. Автор"
    assert out["authors"][0]["affiliations"][0]["organization_ru"] == "Университет"


def test_form_keeps_multiple_affiliations():
    from werkzeug.datastructures import MultiDict

    from ipsas.modules.eng_metadata.forms import (
        apply_form_to_metadata,
        metadata_to_form_defaults,
    )

    base = {
        "title_eng": "T",
        "abstract_eng": "A",
        "keywords_eng": ["k"],
        "references_eng": [],
        "authors": [
            {
                "given_en": "A.",
                "surname_en": "Author",
                "full_name_en": "A. Author",
                "affiliations": [
                    {
                        "organization_en": "Univ One",
                        "organization_ru": "Универ 1",
                        "address_en": "City 1",
                    },
                    {
                        "organization_en": "Univ Two",
                        "organization_ru": "Универ 2",
                        "address_en": "City 2",
                    },
                ],
            }
        ],
    }
    defaults = metadata_to_form_defaults(base)
    assert len(defaults["authors"][0]["affiliations"]) == 2

    form = MultiDict(
        [
            ("title_eng", "T"),
            ("abstract_eng", "A"),
            ("keywords_eng", "k"),
            ("references_count", "0"),
            ("authors_count", "1"),
            ("author_0_given_en", "A."),
            ("author_0_surname_en", "Author"),
            ("author_0_full_name_en", "A. Author"),
            ("author_0_email", ""),
            ("author_0_orcid", ""),
            ("author_0_aff_count", "2"),
            ("author_0_aff_0_org_en", "Univ One EN"),
            ("author_0_aff_0_address_en", "City 1"),
            ("author_0_aff_1_org_en", "Univ Two EN"),
            ("author_0_aff_1_address_en", "City 2"),
        ]
    )
    out = apply_form_to_metadata(base, form)
    affs = out["authors"][0]["affiliations"]
    assert len(affs) == 2
    assert affs[0]["organization_en"] == "Univ One EN"
    assert affs[0]["organization_ru"] == "Универ 1"
    assert affs[1]["organization_en"] == "Univ Two EN"
    assert affs[1]["organization_ru"] == "Универ 2"


def test_upload_page(client):
    resp = client.get("/services/eng-metadata")
    assert resp.status_code == 200
    assert "англоязычных".encode("utf-8") in resp.data


def test_upload_and_review_flow(client, tmp_path):
    zpath = _make_zip(tmp_path / "flow.zip")
    with zpath.open("rb") as fh:
        resp = client.post(
            "/services/eng-metadata/upload",
            data={"zip_file": (fh, "flow.zip")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
    assert resp.status_code in (302, 303)
    loc = resp.headers["Location"]
    assert "/eng-metadata/session/" in loc
    session_id = loc.rstrip("/").split("/")[-1]

    listing = client.get(f"/services/eng-metadata/session/{session_id}")
    assert listing.status_code == 200
    assert b"288752" in listing.data

    review = client.get(
        f"/services/eng-metadata/session/{session_id}/article/288752"
    )
    assert review.status_code == 200
    assert b"Sample Title" in review.data or "Sample Title".encode() in review.data
    body = review.get_data(as_text=True)
    assert "Источник 1" in body
    assert "Источник 2" in body
    assert 'id="eng-refs-count-num"' in body
    assert ">2<" in body or "eng-refs-count-num\">2" in body
    assert "источника" in body
    assert 'name="ref_value_0"' in body
    assert 'name="ref_value_1"' in body
    assert "+ Вставить здесь" in body or "eng-refs-add" in body
    assert "Ссылка на статью (справочно)" in body
    assert _SAMPLE_JSON["article_url"] in body
    assert 'name="article_url"' not in body
    assert "Редактировать как JSON" not in body
    assert "RU (справочно)" in body
    assert "Пример заголовка" in body
    assert "А. Автор" in body
    assert "Университет" in body
    assert "Текст аннотации." in body

    pdf = client.get(
        f"/services/eng-metadata/session/{session_id}/article/288752/pdf"
    )
    assert pdf.status_code == 200
    assert pdf.mimetype == "application/pdf"
    assert pdf.data.startswith(b"%PDF")

    save = client.post(
        f"/services/eng-metadata/session/{session_id}/article/288752/save",
        data={
            "title_eng": "Updated Title",
            "abstract_eng": "Abstract text.",
            "keywords_eng": "kw1\nkw2",
            "references_count": "2",
            "ref_value_0": "Ref one.",
            "ref_value_1": "Ref two.",
            "date_received": "2024-01-01",
            "date_revised": "",
            "date_accepted": "2024-02-01",
            "authors_count": "1",
            "author_0_given_en": "A.",
            "author_0_surname_en": "Author",
            "author_0_full_name_en": "A. Author",
            "author_0_email": "a@example.com",
            "author_0_orcid": "",
            "author_0_aff_count": "1",
            "author_0_aff_0_org_en": "Univ",
            "author_0_aff_0_address_en": "City",
        },
        follow_redirects=False,
    )
    assert save.status_code in (302, 303)
    data = load_article_json(session_id, "288752")
    assert data["title_eng"] == "Updated Title"

    # prepare через ту же форму (form_action=prepare), без formaction
    prep = client.post(
        f"/services/eng-metadata/session/{session_id}/article/288752/save",
        data={
            "form_action": "prepare",
            "title_eng": "Prepared From Form",
            "abstract_eng": "Abstract from prepare.",
            "keywords_eng": "alpha",
            "references_count": "1",
            "ref_value_0": "Only ref.",
            "date_received": "2024-01-01",
            "date_revised": "",
            "date_accepted": "2024-02-01",
            "authors_count": "1",
            "author_0_given_en": "A.",
            "author_0_surname_en": "Author",
            "author_0_full_name_en": "A. Author",
            "author_0_email": "a@example.com",
            "author_0_orcid": "",
            "author_0_aff_count": "1",
            "author_0_aff_0_org_en": "Univ",
            "author_0_aff_0_address_en": "City",
        },
        follow_redirects=True,
    )
    assert prep.status_code == 200
    data = load_article_json(session_id, "288752")
    assert data["title_eng"] == "Prepared From Form"
    assert data["references_eng"] == ["Only ref."]
    body = prep.get_data(as_text=True)
    assert (
        "Подготовка к отправке" in body
        or "подготовлены локально" in body
        or "К сверке" in body
        or "отправлено" in body.lower()
        or "Платформ" in body
        or "Prepared From Form" in body
    )
