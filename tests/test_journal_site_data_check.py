"""Тесты проверки OJS .data по чек-листу «Настройка журнала по умолчанию»."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest

from ipsas.modules.journal_site.data_check import (
    build_data_report_dict,
    evaluate_checklist_item,
    _detect_flags,
)
from ipsas.modules.journal_site.data_export import parse_journal_data
from ipsas.modules.journal_site.default_setup_checklist import DEFAULT_SETUP_CHECKLIST
from ipsas.modules.journal_site.editorial_letter import (
    build_journal_site_editorial_letter,
    build_journal_site_editorial_letter_html,
)
from ipsas.services.check_journal_site import execute as check_journal_site
from ipsas.web.app import create_app

FIXTURE = Path(__file__).parent / "fixtures" / "journal_sample.data.json"


@pytest.fixture()
def sample_bytes() -> bytes:
    return FIXTURE.read_bytes()


@pytest.fixture()
def sample_export(sample_bytes: bytes):
    return parse_journal_data(sample_bytes)


def test_parse_journal_data_locales_and_plugins(sample_export) -> None:
    assert sample_export.path == "9999-0001"
    assert sample_export.journal_title("ru") == "Тестовый журнал"
    assert sample_export.plugins["dimensionsplugin"].enabled is True
    assert sample_export.plugins["altmetricsplugin"].enabled is False


def test_checklist_catalog_non_empty() -> None:
    assert len(DEFAULT_SETUP_CHECKLIST) >= 40
    ids = [i.id for i in DEFAULT_SETUP_CHECKLIST]
    assert "modules.doi" in ids
    assert "step1.title" in ids
    assert "pages.section_articles" in ids


def test_acron_present_with_empty_settings_is_ok(sample_export) -> None:
    # как в реальном .data: settings=[] → enabled=False, но плагин установлен
    from ipsas.modules.journal_site.data_export import PluginInfo

    sample_export.plugins["acronPlugin"] = PluginInfo(
        name="acronPlugin", category="generic", enabled=False, settings={}
    )
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "modules.acron")
    assert evaluate_checklist_item(sample_export, item, ctx)["status"] == "ok"


def test_doi_ok_and_altmetrics_off(sample_export) -> None:
    ctx = _detect_flags(sample_export)
    doi = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "modules.doi")
    alt = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "metrics.altmetrics")
    assert evaluate_checklist_item(sample_export, doi, ctx)["status"] == "ok"
    assert evaluate_checklist_item(sample_export, alt, ctx)["status"] == "ok"


def test_doi_without_prefix_fails(sample_export) -> None:
    sample_export.plugins["DOIPubIdPlugin"].settings["doiPrefix"] = ""
    ctx = _detect_flags(sample_export)
    doi = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "modules.doi")
    assert evaluate_checklist_item(sample_export, doi, ctx)["status"] == "fail"


def test_altmetrics_enabled_fails(sample_export) -> None:
    sample_export.plugins["altmetricsplugin"].enabled = True
    sample_export.plugins["altmetricsplugin"].settings = {"enabled": True}
    ctx = _detect_flags(sample_export)
    alt = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "metrics.altmetrics")
    assert evaluate_checklist_item(sample_export, alt, ctx)["status"] == "fail"


def test_author_guidelines_empty_en(sample_export) -> None:
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step3.guidelines")
    result = evaluate_checklist_item(sample_export, item, ctx)
    assert result["status"] in {"warn", "fail"}


def test_articles_section_rules(sample_export) -> None:
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "pages.section_articles")
    sample_export.sections.insert(
        0,
        {
            "title": {"ru_RU": "Статьи", "en_US": "Articles"},
            "abbrev": {"ru_RU": "СТ", "en_US": "ART"},
            "hideAbout": "1",
            "editorRestriction": "1",
        },
    )
    assert evaluate_checklist_item(sample_export, item, ctx)["status"] == "ok"


def test_indexing_and_history_detected_are_human_readable() -> None:
    from ipsas.modules.journal_site.checklist_messages import enrich_result_row

    indexing = enrich_result_row(
        {
            "id": "step1.indexing_kw",
            "title": "1.8 Индексация",
            "status": "ok",
            "severity": "required",
            "actual": (
                "searchDescription/ru, searchDescription/en, "
                "searchKeywords/ru, searchKeywords/en"
            ),
            "note": "Заполнено.",
        }
    )
    assert "searchDescription" not in indexing["detected"]
    assert "searchKeywords" not in indexing["detected"]
    assert "описание для индексации" in indexing["detected"]
    assert "ключевые слова" in indexing["detected"]
    assert "русский" in indexing["detected"]

    history = enrich_result_row(
        {
            "id": "step1.history",
            "title": "1.9 История",
            "status": "ok",
            "severity": "info",
            "actual": "history/ru; history/en",
            "note": "Заполнено.",
        }
    )
    assert "history/" not in history["detected"]
    assert "история журнала" in history["detected"]
    assert "русский" in history["detected"]
    assert "английский" in history["detected"]

    thumb = enrich_result_row(
        {
            "id": "step5.thumbnail",
            "title": "5.1 Миниатюра",
            "status": "ok",
            "severity": "required",
            "actual": "journalThumbnail/ru, journalThumbnail/en",
        }
    )
    assert "journalThumbnail" not in thumb["detected"]
    assert "миниатюра журнала" in thumb["detected"]

    cover = enrich_result_row(
        {
            "id": "step5.cover",
            "title": "5.2 Обложка",
            "status": "ok",
            "severity": "required",
            "actual": "homepageImage/ru, homepageImage/en",
        }
    )
    assert "homepageImage" not in cover["detected"]
    assert "обложка на главной" in cover["detected"]


def test_plugin_detected_labels_are_human_readable(sample_export) -> None:
    from ipsas.modules.journal_site.checklist_messages import enrich_result_row

    ctx = _detect_flags(sample_export)
    for iid in ("modules.edn", "modules.urn", "modules.url_pubid"):
        item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == iid)
        row = enrich_result_row(evaluate_checklist_item(sample_export, item, ctx))
        detected = (row.get("detected") or "").lower()
        assert "plugin" not in detected
        assert "нет в экспорте" not in detected
        assert detected and detected != "—"


def test_privacy_statement_required(sample_export) -> None:
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step2.privacy")
    assert item.setting_keys == ("privacyStatement",)

    sample_export.settings["privacyStatement"] = {
        "ru_RU": "Журнал соблюдает конфиденциальность персональных данных авторов и рецензентов.",
        "en_US": "The journal respects the confidentiality of personal data of authors and reviewers.",
    }
    assert evaluate_checklist_item(sample_export, item, ctx)["status"] == "ok"

    sample_export.settings["privacyStatement"] = {"ru_RU": "коротко", "en_US": ""}
    bad = evaluate_checklist_item(sample_export, item, ctx)
    assert bad["status"] in {"warn", "fail"}


def test_primary_contact_requires_name_and_email(sample_export) -> None:
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step1.board")
    assert item.kind == "primary_contact"
    ok = evaluate_checklist_item(sample_export, item, ctx)
    assert ok["status"] == "ok"

    sample_export.settings["contactEmail"] = ""
    sample_export.settings["contactName"] = {"ru_RU": "Иванов", "en_US": ""}
    bad = evaluate_checklist_item(sample_export, item, ctx)
    assert bad["status"] in {"warn", "fail"}
    assert "contact.email" in bad["deficits"]
    assert "contact.name_en" in bad["deficits"]


def test_indexing_requires_description_and_keywords(sample_export) -> None:
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step1.indexing_kw")
    assert "searchDescription" in item.setting_keys
    assert "searchKeywords" in item.setting_keys

    sample_export.settings["searchDescription"] = {
        "ru_RU": "Описание журнала для поиска",
        "en_US": "Journal search description",
    }
    sample_export.settings["searchKeywords"] = {
        "ru_RU": "философия, антропология",
        "en_US": "philosophy, anthropology",
    }
    assert evaluate_checklist_item(sample_export, item, ctx)["status"] == "ok"

    sample_export.settings["searchKeywords"] = {"ru_RU": "", "en_US": ""}
    partial = evaluate_checklist_item(sample_export, item, ctx)
    assert partial["status"] in {"warn", "fail"}
    assert any(str(d).startswith("searchKeywords") for d in partial["deficits"])


def test_focus_accepts_focus_scope_desc(sample_export) -> None:
    """В экспортах RAS предметная область часто в focusScopeDesc, не в focusAndScope."""
    sample_export.settings.pop("focusAndScope", None)
    sample_export.settings["focusScopeDesc"] = {
        "ru_RU": "Журнал публикует исследования по прикладной математике и смежным областям.",
        "en_US": "The journal publishes research in applied mathematics and related fields.",
    }
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step2.focus")
    assert "focusScopeDesc" in item.setting_keys
    row = evaluate_checklist_item(sample_export, item, ctx)
    assert row["status"] == "ok"


def test_browse_requires_sections_mode(sample_export) -> None:
    from ipsas.modules.journal_site.data_export import PluginInfo

    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "modules.browse")
    ctx = _detect_flags(sample_export)

    sample_export.plugins["browseplugin"] = PluginInfo(
        name="browseplugin",
        category="generic",
        enabled=True,
        settings={"enableBrowseBySections": True, "enableBrowseByIdentifyTypes": False},
    )
    ok = evaluate_checklist_item(sample_export, item, ctx)
    assert ok["status"] == "ok"
    assert "раздел" in (ok.get("note") or "").lower()

    sample_export.plugins["browseplugin"] = PluginInfo(
        name="browseplugin",
        category="generic",
        enabled=True,
        settings={"enableBrowseBySections": False},
    )
    bad = evaluate_checklist_item(sample_export, item, ctx)
    assert bad["status"] == "fail"
    assert "раздел" in (bad.get("note") or "").lower()


def test_recognition_reports_disabled_when_present(sample_export) -> None:
    from ipsas.modules.journal_site.data_export import PluginInfo

    sample_export.plugins["ResolverPlugin"] = PluginInfo(
        name="ResolverPlugin", category="generic", enabled=False, settings={}
    )
    for key in ("referralplugin", "RecommendBySimilarityPlugin"):
        sample_export.plugins[key] = PluginInfo(
            name=key, category="generic", enabled=False, settings={}
        )
    ctx = _detect_flags(sample_export)
    item = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "modules.recognition")
    row = evaluate_checklist_item(sample_export, item, ctx)
    assert row["status"] == "fail"
    assert "выключен" in (row.get("actual") or "").lower()


def test_build_data_report_dict(sample_bytes: bytes) -> None:
    export = parse_journal_data(sample_bytes)
    report = build_data_report_dict(export, source_name="journal_sample.data.json")
    assert report["check_mode"] == "data"
    assert report["checklist_name"]
    assert report["checklist_sections"]
    assert report["completeness_percent"] >= 0
    assert "mandatory_percent" in report
    assert "must_fix_count" in report
    assert "conclusion" in report
    assert report["fields"]
    assert "ui_status" in report["fields"][0]
    assert "action" in report["fields"][0]


def test_fix_guides_attached_to_failing_items(sample_bytes: bytes) -> None:
    from ipsas.modules.journal_site.checklist_messages import enrich_result_row
    from ipsas.modules.journal_site.fix_guides import get_fix_guide

    export = parse_journal_data(sample_bytes)
    report = build_data_report_dict(export, source_name="sample.data")
    fail_rows = [
        r for r in report["fields"] if r.get("ui_status") in {"fail", "partial", "manual"}
    ]
    assert fail_rows
    for row in fail_rows:
        assert row.get("fix_path"), f"missing fix_path for {row.get('id')}"
        guide = get_fix_guide(str(row["id"]), section=str(row.get("section") or ""))
        assert guide is not None
        enriched = enrich_result_row(dict(row))
        assert enriched["fix_path"] == guide.path
        if guide.steps:
            assert enriched["fix_steps"] == list(guide.steps)


def test_editorial_letter_for_data_checklist(sample_bytes: bytes) -> None:
    export = parse_journal_data(sample_bytes)
    report = build_data_report_dict(export, source_name="sample.data")
    fake_must = [
        {
            "id": "step1.abbreviation",
            "title": "1.1 Сокращённое название",
            "section": "step1",
            "status": "fail",
            "severity": "required",
            "note": "Не заполнено: abbreviation/ru, abbreviation/en",
            "deficits": ["abbreviation/ru", "abbreviation/en"],
        },
        {
            "id": "step1.elibrary",
            "title": "ID eLibrary",
            "section": "step1",
            "status": "fail",
            "severity": "required",
            "note": "пусто",
            "deficits": ["elibraryId"],
        },
        {
            "id": "pages.editorial_board",
            "title": "Редакция",
            "section": "pages",
            "status": "fail",
            "severity": "required",
            "deficits": ["board.both"],
        },
        {
            "id": "step1.board",
            "title": "1.2 Редакция",
            "section": "step1",
            "status": "fail",
            "severity": "required",
            "deficits": ["contact.name_ru", "contact.name_en", "contact.email"],
        },
    ]
    report["must_fix"] = fake_must
    report["staff_must_fix"] = fake_must
    report["staff_partial"] = []
    report["recommendations"] = [
        {"id": "modules.acron", "title": "ACRON", "status": "fail", "note": "выкл"},
    ]
    text = build_journal_site_editorial_letter(report)
    assert "Уважаемые коллеги!" in text
    assert "Национальной платформе" in text
    assert "просим внести следующие изменения" in text
    assert "Указать сокращённое название журнала" in text
    assert "eLIBRARY.RU" in text
    assert "Заполнить раздел «Редакция»" in text
    assert "контактного лица" in text.lower()
    assert "Где исправить:" in text
    assert "Личный кабинет" in text
    assert "Как исправить:" in text
    assert "boardCustomText" not in text
    assert "abbreviation/ru" not in text
    assert "RU/EN" not in text
    assert "ACRON" not in text
    assert "Дата проверки:" in text
    # дата ближе к концу, после списка
    assert text.index("просим внести") < text.index("Дата проверки:")

    html = build_journal_site_editorial_letter_html(report)
    assert "<strong>«" in html
    assert "<ol>" in html
    assert "<li>" in html
    assert "Где исправить:" in html
    assert "Как исправить:" in html
    assert "docs.rfbr.ru" in html
    assert "javascript:" not in html.lower()
    assert "boardCustomText" not in html
    assert html.index("просим внести") < html.index("Дата проверки")


def test_letter_uses_actual_deficits_not_full_check(sample_export) -> None:
    from ipsas.modules.journal_site.checklist_messages import enrich_result_row, letter_fix_actions

    articles = enrich_result_row(
        {
            "id": "pages.section_articles",
            "title": "Раздел Статьи",
            "section": "pages",
            "status": "fail",
            "severity": "required",
            "note": "Проблемы: нет ограничения «только редакторы».",
            "actual": "title=Статьи/Articles; abbrev=СТ/ART; hideAbout=True; editorRestriction=False",
            "deficits": ["section_articles.editor_restriction"],
            "doc_url": "https://docs.rfbr.ru/doc/razdely-i-rubriki-zhurnala-8EwNvt3xjH",
        }
    )
    assert "только редакторы" in articles["action"]
    assert "сокращен" not in articles["action"].lower()
    assert "название" not in articles["action"].lower()

    checklist = enrich_result_row(
        {
            "id": "step3.checklist",
            "title": "Список требований",
            "section": "step3",
            "status": "warn",
            "severity": "required",
            "actual": "ru=6, en=0",
            "deficits": ["checklist.en"],
        }
    )
    assert "английском" in checklist["action"]
    assert "Русская версия списка уже заполнена" in checklist["action"]
    assert "русском и английском" not in checklist["action"]

    report = {
        "staff_must_fix": [articles],
        "staff_partial": [checklist],
        "journal_title": "Abyss",
        "check_mode": "data",
        "generated_at": "03.09.2026, 09:20",
    }
    text = build_journal_site_editorial_letter(report)
    assert "только редакторы" in text
    assert "сокращённое название раздела" not in text.lower()
    assert "английском языке" in text
    assert "Русская версия списка уже заполнена" in text
    assert "3 сентября 2026 года" in text
    assert "Где исправить:" in text
    assert "Разделы" in text
    actions = letter_fix_actions(report)
    assert len(actions) == 2
    html = build_journal_site_editorial_letter_html(report)
    assert "<strong>«Abyss»</strong>" in html
    assert "Где исправить:" in html
    assert "docs.rfbr.ru" in html
    assert "javascript:" not in html.lower()
    assert "<ol>" in html
    assert "<p><strong>Дата проверки:</strong> 3 сентября 2026 года.</p>" in html


def test_home_header_does_not_use_page_header(sample_export) -> None:
    from ipsas.modules.journal_site.data_check import evaluate_checklist_item, _detect_flags
    from ipsas.modules.journal_site.default_setup_checklist import DEFAULT_SETUP_CHECKLIST
    from ipsas.modules.journal_site.checklist_messages import enrich_result_row

    sample_export.settings["homeHeaderTitle"] = {"ru_RU": "Abyss", "en_US": "Abyss"}
    sample_export.settings["pageHeaderTitle"] = {"ru_RU": "", "en_US": ""}
    ctx = _detect_flags(sample_export)
    home = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step5.home_header")
    page = next(i for i in DEFAULT_SETUP_CHECKLIST if i.id == "step5.page_header")
    home_row = enrich_result_row(evaluate_checklist_item(sample_export, home, ctx))
    page_row = enrich_result_row(evaluate_checklist_item(sample_export, page, ctx))
    assert home_row["status"] == "ok"
    assert home_row["ui_status"] == "ok"
    assert "pageHeaderTitle" not in (home_row.get("actual") or "")
    assert page_row["status"] in {"fail", "warn"}
    assert page_row["bucket"] in {"fix", "partial"}


def test_letter_items_follow_checklist_order() -> None:
    from ipsas.modules.journal_site.checklist_messages import letter_fix_actions

    report = {
        "staff_must_fix": [
            {
                "id": "step5.dates",
                "section": "step5",
                "status": "fail",
                "severity": "required",
            },
            {
                "id": "step3.guidelines",
                "section": "step3",
                "status": "fail",
                "severity": "required",
            },
            {
                "id": "step3.copyright",
                "section": "step3",
                "status": "fail",
                "severity": "required",
                "deficits": [
                    "copyright.notice_ru",
                    "copyright.notice_en",
                    "copyright.holder",
                    "copyright.license",
                ],
            },
        ],
        "staff_partial": [
            {
                "id": "step3.checklist",
                "section": "step3",
                "status": "warn",
                "severity": "required",
                "actual": "ru=6, en=0",
                "deficits": ["checklist.en"],
            }
        ],
        "check_mode": "data",
        "journal_title": "Abyss",
        "generated_at": "03.09.2026, 09:20",
    }
    actions = letter_fix_actions(report)
    joined = "\n".join(actions)
    assert joined.index("правила для авторов") < joined.index("список требований")
    assert joined.index("список требований") < joined.index("условия использования")
    assert "указать правообладателя и указать" not in joined
    assert "указать правообладателя и сведения о лицензии" in joined
    assert joined.index("условия использования") < joined.index("даты")


def test_staff_summary_separates_checks_and_unique_remarks() -> None:
    from ipsas.modules.journal_site.checklist_messages import build_staff_summary

    rows = [
        {
            "id": "pages.editorial_board",
            "section": "pages",
            "status": "fail",
            "severity": "required",
            "deficits": ["board.both"],
        },
        {
            "id": "step1.board",
            "section": "step1",
            "status": "fail",
            "severity": "required",
            "deficits": ["contact.email"],
        },
        {
            "id": "step1.title",
            "section": "step1",
            "status": "ok",
            "severity": "required",
        },
    ]
    staff = build_staff_summary(rows)
    assert staff["fail_check_count"] == 2
    assert staff["must_fix_count"] == 2


def test_staff_summary_buckets(sample_bytes: bytes) -> None:
    from ipsas.modules.journal_site.checklist_messages import enrich_result_row

    row = enrich_result_row(
        {
            "id": "step3.checklist",
            "title": "3.2 Список требований к статьям",
            "section": "step3",
            "status": "warn",
            "severity": "required",
            "note": "Список есть не на обоих языках",
            "actual": "ru=6, en=0",
            "deficits": ["checklist.en"],
        }
    )
    assert row["ui_status"] == "warn"
    assert row["bucket"] == "partial"
    assert "RU: 6" in row["detected"] or "русский: 6" in row["detected"]
    assert "EN: отсутствует" in row["detected"] or "английский: отсутствует" in row["detected"]
    assert "английск" in row["action"].lower()
    assert "русском и английском" not in row["action"]

    issn = enrich_result_row(
        {
            "id": "step1.issn",
            "title": "ISSN",
            "section": "step1",
            "status": "warn",
            "severity": "required",
            "actual": "печатный=нет; онлайн=да",
            "deficits": ["issn.print"],
        }
    )
    assert issn["ui_status"] == "manual"
    assert issn["bucket"] == "manual"
    assert "печатный ISSN" in issn["action"]


def test_execute_data_only(sample_bytes: bytes) -> None:
    report = check_journal_site(None, data_file=sample_bytes, data_filename="sample.data")
    assert report["check_mode"] == "data"
    assert report["checklist_sections"]


def test_execute_requires_data_file() -> None:
    with pytest.raises(ValueError, match=r"\.data"):
        check_journal_site(None, data_file=None)


def test_execute_rejects_url_only() -> None:
    with pytest.raises(ValueError, match="ссылке"):
        check_journal_site("https://journals.example/2312-1327/index", data_file=None)


def test_upload_route_accepts_data_file(sample_bytes: bytes) -> None:
    app = create_app(testing=True)
    client = app.test_client()
    resp = client.post(
        "/services/journal-site-check/process",
        data={
            "data_file": (BytesIO(sample_bytes), "journal_sample.data"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Результат проверки" in body or "Тестовый журнал" in body
    assert "Копировать с форматированием" in body
    assert "Скачать TXT" in body
    assert "data-letter-html" in body


def test_upload_rejects_url_only() -> None:
    app = create_app(testing=True)
    client = app.test_client()
    resp = client.post(
        "/services/journal-site-check/process",
        data={"journal_url": "https://journals.example/2312-1327/index"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Загрузите файл" in body or ".data" in body


def test_upload_rejects_bad_extension() -> None:
    app = create_app(testing=True)
    client = app.test_client()
    resp = client.post(
        "/services/journal-site-check/process",
        data={"data_file": (BytesIO(b"{}"), "notes.txt")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert resp.status_code == 200
