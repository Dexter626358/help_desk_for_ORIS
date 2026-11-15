"""Тесты синхронизации разделов журнала из CSV."""

from __future__ import annotations

from pathlib import Path

import pytest

from ipsas.config.settings import reset_settings
from ipsas.modules.journal_sections.csv_io import load_sections_csv
from ipsas.modules.journal_sections.models import ExistingSection
from ipsas.modules.journal_sections.sections import (
    find_by_title_ru,
    is_articles_section,
    list_sections,
    normalize_title,
    order_sections_alpha_keep_articles,
    order_titles_alpha_keep_articles,
)
from ipsas.modules.journal_sections.service import sync_journal_sections
from ipsas.modules.sandbox_journal_setup.urls import parse_journal_url
from ipsas.web.app import create_app


def test_articles_section_stays_in_place():
    assert is_articles_section("Статьи", "СТАТ")
    assert is_articles_section("Articles", "ART")
    assert not is_articles_section("Гидрология", "Гидр")

    sections = [
        ExistingSection("1", "Хроника", "Хрон", ""),
        ExistingSection("2", "Статьи", "СТАТ", ""),
        ExistingSection("3", "Гидрология", "Гидр", ""),
        ExistingSection("4", "Метеорология", "Метео", ""),
    ]
    ordered = order_sections_alpha_keep_articles(sections)
    # «Статьи» были на индексе 1 — остаются там; остальные A→Я вокруг
    assert [s.title for s in ordered] == [
        "Гидрология",
        "Статьи",
        "Метеорология",
        "Хроника",
    ]
    assert order_titles_alpha_keep_articles(
        ["Хроника", "Статьи", "Гидрология"]
    ) == ["Гидрология", "Статьи", "Хроника"]


def test_parse_sections_manager_url():
    base, journal = parse_journal_url(
        "https://f23g45.rcsi.science/2079-7877/manager/sections"
    )
    assert base == "https://f23g45.rcsi.science"
    assert journal == "2079-7877"
    base2, journal2 = parse_journal_url(
        "https://journals.rcsi.science/2079-7877/manager/sections"
    )
    assert base2 == "https://journals.rcsi.science"
    assert journal2 == "2079-7877"


def test_resolve_auth_kind_sandbox_and_platform():
    from ipsas.services.journal_sections import is_sandbox_host, resolve_auth_kind

    class S:
        sandbox_base_url = "https://f23g45.rcsi.science"
        platform_base_url = "https://journals.rcsi.science"

    settings = S()  # type: ignore[arg-type]
    assert is_sandbox_host("https://f23g45.rcsi.science", settings.sandbox_base_url)
    assert resolve_auth_kind("https://f23g45.rcsi.science", settings) == "sandbox"  # type: ignore[arg-type]
    assert (
        resolve_auth_kind("https://journals.rcsi.science", settings) == "platform"  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="не поддерживается"):
        resolve_auth_kind("https://example.com", settings)  # type: ignore[arg-type]

SECTIONS_HTML = """
<table>
  <tr id="section-12">
    <td>Гидрология</td>
    <td>Гидр</td>
    <td><a href="/2079-7877/manager/editSection/12">Edit</a></td>
  </tr>
  <tr id="section-15">
    <td>Хроника</td>
    <td>Хрон</td>
    <td><a href="/2079-7877/manager/editSection/15">Edit</a></td>
  </tr>
</table>
"""

CREATE_FORM_HTML = """
<form method="post" action="/2079-7877/manager/updateSection">
  <input type="text" name="title[ru_RU]" value="" />
  <input type="text" name="abbrev[ru_RU]" value="" />
  <input type="text" name="policy[ru_RU]" value="" />
  <input type="text" name="title[en_US]" value="" />
  <input type="text" name="abbrev[en_US]" value="" />
  <input type="text" name="policy[en_US]" value="" />
</form>
"""


def test_normalize_and_list_sections():
    assert normalize_title("  Гидрология  ") == "гидрология"
    sections = list_sections(SECTIONS_HTML, "https://f23g45.rcsi.science/2079-7877/manager/sections")
    assert len(sections) == 2
    assert sections[0].section_id == "12"
    assert sections[0].title == "Гидрология"
    assert sections[0].abbrev == "Гидр"
    assert "editSection/12" in sections[0].edit_url
    found = find_by_title_ru(sections, "гидрология")
    assert found is not None and found.section_id == "12"
    assert find_by_title_ru(sections, "Метеорология") is None


FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "journal_sections.csv"


def test_load_sections_csv_sample():
    specs = load_sections_csv(FIXTURE_CSV)
    assert len(specs) == 8
    assert specs[0].number == 1
    assert specs[0].title_ru.startswith("Физическая география")
    assert specs[0].short_title_en == "PGLG"
    assert specs[-1].title_ru == "Хроника"


def test_load_sections_csv_validation(tmp_path: Path):
    bad = tmp_path / "bad.csv"
    bad.write_text("number;title_ru\n1;Только RU\n", encoding="utf-8")
    with pytest.raises(ValueError, match="нет колонок"):
        load_sections_csv(bad)


def test_sync_dry_run_creates_missing_only():
    class FakeAuth:
        base_url = "https://f23g45.rcsi.science"
        calls: list[str] = []

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            self.calls.append(url)
            if "createSection" in url:
                return CREATE_FORM_HTML
            return SECTIONS_HTML

        def request(
            self,
            url: str,
            *,
            data=None,
            method: str | None = None,
            headers=None,
        ):
            raise AssertionError("dry_run не должен POST")

    report = sync_journal_sections(
        FakeAuth(),  # type: ignore[arg-type]
        journal_url_raw="https://f23g45.rcsi.science/2079-7877",
        csv_path=FIXTURE_CSV,
        dry_run=True,
        delay=0,
    )
    assert report.error is None
    assert report.existing_count == 2
    by_title = {r.title_ru: r.status for r in report.results}
    assert by_title["Гидрология"] == "exists"
    assert by_title["Хроника"] == "exists"
    assert by_title["Метеорология"] == "dry_run"
    assert sum(1 for r in report.results if r.status == "dry_run") == 6
    assert sum(1 for r in report.results if r.status == "exists") == 2


def test_sync_apply_posts_create_form():
    posted: list[dict[str, str]] = []

    class FakeAuth:
        base_url = "https://f23g45.rcsi.science"

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            if "createSection" in url:
                return CREATE_FORM_HTML
            return SECTIONS_HTML

        def request(
            self,
            url: str,
            *,
            data=None,
            method: str | None = None,
            headers=None,
        ):
            as_dict = {}
            if isinstance(data, list):
                as_dict = {k: v for k, v in data}
            elif isinstance(data, dict):
                as_dict = dict(data)
            posted.append(as_dict)
            return 200, "https://f23g45.rcsi.science/2079-7877/manager/sections", b"ok"

    report = sync_journal_sections(
        FakeAuth(),  # type: ignore[arg-type]
        journal_url_raw="https://f23g45.rcsi.science/2079-7877",
        csv_path=FIXTURE_CSV,
        dry_run=False,
        delay=0,
    )
    assert report.ok
    assert report.to_dict()["created"] == 6
    assert len(posted) == 6
    first = posted[0]
    assert first["title[ru_RU]"]
    assert first["title[en_US]"]
    assert first["abbrev[ru_RU]"]
    assert first["abbrev[en_US]"]
    assert first.get("hideAbout") == "1"
    assert first.get("editorRestriction") == "1"


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


def test_journal_sections_page_renders(client):
    resp = client.get("/services/journal-sections")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Синхронизация разделов" in body
    assert "csv_file" in body
    assert "journal_url" in body
    assert "sort_alpha" in body


def test_sort_alpha_dry_run_message():
    class FakeAuth:
        base_url = "https://f23g45.rcsi.science"

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            return SECTIONS_HTML

        def request(self, *args, **kwargs):
            raise AssertionError("dry_run+sort не должен вызывать moveSection")

    report = sync_journal_sections(
        FakeAuth(),  # type: ignore[arg-type]
        journal_url_raw="https://f23g45.rcsi.science/2079-7877",
        csv_path=FIXTURE_CSV,
        dry_run=True,
        sort_alpha=True,
        delay=0,
    )
    assert report.sort_status == "dry_run"
    assert report.sort_message
    assert "Гидрология" in report.sort_message
    assert "будет порядок" in report.sort_message


def test_sort_alpha_only_without_csv():
    """Сортировку можно запускать без CSV."""
    unsorted = """
    <table>
      <tr id="section-15">
        <td>Хроника</td><td>Хрон</td>
        <td><a href="/j/manager/editSection/15">Edit</a></td>
      </tr>
      <tr id="section-12">
        <td>Гидрология</td><td>Гидр</td>
        <td><a href="/j/manager/editSection/12">Edit</a></td>
      </tr>
    </table>
    """

    class FakeAuth:
        base_url = "https://f23g45.rcsi.science"

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            return unsorted

        def request(self, *args, **kwargs):
            raise AssertionError("dry_run не должен POST/move")

    report = sync_journal_sections(
        FakeAuth(),  # type: ignore[arg-type]
        journal_url_raw="https://f23g45.rcsi.science/2079-7877",
        csv_path=None,
        dry_run=True,
        sort_alpha=True,
        delay=0,
    )
    assert report.error is None
    assert report.results == []
    assert report.sort_status == "dry_run"
    assert "Гидрология" in (report.sort_message or "")
    assert "Хроника" in (report.sort_message or "")


def test_neither_csv_nor_sort_is_error():
    class FakeAuth:
        base_url = "https://example.test"

        def get_text(self, *args, **kwargs):
            raise AssertionError("не должны ходить в сеть")

    report = sync_journal_sections(
        FakeAuth(),  # type: ignore[arg-type]
        journal_url_raw="https://f23g45.rcsi.science/2079-7877",
        csv_path=None,
        dry_run=True,
        sort_alpha=False,
    )
    assert report.error
    assert "CSV" in report.error


def test_sort_alpha_apply_calls_move_section(tmp_path: Path):
    moves: list[str] = []
    unsorted = """
    <table>
      <tr id="section-15">
        <td>Хроника</td><td>Хрон</td>
        <td><a href="/j/manager/editSection/15">Edit</a></td>
      </tr>
      <tr id="section-12">
        <td>Гидрология</td><td>Гидр</td>
        <td><a href="/j/manager/editSection/12">Edit</a></td>
      </tr>
    </table>
    """
    sorted_html = """
    <table>
      <tr id="section-12">
        <td>Гидрология</td><td>Гидр</td>
        <td><a href="/j/manager/editSection/12">Edit</a></td>
      </tr>
      <tr id="section-15">
        <td>Хроника</td><td>Хрон</td>
        <td><a href="/j/manager/editSection/15">Edit</a></td>
      </tr>
    </table>
    """

    class FakeAuth:
        base_url = "https://f23g45.rcsi.science"
        phase = 0

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            if self.phase == 0:
                return unsorted
            return sorted_html

        def request(
            self,
            url: str,
            *,
            data=None,
            method: str | None = None,
            headers=None,
        ):
            assert "moveSection" in url
            moves.append(url)
            self.phase = 1
            return 200, url, b"ok"

    csv_path = tmp_path / "two.csv"
    csv_path.write_text(
        "number;title_ru;short_title_ru;title_en;short_title_en\n"
        "1;Гидрология;Гидр;Hydrology;Hydr\n"
        "2;Хроника;Хрон;Chronicle;Chron\n",
        encoding="utf-8",
    )
    report = sync_journal_sections(
        FakeAuth(),  # type: ignore[arg-type]
        journal_url_raw="https://f23g45.rcsi.science/2079-7877",
        csv_path=csv_path,
        dry_run=False,
        sort_alpha=True,
        delay=0,
    )
    assert report.sort_status == "sorted"
    assert moves
    assert any("id=12" in u for u in moves)
