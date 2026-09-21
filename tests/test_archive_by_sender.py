"""Тесты архивации «Новые» по отправителю."""

from __future__ import annotations

import pytest

from ipsas.config.settings import reset_settings
from ipsas.modules.archive_by_sender.models import SubmissionInfo
from ipsas.modules.archive_by_sender.parser import (
    build_skip_payload,
    extract_sender,
    parse_email_form,
    parse_journal_ref,
    parse_submissions,
    sender_matches,
)
from ipsas.modules.archive_by_sender.service import archive_submission, process_journal
from ipsas.services.archive_by_sender import parse_senders_text
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


def test_parse_journal_ref_variants():
    assert parse_journal_ref("0002-337X") == "0002-337X"
    assert (
        parse_journal_ref("https://journals.rcsi.science/0002-337X/") == "0002-337X"
    )
    assert (
        parse_journal_ref(
            "https://journals.rcsi.science/0002-337X/editor/submissions/submissionsUnassigned"
        )
        == "0002-337X"
    )
    assert parse_journal_ref("journals.rcsi.science/0005-2310/about") == "0005-2310"


def test_parse_journal_ref_rejects_empty():
    try:
        parse_journal_ref("   ")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_sender_matches_normalized():
    assert sender_matches(
        "  Сергей Николаевич Гусев ",
        ["Сергей Николаевич Гусев", "Другой"],
    )
    assert not sender_matches("Иван Иванов", ["Сергей Николаевич Гусев"])


def test_parse_senders_text_default_and_custom():
    defaults = parse_senders_text("")
    assert "Сергей Николаевич Гусев" in defaults
    custom = parse_senders_text("Алла\n\nБорис\n")
    assert custom == ["Алла", "Борис"]


def test_parse_submissions_and_sender():
    list_html = """
    <table>
      <tr>
        <td>129710</td>
        <td>2024-01-02</td>
        <td>Статьи</td>
        <td>Author A</td>
        <td><a href="/0002-337X/editor/submission/129710">Sample Title</a></td>
      </tr>
    </table>
    """
    items = parse_submissions(list_html)
    assert len(items) == 1
    assert items[0].article_id == 129710
    assert items[0].title == "Sample Title"

    detail = """
    <table>
      <tr><td class="label">Отправитель</td><td class="value">Сергей Николаевич Гусев</td></tr>
    </table>
    """
    assert extract_sender(detail) == "Сергей Николаевич Гусев"


def test_parse_email_form_skip_button():
    html = """
    <form id="emailForm" action="/0002-337X/editor/email" method="post">
      <input type="hidden" name="articleId" value="129710" />
      <textarea name="body">Hello</textarea>
      <input type="submit" name="send[skip]" value="Пропустить" />
      <input type="submit" name="send" value="Отправить" />
    </form>
    """
    form = parse_email_form(html)
    assert form.skip_field == "send[skip]"
    payload = build_skip_payload(form)
    assert payload["send[skip]"] == "Пропустить"
    assert payload["articleId"] == "129710"
    assert payload["body"] == "Hello"


class _FakeAuth:
    base_url = "https://journals.rcsi.science"

    def __init__(self, pages: dict[str, str], *, post_status: int = 200) -> None:
        self.pages = pages
        self.post_status = post_status
        self.posts: list[tuple[str, dict[str, str]]] = []

    def get_text(self, url: str, encoding: str = "utf-8") -> str:
        del encoding
        for key, html in self.pages.items():
            if key in url:
                return html
        raise AssertionError(f"unexpected GET {url}")

    def request(
        self,
        url: str,
        *,
        data: dict[str, str] | None = None,
        method: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes]:
        del method, headers
        if data is not None:
            self.posts.append((url, dict(data)))
            return self.post_status, url, b"ok"
        return 200, url, self.get_text(url).encode("utf-8")


def test_archive_submission_dry_run_and_skip():
    detail = """
    <td class="label">Отправитель</td><td>Сергей Николаевич Гусев</td>
    """
    reject = """
    <form id="emailForm" action="/0002-337X/editor/email" method="post">
      <input type="hidden" name="articleId" value="1" />
      <input type="submit" name="send[skip]" value="Пропустить" />
    </form>
    """
    auth = _FakeAuth(
        {
            "/editor/submission/1": detail,
            "unsuitableSubmission": reject,
        }
    )
    sub = SubmissionInfo(
        article_id=1,
        submit_date="",
        section="",
        authors="",
        title="T",
        url="/0002-337X/editor/submission/1",
    )
    result = archive_submission(
        auth,
        "0002-337X",
        sub,
        sender_names=["Сергей Николаевич Гусев"],
        dry_run=True,
    )
    assert result.status == "dry_run"
    assert auth.posts == []

    other = archive_submission(
        auth,
        "0002-337X",
        sub,
        sender_names=["Другой Человек"],
        dry_run=True,
    )
    assert other.status == "skipped_sender"


def test_archive_submission_archives_with_verify():
    detail = """
    <td class="label">Отправитель</td><td>Сергей Николаевич Гусев</td>
    """
    reject = """
    <form id="emailForm" action="/0002-337X/editor/email" method="post">
      <input type="hidden" name="articleId" value="1" />
      <input type="submit" name="send[skip]" value="Пропустить" />
    </form>
    """
    # после архивации список без статьи
    unassigned = "<table></table>"
    auth = _FakeAuth(
        {
            "/editor/submission/1": detail,
            "unsuitableSubmission": reject,
            "submissionsUnassigned": unassigned,
        }
    )
    sub = SubmissionInfo(
        article_id=1,
        submit_date="",
        section="",
        authors="",
        title="T",
        url="/0002-337X/editor/submission/1",
    )
    result = archive_submission(
        auth,
        "0002-337X",
        sub,
        sender_names=["Сергей Николаевич Гусев"],
        dry_run=False,
        verify=True,
    )
    assert result.status == "archived"
    assert len(auth.posts) == 1
    assert "send[skip]" in auth.posts[0][1]


def test_process_journal_limit(monkeypatch):
    calls: list[int] = []

    def _fake_fetch(auth, journal, *, delay=0.0):
        del auth, journal, delay
        return [
            SubmissionInfo(i, "", "", "", f"t{i}", f"/x/editor/submission/{i}")
            for i in range(1, 4)
        ]

    def _fake_archive(auth, journal, submission, **kwargs):
        del auth, journal, kwargs
        calls.append(submission.article_id)
        return type(
            "R",
            (),
            {
                "journal": "j",
                "article_id": submission.article_id,
                "title": submission.title,
                "sender": "s",
                "status": "dry_run",
                "message": "ok",
            },
        )()

    monkeypatch.setattr(
        "ipsas.modules.archive_by_sender.service.fetch_all_unassigned_submissions",
        _fake_fetch,
    )
    monkeypatch.setattr(
        "ipsas.modules.archive_by_sender.service.archive_submission",
        _fake_archive,
    )
    auth = _FakeAuth({})
    results = process_journal(
        auth,
        "0002-337X",
        sender_names=["s"],
        limit=2,
        dry_run=True,
        delay=0,
    )
    assert [r.article_id for r in results] == [1, 2]
    assert calls == [1, 2]


def test_archive_page_renders(client):
    resp = client.get("/services/archive-by-sender")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Архивация рукописей" in body
    assert "journal_url" in body
