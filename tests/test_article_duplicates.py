"""Тесты поиска дублей статей."""

from __future__ import annotations

from pathlib import Path

import pytest

from ipsas.config.settings import reset_settings
from ipsas.modules.article_duplicates.models import QueueSubmission, XmlArticle
from ipsas.modules.article_duplicates.details import (
    metadata_fields_to_detail,
    parse_submission_detail,
    parse_view_metadata_fields,
    view_metadata_url,
)
from ipsas.modules.article_duplicates.queues import parse_queue_submissions
from ipsas.modules.article_duplicates.service import (
    archive_submission_skip_email,
    find_duplicates,
)
from ipsas.modules.article_duplicates.similarity import (
    annotate_uniqueness,
    author_similarity,
    author_tokens,
    authors_order_match,
    explain_match,
    qualifies,
    rank_candidates,
    score_pair,
    title_similarity,
)
from ipsas.modules.article_duplicates.issue_source import article_dict_to_xml_article
from ipsas.modules.article_duplicates.xml_source import load_xml_articles
from ipsas.web.app import create_app

FIXTURE_XML = Path(__file__).resolve().parent / "fixtures" / "journal_sample.xml"


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


def test_load_xml_articles_from_fixture():
    articles = load_xml_articles(FIXTURE_XML)
    assert len(articles) >= 1
    first = articles[0]
    assert "Моделирование" in first.title or "Modeling" in first.title_en
    assert "Иванов" in first.authors
    assert first.title_en
    assert "управление" in first.keywords
    assert "control" in first.keywords_en
    assert first.publication_date == "2025"


def test_author_tokens_and_full_match():
    assert "иванов" in author_tokens("Иванов И. И., Петров А. С.")
    score, full = author_similarity(
        "Иванов И. И., Петров А. С.",
        "Иванов И.И., Петров А.С.",
    )
    assert score == 1.0
    assert full is True

    score2, full2 = author_similarity(
        "Иванов И. И., Петров А. С.",
        "Иванов И. И., Орлов Д. И.",
    )
    assert 0 < score2 < 1
    assert full2 is False


def test_title_similarity_related_vs_noise():
    related_a = "Влияние температуры на механические свойства полимерных композитов"
    related_b = "Влияние температуры на свойства полимерных композиционных материалов"
    noise_b = "Особенности преподавания математики в начальной школе"
    related = title_similarity(related_a, related_b)
    noise = title_similarity(related_a, noise_b)
    assert related > 0.55
    assert related > noise
    assert noise < 0.45


def test_truncated_queue_authors_and_title_still_match():
    xml = XmlArticle(
        index=1,
        title=(
            "Закономерности удерживания спиртов, кетонов и сахаров "
            "на сульфокатионообменнике в H+-, Ca2+- и La3+-формах"
        ),
        title_ru=(
            "Закономерности удерживания спиртов, кетонов и сахаров "
            "на сульфокатионообменнике в H+-, Ca2+- и La3+-формах"
        ),
        title_en="",
        authors=(
            "Лаптев А. Ю., Балашова С. С., Рожманова Н. Б., "
            "Севко А. В., Казакова В. Д., Нестеренко П. Н."
        ),
        abstract="",
    )
    sub = QueueSubmission(
        article_id=283834,
        submit_date="01.01.2024",
        section="ОС",
        authors="Лаптев, Балашова, Рожманова,...",
        title="ЗАКОНОМЕРНОСТИ УДЕРЖИВАНИЯ СПИРТОВ,...",
        url="/j/editor/submission/283834",
        queue="in_editing",
        queue_label="В редакции",
    )
    ranked = rank_candidates(xml, [sub], top_k=5)
    assert [c.submission.article_id for c in ranked] == [283834]
    assert ranked[0].authors_full_match is True
    assert ranked[0].title_score >= 0.55


def test_qualifies_full_match_by_author_count():
    # 3+ авторов: full match без title.
    assert qualifies(
        title_score=0.05,
        author_score=1.0,
        authors_full_match=True,
        ref_author_count=3,
    )
    # 2 автора: нужен слабый title/abstract.
    assert not qualifies(
        title_score=0.05,
        author_score=1.0,
        authors_full_match=True,
        abstract_score=0.0,
        ref_author_count=2,
    )
    assert qualifies(
        title_score=0.22,
        author_score=1.0,
        authors_full_match=True,
        ref_author_count=2,
    )
    # 1 автор: нужен заметный сигнал.
    assert not qualifies(
        title_score=0.10,
        author_score=1.0,
        authors_full_match=True,
        abstract_score=0.0,
        ref_author_count=1,
    )
    assert qualifies(
        title_score=0.30,
        author_score=1.0,
        authors_full_match=True,
        ref_author_count=1,
    )


def test_rank_candidates_authors_bypass_and_filters_noise():
    # 3 автора — full match проходит даже при чужом названии.
    xml = XmlArticle(
        index=1,
        title="Совершенно другое название статьи",
        title_ru="Совершенно другое название статьи",
        title_en="",
        authors="Сидорова Е. В., Кузнецов П. А., Орлова М. И.",
        abstract="",
    )
    same_authors = QueueSubmission(
        article_id=231,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Сидорова Е. В., Кузнецов П. А., Орлова М. И.",
        title="Адаптация растений к водному стрессу",
        url="/j/editor/submission/231",
        queue="unassigned",
        queue_label="Новые",
    )
    weak_author = QueueSubmission(
        article_id=500,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Сидорова Е. В., Орлов Д. И.",
        title="Совсем другая научная тема без пересечений",
        url="/j/editor/submission/500",
        queue="unassigned",
        queue_label="Новые",
    )
    noise = QueueSubmission(
        article_id=999,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Смирнов А. В.",
        title="Несвязанная тема про металлы",
        url="/j/editor/submission/999",
        queue="unassigned",
        queue_label="Новые",
    )
    ranked = rank_candidates(
        xml, [noise, weak_author, same_authors], top_k=5
    )
    ids = [c.submission.article_id for c in ranked]
    assert 231 in ids
    assert 999 not in ids
    assert 500 not in ids
    assert ranked[0].authors_full_match is True


def test_rank_candidates_single_author_needs_title_signal():
    xml = XmlArticle(
        index=1,
        title="Особенности лазерной абляции тонких плёнок",
        title_ru="Особенности лазерной абляции тонких плёнок",
        title_en="",
        authors="Иванов И. И.",
        abstract="",
    )
    unrelated = QueueSubmission(
        article_id=10,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Иванов И. И.",
        title="Методика преподавания математики в школе",
        url="/j/editor/submission/10",
        queue="unassigned",
        queue_label="Новые",
    )
    related = QueueSubmission(
        article_id=11,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Иванов И. И.",
        title="Особенности лазерной абляции тонких плёнок металлов",
        url="/j/editor/submission/11",
        queue="unassigned",
        queue_label="Новые",
    )
    ranked = rank_candidates(xml, [unrelated, related], top_k=5)
    ids = [c.submission.article_id for c in ranked]
    assert 11 in ids
    assert 10 not in ids


def test_explain_match_and_uniqueness_for_ideal_pair():
    xml = XmlArticle(
        index=8,
        title="Разделение форм селена и их определение с использованием сорбента",
        title_ru="Разделение форм селена и их определение с использованием сорбента",
        title_en="",
        authors="Дидух-Шадрина С.Л., Белякович Д., Лосев В., Метелица С.",
        abstract="",
    )
    sub = QueueSubmission(
        article_id=408280,
        submit_date="10.04.2026",
        section="Оригинальные статьи",
        authors="Дидух-Шадрина С. Л., Белякович Д., Лосев В., Метелица С.",
        title="Разделение форм селена и их определение с использованием сорбента",
        url="/j/editor/submission/408280",
        queue="in_review",
        queue_label="На рецензии",
    )
    assert authors_order_match(xml.authors, sub.authors) is True
    cand = score_pair(xml, sub)
    assert cand.authors_full_match is True
    assert cand.confidence == "high"
    assert any("Названия идентичны" in r or "почти" in r for r in cand.match_reasons)
    assert any("все 4 авторов" in r for r in cand.match_reasons)
    assert any("Порядок авторов" in r for r in cand.match_reasons)
    annotate_uniqueness([cand])
    assert cand.uniqueness == "unique"

    reasons, _ = explain_match(
        xml,
        sub,
        title_score=cand.title_score,
        author_score=cand.author_score,
        abstract_score=0.0,
        authors_full_match=True,
    )
    assert reasons


def test_rank_candidates_diversified_keeps_author_and_title_hits():
    xml = XmlArticle(
        index=1,
        title="Сорбционное концентрирование селена из водных растворов",
        title_ru="Сорбционное концентрирование селена из водных растворов",
        title_en="",
        authors="Петров А. А., Сидоров Б. Б., Козлов В. В.",
        abstract="",
    )
    # Сильное название, другие авторы.
    title_hit = QueueSubmission(
        article_id=1,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Новиков К. К.",
        title="Сорбционное концентрирование селена из водных растворов",
        url="/j/editor/submission/1",
        queue="unassigned",
        queue_label="Новые",
    )
    # Те же 3 автора, чужое название — проходит как full match ≥3.
    author_hit = QueueSubmission(
        article_id=2,
        submit_date="2024-01-01",
        section="Статьи",
        authors="Петров А. А., Сидоров Б. Б., Козлов В. В.",
        title="Совершенно иная тема про педагогику",
        url="/j/editor/submission/2",
        queue="unassigned",
        queue_label="Новые",
    )
    # Ещё несколько похожих по названию, чтобы забить слоты без diversity.
    extras = [
        QueueSubmission(
            article_id=100 + i,
            submit_date="2024-01-01",
            section="Статьи",
            authors=f"Автор{i} А. А.",
            title=f"Сорбционное концентрирование селена вариант {i}",
            url=f"/j/editor/submission/{100 + i}",
            queue="unassigned",
            queue_label="Новые",
        )
        for i in range(5)
    ]
    ranked = rank_candidates(
        xml, [title_hit, author_hit, *extras], top_k=5
    )
    ids = [c.submission.article_id for c in ranked]
    assert 1 in ids
    assert 2 in ids


def test_parse_queue_submissions_with_status_and_full_title_attr():
    html = """
    <table>
      <tr>
        <td>100</td>
        <td>2024-02-01</td>
        <td>Статьи</td>
        <td>Author A</td>
        <td><a href="/0320-7919/editor/submission/100"
               title="Полное длинное название статьи кандидата">Title A…</a></td>
        <td>Published</td>
      </tr>
      <tr>
        <td>101</td>
        <td>2024-02-02</td>
        <td>Статьи</td>
        <td>Author B</td>
        <td><a href="/0320-7919/editor/submission/101">Title B</a></td>
      </tr>
    </table>
    """
    items = parse_queue_submissions(
        html, queue="in_review", queue_label="На рецензии"
    )
    assert len(items) == 2
    assert items[0].is_published is True
    assert items[0].title == "Полное длинное название статьи кандидата"
    assert items[0].title_from_list.startswith("Title A")
    assert items[1].is_published is False
    assert items[1].article_id == 101


def test_parse_queue_submissions_editing_spacer_and_submissionEditing_url():
    html = """
    <tr valign="top" class="highlightCopyediting">
      <td>328129</td>
      <td>15.10.2025</td>
      <td>ОС</td>
      <td>Панюкова, Савонина, Марютина</td>
      <td style="white-space: nowrap;"></td>
      <td><a href="https://journals.rcsi.science/0044-4502/editor/submissionEditing/328129"
             class="action">Влияние физико-химических...</a></td>
      <td>&mdash;</td>
    </tr>
    """
    items = parse_queue_submissions(
        html, queue="in_editing", queue_label="В редакции"
    )
    assert len(items) == 1
    assert items[0].article_id == 328129
    assert "Панюкова" in items[0].authors
    assert items[0].url.endswith("/editor/submission/328129")
    assert "физико-химических" in items[0].title


def test_parse_submission_detail_fields():
    html = """
    <table class="data">
      <tr><td class="label">Название</td>
          <td class="value">Полное название рукописи без сокращений</td></tr>
      <tr><td class="label">Авторы</td>
          <td class="value">Иванов И. И., Петров П. П.</td></tr>
      <tr><td class="label">Раздел</td><td class="value">Статьи</td></tr>
      <tr><td class="label">Отправитель</td><td class="value">Сидоров С. С.</td></tr>
      <tr><td class="label">Статус</td><td class="value">На рецензии</td></tr>
      <tr><td class="label">Дата подачи</td><td class="value">2024-03-01</td></tr>
      <tr><td class="label">Аннотация</td>
          <td class="value">Текст аннотации кандидата.</td></tr>
      <tr><td class="label">Ключевые слова</td>
          <td class="value">микроскопия; идентификация</td></tr>
    </table>
    """
    detail = parse_submission_detail(html)
    assert detail["title"] == "Полное название рукописи без сокращений"
    assert "Иванов" in detail["authors"]
    assert detail["section"] == "Статьи"
    assert detail["sender"] == "Сидоров С. С."
    assert detail["ojs_status"] == "На рецензии"
    assert detail["submit_date"] == "2024-03-01"
    assert "аннотации" in detail["abstract"]
    assert "микроскопия" in detail["keywords"]


def test_parse_view_metadata_fields_and_url():
    html = """
    <form method="post" action="/0044-4502/editor/saveMetadata">
      <textarea name="title[ru_RU]">Русское название</textarea>
      <textarea name="title[en_US]">English title</textarea>
      <textarea name="abstract[ru_RU]">&lt;p&gt;Аннотация RU&lt;/p&gt;</textarea>
      <textarea name="abstract[en_US]">Abstract EN</textarea>
      <textarea name="subject[ru_RU]">слово1, слово2</textarea>
      <textarea name="subject[en_US]">kw1; kw2</textarea>
    </form>
    """
    fields = parse_view_metadata_fields(html)
    detail = metadata_fields_to_detail(fields)
    assert detail["title"] == "Русское название"
    assert detail["title_en"] == "English title"
    assert detail["abstract"] == "Аннотация RU"
    assert "<p>" not in detail["abstract"]
    assert "слово1" in detail["keywords"]
    assert "слово2" in detail["keywords"]
    assert "kw1" in detail["keywords_en"]
    assert view_metadata_url(
        "https://journals.rcsi.science/0044-4502/editor/submission/412082"
    ) == "https://journals.rcsi.science/0044-4502/editor/viewMetadata/412082"


def test_article_dict_to_xml_article_from_issue_page():
    item = article_dict_to_xml_article(
        {
            "title_ru": "Влияние температуры на свойства материалов",
            "title_en": "Temperature effects on materials",
            "authors_ru": ["Иванов И. И.", "Петров П. П."],
            "abstract_ru": "Краткое резюме исследования.",
            "keywords_ru": ["температура", "материалы"],
            "keywords_en": ["temperature", "materials"],
            "pages": "12-18",
            "publication_date_display": "15.03.2025",
        },
        index=3,
    )
    assert item is not None
    assert item.index == 3
    assert item.title.startswith("Влияние")
    assert item.title_en.startswith("Temperature")
    assert "Иванов" in item.authors
    assert item.abstract.startswith("Краткое")
    assert "температура" in item.keywords
    assert "temperature" in item.keywords_en
    assert item.pages == "12-18"
    assert item.publication_date == "15.03.2025"

    via_iso = article_dict_to_xml_article(
        {
            "title_ru": "Статья без display-даты",
            "publication_date": "2025-08-15",
        },
        index=1,
    )
    assert via_iso is not None
    assert via_iso.publication_date == "15.08.2025"

    via_fallback = article_dict_to_xml_article(
        {"title_ru": "Статья без собственной даты"},
        index=2,
        fallback_date="Том 80 № 3 (2025)",
    )
    assert via_fallback is not None
    assert via_fallback.publication_date == "Том 80 № 3 (2025)"

    assert article_dict_to_xml_article({"title_ru": "", "title_en": ""}, 1) is None


def test_find_duplicates_offline_corpus():
    class FakeAuth:
        base_url = "https://journals.rcsi.science"

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            raise AssertionError("сеть не нужна при готовом corpus")

        def request(self, *args, **kwargs):
            raise AssertionError("сеть не нужна")

    corpus = [
        QueueSubmission(
            article_id=10,
            submit_date="2024-01-01",
            section="Статьи",
            authors="Иванов И.И., Петров П.П.",
            title="Моделирование систем управления",
            url="/0320-7919/editor/submission/10",
            queue="in_review",
            queue_label="На рецензии",
        ),
        QueueSubmission(
            article_id=11,
            submit_date="2024-01-01",
            section="Статьи",
            authors="Другой А. А.",
            title="Совершенно иная тема",
            url="/0320-7919/editor/submission/11",
            queue="unassigned",
            queue_label="Новые",
        ),
    ]
    groups, used = find_duplicates(
        FakeAuth(),  # type: ignore[arg-type]
        journal="0320-7919",
        xml_path=FIXTURE_XML,
        submissions=corpus,
        delay=0,
    )
    assert used is corpus
    assert groups
    first = groups[0]
    assert first.candidates
    assert first.candidates[0].submission.article_id == 10
    assert first.candidates[0].submission.url.startswith("https://")


def test_find_duplicates_from_issue_articles_list():
    class FakeAuth:
        base_url = "https://journals.rcsi.science"

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            raise AssertionError("сеть не нужна при готовом corpus")

        def request(self, *args, **kwargs):
            raise AssertionError("сеть не нужна")

    articles = [
        XmlArticle(
            index=1,
            title="Моделирование систем управления",
            title_ru="Моделирование систем управления",
            title_en="",
            authors="Иванов И.И., Петров П.П.",
            abstract="",
        )
    ]
    corpus = [
        QueueSubmission(
            article_id=42,
            submit_date="2024-01-01",
            section="Статьи",
            authors="Иванов И.И., Петров П.П.",
            title="Моделирование систем управления",
            url="/0044-4502/editor/submission/42",
            queue="unassigned",
            queue_label="Новые",
        )
    ]
    groups, used = find_duplicates(
        FakeAuth(),  # type: ignore[arg-type]
        journal="0044-4502",
        articles=articles,
        submissions=corpus,
        delay=0,
    )
    assert used is corpus
    assert len(groups) == 1
    assert groups[0].candidates[0].submission.article_id == 42


def test_archive_skip_published_and_dry_run():
    class FakeAuth:
        base_url = "https://journals.rcsi.science"
        calls: list[str] = []

        def get_text(self, url: str, encoding: str = "utf-8") -> str:
            self.calls.append(url)
            if "unsuitableSubmission" in url:
                return """
                <form id="emailForm" action="/0320-7919/editor/email">
                  <input type="hidden" name="articleId" value="10" />
                  <textarea name="body">text</textarea>
                  <input type="submit" name="send[skip]" value="Пропустить" />
                </form>
                """
            return "<html></html>"

        def request(self, *args, **kwargs):
            raise AssertionError("dry_run не должен POST")

    published = QueueSubmission(
        article_id=5,
        submit_date="",
        section="",
        authors="",
        title="Pub",
        url="",
        queue="in_editing",
        queue_label="В редакции",
        ojs_status="Published",
    )
    active = QueueSubmission(
        article_id=10,
        submit_date="",
        section="",
        authors="",
        title="Active",
        url="/0320-7919/editor/submission/10",
        queue="unassigned",
        queue_label="Новые",
    )
    auth = FakeAuth()
    skipped = archive_submission_skip_email(
        auth, "0320-7919", published, dry_run=True  # type: ignore[arg-type]
    )
    assert skipped.status == "skipped_published"

    dry = archive_submission_skip_email(
        auth, "0320-7919", active, dry_run=True  # type: ignore[arg-type]
    )
    assert dry.status == "dry_run"
    assert any("unsuitableSubmission" in u for u in auth.calls)


def test_duplicates_page_renders(client):
    resp = client.get("/services/article-duplicates")
    assert resp.status_code == 200
    assert "Поиск дублей статей".encode("utf-8") in resp.data
    assert b'name="issue_url"' in resp.data
    assert "опубликованный выпуск".encode("utf-8") in resp.data
