"""Тесты валидаторов метаданных выпуска."""

import re

from ipsas.modules.issue_metadata import validators as v


def test_validate_doi_double_slash_is_error() -> None:
    err = v.validate_doi("10.37791//2687-0649-2024-19-4-94-106")
    assert err is not None
    assert "//" in err or "двойной" in err.lower()


def test_validate_doi_ok() -> None:
    assert v.validate_doi("10.37791/2687-0649-2024-19-4-94-106") is None


def test_extract_pages_from_doi() -> None:
    assert v.extract_pages_from_doi("10.37791/2687-0649-2024-19-4-94-106") == (94, 106)


def test_keywords_language_mismatch_is_error() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["distributed computing", "geodistributed systems"],
        "keywords_en": ["распределенные вычисления", "геораспределенные системы"],
        "keywords_ru_count": 2,
        "keywords_en_count": 2,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_en": ["Ivanov I. I."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf", "lang": "RU", "locked": False}],
        "reference_first": "Ivanov, 2020.",
    }
    issues = v.build_article_issues(article)
    texts = [i["text"] for i in issues]
    assert any("ключевых слов" in t and "RU" in t for t in texts)
    assert any("ключевых слов" in t and "EN" in t for t in texts)
    assert any(i["severity"] == "error" and "ключевых" in str(i["text"]) for i in issues)


def test_mixed_script_in_reference_warning() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["вычисления"],
        "keywords_en": ["computing"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "reference_ru_first": "СKonyavsky V. A. Something",
    }
    issues = v.build_article_issues(article)
    assert any("кириллица" in str(i["text"]).lower() or "латиница" in str(i["text"]).lower() for i in issues)


def test_volume_not_required_for_issue_only_journal() -> None:
    warnings = v.build_issue_warnings(
        {
            "journal_title": "Журнал",
            "issue_title": "№ 2 (2024)",
            "uses_volume": False,
            "issue": "2",
            "year": "2024",
            "article_urls": ["https://example.com/article/view/1"] * 11,
            "article_count": 11,
            "issn": "1234-5678",
            "eissn": "1234-5679",
            "issue_galleys": [{"url": "https://example.com/issue.pdf"}],
            "cover_url": "https://example.com/cover.jpg",
            "issue_language": "ru",
            "publication_date": "2024-06-01",
        },
        articles=[],
    )
    texts = [str(w["text"]) for w in warnings]
    assert not any("Том" in t or "том" in t for t in texts)


def test_volume_required_when_journal_uses_volumes() -> None:
    warnings = v.build_issue_warnings(
        {
            "journal_title": "Журнал",
            "issue_title": "Том 19, № 4 (2024)",
            "uses_volume": True,
            "volume": None,
            "issue": "4",
            "year": "2024",
            "article_urls": ["u1"],
            "article_count": 1,
            "issn": "1234-5678",
            "eissn": "1234-5679",
            "issue_galleys": [{"url": "https://example.com/issue.pdf"}],
            "cover_url": "https://example.com/cover.jpg",
            "issue_language": "ru",
            "publication_date": "2024-06-01",
        },
        articles=[],
    )
    assert any(
        w.get("severity") == "error" and "Том предусмотрен" in str(w.get("text"))
        for w in warnings
    )


def test_same_article_duplicate_pdf_urls_not_flagged() -> None:
    """Один и тот же PDF в meta и pdf_files одной статьи — не дубль между статьями."""
    url = "https://journals.example/article/view/1/pdf"
    articles = [
        {
            "identifiers": {"doi": "10.1/a", "internal_id": "1", "pdf_url": url},
            "pdf_files": [{"url": url, "lang": "RU"}, {"url": url, "lang": "EN"}],
            "title_ru": "Статья номер один длинное название",
            "title_en": "Article one long enough title",
        },
        {
            "identifiers": {"doi": "10.1/b", "internal_id": "2", "pdf_url": url + "/other"},
            "pdf_files": [{"url": url + "/other"}],
            "title_ru": "Статья номер два длинное название",
            "title_en": "Article two long enough title",
        },
    ]
    warnings: list = []
    v._check_cross_article_duplicates(articles, warnings)
    assert not any("Дублирующийся PDF" in str(w.get("text")) for w in warnings)


def test_cross_article_shared_pdf_url_is_flagged() -> None:
    url = "https://journals.example/shared.pdf"
    articles = [
        {
            "identifiers": {"doi": "10.1/a", "internal_id": "1"},
            "pdf_files": [{"url": url}],
            "title_ru": "Статья номер один длинное название",
        },
        {
            "identifiers": {"doi": "10.1/b", "internal_id": "2"},
            "pdf_files": [{"url": url}],
            "title_ru": "Статья номер два длинное название",
        },
    ]
    warnings: list = []
    v._check_cross_article_duplicates(articles, warnings)
    assert any("Дублирующийся PDF URL у статей 1 и 2" in str(w.get("text")) for w in warnings)


def test_issn_same_and_missing_issue_pdf_warnings() -> None:
    warnings = v.build_issue_warnings(
        {
            "journal_title": "Journal",
            "journal_title_ru": "Журнал",
            "issue_title": "№ 1",
            "volume": "19",
            "issue": "4",
            "year": "2024",
            "article_urls": ["https://example.com/article/view/1"],
            "article_count": 1,
            "issn": "2687-0649",
            "eissn": "2687-0649",
            "issue_galleys": [],
        },
        articles=[],
    )
    texts = [str(w["text"]) for w in warnings]
    assert any("ISSN совпадают" in t for t in texts)
    assert any("PDF выпуска" in t for t in texts)


def test_page_order_warning() -> None:
    articles = [
        {
            "identifiers": {"doi": "10.37791/2687-0649-2024-19-4-107-125"},
            "title_ru": "A",
        },
        {
            "identifiers": {"doi": "10.37791/2687-0649-2024-19-4-94-106"},
            "title_ru": "B",
        },
    ]
    warnings = v.build_issue_warnings(
        {
            "journal_title": "J",
            "issue_title": "T",
            "volume": "1",
            "issue": "1",
            "year": "2024",
            "article_urls": ["u1", "u2"],
            "article_count": 2,
            "issn": "1234-5678",
            "eissn": "1234-5679",
            "issue_galleys": [{"url": "https://example.com/issue.pdf"}],
        },
        articles=articles,
    )
    assert any("последовательности страниц" in str(w["text"]) or "Пересечение" in str(w["text"]) for w in warnings)


def test_page_gap_is_warning() -> None:
    articles = [
        {"identifiers": {"doi": "10.1/x-2024-1-1-3-10"}, "title_ru": "Первая статья выпуска"},
        {"identifiers": {"doi": "10.1/x-2024-1-1-18-28"}, "title_ru": "Вторая статья выпуска"},
    ]
    warnings = v.build_issue_warnings(
        {
            "journal_title": "J",
            "issue_title": "№ 1 (2024)",
            "issue": "1",
            "year": "2024",
            "article_urls": ["u1", "u2"],
            "article_count": 2,
            "issn": "1234-5678",
            "eissn": "1234-5679",
            "issue_galleys": [{"url": "https://example.com/issue.pdf"}],
            "uses_volume": False,
        },
        articles=articles,
    )
    gaps = [w for w in warnings if "Пропуск страниц" in str(w["text"])]
    assert gaps
    assert gaps[0]["severity"] == "warning"


def test_cross_article_duplicates() -> None:
    articles = [
        {
            "identifiers": {"doi": "10.1234/same.doi", "internal_id": "111", "pdf_url": "https://x/a.pdf"},
            "title_ru": "Одинаковое длинное название статьи",
            "title_en": "Same long english article title",
            "pdf_files": [{"url": "https://x/a.pdf"}],
        },
        {
            "identifiers": {"doi": "10.1234/same.doi", "internal_id": "111", "pdf_url": "https://x/a.pdf"},
            "title_ru": "Одинаковое длинное название статьи",
            "title_en": "Same long english article title",
            "pdf_files": [{"url": "https://x/a.pdf"}],
        },
    ]
    warnings = v.build_issue_warnings(
        {
            "journal_title": "J",
            "issue_title": "T",
            "issue": "1",
            "year": "2024",
            "article_urls": ["u1", "u2"],
            "article_count": 2,
            "issn": "1234-5678",
            "eissn": "1234-5679",
            "issue_galleys": [{"url": "https://example.com/issue.pdf"}],
            "uses_volume": False,
        },
        articles=articles,
    )
    texts = [str(w["text"]) for w in warnings]
    assert any("Дублирующийся DOI" in t for t in texts)
    assert any("article_id" in t for t in texts)
    assert any("название" in t for t in texts)
    assert any("PDF URL" in t for t in texts)


def test_references_language_distribution_is_not_parallel_lists() -> None:
    """12 RU + 7 EN в одном списке — это распределение языка, не ошибка."""
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Obtaining of ammonium paratungstate for research",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["рений"],
        "keywords_en": ["rhenium"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 19,
        "references_ru_count": 12,
        "references_en_count": 7,
        "references_mode": "single_list",
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 10,
        "page_end": 20,
        "pages": "10–20",
        "reference_ru_first": "Петухов, О.Ф. Рений.",
        "reference_en_first": "Werner, T.T. Rhenium mineral resources.",
    }
    issues = v.build_article_issues(article)
    assert not any("источников RU и EN" in str(i["text"]) for i in issues)
    assert not any("два самостоятельных блока" in str(i["text"]).lower() for i in issues)
    assert article.get("references_report", {}).get("total") == 19


def test_parallel_bibliography_blocks_count_mismatch() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "abstract_ru_stats": {"length": 40},
        "abstract_en_stats": {"length": 40},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 40,
        "references_mode": "parallel_blocks",
        "references_parallel_ru_count": 39,
        "references_parallel_en_count": 30,
        "references": ["1. Ref A"] * 5,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
    }
    issues = v.build_article_issues(article)
    assert any("самостоятельных блока" in str(i["text"]) for i in issues)


def test_article_data_prefers_jats_over_html() -> None:
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    parser = IssueMetadataParser()
    article: dict = {
        "title_ru": "HTML заголовок",
        "title_en": "HTML title translit wrong",
        "page_abstract_ru": " ".join(["слово"] * 40),
        "page_abstract_en": None,
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": None,
        "page_keywords_ru": ["старый"],
        "page_keywords_en": [],
        "keywords_ru": ["старый"],
        "keywords_en": [],
        "authors_ru": ["Старый А. А."],
        "authors_en": [],
        "authors": ["Старый А. А."],
        "authors_count": 1,
        "identifiers": {"doi": "10.1/html", "internal_id": "99"},
        "pages_sources": {"html": "1-2"},
        "organizations": [],
        "affiliations": [],
    }
    xml = {
        "title_ru": "Правильный заголовок из JATS",
        "title_en": "Proper English title from JATS XML",
        "abstract_ru": " ".join(["аннотация"] * 50),
        "abstract_en": " ".join(["abstract"] * 50),
        "keywords_ru": ["модель", "реактор"],
        "keywords_en": ["model", "reactor"],
        "authors_ru": ["Иванов И. И.", "Петров П. П."],
        "authors_en": ["Ivanov I. I.", "Petrov P. P."],
        "authors": ["Иванов И. И.", "Петров П. П."],
        "authors_count": 2,
        "orcids": ["0000-0001-2345-6789"],
        "emails": ["ivanov@example.com"],
        "identifiers": {"doi": "10.1/jats", "internal_id": None},
        "pages_jats": "18-28",
        "jats_affiliations": [{"id": "aff1", "name": "Институт", "lang": "ru", "empty": False}],
        "contributor_affiliation_refs": [],
        "broken_affiliation_refs": [],
        "references_by_lang": {
            "mode": "single_list",
            "total_refs": 2,
            "items": ["1. Ref A", "2. Ref B"],
            "ru": {"count": 0},
            "en": {"count": 2, "first": "1. Ref A", "last": "2. Ref B"},
            "unk": {"count": 0},
        },
        "article_type": "research-article",
        "publication_date": "2023-01-15",
    }
    parser._merge_jats_into_article(article, {}, xml)
    assert article["data_source"] == "jats"
    assert article["title_ru"] == "Правильный заголовок из JATS"
    assert article["title_en"] == "Proper English title from JATS XML"
    assert article["page_title_ru"] == "HTML заголовок"
    assert "аннотация" in article["abstract_ru"]
    assert article["abstract_en_from_jats_only"] is True
    assert article["keywords_en"] == ["model", "reactor"]
    assert article["authors_count"] == 2
    assert article["authors_ru"][0].startswith("Иванов")
    assert article["orcids"] == ["0000-0001-2345-6789"]
    assert article["identifiers"]["doi"] == "10.1/jats"
    assert article["pages"] in {"18-28", "18–28"}
    assert article["pages_source"] == "jats"
    assert article["references_count"] == 2
    assert article["organizations"] == ["Институт"]


def test_jats_extracts_authors_from_contrib() -> None:
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    parser = IssueMetadataParser()
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <article article-type="research-article">
      <front><article-meta>
        <title-group>
          <article-title xml:lang="ru">Заголовок</article-title>
          <trans-title-group xml:lang="en"><trans-title>Title</trans-title></trans-title-group>
        </title-group>
        <contrib-group>
          <contrib contrib-type="author" corresp="yes">
            <name name-style="western">
              <surname>Иванов</surname><given-names>И. И.</given-names>
            </name>
            <contrib-id contrib-id-type="orcid">https://orcid.org/0000-0002-1825-0097</contrib-id>
            <email>a@b.ru</email>
            <xref ref-type="aff" rid="aff1"/>
          </contrib>
          <aff id="aff1">МГУ</aff>
        </contrib-group>
        <fpage>10</fpage><lpage>20</lpage>
      </article-meta></front>
    </article>
    """.encode("utf-8")
    parsed = parser._parse_jats_xml(xml)
    assert any("Иванов" in a for a in (parsed.get("authors") or []))
    assert parsed.get("orcids")
    assert "0000-0002-1825-0097" in str(parsed["orcids"][0])
    assert parsed.get("emails") == ["a@b.ru"]
    assert parsed.get("pages_jats") == "10-20"


def test_jats_name_alternatives_count_each_contrib_once() -> None:
    """RU+EN name в name-alternatives — один автор; оба языка заполняются."""
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <article>
      <front><article-meta>
        <contrib-group>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="en"><surname>Nekhoroshev</surname><given-names>Vitalii Olegovich</given-names></name>
              <name xml:lang="ru"><surname>Нехорошев</surname><given-names>Виталий Олегович</given-names></name>
            </name-alternatives>
          </contrib>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="ru"><surname>Дерусова</surname><given-names>Дарья Александровна</given-names></name>
              <name xml:lang="en"><surname>Derusova</surname><given-names>Daria Alexandrovna</given-names></name>
            </name-alternatives>
          </contrib>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="en"><surname>Belikov</surname><given-names>Rostislav Konstantinovich</given-names></name>
              <name xml:lang="ru"><surname>Беликов</surname><given-names>Ростислав Константинович</given-names></name>
            </name-alternatives>
          </contrib>
        </contrib-group>
      </article-meta></front>
    </article>
    """.encode("utf-8")
    parsed = IssueMetadataParser()._parse_jats_xml(xml)
    assert parsed["authors_count"] == 3
    assert len(parsed["authors_ru"]) == 3
    assert len(parsed["authors_en"]) == 3
    assert any("Нехорошев" in a for a in parsed["authors_ru"])
    assert any("Derusova" in a for a in parsed["authors_en"])
    assert any("Дерусова" in a for a in parsed["authors_ru"])
    # Primary list — по одному на contrib
    assert len(parsed["authors"]) == 3


def test_bibliography_prefers_jats_list_over_html() -> None:
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    parser = IssueMetadataParser()
    article: dict = {
        "references": ["1. HTML only truncated ref"],
        "references_count": 1,
        "references_mode": "single_list",
        "identifiers": {},
        "pages_sources": {},
    }
    xml_parsed = {
        "references_by_lang": {
            "mode": "single_list",
            "total_refs": 3,
            "items": [
                "1. First complete JATS reference. Journal. 2020.",
                "2. Second complete JATS reference. Journal. 2019.",
                "3. Third complete JATS reference. Journal. 2018.",
            ],
            "ru": {"count": 0, "first": None, "last": None},
            "en": {"count": 3, "first": "1. First", "last": "3. Third"},
            "unk": {"count": 0, "first": None, "last": None},
        }
    }
    parser._merge_jats_into_article(article, {}, xml_parsed)
    assert article["references_source"] == "jats"
    assert article["references_count"] == 3
    assert len(article["references"]) == 3
    assert article.get("references_html_count") == 1
    assert "JATS" in article["references"][0] or "First complete" in article["references"][0]


def test_bibliography_lang_stats_sum_to_total() -> None:
    """Для одного списка RU+EN+unk = числу записей; total не ломается."""
    article = {
        "references_mode": "single_list",
        "references_count": 32,
        "references_ru_count": 5,  # устаревшие/битые счётчики
        "references_en_count": 2,
        "references_unk_count": 0,
        "references": [
            "1. Иванов И. И. Исследование материалов. М., 2020.",
            "2. Yang Z. Riveting damage behavior. Materials. 2020;13:5670.",
            "3. Петров П. Ещё одна русская ссылка на кириллице.",
        ] + [f"{i}. Smith J. English reference number {i}. Journal. 2019." for i in range(4, 33)],
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "identifiers": {"doi": "10.1234/x"},
        "authors_ru": ["Иванов И. И."],
        "authors_en": ["Ivanov I. I."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "pages_sources": {"html": "1-2"},
    }
    assert len(article["references"]) == 32
    v.recompute_bibliography_lang_stats(article)
    assert article["references_count"] == 32
    assert (
        int(article["references_ru_count"])
        + int(article["references_en_count"])
        + int(article["references_unk_count"])
        == 32
    )
    assert int(article["references_en_count"]) >= 20
    assert int(article["references_ru_count"]) >= 2


def test_bibliography_correct_doi_url_not_glued_or_broken() -> None:
    """Полный DOI URL и номер статьи 5670 не должны давать ложные glue/broken."""
    items = [
        (
            "1. Yang Z., Jiang R.S., Zuo Y.J. Riveting damage behavior and mechanical "
            "performance assessments of CFRP/Ti stacks under different rivet dies. "
            "Materials. 2020;13(24):5670. https://doi.org/10.3390/ma13245670"
        ),
        "2. Author A. Short title. Journal. 2019;10:1-5.",
    ]
    analysis = v.analyze_bibliography_items(items)
    assert analysis["glued"] == []
    assert analysis["broken"] == []


def test_bibliography_truly_glued_and_broken_still_detected() -> None:
    items = [
        (
            "1. First A. Title one. Journal. 2019;1:1-2. https://doi.org/10.1/abc "
            "2. Second B. Title two. 2020."
        ),
        "2. Incomplete ref ending with doi:",
        "3. Truncated http://",
    ]
    analysis = v.analyze_bibliography_items(items)
    assert analysis["glued"]
    assert analysis["broken"]


def test_bibliography_suspicious_normalization() -> None:
    items = [
        "1. Saldan~a J. The coding manual. 2021.",
        "2. PerezRey A. Something about science. 2020. P. 500-10.",
        "3. Normal Author. Good reference. 2019. P. 10-15.",
    ]
    analysis = v.analyze_bibliography_items(items)
    reasons = " ".join(s["reason"] for s in analysis["suspicious"])
    assert "~" in reasons or "нормализац" in reasons
    assert "PerezRey" in reasons or "склеенное" in reasons
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "abstract_ru_stats": {"length": 40},
        "abstract_en_stats": {"length": 40},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 3,
        "references_mode": "single_list",
        "references": items,
        "references_ru_count": 0,
        "references_en_count": 3,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
    }
    issues = v.build_article_issues(article)
    assert any("Подозрительная запись" in str(i["text"]) for i in issues)
    assert all(i.get("severity") == "warning" for i in issues if "Подозрительная" in str(i["text"]))


def test_keywords_count_mismatch_suppressed_when_en_missing() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Obtaining of ammonium paratungstate for research",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["a", "b", "c"],
        "keywords_en": [],
        "keywords_ru_count": 3,
        "keywords_en_count": 0,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
        "pages": "1–2",
    }
    issues = v.build_article_issues(article)
    texts = [str(i["text"]) for i in issues]
    assert any("Отсутствуют ключевые слова EN" in t for t in texts)
    assert not any("Количество ключевых слов должно совпадать" in t for t in texts)
    assert not any("различается" in t and "ключевых" in t for t in texts)


def test_numeric_edn_rejected() -> None:
    assert v.looks_like_edn("257722") is False
    assert v.looks_like_edn("DFDQUW") is True
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Obtaining of ammonium for research study",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["рений"],
        "keywords_en": ["rhenium"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl", "edn": "257722"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
        "pages": "1–2",
    }
    v.apply_article_validation(article)
    assert article["identifiers"]["edn"] is None
    assert article["identifiers"]["internal_id"] == "257722"
    assert any("внутренний ID" in e for e in article["errors"]) or any(
        "внутренний ID" in p for p in article["problems"]
    )


def test_transliteration_and_caps_title() -> None:
    title_ru = "Применение блочно-модульного метода для оптимизации"
    title_en = "PRIMENENIE BLOChNO-MODUL'NOGO METODA DLYa OPTIMIZATSII"
    assert v.looks_like_transliteration(title_en, title_ru)
    assert v.looks_like_transliteration("POLUChENIE PARAVOL'FRAMATA AMMONIYa DLYa ISSLEDOVANIYa")
    assert not v.looks_like_transliteration(
        "Application of the block-modular method for process optimization",
        title_ru,
    )
    article = {
        "title_ru": "ПОЛУЧЕНИЕ ПАРАВОЛЬФРАМАТА АММОНИЯ ДЛЯ ИССЛЕДОВАНИЯ СВОЙСТВ",
        "title_en": "POLUChENIE PARAVOL'FRAMATA AMMONIYa DLYa ISSLEDOVANIYa",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["рений"],
        "keywords_en": ["rhenium"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
        "pages": "1–2",
    }
    issues = v.build_article_issues(article)
    translit = [i for i in issues if "транслитерац" in str(i["text"])]
    assert translit
    assert translit[0]["severity"] == "error"
    assert not any("прописными" in str(i["text"]) for i in issues)


def test_author_initials_punctuation_and_orcid_email() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title for checks",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Зароченцев В. М", "Рутковский А. Л"],
        "authors_en": ["Zarochentsev V. M", "Rutkovskiy A. L"],
        "authors_count": 2,
        "orcids": ["0000-0001-2345-678X", "bad-orcid"],
        "emails": ["a@example.com", "a@example.com"],
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
        "pages": "1–2",
    }
    issues = v.build_article_issues(article)
    texts = [str(i["text"]) for i in issues]
    assert any("завершающей точки" in t or "Пунктуация инициалов" in t for t in texts)
    assert any("ORCID" in t for t in texts)
    assert any("один и тот же email" in t.lower() or "Один и тот же email" in t for t in texts)


def test_orcid_accepts_0009_range() -> None:
    assert v.validate_orcid("0009-0009-0760-4783") is None
    assert v.validate_orcid("https://orcid.org/0009-0009-0760-4783") is None
    assert v.validate_orcid("0000-0002-1825-0097") is None
    err = v.validate_orcid("bad-orcid")
    assert err is not None and "ORCID" in err


def test_full_author_names_do_not_trigger_initials_punctuation() -> None:
    """Полные ФИО, разделённые при join через «;», не должны давать ложное замечание."""
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title for checks",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": [
            "Нехорошев Виталий Олегович",
            "Беликов Ростислав Константинович",
            "Дерусова Дарья Александровна",
        ],
        "authors_en": [
            "Nekhoroshev Vitalii Olegovich",
            "Belikov Rostislav Konstantinovich",
            "Derusova Daria Alexandrovna",
        ],
        "authors_count": 3,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
        "pages": "1–2",
    }
    issues = v.build_article_issues(article)
    texts = [str(i["text"]) for i in issues]
    assert not any("Пунктуация инициалов" in t for t in texts)
    assert not any("завершающей точки" in t for t in texts)


def test_group_findings_collapses_same_message() -> None:
    from ipsas.modules.issue_metadata.report_summary import group_findings

    grouped = group_findings(
        [{"text": "ISSN совпадают", "severity": "warning", "field": "issn", "category": "issue"}],
        [
            {
                "issues": [
                    {"text": "Отсутствует название статьи (EN)", "severity": "error", "field": "title_en", "category": "texts"}
                ],
                "errors": ["Отсутствует название статьи (EN)"],
                "problems": [],
            },
            {
                "issues": [
                    {"text": "Отсутствует название статьи (EN)", "severity": "error", "field": "title_en", "category": "texts"},
                    {"text": "Не указаны страницы статьи", "severity": "warning", "field": "pages", "category": "consistency"},
                ],
                "errors": ["Отсутствует название статьи (EN)"],
                "problems": ["Не указаны страницы статьи"],
            },
        ],
    )
    assert len(grouped["errors"]) == 1
    assert grouped["errors"][0]["count"] == 2
    assert grouped["errors"][0]["articles"] == [1, 2]
    assert grouped["errors"][0]["category"] == "texts"
    assert "by_category" in grouped
    assert len(grouped["by_category"]) == 7
    texts_cat = next(c for c in grouped["by_category"] if c["id"] == "texts")
    assert texts_cat["error_count"] >= 2
    issue_cat = next(c for c in grouped["by_category"] if c["id"] == "issue")
    assert issue_cat["warning_count"] >= 1


def test_rules_catalog_has_seven_categories() -> None:
    from ipsas.modules.issue_metadata.rules import CATEGORIES, RULES, resolve_category

    assert len(CATEGORIES) == 7
    assert 40 <= len(RULES) <= 80
    assert resolve_category(field="doi") == "identifiers"
    assert resolve_category(field="references") == "bibliography"
    assert resolve_category(field="pages") == "consistency"
    assert resolve_category(text="PDF выпуска отсутствует") == "files"


def test_apply_article_validation_splits_severity() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["distributed"],
        "keywords_en": ["computing"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.37791//bad"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "errors": ["parse boom"],
        "page_start": 1,
        "page_end": 2,
        "pages": "1–2",
    }
    v.apply_article_validation(article)
    assert "parse boom" in article["errors"]
    assert any("двойной" in e or "//" in e for e in article["errors"])


def test_missing_keywords_en_without_count_mismatch() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "page_keywords_ru": ["а", "б", "в", "г", "д", "е", "ж", "з"],
        "page_keywords_en": [],
        "keywords_ru": ["а", "б", "в", "г", "д", "е", "ж", "з"],
        "keywords_en": [],
        "keywords_ru_count": 8,
        "keywords_en_count": 0,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
    }
    issues = v.build_article_issues(article)
    texts = [str(i["text"]) for i in issues]
    assert any(t == "Отсутствуют ключевые слова EN" for t in texts)
    assert not any("различается" in t and "ключевых" in t for t in texts)
    assert not any("должно совпадать" in t for t in texts)


def test_empty_affiliation_on_page_is_error() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 60),
        "abstract_en": " ".join(["word"] * 60),
        "abstract_ru_stats": {"length": 60},
        "abstract_en_stats": {"length": 60},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Зароченцев В. М."],
        "authors_count": 1,
        "affiliations_section_present": True,
        "author_affiliation_refs": [[1]],
        "page_affiliations": [{"index": 1, "name": "", "displayed": False}],
        "affiliations": [],
        "organizations": [],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
    }
    issues = v.build_article_issues(article)
    assert any(
        i.get("severity") == "error"
        and "аффилиацию 1" in str(i["text"])
        and "отсутствует" in str(i["text"])
        for i in issues
    )


def test_abstract_missing_on_page_even_if_jats() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "page_abstract_ru": " ".join(["слово"] * 40),
        "page_abstract_en": "",
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "abstract_en_from_jats_only": True,
        "abstract_ru_stats": {"length": 40},
        "abstract_en_stats": {"length": 40},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "page_keywords_ru": ["метод"],
        "page_keywords_en": ["method"],
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "page_start": 1,
        "page_end": 2,
    }
    issues = v.build_article_issues(article)
    assert any("не представлена на публичной странице" in str(i["text"]) and "Английская" in str(i["text"]) for i in issues)


def test_analyze_doi_stages() -> None:
    a = v.analyze_doi("10.31857/S0869573324021828")
    assert a["present"] is True
    assert a["format_ok"] is True
    bad = v.analyze_doi("https://doi.org/10.31857/S0869573324021828.")
    assert bad["present"] is True
    assert bad["format_ok"] is False
    assert any("URL" in m or "точк" in m.lower() or "doi:" in m.lower() for m in bad["format_errors"])


def test_collect_page_affiliations_empty_name() -> None:
    from lxml import html
    from ipsas.modules.issue_metadata.parsers import collect_page_affiliations

    root = html.fromstring(
        """
        <div class="item authors"><span class="affiliation">1</span></div>
        <div class="item affiliations"><h2>Affiliations</h2><div><sup>1</sup></div></div>
        """
    )
    data = collect_page_affiliations(root)
    assert data["affiliations_section_present"]
    assert any(it.get("index") == 1 and not it.get("name") for it in data["page_affiliations"])


def test_edn_is_six_letters_only() -> None:
    assert v.looks_like_edn("DFDQUW") is True
    assert v.looks_like_edn("257724") is False
    assert v.looks_like_edn("ABC123") is False
    assert v.looks_like_edn("ABCDEFG") is False
    err = v.validate_edn("257724")
    assert err is not None
    assert "внутренний ID" in err or "платформ" in err


def test_pages_from_jats_preferred_over_html_when_both_present() -> None:
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "abstract_ru_stats": {"length": 40},
        "abstract_en_stats": {"length": 40},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {
            "doi": "10.31857/S0869573324021828-18-28",
            "internal_id": "257724",
            "edn": None,
        },
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "pages_sources": {"html": "18–28", "jats": "18-28", "ojs_meta": None, "biblio": None},
    }
    v.enrich_article_pages(article)
    assert article["pages"] == "18–28"
    assert article["page_start"] == 18
    assert article["page_end"] == 28
    assert article["pages_source"] == "jats"
    assert article.get("pages_from_doi_unconfirmed") is False
    issues = v.build_article_issues(article)
    assert not any("Ошибка согласованности" in str(i["text"]) for i in issues)


def test_pages_sources_conflict_ignored_when_jats_present() -> None:
    """HTML/meta могут расходиться с JATS — ошибки согласованности нет, канон из JATS."""
    article = {
        "title_ru": "Достаточно длинное название статьи",
        "title_en": "Long enough English title here",
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "abstract_ru_stats": {"length": 40},
        "abstract_en_stats": {"length": 40},
        "keywords_ru": ["метод"],
        "keywords_en": ["method"],
        "keywords_ru_count": 1,
        "keywords_en_count": 1,
        "references_count": 1,
        "identifiers": {"doi": "10.1234/abcdef.ghijkl", "internal_id": "1"},
        "authors_ru": ["Иванов И. И."],
        "authors_count": 1,
        "organizations": ["Org"],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "pages_sources": {"html": "1", "jats": "45-57", "ojs_meta": "45-57", "biblio": None},
    }
    v.enrich_article_pages(article)
    assert article["pages_source"] == "jats"
    assert article["page_start"] == 45
    assert article["page_end"] == 57
    assert article.get("pages_sources_conflict") is False
    issues = v.build_article_issues(article)
    assert not any("Ошибка согласованности" in str(i["text"]) for i in issues)


def test_page_keywords_bilingual_blocks_not_mixed() -> None:
    from lxml import html
    from ipsas.modules.issue_metadata.parsers import parse_article_page
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    root = html.fromstring(
        """
        <html><body>
          <div id="articleSubject">
            <h2>Keywords</h2>
            <div><a href="#">mathematical model</a>, <a href="#">leaching</a></div>
          </div>
          <div id="articleKeywords">
            <h2>Ключевые слова</h2>
            <div><a href="#">математическая модель</a>, <a href="#">выщелачивание</a></div>
          </div>
        </body></html>
        """
    )
    data = parse_article_page(root, "https://example.com/a", detect_lang=IssueMetadataParser._detect_lang)
    assert "leaching" in data["keywords_en"]
    assert any("выщелачивание" in k for k in data["keywords_ru"])
    assert not any(re.search(r"[А-Яа-я]", k) for k in data["keywords_en"])


def test_keywords_heading_with_cyrillic_content_is_ru() -> None:
    from lxml import html
    from ipsas.modules.issue_metadata.parsers import parse_article_page
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    root = html.fromstring(
        """
        <html><body>
          <div id="articleSubject">
            <h2>Keywords</h2>
            <div><a href="#">математическая модель</a></div>
          </div>
        </body></html>
        """
    )
    data = parse_article_page(root, "https://example.com/a", detect_lang=IssueMetadataParser._detect_lang)
    assert data["keywords_ru"] == ["математическая модель"]
    assert data["keywords_en"] == []


def test_empty_page_keywords_en_without_jats_flag_is_not_jats_message() -> None:
    article = {
        "title_ru": "Достаточно длинное русское название статьи для теста",
        "title_en": "A proper English title for validation testing here",
        "page_abstract_ru": " ".join(["слово"] * 40),
        "page_abstract_en": " ".join(["word"] * 40),
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "page_keywords_ru": ["модель", "реактор"],
        "page_keywords_en": [],
        "keywords_ru": ["модель", "реактор"],
        "keywords_en": ["model", "reactor"],
        "keywords_ru_count": 2,
        "keywords_en_count": 2,
        "keywords_en_from_jats_only": False,
        "identifiers": {"doi": "10.1234/x"},
        "affiliations": ["Org"],
        "organizations": ["Org"],
        "page_affiliations": [{"index": 1, "name": "Org"}],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "pages_sources": {"html": "1-2"},
        "references_count": 1,
        "references_mode": "single_list",
        "authors_ru": ["Иванов И. И."],
        "authors_en": ["Ivanov I. I."],
        "authors_count": 1,
    }
    issues = v.build_article_issues(article)
    texts = [str(i["text"]) for i in issues]
    assert any("Отсутствуют ключевые слова EN" in t for t in texts)
    assert not any("есть в JATS" in t and "EN" in t for t in texts)


def test_doi_resolve_failure_does_not_create_warning() -> None:
    """Неуспешный resolve doi.org не должен попадать в замечания."""
    article = {
        "title_ru": "Достаточно длинное название статьи для теста",
        "title_en": "A proper English title for validation testing here",
        "page_abstract_ru": " ".join(["слово"] * 40),
        "page_abstract_en": " ".join(["word"] * 40),
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "page_keywords_ru": ["a", "b", "c"],
        "page_keywords_en": ["a", "b", "c"],
        "keywords_ru": ["a", "b", "c"],
        "keywords_en": ["a", "b", "c"],
        "identifiers": {"doi": "10.1234/not-registered-yet"},
        "doi_check": {
            "present": True,
            "format_ok": True,
            "resolved": False,
            "resolve_error": "HTTP 404",
            "normalized": "10.1234/not-registered-yet",
            "format_errors": [],
        },
        "affiliations": ["Org"],
        "organizations": ["Org"],
        "page_affiliations": [{"index": 1, "name": "Org"}],
        "pdf_files": [{"url": "https://example.com/a.pdf"}],
        "pages_sources": {"html": "1-2"},
        "references_count": 1,
        "references_mode": "single_list",
        "authors_ru": ["Иванов И. И."],
        "authors_en": ["Ivanov I. I."],
        "authors_count": 1,
    }
    issues = v.build_article_issues(article)
    assert not any("doi.org" in str(i.get("text") or "").lower() for i in issues)
    assert not any("не разрешается" in str(i.get("text") or "") for i in issues)


def test_collect_pages_from_html_block() -> None:
    from lxml import html
    from ipsas.modules.issue_metadata.parsers import collect_article_pages_from_html

    root = html.fromstring(
        """
        <html><head>
        <meta name="citation_firstpage" content="18"/>
        <meta name="citation_lastpage" content="28"/>
        </head><body>
        <div class="item pages"><span class="label">Pages:</span>
          <span class="value">18–28</span></div>
        </body></html>
        """
    )
    data = collect_article_pages_from_html(root)
    assert data["page_start"] == 18 and data["page_end"] == 28
    assert data["ojs_meta"] in {"18-28", "18–28"}
    assert data["html"] is not None or data["pages"]
