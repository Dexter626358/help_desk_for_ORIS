"""Регрессии по аудиту слияния HTML/JATS для issue metadata."""

from __future__ import annotations

from lxml import html

from ipsas.modules.issue_metadata import parsers as issue_parsers
from ipsas.modules.issue_metadata import validators as v
from ipsas.modules.issue_metadata_parser import IssueMetadataParser


def test_issn_not_overwritten_by_article_eissn() -> None:
    page = html.fromstring(
        """
        <html><head></head><body>
          <div id="headerIssn">ISSN 0869-5733 (Print) ISSN 3034-5391 (Online)</div>
          <h1>No 1 (2023)</h1>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(
        page, "https://journals.rcsi.science/0869-5733/issue/view/8810"
    )
    assert meta["issn"] == "0869-5733"
    assert meta["eissn"] == "3034-5391"

    # Эмуляция старого бага: citation_issn статьи = eISSN
    issue = dict(meta)
    articles = [{"issn": "3034-5391"}]
    if not issue.get("issn"):
        issue["issn"] = articles[0]["issn"]
    assert issue["issn"] == "0869-5733"
    assert issue["eissn"] == "3034-5391"


def test_jats_pages_become_resolved_pages() -> None:
    article: dict = {
        "pages_sources": {"html": None, "jats": "18-28"},
        "identifiers": {},
    }
    v.enrich_article_pages(article)
    assert article["pages"] in {"18-28", "18–28"}
    assert article["pages_source"] == "jats"
    assert article["page_start"] == 18
    assert article["page_end"] == 28


def test_numeric_edn_candidate_is_internal_id() -> None:
    parser = IssueMetadataParser()
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <article>
      <front><article-meta>
        <article-id pub-id-type="edn">257724</article-id>
        <fpage>18</fpage><lpage>28</lpage>
      </article-meta></front>
    </article>
    """
    parsed = parser._parse_jats_xml(xml)
    assert parsed["identifiers"]["internal_id"] == "257724"
    assert parsed["identifiers"]["edn"] is None
    assert parsed["pages_jats"] == "18-28"


def test_invalid_edn_not_treated_as_internal_id() -> None:
    parser = IssueMetadataParser()
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <article>
      <front><article-meta>
        <codes><edn>INVALID</edn></codes>
      </article-meta></front>
    </article>
    """
    parsed = parser._parse_jats_xml(xml)
    assert parsed["identifiers"]["edn"] is None
    assert parsed["identifiers"]["internal_id"] is None
    assert parsed["identifiers"]["invalid_edn"] == "INVALID"


def test_jats_titles_and_affiliations_extracted() -> None:
    parser = IssueMetadataParser()
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <article article-type="research-article" xml:lang="ru">
      <front><article-meta>
        <title-group>
          <article-title xml:lang="ru">Применение блочно-модульного метода</article-title>
          <trans-title-group xml:lang="en">
            <trans-title>Application of the block-modular method</trans-title>
          </trans-title-group>
        </title-group>
        <contrib-group>
          <contrib contrib-type="author">
            <name><surname>Иванов</surname><given-names>И. И.</given-names></name>
            <xref ref-type="aff" rid="aff1"/>
          </contrib>
          <aff id="aff1">Северо-Кавказский горно-металлургический институт</aff>
        </contrib-group>
      </article-meta></front>
    </article>
    """.encode("utf-8")
    parsed = parser._parse_jats_xml(xml)
    assert parsed["title_ru"] and "блочно" in parsed["title_ru"].lower()
    assert parsed["title_en"] and "block" in parsed["title_en"].lower()
    assert parsed["article_type"] == "research-article"
    assert parsed["jats_affiliations"]
    assert parsed["jats_affiliations"][0]["id"] == "aff1"
    assert parsed["broken_affiliation_refs"] == []


def test_broken_aff_ref_reported() -> None:
    parser = IssueMetadataParser()
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <article><front><article-meta>
      <contrib-group>
        <contrib contrib-type="author">
          <name><surname>Petrov</surname></name>
          <xref ref-type="aff" rid="aff99"/>
        </contrib>
        <aff id="aff1">Org</aff>
      </contrib-group>
    </article-meta></front></article>
    """
    parsed = parser._parse_jats_xml(xml)
    assert any(b.get("rid") == "aff99" for b in parsed["broken_affiliation_refs"])


def test_recovered_jats_warning_preserved_on_merge() -> None:
    parser = IssueMetadataParser()
    # Незакрытый тег — recover
    xml = b"""<?xml version="1.0"?><article><front><article-meta>
      <article-title>Title</article-title>
      <abstract xml:lang="en"><p>Text
    </article-meta></front></article>"""
    parsed = parser._parse_jats_xml(xml)
    assert parsed.get("warnings")
    article: dict = {
        "pages_sources": {},
        "identifiers": {},
        "page_abstract_ru": None,
        "page_abstract_en": None,
        "page_keywords_ru": [],
        "page_keywords_en": [],
    }
    issue: dict = {}
    parser._merge_jats_into_article(article, issue, parsed)
    assert article.get("source_warnings")
    v.apply_article_validation(article)
    assert any("восстановления" in str(i.get("text") or "") for i in article.get("issues") or [])


def test_merge_pdfs_keeps_both_sources() -> None:
    merged = IssueMetadataParser._merge_pdf_files(
        [{"url": "https://ex/a.pdf", "lang": "RU"}],
        [{"url": "https://ex/b.pdf", "lang": "EN"}, {"url": "https://ex/a.pdf", "lang": "RU"}],
    )
    assert len(merged) == 2


def test_issue_warnings_preserved_when_rebuilding() -> None:
    parser = IssueMetadataParser()
    existing = [{"text": "limit warning", "severity": "warning", "field": "article_urls"}]
    generated = [{"text": "other", "severity": "warning", "field": "issn"}]
    out = parser._dedupe_warnings(existing + generated)
    assert len(out) == 2


def test_detect_lang_ignores_numbers() -> None:
    assert IssueMetadataParser._detect_lang("257724") is None
    assert IssueMetadataParser._detect_lang("Применение метода") == "ru"
    assert IssueMetadataParser._detect_lang("Application of method") == "en"


def test_publication_date_rejects_impossible_day() -> None:
    parser = IssueMetadataParser()
    xml = b"""<?xml version="1.0"?><article><front><article-meta>
      <pub-date pub-type="epub"><year>2024</year><month>2</month><day>31</day></pub-date>
    </article-meta></front></article>"""
    parsed = parser._parse_jats_xml(xml)
    assert parsed["publication_date"] is None


def test_jats_orgs_missing_on_page_is_error() -> None:
    article = {
        "title_ru": "Достаточно длинное русское название статьи для теста",
        "title_en": "A proper English title for validation testing here",
        "page_abstract_ru": " ".join(["слово"] * 40),
        "page_abstract_en": " ".join(["word"] * 40),
        "abstract_ru": " ".join(["слово"] * 40),
        "abstract_en": " ".join(["word"] * 40),
        "page_keywords_ru": ["a", "b", "c"],
        "page_keywords_en": ["a", "b", "c"],
        "keywords_ru": ["a", "b", "c"],
        "keywords_en": ["a", "b", "c"],
        "identifiers": {"doi": "10.1234/x"},
        "page_affiliations": [],
        "affiliations": [],
        "organizations": [],
        "jats_affiliations": [{"id": "aff1", "name": "Институт", "lang": "ru", "empty": False}],
        "pdf_files": [{"url": "https://ex/a.pdf"}],
        "pages_sources": {"html": "1-2"},
        "references_count": 1,
        "references_mode": "single_list",
    }
    issues = v.build_article_issues(article)
    assert any("не отображаются на публичной странице" in str(i.get("text")) for i in issues)
