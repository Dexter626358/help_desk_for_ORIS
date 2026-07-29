"""Тесты отображения авторов/источников в отчёте выпуска."""

from __future__ import annotations

from ipsas.modules.issue_metadata.report_display import (
    build_authors_table,
    enrich_article_report_display,
)


def test_authors_table_pairs_ru_en_and_unique_affs() -> None:
    article = {
        "authors_count": 2,
        "authors_ru": ["Басков К.М.", "Краснолобов И.И."],
        "authors_en": ["Baskov K.M.", "Krasnolobov I.I."],
        "affiliations_ru": [
            "Институт теоретической и прикладной электродинамики Российской академии наук"
        ],
        "affiliations_en": [
            "Institute for Theoretical and Applied Electromagnetics of Russian Academy of Sciences"
        ],
    }
    rows = build_authors_table(article)
    assert len(rows) == 2
    assert rows[0]["name_ru"] == "Басков К.М."
    assert rows[0]["name_en"] == "Baskov K.M."
    assert "электродинамики" in rows[0]["affiliation_ru"]
    assert "Electromagnetics" in rows[0]["affiliation_en"]
    assert rows[1]["affiliation_ru"] == rows[0]["affiliation_ru"]

    enrich_article_report_display(article)
    assert article["unique_affiliations_ru"] == 1
    assert article["unique_affiliations_en"] == 1


def test_references_preview_unspecified_as_unk() -> None:
    article = {
        "references_lang_source": "unspecified",
        "references_count": 2,
        "references_ru_count": 0,
        "references_en_count": 0,
        "references_unk_count": 0,
        "references": [
            "1. First source. Journal. 2013.",
            "2. Second source. Journal. 2005.",
        ],
        "reference_first": "1. First source. Journal. 2013.",
        "reference_last": "2. Second source. Journal. 2005.",
        "authors_count": 0,
    }
    enrich_article_report_display(article)
    assert article["references_display_ru"] == 0
    assert article["references_display_en"] == 0
    assert article["references_display_unk"] == 2
    assert article["references_status"]["comparison"].startswith("✅")
    assert len(article["references_preview"]) == 1
    assert article["references_preview"][0]["lang"] == "UNK"
    assert "First source" in article["references_preview"][0]["first"]
    assert "Second source" in article["references_preview"][0]["last"]


def test_article_model_roundtrip_keeps_affiliations_for_authors_table() -> None:
    from ipsas.modules.issue_metadata.models import Article
    from ipsas.modules.issue_metadata.report_display import enrich_article_report_display

    raw = {
        "url": "https://example.com/a",
        "authors_count": 2,
        "authors_ru": ["Басков К.М.", "Краснолобов И.И."],
        "authors_en": ["Baskov K.M.", "Krasnolobov I.I."],
        "affiliations_ru": [
            "Институт теоретической и прикладной электродинамики Российской академии наук"
        ],
        "affiliations_en": [
            "Institute for Theoretical and Applied Electromagnetics of Russian Academy of Sciences"
        ],
        "references_count": 2,
        "references_lang_source": "unspecified",
        "references": ["1. First. 2013.", "2. Second. 2005."],
        "reference_first": "1. First. 2013.",
        "reference_last": "2. Second. 2005.",
    }
    art = Article.from_mapping(raw).to_dict()
    assert art["affiliations_ru"]
    assert art["affiliations_en"]
    enrich_article_report_display(art)
    assert len(art["authors_table"]) == 2
    assert "электродинамики" in art["authors_table"][0]["affiliation_ru"]
    assert "Electromagnetics" in art["authors_table"][0]["affiliation_en"]
    assert art["unique_affiliations_ru"] == 1
    assert art["references_preview"][0]["lang"] == "UNK"


def test_authors_table_from_jats_aff_refs() -> None:
    article = {
        "authors_count": 1,
        "authors_ru": ["Иванов И.И."],
        "authors_en": ["Ivanov I.I."],
        "jats_affiliations": [
            {"id": "aff1", "name": "МГТУ им. Н.Э. Баумана", "lang": "ru"},
            {"id": "aff1-en", "name": "Bauman Moscow State Technical University", "lang": "en"},
        ],
        "contributor_affiliation_refs": [
            {"author": "Иванов И.И.", "rid": ["aff1", "aff1-en"]},
        ],
    }
    rows = build_authors_table(article)
    assert rows[0]["affiliation_ru"] == "МГТУ им. Н.Э. Баумана"
    assert "Bauman" in rows[0]["affiliation_en"]
