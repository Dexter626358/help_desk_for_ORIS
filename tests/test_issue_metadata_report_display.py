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


def test_synthetic_aff_ids_without_aff_id_attribute() -> None:
    """Как в 417122: <aff> без id, xref rid=aff1/aff2, institution xml:lang."""
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <article>
      <front><article-meta>
        <contrib-group>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="ru"><surname>Васильев</surname><given-names>Алексей</given-names></name>
              <name xml:lang="en"><surname>Vasiliev</surname><given-names>Alexey V.</given-names></name>
            </name-alternatives>
            <xref ref-type="aff" rid="aff1"/>
          </contrib>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="ru"><surname>Перов</surname><given-names>Дмитрий</given-names></name>
              <name xml:lang="en"><surname>Perov</surname><given-names>Dmitry V.</given-names></name>
            </name-alternatives>
            <xref ref-type="aff" rid="aff2"/>
          </contrib>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="ru"><surname>Бирюков</surname><given-names>Дмитрий</given-names></name>
              <name xml:lang="en"><surname>Biryukov</surname><given-names>Dmitry Yu.</given-names></name>
            </name-alternatives>
            <xref ref-type="aff" rid="aff1"/>
          </contrib>
          <contrib contrib-type="author">
            <name-alternatives>
              <name xml:lang="ru"><surname>Костин</surname><given-names>Владимир</given-names></name>
              <name xml:lang="en"><surname>Kostin</surname><given-names>Vladimir N.</given-names></name>
            </name-alternatives>
          </contrib>
          <aff><institution xml:lang="ru">Уральский федеральный университет</institution></aff>
          <aff><institution xml:lang="en">Ural Federal University</institution></aff>
          <aff><institution xml:lang="en">Institute of Metal Physics</institution></aff>
          <aff><institution xml:lang="ru">Институт физики металлов</institution></aff>
        </contrib-group>
      </article-meta></front>
    </article>
    """.encode("utf-8")
    parsed = IssueMetadataParser()._parse_jats_xml(xml)
    affs = parsed["jats_affiliations"]
    ids = {a.get("id") for a in affs}
    assert "aff1" in ids and "aff2" in ids
    assert parsed["broken_affiliation_refs"] == []
    refs = parsed["contributor_affiliation_refs"]
    assert len(refs) == 4
    assert refs[0]["rid"] == ["aff1"]
    assert refs[2]["rid"] == ["aff1"]
    assert refs[3]["rid"] == []

    article = {
        "authors_count": 4,
        "authors_ru": ["Васильев Алексей", "Перов Дмитрий", "Бирюков Дмитрий", "Костин Владимир"],
        "authors_en": ["Vasiliev Alexey V.", "Perov Dmitry V.", "Biryukov Dmitry Yu.", "Kostin Vladimir N."],
        "jats_affiliations": affs,
        "contributor_affiliation_refs": refs,
        "affiliations_ru": ["Уральский федеральный университет", "Институт физики металлов"],
        "affiliations_en": ["Ural Federal University", "Institute of Metal Physics"],
    }
    rows = build_authors_table(article)
    assert "Уральский" in rows[0]["affiliation_ru"]
    assert "Ural" in rows[0]["affiliation_en"]
    assert "Институт" in rows[1]["affiliation_ru"] or "физики" in rows[1]["affiliation_ru"]
    assert "Уральский" in rows[2]["affiliation_ru"]  # Бирюков → aff1
    assert rows[2]["problems"] == []
    # Без xref — мягкое замечание, не ложные «нет аффилиации RUS/ENG»
    assert any("не указана организация" in p for p in rows[3]["problems"])
    assert "нет аффилиации (RUS)" not in rows[3]["problems"]


def test_glued_references_preview_shows_first_and_last_parts() -> None:
    text = (
        "1. БОГАЧЕВА Д.Н. Книга. 2024. – 772 с.2. ВЕНТЦЕЛЬ Е.С. Введение. 1964. – 388 с."
        "3. КОРЕПАНОВ В.О. Статья // Журнал. – 2011. – С. 66–73.4. ЛАРЮШИН И.Д. Модель. 2020."
    )
    article = {
        "references": [text],
        "references_count": 1,
        "references_lang_source": "unspecified",
    }
    enrich_article_report_display(article)
    assert article["references_display_unk"] >= 4
    row = article["references_preview"][0]
    assert row["lang"] == "UNK"
    assert row["count"] >= 4
    assert "БОГАЧЕВА" in row["first"]
    assert "ЛАРЮШИН" in row["last"]
    assert not row["same"]
