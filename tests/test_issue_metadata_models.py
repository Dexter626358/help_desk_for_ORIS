"""Тесты моделей и парсинга метаданных выпуска."""

from lxml import html

from ipsas.modules.issue_metadata.models import IssueParseResult
from ipsas.modules.issue_metadata import parsers as issue_parsers


def test_article_roundtrip_preserves_extended_fields() -> None:
    raw = {
        "issue": {
            "issue_title": "№ 1 (35) (2026)",
            "issue_serial": "35",
            "cover_url": "https://example.com/cover.jpg",
            "issue_galleys": [{"url": "https://example.com/issue.pdf", "lang": "RU"}],
            "warnings": [],
        },
        "articles": [
            {
                "url": "https://example.com/article/1",
                "abstract_ru": "Текст аннотации на русском языке достаточной длины для теста.",
                "abstract_en": "English abstract text for testing purposes.",
                "references_ru_count": 3,
                "references_en_count": 2,
                "references_unk_count": 1,
                "references_count": 6,
                "reference_ru_first": "Ref RU 1",
                "organizations": ["Org A"],
                "organizations_count": 1,
                "pdf_files": [{"url": "https://example.com/a.pdf", "lang": "RU", "locked": False}],
                "identifiers": {"doi": "10.1234/test", "edn": "ABCDEF"},
            }
        ],
    }
    result = IssueParseResult.from_mapping(raw).to_dict()
    article = result["articles"][0]
    assert article["abstract_ru"].startswith("Текст аннотации")
    assert article["references_ru_count"] == 3
    assert article["references_unk_count"] == 1
    assert article["organizations_count"] == 1
    assert result["issue"]["cover_url"] == "https://example.com/cover.jpg"
    assert result["issue"]["issue_serial"] == "35"
    assert len(result["issue"]["issue_galleys"]) == 1


def test_parse_issue_title_ru_serial_format() -> None:
    page = html.fromstring("<html><head><title>№ 1 (35) (2026)</title></head><body><h1>№ 1 (35) (2026)</h1></body></html>")
    meta = issue_parsers.parse_issue_page(page, "https://example.com/j/issue/view/1")
    assert meta["issue"] == "1"
    assert meta["issue_serial"] == "35"
    assert meta["volume"] is None
    assert meta["year"] == "2026"


def test_parse_issue_identifiers_english_no_serial_format() -> None:
    ids = issue_parsers.parse_issue_identifiers("No 1 (35) (2026)")
    assert ids["issue"] == "1"
    assert ids["issue_serial"] == "35"
    assert ids["volume"] is None
    assert ids["year"] == "2026"


def test_jats_references_without_xml_lang_detect_russian() -> None:
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<article xmlns:xlink="http://www.w3.org/1999/xlink">
<back><ref-list>
<ref id="B1"><mixed-citation>Бернштейн, 1994 — Бернштейн Б. Лекции по физике.</mixed-citation></ref>
<ref id="B2"><mixed-citation>Chalmers, 1996 — Chalmers D. The Conscious Mind. Oxford, 1996.</mixed-citation></ref>
</ref-list></back></article>""".encode("utf-8")
    parsed = IssueMetadataParser()._parse_jats_xml(xml)
    refs = parsed["references_by_lang"]
    assert refs["ru"]["count"] == 1
    assert refs["en"]["count"] == 1
    assert refs["unk"]["count"] == 0
