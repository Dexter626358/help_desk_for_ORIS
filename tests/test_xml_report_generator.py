"""Тесты анализа XML журнала и генерации HTML-отчёта."""

from __future__ import annotations

from pathlib import Path

import pytest

from ipsas.modules.journal_xml_analyzer import analyze_journal_xml
from ipsas.modules.xml_report_generator import generate_xml_html_report

SAMPLE_JOURNAL_XML = """<?xml version="1.0" encoding="UTF-8"?>
<journal>
  <titleid>123</titleid>
  <issn>1234-5678</issn>
  <eissn>8765-4321</eissn>
  <journalInfo lang="RUS"><title>Тестовый журнал</title></journalInfo>
  <journalInfo lang="ENG"><title>Test Journal</title></journalInfo>
  <issue>
    <volume>1</volume>
    <number>2</number>
    <dateUni>2026</dateUni>
    <pages>1-20</pages>
    <articles>
      <article>
        <pages>1-2</pages>
        <artType>RAR</artType>
        <artTitles>
          <artTitle lang="RUS">Русский заголовок</artTitle>
          <artTitle lang="ENG">English title</artTitle>
        </artTitles>
        <authors>
          <author>
            <individInfo lang="RUS">
              <surname>Иванов</surname>
              <initials>И.И.</initials>
              <orgName>Организация</orgName>
            </individInfo>
            <individInfo lang="ENG">
              <surname>Ivanov</surname>
              <initials>I.I.</initials>
              <orgName>Organization</orgName>
            </individInfo>
          </author>
        </authors>
        <abstracts>
          <abstract lang="RUS">{words}</abstract>
          <abstract lang="ENG">{eng_words}</abstract>
        </abstracts>
        <keywords>
          <kwdGroup lang="RUS"><keyword>тест</keyword><keyword>наука</keyword></kwdGroup>
          <kwdGroup lang="ENG"><keyword>test</keyword><keyword>science</keyword></kwdGroup>
        </keywords>
        <references>
          <reference><refInfo lang="RUS"><text>Источник 1</text></refInfo></reference>
          <reference><refInfo lang="ENG"><text>Reference 1</text></refInfo></reference>
          <reference><refInfo lang="UNK"><text>Universal ref</text></refInfo></reference>
        </references>
      </article>
      <article>
        <pages>3-5</pages>
        <artType>RAR</artType>
        <artTitles>
          <artTitle lang="RUS">Только русский</artTitle>
        </artTitles>
        <authors>
          <author>
            <individInfo lang="RUS">
              <surname>Петров</surname>
              <initials>П.П.</initials>
            </individInfo>
          </author>
        </authors>
        <abstracts>
          <abstract lang="RUS">{words}</abstract>
        </abstracts>
        <keywords>
          <kwdGroup lang="RUS"><keyword>только</keyword></kwdGroup>
        </keywords>
        <references>
          <reference><refInfo lang="RUS"><text>Источник А</text></refInfo></reference>
        </references>
      </article>
    </articles>
  </issue>
</journal>
"""


def _write_sample(tmp_path: Path) -> Path:
    xml_path = tmp_path / "sample_journal.xml"
    words = "слово " * 80
    eng_words = "word " * 80
    xml_path.write_text(
        SAMPLE_JOURNAL_XML.format(words=words, eng_words=eng_words),
        encoding="utf-8",
    )
    return xml_path


def test_analyze_journal_xml_summary_and_articles(tmp_path: Path):
    xml_path = _write_sample(tmp_path)
    report = analyze_journal_xml(xml_path)

    assert report["journal"]["titleid"] == "123"
    assert report["journal"]["title_ru"] == "Тестовый журнал"
    assert report["journal"]["title_en"] == "Test Journal"
    assert report["issue"]["volume"] == "1"
    assert report["issue"]["number"] == "2"
    assert report["summary"]["articles_total"] == 2
    assert report["summary"]["articles_with_errors"] >= 1
    assert report["summary"]["missing_eng_title"] == 1
    assert report["summary"]["missing_eng_abstract"] == 1
    assert report["summary"]["missing_eng_keywords"] == 1

    ok_article = report["articles"][0]
    assert ok_article["has_title_en"] is True
    assert ok_article["references_rus"] == 1
    assert ok_article["references_eng"] == 1
    assert ok_article["references_unk"] == 1
    assert ok_article["authors_count"] == 1
    assert ok_article["authors"][0]["name_ru"] == "Иванов И.И."
    assert ok_article["authors"][0]["affiliation_en"] == "Organization"
    assert not any("источник" in t.lower() for t in ok_article["critical_issues"])
    assert not any("UNK" in t for _, t in ok_article["issues"])
    preview_langs = {p["lang"] for p in ok_article["references_preview"]}
    assert preview_langs == {"RUS", "ENG", "UNK"}
    rus_prev = next(p for p in ok_article["references_preview"] if p["lang"] == "RUS")
    assert rus_prev["first"] == "Источник 1"
    assert rus_prev["same"] is True

    bad_article = report["articles"][1]
    assert bad_article["has_title_en"] is False
    assert bad_article["severity"] == "error"
    assert any("нет названия (ENG)" in t for t in bad_article["critical_issues"])
    assert any("аффилиац" in t for t in bad_article["secondary_issues"])
    # Только RUS-источники — не ошибка
    assert not any("нет источников" in t for t in bad_article["critical_issues"])


def test_references_unk_only_is_ok():
    from report_generator import collect_article_issues, validate_references_data

    status = validate_references_data({"UNK": ["Ref 1", "Ref 2"]})
    assert "❌" not in status["comparison"]
    assert status["total"] == "2"

    article = {
        "titles": {"RUS": "Т", "ENG": "T"},
        "abstracts": {
            "RUS": {"full_text": "a " * 80},
            "ENG": {"full_text": "b " * 80},
        },
        "keywords": {"RUS": ["a"], "ENG": ["b"]},
        "references": {"UNK": ["Ref 1"]},
        "authors": [
            {
                "RUS": {"surname": "Иванов", "initials": "И.И.", "orgName": "Орг"},
                "ENG": {"surname": "Ivanov", "initials": "I.I.", "orgName": "Org"},
            }
        ],
    }
    issues = collect_article_issues(article)
    assert not any("источник" in t.lower() for _, t in issues)


def test_references_empty_is_critical():
    from report_generator import collect_article_issues, validate_references_data

    status = validate_references_data({})
    assert "❌" in status["comparison"]

    article = {
        "titles": {"RUS": "Т", "ENG": "T"},
        "abstracts": {
            "RUS": {"full_text": "a " * 80},
            "ENG": {"full_text": "b " * 80},
        },
        "keywords": {"RUS": ["a"], "ENG": ["b"]},
        "references": {},
        "authors": [
            {
                "RUS": {"surname": "Иванов", "initials": "И.И.", "orgName": "Орг"},
                "ENG": {"surname": "Ivanov", "initials": "I.I.", "orgName": "Org"},
            }
        ],
    }
    issues = collect_article_issues(article)
    assert ("critical", "нет источников") in issues


def test_analyze_journal_xml_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        analyze_journal_xml(tmp_path / "missing.xml")


def test_analyze_journal_xml_invalid_xml(tmp_path: Path):
    bad = tmp_path / "bad.xml"
    bad.write_text("<journal><broken>", encoding="utf-8")
    with pytest.raises(ValueError):
        analyze_journal_xml(bad)


def test_generate_xml_html_report_creates_html(tmp_path: Path):
    xml_path = _write_sample(tmp_path)
    out_path = tmp_path / "report.html"
    result_path = generate_xml_html_report(xml_path, out_path)

    assert result_path == out_path
    assert out_path.exists()
    html = out_path.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html
    assert "Отчет по XML файлу" in html
    assert "Источники (UNK)" in html


def test_generate_xml_html_report_missing_input_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        generate_xml_html_report(tmp_path / "missing.xml", tmp_path / "out.html")
