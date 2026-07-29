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
    assert ok_article["unique_affiliations_ru"] == 1
    assert ok_article["unique_affiliations_en"] == 1
    assert ok_article["unique_affiliations_ru_items"] == ["Организация"]
    assert ok_article["unique_affiliations_en_items"] == ["Organization"]
    assert ok_article["authors"][0]["name_ru"] == "Иванов И.И."
    assert ok_article["authors"][0]["affiliation_en"] == "Organization"
    assert ok_article["keywords_ru_preview"]["preview"] == "тест ... наука"
    assert ok_article["keywords_en_preview"]["preview"] == "test ... science"
    assert ok_article["keywords_ru_preview"]["count"] == 2
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


def test_numbered_references_are_secondary_warning():
    from ipsas.modules.journal_xml_report import collect_article_issues

    article = {
        "titles": {"RUS": "Т", "ENG": "T"},
        "abstracts": {
            "RUS": {"full_text": "a " * 80},
            "ENG": {"full_text": "b " * 80},
        },
        "keywords": {"RUS": ["а"], "ENG": ["b"]},
        "references": {
            "RUS": ["1. Источник А", "2. Источник Б"],
            "ENG": ["Reference without number"],
        },
        "authors": [
            {
                "RUS": {"surname": "Иванов", "initials": "И.И.", "orgName": "Орг"},
                "ENG": {"surname": "Ivanov", "initials": "I.I.", "orgName": "Org"},
            }
        ],
    }
    issues = collect_article_issues(article)
    assert any(
        s == "secondary" and "нумерац" in t and "2 из 3" in t
        for s, t in issues
    )


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


def test_nested_markup_in_title_and_abstract_is_fully_extracted(tmp_path: Path):
    xml_path = tmp_path / "nested.xml"
    xml_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<journal>
  <titleid>1</titleid>
  <issue><articles>
    <article>
      <pages>1</pages>
      <artTitles>
        <artTitle lang="RUS">Наночастицы меди и ее оксидов в синтезе <i>N</i>-гетероциклов</artTitle>
        <artTitle lang="ENG">Copper and Copper Oxide Nanoparticles in the Synthesis of <i>N</i>-Heterocycles</artTitle>
      </artTitles>
      <authors>
        <author>
          <individInfo lang="RUS"><surname>Иванов</surname><initials>И.И.</initials><orgName>Орг</orgName></individInfo>
          <individInfo lang="ENG"><surname>Ivanov</surname><initials>I.I.</initials><orgName>Org</orgName></individInfo>
        </author>
      </authors>
      <abstracts>
        <abstract lang="RUS">Изучено <b>влияние</b> наночастиц на синтез гетероциклов в различных условиях.</abstract>
        <abstract lang="ENG">The <b>effect</b> of nanoparticles on the synthesis of heterocycles was studied.</abstract>
      </abstracts>
      <keywords>
        <kwdGroup lang="RUS"><keyword>а</keyword></kwdGroup>
        <kwdGroup lang="ENG"><keyword>b</keyword></kwdGroup>
      </keywords>
      <references>
        <reference><refInfo lang="UNK"><text>Ref</text></refInfo></reference>
      </references>
    </article>
  </articles></issue>
</journal>
""",
        encoding="utf-8",
    )
    report = analyze_journal_xml(xml_path)
    article = report["articles"][0]
    assert "N-гетероциклов" in article["title_ru"]
    assert "N-Heterocycles" in article["title_en"]
    assert article["title_ru"].startswith("Наночастицы меди")
    assert "влияние" in article["abstract_ru"]["full_text"]
    assert article["abstract_ru"]["full_text"].startswith("Изучено")
    assert "effect" in article["abstract_en"]["full_text"]
    with pytest.raises(FileNotFoundError):
        analyze_journal_xml(tmp_path / "missing.xml")


def test_unique_affiliations_count_across_authors(tmp_path: Path):
    xml_path = tmp_path / "aff.xml"
    xml_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<journal>
  <titleid>1</titleid>
  <issue><articles>
    <article>
      <pages>1</pages>
      <artTitles>
        <artTitle lang="RUS">Заголовок</artTitle>
        <artTitle lang="ENG">Title</artTitle>
      </artTitles>
      <authors>
        <author>
          <individInfo lang="RUS"><surname>Иванов</surname><initials>И.И.</initials><orgName>МГУ; ИОХ РАН</orgName></individInfo>
          <individInfo lang="ENG"><surname>Ivanov</surname><initials>I.I.</initials><orgName>MSU; IOC RAS</orgName></individInfo>
        </author>
        <author>
          <individInfo lang="RUS"><surname>Петров</surname><initials>П.П.</initials><orgName>МГУ</orgName></individInfo>
          <individInfo lang="ENG"><surname>Petrov</surname><initials>P.P.</initials><orgName>MSU</orgName></individInfo>
        </author>
      </authors>
      <abstracts>
        <abstract lang="RUS">слово слово слово</abstract>
        <abstract lang="ENG">word word word</abstract>
      </abstracts>
      <keywords>
        <kwdGroup lang="RUS"><keyword>а</keyword></kwdGroup>
        <kwdGroup lang="ENG"><keyword>b</keyword></kwdGroup>
      </keywords>
      <references>
        <reference><refInfo lang="UNK"><text>Ref</text></refInfo></reference>
      </references>
    </article>
  </articles></issue>
</journal>
""",
        encoding="utf-8",
    )
    article = analyze_journal_xml(xml_path)["articles"][0]
    assert article["authors_count"] == 2
    assert article["unique_affiliations_ru"] == 2
    assert article["unique_affiliations_en"] == 2
    assert set(article["unique_affiliations_ru_items"]) == {"МГУ", "ИОХ РАН"}
    assert set(article["unique_affiliations_en_items"]) == {"MSU", "IOC RAS"}


def test_duplicate_reference_text_is_secondary_warning(tmp_path: Path):
    xml_path = tmp_path / "dup_refs.xml"
    xml_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<journal>
  <titleid>1</titleid>
  <issue><articles>
    <article>
      <pages>1</pages>
      <artTitles>
        <artTitle lang="RUS">Заголовок</artTitle>
        <artTitle lang="ENG">Title</artTitle>
      </artTitles>
      <authors>
        <author>
          <individInfo lang="RUS"><surname>Иванов</surname><initials>И.И.</initials><orgName>Орг</orgName></individInfo>
          <individInfo lang="ENG"><surname>Ivanov</surname><initials>I.I.</initials><orgName>Org</orgName></individInfo>
        </author>
      </authors>
      <abstracts>
        <abstract lang="RUS">слово слово слово</abstract>
        <abstract lang="ENG">word word word</abstract>
      </abstracts>
      <keywords>
        <kwdGroup lang="RUS"><keyword>а</keyword></kwdGroup>
        <kwdGroup lang="ENG"><keyword>b</keyword></kwdGroup>
      </keywords>
      <references>
        <reference>
1. Акжигитова Н.И. 1982. Галофильная растительность.
          <refinfo lang="ANY">
            <text>Акжигитова Н.И. 1982. Галофильная растительность.</text>
          </refinfo>
        </reference>
        <reference>
          <refInfo lang="UNK"><text>Чистый источник без дубля</text></refInfo>
        </reference>
      </references>
    </article>
  </articles></issue>
</journal>
""",
        encoding="utf-8",
    )
    article = analyze_journal_xml(xml_path)["articles"][0]
    assert article["references_duplicate_text_count"] == 1
    assert article["references_numbered_count"] >= 1
    issues_text = " ".join(t for _, t in article["issues"])
    assert "дублирование текста источников" in issues_text
    assert "нумерац" in issues_text


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
    assert "Отчёт о качестве XML" in html
    assert "Сводка" in html
    assert "Журнал и выпуск" in html
    assert "Тестовый журнал" in html
    assert "English title" in html
    # Тот же partial, что и web: без UI-кнопок
    assert "Новая проверка" not in html
    assert "На главную" not in html


def test_generate_xml_html_report_missing_input_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        generate_xml_html_report(tmp_path / "missing.xml", tmp_path / "out.html")


def test_keywords_missing_eng_is_critical_for_scientific():
    """Если на RUS есть ключевые слова — на ENG они обязательны для научной статьи."""
    from ipsas.modules.journal_xml_report import collect_article_issues

    article = {
        "art_type": "RAR",
        "titles": {"RUS": "Т", "ENG": "T"},
        "abstracts": {
            "RUS": {"full_text": "a " * 80},
            "ENG": {"full_text": "b " * 80},
        },
        "keywords": {"RUS": ["а"]},
        "references": {"UNK": ["Ref"]},
        "authors": [
            {
                "RUS": {"surname": "Иванов", "initials": "И.И.", "orgName": "Орг"},
                "ENG": {"surname": "Ivanov", "initials": "I.I.", "orgName": "Org"},
            }
        ],
    }
    issues = collect_article_issues(article)
    assert ("critical", "нет ключевых слов (ENG)") in issues


def test_title_only_congratulatory_needs_only_titles():
    from ipsas.modules.journal_xml_report import (
        classify_article_metadata_profile,
        collect_article_issues,
    )

    article = {
        "art_type": "PER",
        "titles": {
            "RUS": "Поздравление юбиляру",
            "ENG": "Congratulations to the jubilee",
        },
        "abstracts": {},
        "keywords": {},
        "references": {},
        "authors": [],
    }
    assert classify_article_metadata_profile(article) == "title_only"
    issues = collect_article_issues(article)
    assert issues == []


def test_title_only_jubilee_ignores_missing_affiliations():
    from ipsas.modules.journal_xml_report import (
        classify_article_metadata_profile,
        collect_article_issues,
    )

    article = {
        "art_type": "PER",
        "titles": {
            "RUS": "АНАТОЛИЮ ВИКТОРОВИЧУ КАРПОВУ — 70 ЛЕТ!",
            "ENG": "ANATOLY VIKTOROVICH KARPOV — 70 YEARS!",
        },
        "abstracts": {},
        "keywords": {},
        "references": {},
        "authors": [
            {
                "RUS": {"surname": "Редакция", "initials": "", "orgName": ""},
                "ENG": {"surname": "Editorial", "initials": "", "orgName": ""},
            }
        ],
    }
    assert classify_article_metadata_profile(article) == "title_only"
    issues = collect_article_issues(article)
    assert not any("аффилиац" in t for _, t in issues)
    assert issues == []


def test_title_only_book_review_by_title_heuristic():
    from ipsas.modules.journal_xml_report import (
        classify_article_metadata_profile,
        collect_article_issues,
    )

    article = {
        "art_type": "",
        "titles": {
            "RUS": "Отзыв на монографию Иванова",
            "ENG": "Review of the monograph by Ivanov",
        },
        "abstracts": {},
        "keywords": {},
        "references": {},
        "authors": [],
    }
    assert classify_article_metadata_profile(article) == "title_only"
    issues = collect_article_issues(article)
    assert issues == []


def test_title_only_still_requires_eng_title():
    from ipsas.modules.journal_xml_report import collect_article_issues

    article = {
        "art_type": "BRV",
        "titles": {"RUS": "Отзыв на книгу"},
        "abstracts": {},
        "keywords": {},
        "references": {},
        "authors": [],
    }
    issues = collect_article_issues(article)
    assert ("critical", "нет названия (ENG)") in issues


def test_title_only_eng_abstract_required_if_rus_present():
    from ipsas.modules.journal_xml_report import collect_article_issues

    article = {
        "art_type": "PER",
        "titles": {"RUS": "Поздравление", "ENG": "Congratulations"},
        "abstracts": {"RUS": {"full_text": "Текст поздравления"}},
        "keywords": {},
        "references": {},
        "authors": [],
    }
    issues = collect_article_issues(article)
    assert ("critical", "аннотация ENG: отсутствует") in issues


def test_scientific_requires_rus_abstract_keywords_authors():
    from ipsas.modules.journal_xml_report import collect_article_issues

    article = {
        "art_type": "RAR",
        "titles": {"RUS": "Исследование", "ENG": "Research"},
        "abstracts": {},
        "keywords": {},
        "references": {},
        "authors": [],
    }
    issues = collect_article_issues(article)
    texts = {t for _, t in issues}
    assert "аннотация RUS: отсутствует" in texts
    assert "нет ключевых слов" in texts
    assert "нет авторов" in texts
    assert "нет источников" in texts


def test_references_lang_case_normalized(tmp_path: Path):
    """lang='rus'/'eng' должны нормализоваться в RUS/ENG."""
    xml_path = tmp_path / "case.xml"
    xml_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<journal>
  <titleid>1</titleid>
  <issue><articles>
    <article>
      <pages>1</pages>
      <artTitles>
        <artTitle lang="rus">Заголовок</artTitle>
        <artTitle lang="eng">Title</artTitle>
      </artTitles>
      <authors>
        <author>
          <individInfo lang="rus"><surname>Иванов</surname><initials>И.И.</initials><orgName>Орг</orgName></individInfo>
          <individInfo lang="eng"><surname>Ivanov</surname><initials>I.I.</initials><orgName>Org</orgName></individInfo>
        </author>
      </authors>
      <abstracts>
        <abstract lang="rus">слово слово слово</abstract>
        <abstract lang="eng">word word word</abstract>
      </abstracts>
      <keywords>
        <kwdGroup lang="rus"><keyword>а</keyword></kwdGroup>
        <kwdGroup lang="eng"><keyword>b</keyword></kwdGroup>
      </keywords>
      <references>
        <reference><refInfo lang="unk"><text>Ref</text></refInfo></reference>
      </references>
    </article>
  </articles></issue>
</journal>
""",
        encoding="utf-8",
    )
    report = analyze_journal_xml(xml_path)
    article = report["articles"][0]
    assert article["has_title_ru"] is True
    assert article["has_title_en"] is True
    assert article["abstract_ru"]["present"] is True
    assert article["abstract_en"]["present"] is True
    assert article["keywords_ru"] == 1
    assert article["keywords_en"] == 1
    assert article["references_unk"] == 1
    assert article["authors"][0]["name_ru"] == "Иванов И.И."
