"""Тесты письма для редакции по результатам валидации XML."""

from __future__ import annotations

from ipsas.modules.journal_xml.editorial_letter import (
    build_xml_editorial_letter,
    group_xml_findings,
)


def test_group_xml_findings_merges_articles_and_schema():
    findings = group_xml_findings(
        {
            "articles": [
                {
                    "index": 1,
                    "critical_issues": ["Отсутствует DOI"],
                    "secondary_issues": ["Нет аннотации EN"],
                },
                {
                    "index": 2,
                    "critical_issues": ["Отсутствует DOI"],
                    "secondary_issues": [],
                },
            ]
        },
        schema_result={
            "errors": [{"line": 10, "message": "Element invalid"}],
            "warnings": [{"message": "Deprecated attr"}],
        },
        metadata_error=None,
    )
    assert findings["errors"]
    doi = next(x for x in findings["errors"] if x["text"] == "Отсутствует DOI")
    assert doi["count"] == 2
    assert doi["articles"] == [1, 2]
    schema_err = next(x for x in findings["errors"] if "[Схема]" in x["text"])
    assert schema_err["issue_level"] is True
    assert "Строка 10" in schema_err["text"]
    assert findings["warnings"]
    assert any("Нет аннотации EN" in w["text"] for w in findings["warnings"])


def test_build_xml_editorial_letter_lists_issues():
    text = build_xml_editorial_letter(
        report={
            "journal": {"title_ru": "Тестовый журнал"},
            "issue": {"volume": "3", "number": "1", "date_uni": "2025"},
            "articles": [
                {
                    "index": 1,
                    "critical_issues": ["Нет keywords EN"],
                    "secondary_issues": [],
                }
            ],
        },
        schema_result={"errors": [], "warnings": []},
        source_file="issue.xml",
        generated_at="01.01.2026 12:00",
    )
    assert "Уважаемые коллеги!" in text
    assert "XML-файла" in text
    assert "issue.xml" in text
    assert "Тестовый журнал" in text
    assert "замечаний высокой значимости — 1" in text
    assert "Просим ознакомиться с результатами проверки" in text
    assert "Просим устранить" not in text
    assert "Национальной платформы периодических научных изданий" in text


def test_build_xml_editorial_letter_ok_case():
    text = build_xml_editorial_letter(
        report={
            "journal": {"title_ru": "Journal"},
            "issue": {},
            "articles": [{"index": 1, "critical_issues": [], "secondary_issues": []}],
        },
        schema_result={"valid": True, "errors": [], "warnings": []},
        source_file="ok.xml",
    )
    assert "замечаний высокой значимости" in text
    assert "не выявлено" in text
    assert "Просим устранить" not in text
