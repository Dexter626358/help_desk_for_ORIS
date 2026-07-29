"""Тесты письма для редакции по результатам проверки выпуска."""

from __future__ import annotations

from ipsas.modules.issue_metadata.editorial_letter import build_editorial_letter


def test_editorial_letter_lists_errors_and_warnings():
    text = build_editorial_letter(
        result={
            "issue": {
                "journal_title_ru": "Тестовый журнал",
                "volume": "3",
                "issue": "1",
                "year": "2025",
            },
            "articles": [{}, {}],
        },
        findings={
            "errors": [
                {
                    "text": "Отсутствует DOI",
                    "count": 2,
                    "articles": [1, 2],
                    "issue_level": False,
                }
            ],
            "warnings": [
                {
                    "text": "Нет аннотации на английском",
                    "count": 1,
                    "articles": [1],
                    "issue_level": False,
                }
            ],
            "by_category": [
                {
                    "id": "identifiers",
                    "title": "Идентификаторы",
                    "total": 2,
                    "error_count": 2,
                    "warning_count": 0,
                }
            ],
        },
        issue_url="https://journals.example/issue/1",
        generated_at="01.01.2026 12:00",
    )
    assert "Уважаемые коллеги!" in text
    assert "Тестовый журнал" in text
    assert "том 3" in text
    assert "№ 1" in text
    assert "ошибок (требуют исправления): 2" in text
    assert "Отсутствует DOI" in text
    assert "статьи: 1, 2" in text
    assert "Нет аннотации на английском" in text
    assert "Идентификаторы" in text
    assert "https://journals.example/issue/1" in text


def test_editorial_letter_ok_case():
    text = build_editorial_letter(
        result={"issue": {"journal_title": "Journal"}, "articles": []},
        findings={"errors": [], "warnings": [], "by_category": []},
    )
    assert "критичных замечаний не выявлено" in text
    assert "Необходимо исправить" not in text
    assert "служба поддержки национальной платформы периодических научных изданий" in text
