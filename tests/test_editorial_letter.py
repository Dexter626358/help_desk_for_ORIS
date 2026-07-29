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
                    "text": "Несоответствие метаданных организаций RU/EN: отсутствует английская форма названия",
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
                    "id": "authors_orgs",
                    "title": "Авторы и организации",
                    "total": 2,
                    "error_count": 2,
                    "warning_count": 0,
                },
                {
                    "id": "texts",
                    "title": "Названия и тексты",
                    "total": 1,
                    "error_count": 0,
                    "warning_count": 1,
                },
            ],
        },
        issue_url="https://journals.example/issue/1",
        generated_at="01.01.2026 12:00",
    )
    assert "Уважаемые коллеги!" in text
    assert "Тестовый журнал" in text
    assert "том 3" in text
    assert "№ 1" in text
    assert "за 2025 год" in text
    assert "замечаний высокой значимости — 2" in text
    assert "замечаний, требующих дополнительной проверки, — 1" in text
    assert "ошибок (требуют исправления)" not in text
    assert "Предупреждения (рекомендуется исправить)" not in text
    assert "Просим устранить" not in text
    assert "Просим ознакомиться с результатами проверки" in text
    assert "возможные неточности" in text
    assert "не всегда свидетельствуют об ошибке" in text
    assert "Основные замечания связаны" in text
    assert "организаций" in text
    assert "https://journals.example/issue/1" in text
    assert "Национальной платформы периодических научных изданий" in text


def test_editorial_letter_ok_case():
    text = build_editorial_letter(
        result={"issue": {"journal_title": "Journal"}, "articles": []},
        findings={"errors": [], "warnings": [], "by_category": []},
    )
    assert "замечаний высокой значимости" in text
    assert "не выявлено" in text
    assert "Просим устранить" not in text
    assert "служба поддержки" in text
    assert "Национальной платформы периодических научных изданий" in text
