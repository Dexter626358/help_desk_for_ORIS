"""Тесты письма редакции по проверке сайта журнала."""

from __future__ import annotations

from ipsas.modules.journal_site.editorial_letter import build_journal_site_editorial_letter


def _sample_report(*, with_problems: bool = True) -> dict:
    missing = {
        "id": "policy.peer_review",
        "title": "Рецензирование",
        "category": "policies",
        "requirement": "required",
        "applicable": True,
        "status": "missing",
        "score": 0,
        "max_score": 3,
        "critical": True,
        "page_label": "О журнале → Редакционная политика",
        "value_preview": "",
    }
    optional = {
        "id": "quality.orcid_team",
        "title": "ORCID членов редколлегии",
        "category": "quality",
        "requirement": "optional",
        "applicable": True,
        "status": "missing",
        "score": 0,
        "max_score": 3,
        "critical": False,
        "page_label": "О журнале → Редакция",
        "value_preview": "",
    }
    filled = {
        "id": "journal.title",
        "title": "Название журнала",
        "category": "homepage",
        "requirement": "required",
        "applicable": True,
        "status": "complete",
        "score": 3,
        "max_score": 3,
        "critical": True,
        "page_label": "Главная",
        "value_preview": "Test Journal",
    }
    en_fields = [filled, missing, optional] if with_problems else [filled]
    ru_fields = [filled, missing, optional] if with_problems else [filled]
    must = [missing] if with_problems else []
    tips = [optional] if with_problems else []
    return {
        "journal_title": "Test Journal",
        "base_url": "https://journals.example/2312-1327",
        "generated_at": "29.07.2026 12:00",
        "completeness_percent": 80.0 if with_problems else 100.0,
        "quality_percent": 40.0 if with_problems else 90.0,
        "completeness_ru": 85.0 if with_problems else 100.0,
        "completeness_en": 75.0 if with_problems else 100.0,
        "level": "medium" if with_problems else "high",
        "level_label": "средняя заполненность" if with_problems else "хорошо заполнен",
        "locales": {
            "ru": {
                "lang": "ru",
                "label": "Русский",
                "locale_code": "ru_RU",
                "fields": ru_fields,
                "must_fix": must,
                "recommendations": tips,
                "required_filled": 1,
                "required_total": 2 if with_problems else 1,
            },
            "en": {
                "lang": "en",
                "label": "English",
                "locale_code": "en_US",
                "fields": en_fields,
                "must_fix": must,
                "recommendations": tips,
                "required_filled": 1,
                "required_total": 2 if with_problems else 1,
            },
        },
    }


def test_journal_site_letter_lists_missing_fields():
    text = build_journal_site_editorial_letter(
        _sample_report(with_problems=True),
        journal_url="https://journals.example/2312-1327/index",
    )
    assert "Уважаемые коллеги!" in text
    assert "автоматизированной проверки заполненности" in text
    assert "Test Journal" in text
    assert "базовая заполненность" in text
    assert "Необходимо заполнить" in text
    assert "Рецензирование" in text
    assert "Дополнительные рекомендации" in text
    assert "ORCID членов редколлегии" in text
    assert "служба поддержки национальной платформы" in text


def test_journal_site_letter_ok_case():
    text = build_journal_site_editorial_letter(_sample_report(with_problems=False))
    assert "замечаний нет" in text or "незаполненных обязательных полей не выявлено" in text
    assert "Необходимо заполнить" not in text
