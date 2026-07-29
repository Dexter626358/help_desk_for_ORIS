"""Тесты игнора допустимых отклонений XSD (issue/@type)."""

from __future__ import annotations

from pathlib import Path

from ipsas.modules.xml_validator import (
    XMLValidator,
    _filter_schema_errors,
    _is_ignored_schema_error,
)


def test_issue_type_attribute_error_is_ignored():
    err = {
        "line": 9,
        "column": 0,
        "message": "Элемент 'issue', атрибут 'type': Атрибут 'type' не разрешен.",
        "element": "issue",
        "path": "/journal/issue",
    }
    assert _is_ignored_schema_error(err) is True
    assert _filter_schema_errors([err, {"message": "Другая ошибка", "element": "article"}]) == [
        {"message": "Другая ошибка", "element": "article"}
    ]


def test_journal3_xsd_accepts_issue_type(tmp_path: Path):
    schemas_dir = Path(__file__).resolve().parents[1] / "schemas"
    xsd = schemas_dir / "journal3.xsd"
    if not xsd.exists():
        return

    xml = tmp_path / "minimal.xml"
    xml.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<journal>
  <titleid>1</titleid>
  <issn>1234-5678</issn>
  <issue type="regular">
    <volume>1</volume>
    <number>1</number>
    <dateUni>2025</dateUni>
    <articles/>
  </issue>
</journal>
""",
        encoding="utf-8",
    )
    validator = XMLValidator()
    assert validator.load_schema(xsd)
    result = validator.validate_xml_file(xml)
    type_errs = [
        e
        for e in result.get("errors") or []
        if "type" in str(e.get("message") or "").lower()
        and "issue" in str(e.get("element") or e.get("path") or "").lower()
    ]
    assert type_errs == []
