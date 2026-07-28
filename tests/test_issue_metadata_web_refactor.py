"""Тесты рефакторинга issue_metadata web + findings helper."""

from __future__ import annotations

from ipsas.common.models import Finding, Severity
from ipsas.modules.issue_metadata.findings import (
    finding_from_validation_message,
    warning_dict,
)
from ipsas.modules.issue_metadata.models import ValidationMessage


def test_warning_dict_compatible_with_templates():
    w = warning_dict("Нет DOI", severity="error", field="doi", code="doi.missing")
    assert w["text"] == "Нет DOI"
    assert w["severity"] == "error"
    assert w["field"] == "doi"
    assert w["code"] == "doi.missing"


def test_validation_message_finding_bridge():
    msg = ValidationMessage(text="x", severity="warning", field="title")
    finding = msg.to_finding()
    assert isinstance(finding, Finding)
    assert finding.severity == Severity.WARNING
    back = ValidationMessage.from_finding(finding)
    assert back.text == "x"
    assert finding_from_validation_message(back).message == "x"


def test_issue_metadata_blueprint_importable():
    from ipsas.web.app import create_app
    from ipsas.web.issue_metadata import issue_metadata_bp

    app = create_app()
    assert "issue_metadata" in app.blueprints
    assert issue_metadata_bp.name == "issue_metadata"
    client = app.test_client()
    assert client.get("/services/issue-metadata-parser").status_code == 200
