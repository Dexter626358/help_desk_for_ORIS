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


def test_run_parse_task_reports_progress_steps(monkeypatch):
    from ipsas.services import audit_published_issue as svc

    seen: list[tuple[str, int]] = []

    def fake_parse(issue_url: str, *, on_progress=None):
        assert issue_url == "https://example.com/issue/1"
        if on_progress:
            on_progress("fetch")
            on_progress("articles")
            on_progress("report")
        return {"issue": {}, "articles": [], "notice": None}

    def fake_set(task_id: str, **kwargs):
        if "progress" in kwargs:
            seen.append((str(kwargs["progress"]), int(kwargs["progress_step"])))

    def fake_release(**kwargs):
        return None

    monkeypatch.setattr(svc, "parse_issue", fake_parse)
    svc.run_parse_task(
        task_id="abc",
        issue_url="https://example.com/issue/1",
        user_key="anonymous",
        task_set=fake_set,
        release_inflight=fake_release,
    )
    assert ("fetch", 1) in seen
    assert ("articles", 2) in seen
    assert ("report", 3) in seen


def test_issue_metadata_status_returns_progress_step(monkeypatch):
    from ipsas.web.app import create_app
    from ipsas.web.issue_metadata import routes as im_routes

    app = create_app()
    task_id = "progressstatus1"

    def fake_get(tid: str):
        assert tid == task_id
        return {
            "status": "running",
            "started_at": 0,
            "progress": "articles",
            "progress_step": 2,
            "issue_url": "https://example.com/i",
        }

    monkeypatch.setattr(im_routes, "task_get", fake_get)
    client = app.test_client()
    resp = client.get(f"/services/issue-metadata-parser/status/{task_id}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "running"
    assert data["progress"] == "articles"
    assert data["progress_step"] == 2
