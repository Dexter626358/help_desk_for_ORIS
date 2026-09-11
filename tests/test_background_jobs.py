"""Ограничение фоновых задач и POST-only для парсера выпуска."""

from __future__ import annotations

import threading

import pytest

from ipsas.jobs.executor import (
    BoundedJobExecutor,
    JobQueueFullError,
    reset_job_executor,
)
from ipsas.jobs.issue_metadata import interrupt_stale_running_tasks, task_get, task_set
from ipsas.config.settings import reset_settings
from ipsas.web.app import create_app


@pytest.fixture()
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("IPSAS_ENV", "development")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    reset_settings()
    reset_job_executor()
    from ipsas.config import settings as settings_mod

    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path / "temp")
    (tmp_path / "temp").mkdir(parents=True, exist_ok=True)
    app = create_app(testing=True)
    yield app.test_client()
    reset_job_executor()
    reset_settings()


def test_issue_metadata_process_rejects_get(client):
    resp = client.get(
        "/services/issue-metadata-parser/process",
        query_string={"issue_url": "https://example.org/issue/1"},
    )
    assert resp.status_code == 405


def test_bounded_executor_rejects_when_full():
    reset_job_executor()
    ex = BoundedJobExecutor(max_workers=1, max_queued=0)
    started = threading.Event()
    release = threading.Event()

    def blocker():
        started.set()
        release.wait(timeout=2)

    fut = ex.submit(blocker)
    assert started.wait(timeout=1)
    with pytest.raises(JobQueueFullError):
        ex.submit(lambda: None)
    release.set()
    fut.result(timeout=2)
    ex.shutdown(wait=True)


def test_interrupt_stale_running_tasks(tmp_path, monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "development")
    reset_settings()
    from ipsas.config import settings as settings_mod

    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path)
    task_id = "abc123deadbeef"
    task_set(task_id, status="running", issue_url="https://example.org/x")
    marked = interrupt_stale_running_tasks(reason="interrupted: test")
    assert marked == 1
    task = task_get(task_id)
    assert task is not None
    assert task["status"] == "error"
    assert "interrupted" in task["error"]
    reset_settings()