from __future__ import annotations

from pathlib import Path

from lxml import etree

from ipsas.modules.reference_cleaner import clean_references_with_stats
from ipsas.utils.operation_history import list_operations, record_operation


def test_clean_references_returns_samples():
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<root>
  <references>
    <reference>1. Sample Author. Title. 2020.</reference>
  </references>
</root>
"""
    tree = etree.fromstring(xml).getroottree()
    _, stats, samples = clean_references_with_stats(tree)
    assert stats.total_references == 1
    assert stats.changed_references >= 1
    assert isinstance(samples, list)
    assert samples
    assert "before" in samples[0] and "after" in samples[0]


def test_operation_history_roundtrip(monkeypatch, tmp_path):
    class _Settings:
        temp_dir = tmp_path

    monkeypatch.setattr("ipsas.utils.operation_history.get_settings", lambda: _Settings())
    record_operation(tool="test", title="Тест", status="ok", detail="detail", url="/dashboard")
    items = list_operations(limit=5)
    assert items
    assert items[0]["title"] == "Тест"
    assert items[0]["status"] == "ok"
