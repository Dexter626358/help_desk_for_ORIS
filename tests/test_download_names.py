"""Тесты имён скачиваемых файлов выпуска."""

from __future__ import annotations

from pathlib import Path

from ipsas.utils.download_names import (
    attachment_filename_from_xml,
    basename_from_issue_meta,
    basename_from_journal_xml,
    build_issue_download_basename,
    build_issue_download_filename,
    strip_temp_prefix,
)


def test_basename_with_volume_and_number() -> None:
    assert (
        build_issue_download_basename("0130-3082", "2026", "4", "2")
        == "0130-3082_2026_4_2"
    )


def test_basename_without_volume() -> None:
    assert build_issue_download_basename("0130-3082", 2026, None, "8") == "0130-3082_2026_8"


def test_basename_from_date_uni() -> None:
    assert (
        build_issue_download_basename("0869-5733", "2025-03-01", "12", "1")
        == "0869-5733_2025_12_1"
    )


def test_basename_prefers_issn_over_eissn() -> None:
    assert (
        build_issue_download_basename(None, "2024", "1", "2", eissn="1234-5678")
        == "1234-5678_2024_1_2"
    )


def test_filename_with_extension() -> None:
    assert (
        build_issue_download_filename("0130-3082", "2026", "4", "2", extension=".xml")
        == "0130-3082_2026_4_2_report.xml"
    )


def test_basename_from_issue_meta_dict() -> None:
    assert (
        basename_from_issue_meta(
            {"issn": "0130-3082", "year": "2026", "volume": "4", "issue": "2"}
        )
        == "0130-3082_2026_4_2"
    )


def test_strip_temp_prefix() -> None:
    assert (
        strip_temp_prefix("20260727_183415_2e6d9375_0130-3082_2026_4_2_report.html")
        == "0130-3082_2026_4_2_report.html"
    )


def test_basename_from_journal_xml(tmp_path: Path) -> None:
    xml = tmp_path / "issue.xml"
    xml.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <root>
          <issn>0130-3082</issn>
          <eissn>3034-5391</eissn>
          <issue>
            <volume>4</volume>
            <number>2</number>
            <dateUni>2026</dateUni>
          </issue>
        </root>
        """,
        encoding="utf-8",
    )
    assert basename_from_journal_xml(xml) == "0130-3082_2026_4_2"
    assert attachment_filename_from_xml(xml, extension=".html") == "0130-3082_2026_4_2_report.html"


def test_basename_journal_xml_without_volume(tmp_path: Path) -> None:
    xml = tmp_path / "issue.xml"
    xml.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <root>
          <issn>0130-3082</issn>
          <issue>
            <number>8</number>
            <dateUni>2025</dateUni>
          </issue>
        </root>
        """,
        encoding="utf-8",
    )
    assert basename_from_journal_xml(xml) == "0130-3082_2025_8"
