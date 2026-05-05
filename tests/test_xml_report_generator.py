"""Тесты генерации HTML-отчёта по XML."""

from __future__ import annotations

from pathlib import Path

import pytest

from ipsas.modules.xml_report_generator import generate_xml_html_report


def test_generate_xml_html_report_creates_html(tmp_path: Path):
    xml_path = tmp_path / "sample.xml"
    xml_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <titleid>123</titleid>
  <journalInfo lang="RUS"><title>Test Journal</title></journalInfo>
  <issue>
    <volume>1</volume>
    <number>1</number>
    <dateUni>2026</dateUni>
    <pages>1-10</pages>
  </issue>
  <articles>
    <article>
      <pages>1-2</pages>
      <artType>test</artType>
      <artTitles>
        <artTitle lang="RUS">Русский заголовок</artTitle>
        <artTitle lang="ENG">English title</artTitle>
      </artTitles>
      <authors>
        <author>
          <individInfo lang="RUS">
            <surname>Иванов</surname>
            <initials>И.И.</initials>
            <orgName>Организация</orgName>
          </individInfo>
          <individInfo lang="ENG">
            <surname>Ivanov</surname>
            <initials>I.I.</initials>
            <orgName>Organization</orgName>
          </individInfo>
        </author>
      </authors>
      <abstracts>
        <abstract lang="RUS">""" + ("слово " * 80) + """</abstract>
        <abstract lang="ENG">""" + ("word " * 80) + """</abstract>
      </abstracts>
      <keywords>
        <kwdGroup lang="RUS"><keyword>тест</keyword></kwdGroup>
        <kwdGroup lang="ENG"><keyword>test</keyword></kwdGroup>
      </keywords>
      <references>
        <reference><refInfo lang="RUS"><text>Источник 1</text></refInfo></reference>
        <reference><refInfo lang="ENG"><text>Reference 1</text></refInfo></reference>
      </references>
    </article>
  </articles>
</root>
""",
        encoding="utf-8",
    )

    out_path = tmp_path / "report.html"
    result_path = generate_xml_html_report(xml_path, out_path)

    assert result_path == out_path
    assert out_path.exists()

    html = out_path.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html
    assert "Отчет по XML файлу" in html


def test_generate_xml_html_report_missing_input_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        generate_xml_html_report(tmp_path / "missing.xml", tmp_path / "out.html")

