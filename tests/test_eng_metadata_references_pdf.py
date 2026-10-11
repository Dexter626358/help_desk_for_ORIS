"""Unit tests: PDF literature unwrap for ENG Raw editor."""

from __future__ import annotations

import pytest

from ipsas.config.settings import reset_settings
from ipsas.modules.eng_metadata.references_pdf import (
    join_lines,
    normalize_line,
    parse_references,
    serialize_references,
    strip_leading_reference_number,
    strip_reference_numbers,
)
from ipsas.web.app import create_app


@pytest.fixture()
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("IPSAS_ENV", "development")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    reset_settings()
    from ipsas.config import settings as settings_mod

    settings = settings_mod.get_settings()
    monkeypatch.setattr(settings, "temp_dir", tmp_path / "temp")
    (tmp_path / "temp").mkdir(parents=True, exist_ok=True)
    application = create_app(testing=True)
    yield application
    reset_settings()


@pytest.fixture()
def client(app):
    return app.test_client()

NUMBERED_PDF = """\
117. Akti F., Balci S., Dogu T. Role of synthesis media
on properties of tin and copper incorporated
SBA-15 catalysts and their activity in selective
oxidation of ethanol // Mater. Chem. Phys. 2019.
Vol. 223. Pp. 249–259.
https://doi.org/10.1016/j.matchemphys.2018.10.068
118. Lee J.S., Han G.B., Kang M. Low-temperature
steam reforming of ethanol for carbon monoxide-free
hydrogen production over Sn-incorporated SBA-15 catalysts
// Energy. 2012. Vol. 44. No. 1. Pp. 248–256.
https://doi.org/10.1016/j.energy.2012.06.032
"""

EXPECTED_1 = (
    "117. Akti F., Balci S., Dogu T. Role of synthesis media on properties of tin "
    "and copper incorporated SBA-15 catalysts and their activity in selective "
    "oxidation of ethanol // Mater. Chem. Phys. 2019. Vol. 223. Pp. 249–259. "
    "https://doi.org/10.1016/j.matchemphys.2018.10.068"
)

EXPECTED_2 = (
    "118. Lee J.S., Han G.B., Kang M. Low-temperature steam reforming of ethanol "
    "for carbon monoxide-free hydrogen production over Sn-incorporated SBA-15 "
    "catalysts // Energy. 2012. Vol. 44. No. 1. Pp. 248–256. "
    "https://doi.org/10.1016/j.energy.2012.06.032"
)

UNNUMBERED_PDF = """\
Akti F., Balci S., Dogu T. Role of synthesis media
on properties of tin and copper incorporated
SBA-15 catalysts and their activity in selective
oxidation of ethanol // Mater. Chem. Phys. 2019.
Vol. 223. Pp. 249–259.
https://doi.org/10.1016/j.matchemphys.2018.10.068
Lee J.S., Han G.B., Kang M. Low-temperature
steam reforming of ethanol for carbon monoxide-free
hydrogen production over Sn-incorporated SBA-15 catalysts
// Energy. 2012. Vol. 44. No. 1. Pp. 248–256.
https://doi.org/10.1016/j.energy.2012.06.032
"""


def test_normalize_line_nbsp_and_spaces() -> None:
    assert normalize_line("  A\u00a0B\t\tC  ") == "A B C"


def test_join_broken_doi_url_without_space() -> None:
    assert (
        join_lines(
            "https://doi.org/10.1016/",
            "j.energy.2012.06.032",
        )
        == "https://doi.org/10.1016/j.energy.2012.06.032"
    )


def test_join_ordinary_lines_with_space() -> None:
    assert (
        join_lines("Role of synthesis media", "on properties of tin")
        == "Role of synthesis media on properties of tin"
    )


def test_join_keeps_hyphen_conservatively() -> None:
    assert join_lines("low-", "temperature") == "low-temperature"
    assert join_lines("SBA-15", "catalysts") == "SBA-15 catalysts"


def test_numbered_pdf_fixture() -> None:
    result = parse_references(NUMBERED_PDF)
    assert len(result.references) == 2
    assert result.references[0].text == EXPECTED_1
    assert result.references[1].text == EXPECTED_2
    assert "117." in result.references[0].text
    assert "118." in result.references[1].text
    assert result.joined_breaks >= 8


def test_unnumbered_pdf_fixture() -> None:
    result = parse_references(UNNUMBERED_PDF)
    assert len(result.references) == 2
    assert result.references[0].text.startswith("Akti F.")
    assert result.references[1].text.startswith("Lee J.S.")
    assert "https://doi.org/10.1016/j.matchemphys.2018.10.068" in result.references[0].text
    assert "https://doi.org/10.1016/j.energy.2012.06.032" in result.references[1].text
    # без пробела внутри DOI
    assert "/ j." not in result.text
    assert "10.1016/j.energy" in result.references[1].text


def test_square_brackets_and_empty_lines() -> None:
    text = """\
[1] Smith J. First article title that wraps
on the next physical line // Journal. 2020. Vol. 1. Pp. 1–2.

[2] Brown A. Second article
// Other J. 2021. Pp. 3–4.
"""
    result = parse_references(text)
    assert len(result.references) == 2
    assert result.references[0].text.startswith("[1] Smith J.")
    assert "on the next physical line" in result.references[0].text
    assert result.references[1].text.startswith("[2] Brown A.")


def test_russian_authors() -> None:
    text = """\
Иванов И.И., Петров П.П. Название статьи о катализе
на носителе // Журнал. 2019. Т. 12. С. 10–20.
Сидоров С.С. Другая работа
// Вестник. 2020. С. 5–8.
"""
    result = parse_references(text)
    assert len(result.references) == 2
    assert result.references[0].text.startswith("Иванов И.И.")
    assert "на носителе" in result.references[0].text
    assert result.references[1].text.startswith("Сидоров С.С.")


def test_apa_style_year() -> None:
    text = """\
Smith, J. A., & Brown, B. C. (2024). Article title that
continues here. Journal Name, 12(3), 45–67.
https://doi.org/10.1000/example
Jones, P. (2023). Another title.
Journal, 1(1), 1–2.
"""
    result = parse_references(text)
    assert len(result.references) == 2
    assert "(2024)" in result.references[0].text
    assert result.references[1].text.startswith("Jones, P.")


def test_serialize_roundtrip_lines() -> None:
    result = parse_references(NUMBERED_PDF)
    serialized = serialize_references(result.references)
    assert serialized == result.text
    again = parse_references(serialized)
    assert len(again.references) == 2


def test_does_not_invent_numbers() -> None:
    result = parse_references(UNNUMBERED_PDF)
    assert not result.references[0].text[:3].isdigit()


def test_strip_leading_reference_number() -> None:
    assert (
        strip_leading_reference_number("117. Akti F., Balci S.")
        == "Akti F., Balci S."
    )
    assert strip_leading_reference_number("[3] Smith J. Title.") == "Smith J. Title."
    assert strip_leading_reference_number("12) Brown A. Work.") == "Brown A. Work."
    assert strip_leading_reference_number("№ 4. Иванов И.И.") == "Иванов И.И."
    assert (
        strip_leading_reference_number("Smith J. No number here.")
        == "Smith J. No number here."
    )


def test_strip_reference_numbers_all_lines() -> None:
    text = "\n".join(
        [
            "117. First source about catalysis.",
            "[118] Second source with brackets.",
            "Third without number.",
        ]
    )
    cleaned, changed = strip_reference_numbers(text)
    assert changed == 2
    assert cleaned.splitlines() == [
        "First source about catalysis.",
        "Second source with brackets.",
        "Third without number.",
    ]


def test_unwrap_api_endpoint(client) -> None:
    resp = client.post(
        "/services/eng-metadata/references/unwrap",
        json={"text": NUMBERED_PDF},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 2
    assert data["text"].splitlines()[0] == EXPECTED_1
    assert data["joined_breaks"] >= 1
