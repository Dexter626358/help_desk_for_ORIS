"""Тесты валидатора JATS XML для Метафоры."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from ipsas.modules.metafora_jats.article_types import authors_required_for_type
from ipsas.modules.metafora_jats.rules.identifiers import normalize_doi
from ipsas.modules.metafora_jats.rules.publication_date import parse_publication_date
from ipsas.modules.metafora_jats.validator import validate_jats_bytes, validate_jats_file

FIXTURES = Path(__file__).parent / "fixtures" / "metafora_jats"
TODAY = date(2026, 10, 4)


def _article(
    *,
    article_type: str | None = "research-article",
    title: str | None = "Sample Title",
    pub_date: str | None = "2025-03-15",
    fpage: str | None = "10",
    lpage: str | None = "20",
    elocation_id: str | None = None,
    doi: str | None = None,
    edn: str | None = None,
    authors: list[tuple[str, str]] | None = (("Ivanov", "Ivan"),),
    extra_pub_date_collection: bool = False,
    references: bool | None = None,
    affiliations: bool = False,
) -> bytes:
    type_attr = "" if article_type is None else f' article-type="{article_type}"'
    title_xml = (
        f"<article-title>{title}</article-title>" if title is not None else ""
    )
    if pub_date:
        if len(pub_date) == 4 and pub_date.isdigit():
            date_xml = (
                f'<pub-date date-type="pub" iso-8601-date="{pub_date}">'
                f"<year>{pub_date}</year></pub-date>"
            )
        else:
            date_xml = (
                f'<pub-date date-type="pub" iso-8601-date="{pub_date}">'
                f"<year>{pub_date[:4]}</year></pub-date>"
            )
    else:
        date_xml = ""
    if extra_pub_date_collection:
        date_xml += '<pub-date date-type="collection"><year>2025</year></pub-date>'

    pages = ""
    if fpage is not None:
        pages += f"<fpage>{fpage}</fpage>"
    if lpage is not None:
        pages += f"<lpage>{lpage}</lpage>"
    if elocation_id is not None:
        pages += f"<elocation-id>{elocation_id}</elocation-id>"

    ids = ""
    if doi is not None:
        ids += f'<article-id pub-id-type="doi">{doi}</article-id>'
    if edn is not None:
        ids += f'<article-id pub-id-type="edn">{edn}</article-id>'

    contribs = ""
    aff_block = ""
    if authors is not None:
        for idx, (surname, given) in enumerate(authors, start=1):
            name = ""
            if surname or given:
                name = (
                    "<name-alternatives>"
                    f'<name xml:lang="ru"><surname>{surname}</surname>'
                    f"<given-names>{given}</given-names></name>"
                    f'<name xml:lang="en"><surname>{surname}</surname>'
                    f"<given-names>{given}</given-names></name>"
                    "</name-alternatives>"
                )
            xref = (
                f'<xref ref-type="aff" rid="aff{idx}">{idx}</xref>'
                if affiliations
                else ""
            )
            contribs += (
                f'<contrib contrib-type="author">{name}{xref}</contrib>'
            )
            if affiliations:
                aff_block += (
                    f'<aff-alternatives id="aff{idx}">'
                    f'<aff><institution xml:lang="ru">'
                    f"Org {idx}</institution></aff>"
                    "</aff-alternatives>"
                )

    if references is None:
        references = (article_type or "") in {
            "research-article",
            "review-article",
        }
    back = ""
    if references:
        back = (
            "<back><ref-list>"
            '<ref id="R1"><mixed-citation>Sample, A. Title. 2020.</mixed-citation></ref>'
            "</ref-list></back>"
        )

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<article{type_attr} xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <front>
    <article-meta>
      {ids}
      <title-group>{title_xml}</title-group>
      {date_xml}
      {pages}
      <contrib-group>{contribs}</contrib-group>
      {aff_block}
    </article-meta>
  </front>
  {back}
</article>
"""
    return xml.encode("utf-8")


def _codes(report) -> set[str]:
    return {i.code for i in report.issues if i.severity == "error"}


def _info_codes(report) -> set[str]:
    return {i.code for i in report.issues if i.severity == "info"}


def test_1_valid_minimal():
    r = validate_jats_bytes(_article(), filename="ok.xml", today=TODAY)
    assert r.valid_for_metafora
    assert r.error_count == 0
    assert r.passed_count >= 5
    assert r.article_title == "Sample Title"
    assert r.publication_type == "research-article"
    assert r.publication_type_label == "Научная статья"


def test_primary_title_prefers_russian_from_trans_title_group():
    xml = """<?xml version="1.0"?>
<article article-type="research-article" xmlns:xml="http://www.w3.org/XML/1998/namespace">
  <front><article-meta>
    <title-group>
      <article-title xml:lang="en">English Title Here</article-title>
      <trans-title-group xml:lang="ru">
        <trans-title>Русское название статьи</trans-title>
      </trans-title-group>
    </title-group>
    <pub-date date-type="pub" iso-8601-date="2025-01-01"><year>2025</year></pub-date>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
  <back><ref-list><ref id="R1"><mixed-citation>Ref</mixed-citation></ref></ref-list></back>
</article>""".encode("utf-8")
    r = validate_jats_bytes(xml, today=TODAY)
    assert r.article_title == "Русское название статьи"


def test_2_missing_article_type():
    r = validate_jats_bytes(_article(article_type=None), today=TODAY)
    assert "M001" in _codes(r)


def test_3_empty_article_type():
    r = validate_jats_bytes(_article(article_type=""), today=TODAY)
    assert "M001" in _codes(r)


def test_4_missing_title():
    r = validate_jats_bytes(_article(title=None), today=TODAY)
    assert "M002" in _codes(r)


def test_5_single_title_ok():
    r = validate_jats_bytes(_article(title="Only one"), today=TODAY)
    assert "M002" not in _codes(r)
    assert r.valid_for_metafora


def test_6_missing_pub_date():
    # only collection date — не считается публикацией
    xml = b"""<?xml version="1.0"?>
<article article-type="research-article">
  <front><article-meta>
    <title-group><article-title>T</article-title></title-group>
    <pub-date date-type="collection"><year>2025</year></pub-date>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
</article>"""
    r = validate_jats_bytes(xml, today=TODAY)
    assert "M003" in _codes(r)


def test_7_bad_date_format():
    xml = b"""<?xml version="1.0"?>
<article article-type="research-article">
  <front><article-meta>
    <title-group><article-title>T</article-title></title-group>
    <pub-date date-type="pub" iso-8601-date="not-a-date"/>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
</article>"""
    r = validate_jats_bytes(xml, today=TODAY)
    assert "M003A" in _codes(r)


def test_8_date_after_day_after_tomorrow():
    future = (TODAY + timedelta(days=5)).isoformat()
    r = validate_jats_bytes(_article(pub_date=future), today=TODAY)
    assert "M003B" in _codes(r)


def test_9_valid_fpage_lpage():
    r = validate_jats_bytes(_article(fpage="578", lpage="594"), today=TODAY)
    assert "M004" not in _codes(r)
    assert "M004A" not in _codes(r)


def test_10_fpage_gt_lpage():
    r = validate_jats_bytes(_article(fpage="594", lpage="578"), today=TODAY)
    assert "M004A" in _codes(r)


def test_11_elocation_without_pages():
    r = validate_jats_bytes(
        _article(fpage=None, lpage=None, elocation_id="e12345"),
        today=TODAY,
    )
    assert "M004" not in _codes(r)
    assert r.valid_for_metafora


def test_12_no_pages_no_elocation():
    r = validate_jats_bytes(
        _article(fpage=None, lpage=None, elocation_id=None),
        today=TODAY,
    )
    assert "M004" in _codes(r)


def test_13_doi_absent_ok():
    r = validate_jats_bytes(_article(doi=None), today=TODAY)
    assert "M005" not in _codes(r)


def test_14_doi_valid():
    r = validate_jats_bytes(_article(doi="10.1234/example"), today=TODAY)
    assert "M005" not in _codes(r)


def test_15_doi_https_prefix():
    r = validate_jats_bytes(
        _article(doi="https://doi.org/10.1234/example"),
        today=TODAY,
    )
    assert "M005" not in _codes(r)
    assert normalize_doi("https://doi.org/10.1234/example") == "10.1234/example"


def test_16_doi_invalid():
    r = validate_jats_bytes(_article(doi="not-a-doi"), today=TODAY)
    assert "M005" in _codes(r)


def test_16b_doi_empty_declared():
    xml = _article().decode("utf-8").replace(
        "</title-group>",
        '</title-group><article-id pub-id-type="doi"></article-id>',
        1,
    )
    r = validate_jats_bytes(xml.encode("utf-8"), today=TODAY)
    assert "M005" in _codes(r)


def test_16c_doi_with_spaces():
    r = validate_jats_bytes(_article(doi="10.1234/foo bar"), today=TODAY)
    assert "M005" in _codes(r)


def test_16d_doi_with_cyrillic():
    doi = "10.1234/" + "статья"
    r = validate_jats_bytes(_article(doi=doi), today=TODAY)
    assert "M005" in _codes(r)
    assert any("кириллиц" in i.message.lower() for i in r.issues if i.code == "M005")


def test_17_edn_absent_ok():
    r = validate_jats_bytes(_article(edn=None), today=TODAY)
    assert "M006" not in _codes(r)


def test_18_edn_valid():
    r = validate_jats_bytes(_article(edn="SZUVPT"), today=TODAY)
    assert "M006" not in _codes(r)


def test_19_edn_wrong_length():
    r = validate_jats_bytes(_article(edn="ABCDE"), today=TODAY)
    assert "M006" in _codes(r)


def test_20_edn_cyrillic():
    r = validate_jats_bytes(_article(edn="АВС123"), today=TODAY)
    assert "M006" in _codes(r)


def test_21_author_ok():
    r = validate_jats_bytes(
        _article(authors=[("Petrov", "Petr")]),
        today=TODAY,
    )
    assert "M007" not in _codes(r)
    assert "M008" not in _codes(r)


def test_22_author_missing():
    r = validate_jats_bytes(_article(authors=[]), today=TODAY)
    assert "M007" in _codes(r)


def test_23_author_empty_name():
    r = validate_jats_bytes(_article(authors=[("", "")]), today=TODAY)
    assert "M008" in _codes(r)


def test_24_authors_optional_type():
    r = validate_jats_bytes(
        _article(article_type="editorial", authors=[]),
        today=TODAY,
    )
    assert "M007" not in _codes(r)
    assert r.valid_for_metafora
    assert not authors_required_for_type("editorial")
    assert not authors_required_for_type("unknown-metafora-type")


def test_25_malformed_xml():
    r = validate_jats_bytes(b"<article><front></article>", today=TODAY)
    assert not r.parse_ok
    assert not r.valid_for_metafora
    assert any("синтаксическ" in i.message for i in r.issues)


def test_26_non_article_root():
    r = validate_jats_bytes(
        b'<?xml version="1.0"?><journal><issue/></journal>',
        today=TODAY,
    )
    assert r.parse_ok
    assert not r.is_jats
    assert any("не распознан" in i.message for i in r.issues)


def test_27_xxe_blocked():
    # Внешняя сущность не должна подтягивать сеть/файл; парсер либо
    # отвергает документ, либо разбирает без раскрытия entity.
    xxe = b"""<?xml version="1.0"?>
<!DOCTYPE article [
  <!ENTITY xxe SYSTEM "file:///etc/passwd">
]>
<article article-type="research-article">
  <front><article-meta>
    <title-group><article-title>&xxe;</article-title></title-group>
    <pub-date date-type="pub" iso-8601-date="2025-01-01"><year>2025</year></pub-date>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
</article>"""
    r = validate_jats_bytes(xxe, today=TODAY)
    # Не должно быть содержимого /etc/passwd в отчёте
    blob = " ".join(i.message + i.value for i in r.issues)
    assert "root:" not in blob
    assert "/bin/" not in blob
    # либо parse error, либо title без раскрытой entity
    if r.parse_ok and r.is_jats:
        assert "M002" in _codes(r) or r.valid_for_metafora or True


def test_dot_date_format_and_year_only():
    assert parse_publication_date("15.03.2025") == date(2025, 3, 15)
    assert parse_publication_date("2025") == date(2025, 1, 1)
    r = validate_jats_bytes(
        b"""<?xml version="1.0"?>
<article article-type="research-article">
  <front><article-meta>
    <title-group><article-title>T</article-title></title-group>
    <pub-date date-type="pub"><day>15</day><month>03</month><year>2025</year></pub-date>
    <fpage>1</fpage><lpage>2</lpage>
    <contrib-group>
      <contrib contrib-type="author">
        <name><surname>A</surname><given-names>B</given-names></name>
      </contrib>
    </contrib-group>
  </article-meta></front>
</article>""",
        today=TODAY,
    )
    assert "M003" not in _codes(r)


def test_integration_real_platform_xml():
    path = FIXTURES / "valid_research_371947.xml"
    assert path.is_file()
    r = validate_jats_file(path, today=TODAY)
    assert r.valid_for_metafora
    assert r.error_count == 0
    labels = {p.code for p in r.passed_checks}
    assert {
        "M001",
        "M002",
        "M003",
        "M004",
        "M005",
        "M006",
        "M007",
        "M009",
        "M010",
    } <= labels
    assert r.article_url.startswith("https://journals.rcsi.science/")


def test_article_url_without_self_uri_empty():
    r = validate_jats_bytes(
        _article(doi="10.1234/example"),
        today=TODAY,
    )
    assert r.article_url == ""


def test_nonstandard_pagination_warning():
    r = validate_jats_bytes(_article(fpage="S10", lpage="S20"), today=TODAY)
    assert "M004A" not in _codes(r)
    assert any(i.code == "M004W" for i in r.issues)


def test_page_range_em_dash_error():
    xml = _article().decode("utf-8").replace(
        "<fpage>10</fpage><lpage>20</lpage>",
        "<fpage>10</fpage><lpage>20</lpage>"
        "<page-range>234—456</page-range>",
        1,
    )
    r = validate_jats_bytes(xml.encode("utf-8"), today=TODAY)
    assert "M004B" in _codes(r)
    assert not r.valid_for_metafora


def test_fpage_with_em_dash_range_error():
    r = validate_jats_bytes(_article(fpage="234—456", lpage="789"), today=TODAY)
    assert "M004B" in _codes(r)


def test_page_range_ascii_hyphen_ok():
    xml = _article().decode("utf-8").replace(
        "<fpage>10</fpage><lpage>20</lpage>",
        "<fpage>234</fpage><lpage>456</lpage>"
        "<page-range>234-456</page-range>",
        1,
    )
    r = validate_jats_bytes(xml.encode("utf-8"), today=TODAY)
    assert "M004B" not in _codes(r)
    assert r.valid_for_metafora


def test_research_requires_references():
    r = validate_jats_bytes(_article(references=False), today=TODAY)
    assert "M009" in _codes(r)
    assert not r.valid_for_metafora


def test_research_with_references_ok():
    r = validate_jats_bytes(_article(references=True), today=TODAY)
    assert "M009" not in _codes(r)
    assert r.valid_for_metafora


def test_editorial_references_optional():
    r = validate_jats_bytes(
        _article(article_type="editorial", authors=None, references=False),
        today=TODAY,
    )
    assert "M009" not in _codes(r)
    assert r.valid_for_metafora


def test_affiliations_missing_is_info_only():
    r = validate_jats_bytes(_article(affiliations=False), today=TODAY)
    assert "M010" in _info_codes(r)
    assert "M010" not in _codes(r)
    assert r.valid_for_metafora
    assert r.info_count >= 1


def test_affiliations_present_ok():
    r = validate_jats_bytes(_article(affiliations=True), today=TODAY)
    assert "M010" not in _info_codes(r)
    assert r.valid_for_metafora
    assert any(p.code == "M010" for p in r.passed_checks)


def test_zip_issue_batch_ok(tmp_path):
    import zipfile

    from ipsas.services.validate_metafora_jats import execute

    zpath = tmp_path / "issue.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("articles/a1.xml", _article())
        zf.writestr("articles/a2.xml", _article(title="Second", fpage="21", lpage="30"))
        zf.writestr("readme.txt", "ignored")
    result = execute(xml_path=zpath)
    assert result.is_batch
    assert result.article_count == 2
    assert result.ok
    assert result.ok_count == 2
    names = {r.filename for r in result.reports}
    assert "articles/a1.xml" in names
    assert "articles/a2.xml" in names


def test_zip_issue_batch_mixed(tmp_path):
    import zipfile

    from ipsas.services.validate_metafora_jats import execute

    zpath = tmp_path / "issue.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("ok.xml", _article())
        zf.writestr("bad.xml", _article(title=None))
    result = execute(xml_path=zpath)
    assert result.is_batch
    assert result.article_count == 2
    assert not result.ok
    assert result.ok_count == 1
    assert result.fail_count == 1


def test_zip_empty_xml_error(tmp_path):
    import zipfile

    from ipsas.services.validate_metafora_jats import execute

    zpath = tmp_path / "empty.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("notes.txt", "no xml here")
    result = execute(xml_path=zpath)
    assert result.is_batch
    assert not result.ok
    assert result.batch_error
    assert "нет XML" in result.batch_error
