"""Тесты моделей и парсинга метаданных выпуска."""

from lxml import html

from ipsas.modules.issue_metadata.models import IssueParseResult
from ipsas.modules.issue_metadata import parsers as issue_parsers


def test_article_roundtrip_preserves_extended_fields() -> None:
    raw = {
        "issue": {
            "issue_title": "№ 1 (35) (2026)",
            "issue_serial": "35",
            "cover_url": "https://example.com/cover.jpg",
            "issue_galleys": [{"url": "https://example.com/issue.pdf", "lang": "RU"}],
            "warnings": [],
        },
        "articles": [
            {
                "url": "https://example.com/article/1",
                "abstract_ru": "Текст аннотации на русском языке достаточной длины для теста.",
                "abstract_en": "English abstract text for testing purposes.",
                "references_ru_count": 3,
                "references_en_count": 2,
                "references_unk_count": 1,
                "references_count": 6,
                "reference_ru_first": "Ref RU 1",
                "organizations": ["Org A"],
                "organizations_count": 1,
                "pdf_files": [{"url": "https://example.com/a.pdf", "lang": "RU", "locked": False}],
                "identifiers": {"doi": "10.1234/test", "edn": "ABCDEF"},
            }
        ],
    }
    result = IssueParseResult.from_mapping(raw).to_dict()
    article = result["articles"][0]
    assert article["abstract_ru"].startswith("Текст аннотации")
    assert article["references_ru_count"] == 3
    assert article["references_unk_count"] == 1
    assert article["organizations_count"] == 1
    assert result["issue"]["cover_url"] == "https://example.com/cover.jpg"
    assert result["issue"]["issue_serial"] == "35"
    assert len(result["issue"]["issue_galleys"]) == 1


def test_parse_issue_title_ru_serial_format() -> None:
    page = html.fromstring("<html lang='ru'><head><title>№ 1 (35) (2026)</title></head><body><h1>№ 1 (35) (2026)</h1></body></html>")
    meta = issue_parsers.parse_issue_page(page, "https://example.com/j/issue/view/1")
    assert meta["issue"] == "1"
    assert meta["issue_serial"] == "35"
    assert meta["volume"] is None
    assert meta["year"] == "2026"
    assert meta["uses_volume"] is False
    assert meta["expects_issue_serial"] is True
    assert meta["issue_language"] == "ru"


def test_parse_issue_identifiers_english_no_serial_format() -> None:
    ids = issue_parsers.parse_issue_identifiers("No 1 (35) (2026)")
    assert ids["issue"] == "1"
    assert ids["issue_serial"] == "35"
    assert ids["volume"] is None
    assert ids["year"] == "2026"
    assert ids["uses_volume"] == "0"


def test_parse_issue_galleys_label_inside_anchor() -> None:
    page = html.fromstring(
        """
        <html><body>
          <a href="/issue/view/10/download">
            <div class="galleyLabel">
              <img src="/img/labels/pdf.png" alt="PDF">
              <span class="galleyLanguageLabel">(Russian)</span>
            </div>
          </a>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(page, "https://journals.example/j/issue/view/10")
    assert len(meta["issue_galleys"]) == 1
    assert meta["issue_galleys"][0]["lang"] == "Russian"
    assert meta["issue_galleys"][0]["url"].endswith("/issue/view/10/download")


def test_parse_issue_galleys_label_sibling_link() -> None:
    """Частый случай: label рядом со ссылкой, а не внутри <a>."""
    page = html.fromstring(
        """
        <html><body>
          <div class="issueGalleys">
            <a class="obj_galley_link pdf" href="/journals/issue/view/10/pdf">PDF</a>
            <div class="galleyLabel">
              <img src="/img/labels/pdf.png" alt="PDF">
              <span class="galleyLanguageLabel">(Russian)</span>
            </div>
          </div>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(page, "https://journals.example/j/issue/view/10")
    assert len(meta["issue_galleys"]) == 1
    assert meta["issue_galleys"][0]["lang"] == "Russian"
    assert "/issue/view/10/pdf" in meta["issue_galleys"][0]["url"]
    assert "/article/" not in meta["issue_galleys"][0]["url"]


def test_parse_issue_galleys_ignores_article_pdf_labels() -> None:
    page = html.fromstring(
        """
        <html><body>
          <a href="/article/view/55/pdf">
            <div class="galleyLabel">
              <img src="/img/labels/pdf.png" alt="PDF">
              <span class="galleyLanguageLabel">(Russian)</span>
            </div>
          </a>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(page, "https://journals.example/j/issue/view/10")
    assert meta["issue_galleys"] == []


def test_parse_issue_galleys_restricted_without_href() -> None:
    """Закрытый доступ: есть «Весь выпуск» и PDF-метка, но нет публичной ссылки."""
    page = html.fromstring(
        """
        <html><body>
          <h2>Весь выпуск</h2>
          <div class="galleys">
            <div class="galleyLabel">
              <img src="/img/labels/pdf.png" alt="PDF">
              <span class="galleyLanguageLabel">(Russian)</span>
            </div>
          </div>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(page, "https://journals.example/j/issue/view/10")
    assert len(meta["issue_galleys"]) == 1
    assert meta["issue_galleys"][0]["lang"] == "Russian"
    assert meta["issue_galleys"][0]["url"] is None
    assert meta["issue_galleys"][0]["access_restricted"] is True


def test_parse_article_pdfs_locked_with_href() -> None:
    page = html.fromstring(
        """
        <html><body>
          <table><tr>
            <td class="tocTitle"><a href="/journal/article/view/400189">Title</a></td>
            <td>
              <div class="dark issueArticlesFiles">
                <div class="issueArticlesFilesBlock">
                  <a href="/journal/article/view/400189/682439" class="file">
                    <img src="/img/labels/pdf.png" alt="PDF"><br>
                    <span class="issueArticlesLabel">(Rus)</span>
                    <img class="issueArticlesAccessLogo" src="/icons/text_unlock.png" alt="Доступ закрыт">
                  </a>
                </div>
              </div>
            </td>
          </tr></table>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(page, "https://journals.example/journal/issue/view/10")
    pdfs = meta["article_pdf_files"]["400189"]
    assert len(pdfs) == 1
    assert pdfs[0]["lang"] == "Rus"
    assert pdfs[0]["locked"] is True
    assert "/article/view/400189/682439" in pdfs[0]["url"]


def test_parse_article_pdfs_locked_without_anchor() -> None:
    page = html.fromstring(
        """
        <html><body>
          <div class="obj_article_summary">
            <h3 class="title"><a href="https://journals.example/j/article/view/55">Article</a></h3>
            <div class="dark issueArticlesFiles">
              <div class="issueArticlesFilesBlock">
                <img src="/img/labels/pdf.png" alt="PDF">
                <span class="issueArticlesLabel">(Rus)</span>
                <img class="issueArticlesAccessLogo" src="/icons/text_unlock.png" alt="Доступ закрыт">
              </div>
            </div>
          </div>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(page, "https://journals.example/j/issue/view/10")
    pdfs = meta["article_pdf_files"]["55"]
    assert len(pdfs) == 1
    assert pdfs[0]["url"] is None
    assert pdfs[0]["locked"] is True
    assert pdfs[0]["lang"] == "Rus"


def test_parse_article_page_pdf_galley_without_href() -> None:
    page = html.fromstring(
        """
        <html><body>
          <div class="item galleys">
            <div class="galleyLabel">
              <img src="/img/labels/pdf.png" alt="PDF">
              <span class="galleyLanguageLabel">(Russian)</span>
            </div>
          </div>
        </body></html>
        """
    )
    meta = issue_parsers.parse_article_page(
        page,
        "https://journals.example/j/article/view/55",
        detect_lang=lambda t: "ru" if t and any("а" <= c.lower() <= "я" for c in t) else "en",
    )
    assert len(meta["pdf_files"]) == 1
    assert meta["pdf_files"][0]["lang"] == "Russian"
    assert meta["pdf_files"][0]["url"] is None
    assert meta["pdf_files"][0]["locked"] is True


def test_parse_issue_typed_print_and_online_issn() -> None:
    """Печатный и электронный ISSN с шапки не должны смешиваться."""
    page = html.fromstring(
        """
        <html><head>
          <meta name="citation_issn" content="3034-5391"/>
        </head><body>
          <div id="headerIssn">ISSN 0869-5733 (Print) ISSN 3034-5391 (Online)</div>
          <h1>No 1 (2023)</h1>
        </body></html>
        """
    )
    meta = issue_parsers.parse_issue_page(
        page, "https://journals.rcsi.science/0869-5733/issue/view/8810"
    )
    assert meta["issn"] == "0869-5733"
    assert meta["eissn"] == "3034-5391"
    assert meta["issn"] != meta["eissn"]


def test_jats_references_without_xml_lang_detect_russian() -> None:
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<article xmlns:xlink="http://www.w3.org/1999/xlink">
<back><ref-list>
<ref id="B1"><mixed-citation>Бернштейн, 1994 — Бернштейн Б. Лекции по физике.</mixed-citation></ref>
<ref id="B2"><mixed-citation>Chalmers, 1996 — Chalmers D. The Conscious Mind. Oxford, 1996.</mixed-citation></ref>
</ref-list></back></article>""".encode("utf-8")
    parsed = IssueMetadataParser()._parse_jats_xml(xml)
    refs = parsed["references_by_lang"]
    assert refs["ru"]["count"] == 1
    assert refs["en"]["count"] == 1
    assert refs["unk"]["count"] == 0
    assert refs.get("lang_from_attr") is False


def test_jats_references_language_from_xml_lang_alternatives() -> None:
    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<article>
<back><ref-list>
<ref id="B1">
  <label>1.</label>
  <citation-alternatives>
    <mixed-citation xml:lang="en">Bazulin E.G. Comparison of ultrasonic nondestructive testing systems. Defectoskopiya. 2013. No. 7. Pp. 51–75. (In Russ.)</mixed-citation>
    <mixed-citation xml:lang="ru">Базулин Е.Г. Сравнение систем для ультразвукового неразрушающего контроля // Дефектоскопия. 2013. № 7. С. 51–75.</mixed-citation>
  </citation-alternatives>
</ref>
<ref id="B2">
  <label>2.</label>
  <citation-alternatives>
    <mixed-citation xml:lang="en">Smith J. Pure English source. Nature. 2020.</mixed-citation>
    <mixed-citation xml:lang="ru">Смит Дж. Чисто английский источник // Nature. 2020.</mixed-citation>
  </citation-alternatives>
</ref>
</ref-list></back></article>""".encode("utf-8")
    parsed = IssueMetadataParser()._parse_jats_xml(xml)
    refs = parsed["references_by_lang"]
    assert refs["lang_from_attr"] is True
    assert refs["mode"] == "parallel_citations"
    assert refs["total_refs"] == 2
    assert refs["ru"]["count"] == 2
    assert refs["en"]["count"] == 2
    assert "Базулин" in (refs["ru"]["first"] or "")
    assert "Bazulin" in (refs["en"]["first"] or "")
    # Primary list — одна запись на <ref> (для качества/нумерации)
    assert len(refs["items"]) == 2

    # Merge не должен затирать xml:lang эвристикой по алфавиту primary
    article: dict = {
        "references": ["HTML only"],
        "references_count": 1,
        "references_mode": "single_list",
    }
    IssueMetadataParser()._merge_jats_into_article(article, {}, parsed)
    assert article["references_lang_source"] == "xml_lang"
    assert article["references_mode"] == "parallel_citations"
    assert article["references_ru_count"] == 2
    assert article["references_en_count"] == 2
    assert article["references_count"] == 2

    from ipsas.modules.issue_metadata import validators as v

    v.recompute_bibliography_lang_stats(article)
    assert article["references_ru_count"] == 2
    assert article["references_en_count"] == 2
