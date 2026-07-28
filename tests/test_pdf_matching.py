"""Unit tests for pdf_matching package (pure helpers, offline)."""

from __future__ import annotations

from pathlib import Path

from ipsas.modules.pdf_matcher import (
    ArticleInfo,
    MatchMethod,
    MatchResult,
    PDFEntry,
    PDFMatcher,
    PDFMetadata,
    process_archive,
)
from ipsas.modules.pdf_matching.zip_names import _decode_zip_filename


def test_match_method_values() -> None:
    assert MatchMethod.EDN_EXACT.value == "edn_exact"
    assert MatchMethod.DOI_EXACT.value == "doi_exact"
    assert MatchMethod.DOI_PARTIAL.value == "doi_partial"
    assert MatchMethod.TITLE_HIGH.value == "title_high"
    assert MatchMethod.TITLE_AUTHORS.value == "title_authors"
    assert MatchMethod.PAGES_TITLE.value == "pages_title"
    assert MatchMethod.FALLBACK.value == "fallback"
    assert MatchMethod.UNMATCHED.value == "unmatched"


def test_public_api_exports() -> None:
    assert callable(PDFMatcher)
    assert callable(process_archive)
    assert PDFEntry is not None
    assert ArticleInfo is not None
    assert PDFMetadata is not None
    assert MatchResult is not None


def test_decode_zip_filename_empty() -> None:
    assert _decode_zip_filename("") == ""
    assert _decode_zip_filename("   ") == "   "


def test_decode_zip_filename_ascii_unchanged() -> None:
    assert _decode_zip_filename("article_web.pdf") == "article_web.pdf"


def test_decode_zip_filename_cp866_misread_as_cp437() -> None:
    # Имя в CP866, ошибочно прочитанное как CP437 (типичный случай ZIP на Windows).
    original = "Гудимова_web.pdf"
    mangled = original.encode("cp866").decode("cp437")
    assert mangled != original
    assert _decode_zip_filename(mangled) == original


def test_normalize_text() -> None:
    m = PDFMatcher(adaptive_thresholds=False, verbose=False)
    assert m.normalize_text("") == ""
    assert m.normalize_text("  Hello, World!  ") == "hello world"
    assert m.normalize_text("A\tB\nC") == "a b c"


def test_normalize_doi() -> None:
    m = PDFMatcher(adaptive_thresholds=False, verbose=False)
    assert m.normalize_doi("") == ""
    assert m.normalize_doi("DOI: 10.1234/AbC") == "10.1234/abc"
    assert m.normalize_doi("https://doi.org/10.1234/xyz.") == "10.1234/xyz"
    assert m.normalize_doi("10.1234/foo–bar") == "10.1234/foo-bar"


def test_normalize_edn() -> None:
    m = PDFMatcher(adaptive_thresholds=False, verbose=False)
    assert m.normalize_edn("") == ""
    assert m.normalize_edn("edn: abc12z") == "ABC12Z"
    assert m.normalize_edn("ABCDEFGH") == "ABCDEF"
    assert m.normalize_edn("AB12") == "AB12"


def test_pdf_entry_frozen() -> None:
    pe = PDFEntry(path=Path("x.pdf"), arcname="x.pdf")
    assert pe.path == Path("x.pdf")
    assert pe.arcname == "x.pdf"
