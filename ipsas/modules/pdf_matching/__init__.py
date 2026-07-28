"""Package for matching PDF files to articles in journal XML."""

from ipsas.modules.pdf_matching.matcher import PDFMatcher
from ipsas.modules.pdf_matching.models import (
    ArticleInfo,
    MatchMethod,
    MatchResult,
    PDFEntry,
    PDFMetadata,
)
from ipsas.modules.pdf_matching.service import process_archive

__all__ = [
    "PDFMatcher",
    "process_archive",
    "MatchMethod",
    "PDFEntry",
    "ArticleInfo",
    "PDFMetadata",
    "MatchResult",
]
