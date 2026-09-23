"""Загрузка рисунков выпуска в доп. файлы статей (OJS editor)."""

from __future__ import annotations

from ipsas.modules.issue_supp_images.archive import ImageBundle, parse_images_archive
from ipsas.modules.issue_supp_images.service import process_issue_images
from ipsas.modules.issue_supp_images.toc import IssueArticle, parse_issue_toc

__all__ = [
    "ImageBundle",
    "IssueArticle",
    "parse_images_archive",
    "parse_issue_toc",
    "process_issue_images",
]
