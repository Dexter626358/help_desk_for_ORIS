"""Проверка заполненности публичного сайта журнала (OJS)."""

from ipsas.modules.journal_site.checker import JournalSiteChecker, JournalSiteReport
from ipsas.modules.journal_site.criteria import CRITERIA
from ipsas.modules.journal_site.editorial_letter import build_journal_site_editorial_letter
from ipsas.modules.journal_site.fields import FIELD_SPECS, HOME_FIELD_SPECS
from ipsas.modules.journal_site.parser import normalize_journal_base_url

__all__ = [
    "JournalSiteChecker",
    "JournalSiteReport",
    "CRITERIA",
    "FIELD_SPECS",
    "HOME_FIELD_SPECS",
    "normalize_journal_base_url",
    "build_journal_site_editorial_letter",
]
