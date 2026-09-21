"""Архивация рукописей из «Новые» по отправителю (сотрудники ОРИС)."""

from __future__ import annotations

from ipsas.modules.archive_by_sender.constants import DEFAULT_SENDERS
from ipsas.modules.archive_by_sender.models import ArchiveResult, SubmissionInfo
from ipsas.modules.archive_by_sender.parser import parse_journal_ref
from ipsas.modules.archive_by_sender.service import process_journal

__all__ = [
    "DEFAULT_SENDERS",
    "ArchiveResult",
    "SubmissionInfo",
    "parse_journal_ref",
    "process_journal",
]
