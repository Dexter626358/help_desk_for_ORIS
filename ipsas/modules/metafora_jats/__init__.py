"""Валидатор JATS XML для обязательных требований Метафоры."""

from __future__ import annotations

from ipsas.modules.metafora_jats.models import ValidationReport
from ipsas.modules.metafora_jats.validator import validate_jats_bytes, validate_jats_file

__all__ = [
    "ValidationReport",
    "validate_jats_bytes",
    "validate_jats_file",
]
