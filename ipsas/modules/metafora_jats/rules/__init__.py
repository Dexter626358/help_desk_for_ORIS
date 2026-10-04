"""Правила METAFORA REQUIRED (M001–M009) + справочные (M010)."""

from __future__ import annotations

from ipsas.modules.metafora_jats.rules.affiliations import check_affiliations
from ipsas.modules.metafora_jats.rules.authors import check_authors
from ipsas.modules.metafora_jats.rules.identifiers import check_doi, check_edn
from ipsas.modules.metafora_jats.rules.pagination import check_pagination
from ipsas.modules.metafora_jats.rules.publication_date import check_publication_date
from ipsas.modules.metafora_jats.rules.publication_type import check_publication_type
from ipsas.modules.metafora_jats.rules.references import check_references
from ipsas.modules.metafora_jats.rules.title import check_title

__all__ = [
    "check_affiliations",
    "check_authors",
    "check_doi",
    "check_edn",
    "check_pagination",
    "check_publication_date",
    "check_publication_type",
    "check_references",
    "check_title",
]
