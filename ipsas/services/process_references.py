"""Сценарии обработки списка литературы."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ipsas.modules.bibliography.cleaner import clean_references_with_stats
from ipsas.modules.bibliography.formatter import ReferenceFormatter
from ipsas.modules.bibliography.processor import remove_reference_numbering
from lxml import etree


def remove_numbering(xml_path: Path) -> dict[str, Any]:
    return remove_reference_numbering(xml_path)


def format_structure(xml_path: Path) -> dict[str, Any]:
    return ReferenceFormatter().format_references(xml_path)


def clean_tree(xml_path: Path, output_path: Path) -> tuple[Any, Any]:
    """Очистить references и записать результат. Returns (stats, samples)."""
    parser = etree.XMLParser(
        recover=False,
        remove_blank_text=False,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
        huge_tree=True,
    )
    tree = etree.parse(str(xml_path), parser)
    _, stats, samples = clean_references_with_stats(tree)
    tree.write(
        str(output_path),
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=False,
    )
    return stats, samples
