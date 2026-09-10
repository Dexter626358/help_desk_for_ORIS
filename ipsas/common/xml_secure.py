"""Безопасный разбор XML (защита от XXE / DTD / сети)."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from lxml import etree


def create_secure_parser(
    *,
    recover: bool = False,
    huge_tree: bool = False,
    remove_blank_text: bool = False,
) -> etree.XMLParser:
    """
    XMLParser с отключёнными внешними сущностями и сетью.

    ``huge_tree=True`` только для доверенных больших journal XML при необходимости.
    """
    return etree.XMLParser(
        recover=recover,
        remove_blank_text=remove_blank_text,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
        huge_tree=huge_tree,
    )


def parse_xml_file_elementtree(
    path: str | Path,
    *,
    huge_tree: bool = True,
) -> ET.ElementTree:
    """Разобрать файл через безопасный lxml и вернуть ``xml.etree.ElementTree``."""
    parser = create_secure_parser(huge_tree=huge_tree)
    try:
        root = etree.parse(str(path), parser=parser).getroot()
    except etree.XMLSyntaxError as e:
        raise ET.ParseError(str(e)) from e
    # Round-trip без DTD/entities — совместимость с существующим ET-кодом
    return ET.ElementTree(ET.fromstring(etree.tostring(root, encoding="unicode")))
