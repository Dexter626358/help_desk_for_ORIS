"""Безопасный разбор XML (защита от XXE / DTD / сети)."""

from __future__ import annotations

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
