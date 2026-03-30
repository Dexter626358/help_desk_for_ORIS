from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from lxml import etree


_REFNUM_RE = re.compile(r"^\s*\d+\.\s*")


def remove_reference_number(text: str) -> str:
    """Удалить нумерацию вида `1.` / `23.` только в начале строки."""
    return _REFNUM_RE.sub("", text)


@dataclass
class ReferenceCleaningStats:
    total_references: int = 0
    changed_references: int = 0
    created_refinfo: int = 0
    created_text: int = 0
    removed_children: int = 0
    cleared_reference_text: int = 0
    removed_numbering: int = 0


def _ensure_refinfo(reference_elem: etree._Element) -> etree._Element:
    refinfo = reference_elem.find("./refinfo")
    if refinfo is not None:
        return refinfo
    return etree.SubElement(reference_elem, "refinfo")


def _ensure_refinfo_text(refinfo_elem: etree._Element) -> etree._Element:
    text_elem = refinfo_elem.find("./text")
    if text_elem is not None:
        return text_elem
    return etree.SubElement(refinfo_elem, "text")


def _collect_reference_text(reference_elem: etree._Element) -> str:
    """
    Собрать текст ссылки из <reference>.
    Если есть <refinfo>/<text> — берём его; иначе берём видимый текст из reference.text и потомков.
    """
    refinfo_text = reference_elem.findtext("./refinfo/text")
    if refinfo_text and refinfo_text.strip():
        return refinfo_text

    parts: list[str] = []
    for t in reference_elem.itertext():
        if t and t.strip():
            parts.append(t.strip())
    return " ".join(parts).strip()


def _cleanup_reference(reference_elem: etree._Element, stats: ReferenceCleaningStats) -> None:
    # 1) собрать текст до удаления узлов
    raw_text = _collect_reference_text(reference_elem)
    before_text = reference_elem.text

    # 2) удалить reference.text и все дочерние элементы кроме <refinfo>
    reference_elem.text = None
    if before_text is not None and before_text.strip():
        stats.cleared_reference_text += 1
    refinfo = reference_elem.find("./refinfo")
    removed_now = 0
    for child in list(reference_elem):
        if child is refinfo:
            continue
        reference_elem.remove(child)
        removed_now += 1
    if removed_now:
        stats.removed_children += removed_now

    # 3) если <refinfo> отсутствует — создать
    if refinfo is None:
        stats.created_refinfo += 1
    refinfo = _ensure_refinfo(reference_elem)

    # 4) удалить нумерацию внутри <text>
    text_elem = refinfo.find("./text")
    if text_elem is None:
        stats.created_text += 1
    text_elem = _ensure_refinfo_text(refinfo)
    cleaned = remove_reference_number(raw_text) if raw_text else ""
    if raw_text and cleaned != raw_text:
        stats.removed_numbering += 1

    before_refinfo_text = text_elem.text or ""
    text_elem.text = cleaned

    changed = (
        (before_text or "").strip() != ""
        or removed_now > 0
        or cleaned != before_refinfo_text
        or refinfo is None
    )
    if changed:
        stats.changed_references += 1


def clean_references_with_stats(
    xml_tree: etree._ElementTree,
) -> tuple[etree._ElementTree, ReferenceCleaningStats]:
    stats = ReferenceCleaningStats()
    root = xml_tree.getroot()
    refs = root.findall(".//references/reference")
    stats.total_references = len(refs)
    for ref in refs:
        _cleanup_reference(ref, stats)
    return xml_tree, stats


def clean_references(xml_tree: etree._ElementTree) -> etree._ElementTree:
    """
    Обработать ВСЕ <reference> внутри дерева:
    - удалить дублирующий текст/узлы
    - нормализовать до <reference><refinfo><text>...</text></refinfo></reference>
    - удалить нумерацию в начале текста
    Возвращает то же дерево (мутирует in-place).
    """
    xml_tree, _ = clean_references_with_stats(xml_tree)
    return xml_tree

