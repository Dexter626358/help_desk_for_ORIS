"""Тесты точечного редактирования XML."""

from __future__ import annotations

from pathlib import Path

from lxml import etree

from xml_editor.editor import (
    add_author,
    delete_author,
    update_article_from_form,
)
from xml_editor.parser import parse_article
from xml_editor.utils import parse_xml_file, serialize_tree


def test_update_title_preserves_unknown(sample_path: Path) -> None:
    tree = parse_xml_file(sample_path)
    original_xml = serialize_tree(tree).decode("utf-8")
    assert 'customAttr="keep-me"' in original_xml
    assert "неизвестный служебный блок" in original_xml

    article = parse_article(tree, 0)
    form = {
        "title_rus": "Новое название статьи",
        "title_eng": article["title_eng"],
        "doi": article["doi"],
        "lang": article["lang"],
        "art_type": article["art_type"],
        "section": article["section"],
        "udk": article["udk"],
        "page_first": article["page_first"],
        "page_last": article["page_last"],
        "abstract_rus": article["abstract_rus"],
        "abstract_eng": article["abstract_eng"],
        "keywords_rus": article["keywords_rus"],
        "keywords_eng": article["keywords_eng"],
        "authors": article["authors"],
        "references": article["references"],
    }
    update_article_from_form(tree, 0, form)
    out = serialize_tree(tree).decode("utf-8")
    assert "Новое название статьи" in out
    assert 'customAttr="keep-me"' in out
    assert "неизвестный служебный блок" in out
    assert 'xmlns:x="http://example.com/x"' in out or "x:extra" in out


def test_update_author_surname(sample_path: Path) -> None:
    tree = parse_xml_file(sample_path)
    article = parse_article(tree, 0)
    authors = article["authors"]
    authors[0]["RUS"]["surname"] = "Смирнов"
    form = {
        **{k: article[k] for k in (
            "title_rus", "title_eng", "doi", "lang", "art_type", "section", "udk",
            "page_first", "page_last", "abstract_rus", "abstract_eng",
            "keywords_rus", "keywords_eng", "references",
        )},
        "authors": authors,
    }
    update_article_from_form(tree, 0, form)
    updated = parse_article(tree, 0)
    assert updated["authors"][0]["RUS"]["surname"] == "Смирнов"
    assert updated["authors"][0]["ENG"]["surname"] == "Ivanov"


def test_add_and_delete_author(sample_path: Path) -> None:
    tree = parse_xml_file(sample_path)
    before = len(parse_article(tree, 0)["authors"])
    add_author(tree, 0)
    mid = len(parse_article(tree, 0)["authors"])
    assert mid == before + 1
    delete_author(tree, 0, mid - 1)
    after = len(parse_article(tree, 0)["authors"])
    assert after == before
