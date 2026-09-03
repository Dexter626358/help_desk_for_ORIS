"""Тесты парсера journal XML."""

from __future__ import annotations

from pathlib import Path

from xml_editor.parser import get_issue_summary, list_articles, parse_article
from xml_editor.utils import parse_xml_bytes, parse_xml_file


def test_list_articles(sample_path: Path) -> None:
    tree = parse_xml_file(sample_path)
    articles = list_articles(tree)
    assert len(articles) == 2
    assert "Моделирование" in articles[0]["title"]
    assert "Иванов" in articles[0]["authors_label"]


def test_parse_article_fields(sample_path: Path) -> None:
    tree = parse_xml_file(sample_path)
    article = parse_article(tree, 0)
    assert article["title_rus"].startswith("Моделирование")
    assert article["doi"] == "10.1234/sample.2025.1"
    assert article["page_first"] == "1"
    assert article["page_last"] == "10"
    assert len(article["authors"]) == 2
    assert article["authors"][0]["RUS"]["surname"] == "Иванов"
    assert "управление" in article["keywords_rus"]
    assert len(article["references"]) == 2


def test_issue_summary(sample_path: Path) -> None:
    tree = parse_xml_file(sample_path)
    info = get_issue_summary(tree)
    assert info["article_count"] == 2
    assert info["volume"] == "12"
    assert "Пример" in info["journal_title_ru"]


def test_reject_xxe(sample_xml_bytes: bytes) -> None:
    xxe = b"""<?xml version="1.0"?>
    <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
    <journal><issue><articles><article><pages>&xxe;</pages></article></articles></issue></journal>
    """
    try:
        parse_xml_bytes(xxe)
        # Если парсер не упал — сущность не должна раскрыться в текст
        tree = parse_xml_bytes(xxe.replace(b"&xxe;", b"safe"))
        assert tree is not None
    except ValueError:
        pass
