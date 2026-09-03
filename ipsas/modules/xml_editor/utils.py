"""XPath и утилиты безопасной работы с journal XML (формат eLIBRARY/ORIS)."""

from __future__ import annotations

import re
import secrets
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from lxml import etree

from ipsas.config.settings import get_settings

SESSION_ID_RE = r"^[A-Za-z0-9_-]{16,64}$"
ARTICLE_ID_RE = r"^\d{1,5}$"
ORIGINAL_NAME = "original.xml"
EDITED_NAME = "edited.xml"
META_NAME = "meta.json"
SESSION_TTL_HOURS = 24

# Пути относительно <article> или корня — по реальной структуре journal XML IPSAS
XML_PATHS: dict[str, str] = {
    "articles": ".//issue/articles/article",
    "issue": ".//issue",
    "journal_title_rus": ".//journalInfo[@lang='RUS']/title",
    "journal_title_eng": ".//journalInfo[@lang='ENG']/title",
    "volume": ".//issue/volume",
    "number": ".//issue/number",
    "date_uni": ".//issue/dateUni",
    "issn": "./issn",
    "eissn": "./eissn",
    "titleid": "./titleid",
    "pages": "./pages",
    "art_type": "./artType",
    "lang": "./lang",
    "doi": "./doi",
    "doi_codes": "./codes/doi",
    "udk": "./codes/udk",
    "udk_direct": "./udk",
    "section": "./section",
    "rubric": "./rubric",
    "art_titles": "./artTitles",
    "art_title": "./artTitles/artTitle[@lang=$lang]",
    "authors": "./authors",
    "author": "./authors/author",
    "individ_info": "./individInfo[@lang=$lang]",
    "surname": "./surname",
    "initials": "./initials",
    "first_name": "./firstName",
    "middle_name": "./middleName",
    "org_name": "./orgName",
    "address": "./address",
    "email": "./email",
    "orcid": "./orcid",
    "author_codes": "./authorCodes",
    "author_orcid": "./authorCodes/orcid",
    "abstracts": "./abstracts",
    "abstract": "./abstracts/abstract[@lang=$lang]",
    "keywords": "./keywords",
    "kwd_group": "./keywords/kwdGroup[@lang=$lang]",
    "keyword": "./keyword",
    "references": "./references",
    "reference": "./references/reference",
}

SAFE_PARSER = etree.XMLParser(
    resolve_entities=False,
    no_network=True,
    load_dtd=False,
    recover=False,
    remove_blank_text=False,
)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$", re.I)
_ORCID_RE = re.compile(r"^(?:https?://orcid\.org/)?\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", re.I)
_SESSION_RE = re.compile(SESSION_ID_RE)
_ARTICLE_RE = re.compile(ARTICLE_ID_RE)


def uploads_root() -> Path:
    """Каталог сессий редактора внутри temp IPSAS."""
    path = get_settings().temp_dir / "xml_editor_sessions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_uploads_dir() -> Path:
    return uploads_root()


def new_session_id() -> str:
    return secrets.token_urlsafe(24)


def is_safe_session_id(session_id: str) -> bool:
    return bool(session_id and _SESSION_RE.fullmatch(session_id))


def is_safe_article_id(article_id: str) -> bool:
    return bool(article_id is not None and _ARTICLE_RE.fullmatch(str(article_id)))


def session_dir(session_id: str) -> Path:
    if not is_safe_session_id(session_id):
        raise ValueError("Некорректный идентификатор сессии")
    root = uploads_root()
    path = (root / session_id).resolve()
    uploads = root.resolve()
    if path != uploads and uploads not in path.parents:
        raise ValueError("Некорректный путь сессии")
    return path


def original_path(session_id: str) -> Path:
    return session_dir(session_id) / ORIGINAL_NAME


def edited_path(session_id: str) -> Path:
    return session_dir(session_id) / EDITED_NAME


def meta_path(session_id: str) -> Path:
    return session_dir(session_id) / META_NAME


def cleanup_old_uploads(*, ttl_hours: int = SESSION_TTL_HOURS) -> int:
    """Удалить каталоги сессий старше ttl_hours. Возвращает число удалённых."""
    root = ensure_uploads_dir()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    removed = 0
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            mtime = datetime.fromtimestamp(child.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if mtime < cutoff:
            shutil.rmtree(child, ignore_errors=True)
            removed += 1
    return removed


def parse_xml_bytes(data: bytes) -> etree._ElementTree:
    try:
        root = etree.fromstring(data, parser=SAFE_PARSER)
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"XML синтаксически некорректен: {exc}") from exc
    return etree.ElementTree(root)


def parse_xml_file(path: Path) -> etree._ElementTree:
    data = path.read_bytes()
    return parse_xml_bytes(data)


def serialize_tree(tree: etree._ElementTree) -> bytes:
    return etree.tostring(
        tree,
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    )


def save_tree(tree: etree._ElementTree, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(serialize_tree(tree))


def element_text(elem: Optional[etree._Element]) -> str:
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()


def set_element_text(elem: etree._Element, value: str) -> None:
    """Заменить текстовое содержимое элемента (вложенные узлы редактируемых полей удаляются)."""
    for child in list(elem):
        elem.remove(child)
    elem.text = value if value is not None else None


def find_one(parent: etree._Element, xpath: str, **vars: Any) -> Optional[etree._Element]:
    found = parent.xpath(xpath, **vars)
    return found[0] if found else None


def get_or_create_child(parent: etree._Element, tag: str) -> etree._Element:
    for child in parent:
        if child.tag == tag or (isinstance(child.tag, str) and child.tag.endswith("}" + tag)):
            return child
    elem = etree.SubElement(parent, tag)
    return elem


def ensure_lang_child(
    parent: etree._Element,
    container_tag: str,
    item_tag: str,
    lang: str,
) -> etree._Element:
    container = get_or_create_child(parent, container_tag)
    for child in container:
        local = child.tag.split("}")[-1] if isinstance(child.tag, str) else ""
        if local == item_tag and (child.get("lang") or "").upper() == lang.upper():
            return child
    elem = etree.SubElement(container, item_tag)
    elem.set("lang", lang.upper())
    return elem


def parse_pages(pages_raw: str) -> tuple[str, str]:
    text = (pages_raw or "").strip()
    if not text:
        return "", ""
    m = re.match(r"^(\d+)\s*[-–—]\s*(\d+)$", text)
    if m:
        return m.group(1), m.group(2)
    if text.isdigit():
        return text, text
    return text, ""


def join_pages(first: str, last: str) -> str:
    a = (first or "").strip()
    b = (last or "").strip()
    if a and b and a != b:
        return f"{a}-{b}"
    return a or b


def split_keywords(raw: str) -> list[str]:
    parts = re.split(r"[;；]\s*", (raw or "").strip())
    return [p.strip() for p in parts if p.strip()]


def join_keywords(items: list[str]) -> str:
    return "; ".join(items)


def looks_like_email(value: str) -> bool:
    return bool(_EMAIL_RE.match((value or "").strip()))


def doi_ok(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return True
    return bool(_DOI_RE.match(v))


def orcid_ok(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return True
    return bool(_ORCID_RE.match(v))


def email_ok(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return True
    return bool(_EMAIL_RE.match(v))
