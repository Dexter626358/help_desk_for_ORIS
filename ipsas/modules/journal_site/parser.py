"""Нормализация URL и извлечение значений полей со страниц OJS."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional
from urllib.parse import urljoin, urlparse, urlunparse, unquote

from lxml import html as lxml_html

from ipsas.modules.journal_site.fields import (
    AboutMenuItem,
    _JOURNAL_PATH_SUFFIXES,
    resolve_about_section_category,
    thresholds_for_about_item,
)

_WS_RE = re.compile(r"\s+")
_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
_PHONE_RE = re.compile(
    r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}"
)

# Подписи блоков на главной (EN / RU)
_HOME_LABELS: dict[str, tuple[str, ...]] = {
    "issn": (
        "issn (online)",
        "issn (print)",
        "eissn",
        "issn",
    ),
    "founder": (
        "founder",
        "учредитель",
        "учредители",
    ),
    "editor_in_chief": (
        "editor-in-chief",
        "editor in chief",
        "главный редактор",
    ),
    "frequency": (
        "frequency / assess",
        "frequency",
        "периодичность",
        "периодичность / доступ",
    ),
    "media_certificate": (
        "media registration certificate",
        "свидетельство о регистрации",
        "свидетельство сми",
    ),
    "languages": (
        "languages",
        "language of publication",
        "языки публикации",
        "язык публикации",
    ),
    "publisher": (
        "publisher",
        "издатель",
    ),
}



def norm_space(text: str) -> str:
    return _WS_RE.sub(" ", (text or "").replace("\xa0", " ")).strip()


def normalize_journal_base_url(url: str) -> str:
    """Привести ссылку к корню журнала (без /index, /about/..., query)."""
    raw = (url or "").strip()
    if not raw:
        raise ValueError("URL не указан")
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Некорректный URL журнала")

    path = parsed.path or "/"
    # срезать хвост issue/article/announcement
    lower = path.lower()
    for marker in ("/issue/", "/article/", "/announcement/", "/download/"):
        idx = lower.find(marker)
        if idx > 0:
            path = path[:idx]
            lower = path.lower()
            break

    path = path.rstrip("/") or ""
    lower = path.lower()
    for suffix in _JOURNAL_PATH_SUFFIXES:
        if lower.endswith(suffix.lower()):
            path = path[: -len(suffix)]
            break

    path = path.rstrip("/") or ""
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def journal_page_url(base_url: str, *parts: str) -> str:
    base = base_url.rstrip("/") + "/"
    rel = "/".join(p.strip("/") for p in parts if p)
    return urljoin(base, rel)


def parse_html(content: bytes | str) -> Any:
    if isinstance(content, str):
        return lxml_html.fromstring(content)
    # Без charset в HTML lxml может неверно угадать кодировку кириллицы.
    try:
        return lxml_html.fromstring(content.decode("utf-8"))
    except UnicodeDecodeError:
        return lxml_html.fromstring(content)


def _label_key(text: str) -> str:
    t = norm_space(text).lower().rstrip(":")
    return t


def extract_labeled_value(doc: Any, field_id: str) -> str:
    """Значение поля по подписи <strong>…</strong> на главной."""
    labels = _HOME_LABELS.get(field_id) or ()
    if not labels:
        return ""

    for strong in doc.xpath(".//strong"):
        key = _label_key(strong.text_content() or "")
        if not key:
            continue
        matched = False
        for label in labels:
            if key == label or key.startswith(label):
                matched = True
                break
        if not matched:
            continue

        parent = strong.getparent()
        if parent is None:
            continue
        full = norm_space(parent.text_content() or "")
        # убрать саму подпись из начала
        full_l = full.lower()
        for label in labels:
            # label + optional colon
            for prefix in (f"{label}:", label):
                if full_l.startswith(prefix):
                    return norm_space(full[len(prefix) :])
        # fallback: хвост после strong
        tail = norm_space((strong.tail or ""))
        if tail and tail != ":":
            return tail.lstrip(": ").strip()
        # соседние текстовые узлы / span
        chunks: list[str] = []
        for sib in strong.itersiblings():
            if getattr(sib, "tag", None) == "strong":
                break
            chunks.append(sib.text_content() if hasattr(sib, "text_content") else str(sib))
        value = norm_space(" ".join(chunks)).lstrip(": ").strip()
        if value:
            return value
    return ""


def extract_journal_title(doc: Any) -> str:
    for xp in (
        "string(//*[@id='content']//h1[1])",
        "string(//h1[1])",
        "string(//*[@class='page_title'][1])",
    ):
        title = norm_space(doc.xpath(xp) or "")
        if title and title.lower() not in {"home", "главная"}:
            return title
    return ""


def extract_issn(doc: Any) -> str:
    labeled = extract_labeled_value(doc, "issn")
    if labeled:
        return labeled
    for xp in (
        "string(//*[@id='headerIssn'])",
        "string(//*[contains(@class,'issn')][1])",
    ):
        text = norm_space(doc.xpath(xp) or "")
        if text:
            return text
    # meta
    for meta in doc.xpath("//meta[translate(@name,'ISSN','issn')='citation_issn' or @name='ISSN']"):
        content = norm_space(meta.get("content") or "")
        if content:
            return content
    return ""


def extract_homepage_image(doc: Any) -> str:
    for xp in (
        "//*[@id='homepageImage']//img/@src",
        "//img[contains(@class,'homepageImage')]/@src",
        "//*[contains(@class,'homepage_image')]//img/@src",
        "//img[contains(@alt,'Homepage') or contains(@alt,'homepage')]/@src",
    ):
        vals = doc.xpath(xp)
        if vals:
            return norm_space(str(vals[0]))
    return ""


def extract_description(doc: Any) -> str:
    """Связный текст о журнале на главной (абзацы без подписей strong)."""
    content_nodes = doc.xpath(
        "//*[@id='content'] | //*[contains(@class,'page_index_journal')] | "
        "//*[contains(@class,'pkp_structure_main')] | //main"
    )
    root = content_nodes[0] if content_nodes else doc
    paras: list[str] = []
    for p in root.xpath(".//p"):
        # пропустить абзацы, целиком состоящие из подписи+короткого значения
        if p.xpath("./strong"):
            text = norm_space(p.text_content() or "")
            if len(text) < 120:
                continue
        text = norm_space(p.text_content() or "")
        if len(text) < 40:
            continue
        # отсечь UI-шум
        low = text.lower()
        if any(
            x in low
            for x in (
                "username",
                "password",
                "remember me",
                "search scope",
                "no announcements",
            )
        ):
            continue
        paras.append(text)
    # уникальные, сохранить порядок
    seen: set[str] = set()
    out: list[str] = []
    for p in paras:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return norm_space(" ".join(out[:6]))


def extract_current_issue(doc: Any) -> str:
    for xp in (
        "string(//*[contains(@class,'current_issue')]//h2[1])",
        "string(//*[contains(@class,'current_issue')]//h3[1])",
        "string(//*[@id='homepageCurrentIssue']//h2[1])",
    ):
        text = norm_space(doc.xpath(xp) or "")
        if text:
            return text
    # ссылка на issue/view
    for a in doc.xpath("//a[contains(@href,'/issue/view/')]"):
        text = norm_space(a.text_content() or "")
        href = a.get("href") or ""
        if text and text.lower() not in {"url", "full issue", "полный выпуск"}:
            return text
        if href:
            return href
    # заголовок вида No 1 (2026)
    for h in doc.xpath("//h2|//h3"):
        text = norm_space(h.text_content() or "")
        if re.match(r"^(No|№)\s*\d+", text, re.I):
            return text
    return ""


def section_body_text(doc: Any, section_id: str) -> str:
    nodes = doc.xpath(f"//*[@id='{section_id}']")
    if not nodes:
        return ""
    el = nodes[0]
    full = norm_space(el.text_content() or "")
    # убрать заголовок h2/h3
    for h in el.xpath("./h1|./h2|./h3"):
        ht = norm_space(h.text_content() or "")
        if ht and full.lower().startswith(ht.lower()):
            full = norm_space(full[len(ht) :])
            break
    return full


def _heading_matches(title: str, aliases: tuple[str, ...]) -> bool:
    t = norm_space(title).lower()
    if not t or not aliases:
        return False
    for alias in aliases:
        a = norm_space(alias).lower()
        if not a:
            continue
        if t == a or a in t:
            return True
        # короткий заголовок не должен совпадать лишь как подстрока длинного алиаса («Статьи» ≠ «отзыв статьи»)
        if len(t) >= 12 and t in a:
            return True
    return False


_SKIP_POLICY_SECTION_IDS = frozenset(
    {"content", "main", "pkp_content_main", "pkp_structure_main", "page"}
)


def _policy_page_root(doc: Any) -> Any:
    nodes = doc.xpath(
        "//*[@id='content'] | //*[contains(@class,'pkp_structure_main')] | //main"
    )
    return nodes[0] if nodes else doc


def _skip_policy_section_id(fid: str) -> bool:
    if not fid or fid in _SKIP_POLICY_SECTION_IDS:
        return True
    return fid.startswith("pkp-about")


def _section_element_body(el: Any) -> str:
    full = norm_space(el.text_content() or "")
    for h in el.xpath("./h2|./h3"):
        ht = norm_space(h.text_content() or "")
        if ht and full.lower().startswith(ht.lower()):
            full = norm_space(full[len(ht) :])
            break
    return full


def resolve_policy_section_body(
    doc: Any,
    *,
    fragment_ids: tuple[str, ...] = (),
    title_aliases: tuple[str, ...] = (),
) -> tuple[str, str]:
    """Текст раздела редакционной политики и id якоря (OJS div#…)."""
    if doc is None:
        return "", ""

    root = _policy_page_root(doc)

    for fid in fragment_ids:
        nodes = root.xpath(f".//*[@id='{fid}']")
        if nodes:
            return _section_element_body(nodes[0]), fid

    for el in root.xpath(".//*[@id]"):
        fid = el.get("id") or ""
        if _skip_policy_section_id(fid):
            continue
        for h in el.xpath("./h2"):
            if _heading_matches(h.text_content() or "", title_aliases):
                return _section_element_body(el), fid

    for h in root.xpath(".//h2"):
        if not _heading_matches(h.text_content() or "", title_aliases):
            continue
        parts: list[str] = []
        for sib in h.itersiblings():
            tag = getattr(sib, "tag", None)
            if tag in {"h1", "h2", "h3"}:
                break
            parts.append(sib.text_content() or "")
        body = norm_space(" ".join(parts))
        frag = h.get("id") or ""
        parent = h.getparent()
        if parent is not None and not frag:
            frag = parent.get("id") or ""
        if _skip_policy_section_id(frag):
            frag = ""
        if body or frag:
            return body, frag

    for h1 in root.xpath("./h1"):
        if not _heading_matches(h1.text_content() or "", title_aliases):
            continue
        parts: list[str] = []
        for sib in h1.itersiblings():
            tag = getattr(sib, "tag", None)
            if tag in {"h1", "h2", "h3"}:
                break
            parts.append(sib.text_content() or "")
        return norm_space(" ".join(parts)), "__page_h1__"

    return "", ""


def page_main_text(doc: Any) -> str:
    nodes = doc.xpath(
        "//*[@id='content'] | //*[contains(@class,'pkp_structure_main')] | //main | //article"
    )
    root = nodes[0] if nodes else doc
    text = norm_space(root.text_content() or "")
    # грубо убрать сайдбарные дубли: ограничиваем разумным объёмом отчёта
    return text


_AFFIL_MARKERS_TEAM = (
    "университет",
    "university",
    "институт",
    "institute",
    "академи",
    "academy",
    "центр",
    "center",
    "centre",
    "hospital",
    "клиник",
    "кафедр",
    "department",
    "факультет",
    "faculty",
    "лаборатор",
    "ооо",
    "фгбу",
    "фгану",
    "ран",
    "аграрн",
    "college",
    "образовательн",
)


def _member_entry(raw: str) -> dict[str, Any] | None:
    text = norm_space(raw)
    if len(text) < 5 or len(text) > 320:
        return None
    if not re.search(r"[A-ZА-ЯЁ][a-zа-яё\-']{1,}", text):
        return None
    low = text.lower()
    return {
        "raw": text,
        "has_affiliation": any(a in low for a in _AFFIL_MARKERS_TEAM),
    }


def _items_after_heading(h2: Any) -> list[str]:
    items: list[str] = []
    for sib in h2.itersiblings():
        tag = getattr(sib, "tag", None)
        if tag in {"h1", "h2", "h3", "h4"}:
            break
        for li in sib.xpath(".//li"):
            text = norm_space(li.text_content() or "")
            if text:
                items.append(text)
        if not sib.xpath(".//li"):
            text = norm_space(sib.text_content() or "")
            if text and len(text) < 300:
                items.append(text)
    return items


def _classify_team_section(title: str, element_id: str = "") -> str:
    """chief | deputy | board | editors | technical | other."""
    t = norm_space(title).lower()
    eid = (element_id or "").lower()
    if any(x in t for x in ("главный редактор", "editor-in-chief", "editor in chief")):
        return "chief"
    if any(
        x in t
        for x in (
            "заместитель",
            "deputy editor",
            "deputy editor-in-chief",
            "vice editor",
            "associate editor-in-chief",
        )
    ):
        return "deputy"
    if any(
        x in t
        for x in (
            "редакционная коллегия",
            "редколлегия",
            "editorial board",
            "editorial committee",
            "редакционный совет",
        )
    ):
        return "board"
    if t in {"editors", "редакторы"} or eid == "editors":
        return "editors"
    if any(x in t for x in ("редактор раздела", "section editor", "section editors")):
        return "board"
    if any(
        x in t
        for x in (
            "layout editor",
            "худ. редактор",
            "художествен",
            "copy editor",
            "copyeditor",
            "лит. редактор",
            "proofreader",
            "корректор",
            "технический редактор",
        )
    ) or eid in {"layouteditors", "copyeditors", "proofreadors", "proofreaders"}:
        return "technical"
    return "other"


@dataclass(slots=True)
class EditorialTeamStructure:
    """Состав редакции со страницы /about/editorialTeam."""

    chief: list[dict[str, Any]] = field(default_factory=list)
    deputies: list[dict[str, Any]] = field(default_factory=list)
    board: list[dict[str, Any]] = field(default_factory=list)

    def all_board_members(self) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for m in self.board:
            key = m["raw"].lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(m)
        return out


def extract_editorial_team_structure(doc: Any) -> EditorialTeamStructure:
    """Главный редактор, заместители и члены редколлегии по секциям OJS."""
    root_nodes = doc.xpath(
        "//*[@id='content'] | //*[contains(@class,'pkp_structure_main')] | //main"
    )
    root = root_nodes[0] if root_nodes else doc
    structure = EditorialTeamStructure()
    seen: dict[str, set[str]] = {"chief": set(), "deputy": set(), "board": set()}

    def add(kind: str, raw: str) -> None:
        entry = _member_entry(raw)
        if not entry:
            return
        key = entry["raw"].lower()
        if key in seen[kind]:
            return
        seen[kind].add(key)
        if kind == "chief":
            structure.chief.append(entry)
        elif kind == "deputy":
            structure.deputies.append(entry)
        else:
            structure.board.append(entry)

    for h2 in root.xpath(".//h2"):
        title = norm_space(h2.text_content() or "")
        parent = h2.getparent()
        el_id = parent.get("id") or "" if parent is not None else ""
        kind = _classify_team_section(title, el_id)
        if kind == "technical":
            continue
        if kind == "chief":
            target = "chief"
        elif kind == "deputy":
            target = "deputy"
        elif kind in {"board", "editors"}:
            target = "board"
        else:
            continue
        for line in _items_after_heading(h2):
            add(target, line)

    for node in root.xpath(".//h2|.//h3|.//p|.//li"):
        text = norm_space(node.text_content() or "")
        low = text.lower()
        if "главный редактор" in low or "editor-in-chief" in low:
            add("chief", text)
        if ("заместитель" in low and "редактор" in low) or "deputy editor" in low:
            add("deputy", text)

    # Блок editorialBoard по id (классический OJS)
    for node in root.xpath(".//*[@id='editorialBoard']"):
        for li in node.xpath(".//li"):
            add("board", norm_space(li.text_content() or ""))

    return structure


def extract_editorial_members(doc: Any) -> list[dict[str, Any]]:
    """Члены редколлегии (секция Editors / Editorial Board, без техперсонала)."""
    return extract_editorial_team_structure(doc).all_board_members()


_MAP_SRC_MARKERS = (
    "google.com/maps",
    "maps.google.",
    "yandex.ru/map",
    "yandex.com/map",
    "api-maps.yandex",
    "2gis.",
    "openstreetmap.org",
    "maps.apple.com",
    "static-maps.yandex",
)


def _contact_page_root(doc: Any) -> Any:
    nodes = doc.xpath(
        "//*[@id='content'] | //*[contains(@class,'pkp_structure_main')] | //main"
    )
    return nodes[0] if nodes else doc


def extract_contact_map(doc: Any) -> dict[str, str]:
    """Сигнал карты на странице контактов: iframe/embed/ссылка/изображение."""
    root = _contact_page_root(doc)
    for iframe in root.xpath(".//iframe[@src]|.//embed[@src]"):
        src = iframe.get("src") or ""
        low = src.lower()
        if any(m in low for m in _MAP_SRC_MARKERS) or "map" in low:
            return {"kind": "iframe", "src": src}
    for a in root.xpath(".//a[@href]"):
        href = a.get("href") or ""
        low = href.lower()
        if any(m in low for m in _MAP_SRC_MARKERS):
            return {"kind": "link", "src": href}
    for img in root.xpath(".//img[@src]"):
        src = (img.get("src") or "") + " " + (img.get("alt") or "")
        low = src.lower()
        if any(m in low for m in _MAP_SRC_MARKERS) or "map" in low or "карт" in low:
            return {"kind": "image", "src": img.get("src") or ""}
    return {}


def extract_contact_persons(doc: Any) -> list[dict[str, Any]]:
    """Контактные лица из блока Principal Contact / Редакция (не техподдержка РЦНИ)."""
    root = _contact_page_root(doc)
    block_nodes: list[Any] = []
    for xp in (
        ".//*[@id='principalContact']",
        ".//*[contains(@class,'principalContact')]",
    ):
        block_nodes.extend(root.xpath(xp))

    if not block_nodes:
        for h in root.xpath(".//h2|.//h3"):
            title = norm_space(h.text_content() or "").lower()
            if any(
                x in title
                for x in (
                    "principal contact",
                    "основной контакт",
                    "редакция",
                    "contact person",
                    "контактные лица",
                    "контакты редакции",
                )
            ):
                parent = h.getparent()
                if parent is not None and parent.get("id"):
                    block_nodes.append(parent)
                else:
                    chunks = [h]
                    for sib in h.itersiblings():
                        if getattr(sib, "tag", None) in {"h2", "h3"}:
                            break
                        chunks.append(sib)
                    block_nodes.extend(chunks)
                break

    skip_markers = (
        "journals_support@rcsi.science",
        "info@rcsi.science",
        "rcsi journals",
        "служба поддержки",
        "platform support",
        "technical support",
        "техническая поддержка",
    )
    persons: list[dict[str, Any]] = []
    seen: set[str] = set()
    candidates: list[str] = []
    for node in block_nodes:
        for p in node.xpath(".//p"):
            text = norm_space(p.text_content() or "")
            if text:
                candidates.append(text)
        for li in node.xpath(".//li"):
            text = norm_space(li.text_content() or "")
            if text:
                candidates.append(text)
        # OJS часто кладёт людей в strong+br без <p>
        own = norm_space(node.text_content() or "")
        if own and len(own) < 800 and not candidates:
            candidates.append(own)

    for raw in candidates:
        low = raw.lower()
        if any(x in low for x in skip_markers):
            continue
        if len(raw) < 8 or len(raw) > 500:
            continue
        has_name = bool(re.search(r"[A-ZА-ЯЁ][a-zа-яё\-']{1,}", raw))
        has_role = any(
            x in low
            for x in (
                "редактор",
                "editor",
                "секретар",
                "secretary",
                "контакт",
                "contact",
                "заместитель",
                "deputy",
            )
        )
        has_email = bool(_EMAIL_RE.search(raw))
        has_phone = bool(_PHONE_RE.search(raw)) or "phone" in low or "тел" in low
        if not has_name:
            continue
        if not (has_role or has_email or has_phone):
            continue
        key = raw.lower()[:120]
        if key in seen:
            continue
        seen.add(key)
        persons.append(
            {
                "raw": raw,
                "has_email": has_email,
                "has_phone": has_phone,
                "has_role": has_role,
            }
        )
    return persons


def extract_contact_fields(doc: Any) -> dict[str, str]:
    text = page_main_text(doc)
    email = ""
    m = _EMAIL_RE.search(text)
    if m:
        email = m.group(0)

    principal = ""
    for xp in (
        "//*[contains(@class,'principalContact')]/*",
        "//*[@id='principalContact']",
        "//h2[contains(.,'Principal') or contains(.,'Основн') or contains(.,'Редакц')]/following-sibling::*[1]",
        "//h3[contains(.,'Principal') or contains(.,'Основн') or contains(.,'Редакц')]/following-sibling::*[1]",
    ):
        nodes = doc.xpath(xp)
        if nodes:
            principal = norm_space(nodes[0].text_content() if hasattr(nodes[0], "text_content") else str(nodes[0]))
            if principal:
                break
    if not principal:
        # блок после Principal Contact / Редакция
        for h in doc.xpath("//h2|//h3"):
            title = norm_space(h.text_content() or "").lower()
            if (
                "principal" in title
                or "основн" in title
                or title in {"редакция", "контакты редакции"}
            ):
                chunks: list[str] = []
                for sib in h.itersiblings():
                    if getattr(sib, "tag", None) in {"h2", "h3"}:
                        break
                    chunks.append(sib.text_content())
                principal = norm_space(" ".join(chunks))
                break
    if not principal:
        persons = extract_contact_persons(doc)
        if persons:
            principal = persons[0]["raw"]

    mailing = ""
    for h in doc.xpath("//h2|//h3"):
        title = norm_space(h.text_content() or "").lower()
        if "mailing" in title or "почтовый" in title or "адрес" == title:
            chunks = []
            for sib in h.itersiblings():
                if getattr(sib, "tag", None) in {"h2", "h3"}:
                    break
                chunks.append(sib.text_content())
            mailing = norm_space(" ".join(chunks))
            break

    if len(mailing) < 20 and principal:
        # адрес часто внутри блока основного контакта
        m_addr = re.search(
            r"((?:\d{5,6}.{0,10})?(?:г\.|город|city)?[^.]{0,40}"
            r"(?:ул\.|пр\.|проспект|street|кирова|ленина)[^.]{5,80})",
            principal,
            flags=re.I,
        )
        if m_addr:
            mailing = norm_space(m_addr.group(1))
        elif re.search(r"\d{5,6}", principal) and any(
            x in principal.lower() for x in ("ул.", "г.", "city", "россия", "russia")
        ):
            mailing = principal

    return {
        "contact_principal": principal,
        "contact_email": email,
        "mailing_address": mailing,
    }


def extract_homepage_meta(doc: Any) -> dict[str, str]:
    """Метаданные с главной по подписям strong (учредитель, гл. редактор, доступ…)."""
    out: dict[str, str] = {}
    for field_id in (
        "issn",
        "founder",
        "editor_in_chief",
        "frequency",
        "media_certificate",
        "languages",
        "publisher",
    ):
        value = extract_labeled_value(doc, field_id)
        if value:
            out[field_id] = value
    # Частота / доступ в одной строке: «4 issues per year / Open»
    freq = out.get("frequency") or ""
    if freq and "/" in freq:
        left, right = [norm_space(x) for x in freq.split("/", 1)]
        if left:
            out["frequency"] = left
        if right:
            out["access_hint"] = right
    return out


def extract_homepage_fields(doc: Any) -> dict[str, str]:
    """С главной: описание, ISSN, обложка, текущий выпуск (+ название для отчёта)."""
    meta = extract_homepage_meta(doc)
    return {
        "journal_title": extract_journal_title(doc),
        "description": extract_description(doc),
        "issn": meta.get("issn") or extract_issn(doc),
        "homepage_image": extract_homepage_image(doc),
        "current_issue": extract_current_issue(doc),
        "founder": meta.get("founder", ""),
        "editor_in_chief": meta.get("editor_in_chief", ""),
        "frequency": meta.get("frequency", ""),
        "access_hint": meta.get("access_hint", ""),
        "media_certificate": meta.get("media_certificate", ""),
        "languages": meta.get("languages", ""),
        "publisher": meta.get("publisher", ""),
    }


def extract_policy_fields(doc: Any) -> dict[str, str]:
    ethics_body, _ = resolve_policy_section_body(
        doc,
        title_aliases=(
            "этический кодекс",
            "publication ethics",
            "публикационная этика",
            "code of ethics",
        ),
    )

    return {
        "aims_and_scope": section_body_text(doc, "focusAndScope"),
        "peer_review": section_body_text(doc, "peerReviewProcess"),
        "publication_frequency_policy": section_body_text(doc, "publicationFrequency"),
        "open_access_policy": section_body_text(doc, "openAccessPolicy"),
        "publication_ethics": ethics_body,
    }


def _slug_id(text: str) -> str:
    raw = norm_space(text).lower()
    out = []
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
        elif ch in {" ", "-", "_", "/", "#"}:
            out.append("_")
    slug = "_".join(p for p in "".join(out).split("_") if p)
    return slug[:64] or "item"


def extract_about_menu(doc: Any, *, base_url: str = "") -> list[AboutMenuItem]:
    """Собрать все пункты меню со страницы /about (People / Policies / …)."""
    items: list[AboutMenuItem] = []
    seen: set[str] = set()

    for h2 in doc.xpath(".//h2"):
        section_title = norm_space(h2.text_content() or "")
        if not section_title or len(section_title) > 40:
            continue
        category = resolve_about_section_category(section_title)
        if not category:
            continue

        link_nodes: list[Any] = []
        for sib in h2.itersiblings():
            tag = getattr(sib, "tag", None)
            if tag in {"h1", "h2"}:
                break
            link_nodes.extend(sib.xpath(".//a[@href]"))

        for a in link_nodes:
            title = norm_space(a.text_content() or "")
            href = (a.get("href") or "").strip()
            if not title or not href:
                continue
            if href.startswith("#") and len(href) < 2:
                continue

            abs_url = urljoin(base_url.rstrip("/") + "/", href) if base_url else href
            parsed = urlparse(abs_url)
            fragment = unquote(parsed.fragment or "").strip()
            page_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
            if not parsed.scheme or not parsed.netloc:
                continue

            path = parsed.path or ""
            min_filled, min_weak, weight = thresholds_for_about_item(title, fragment, path)
            if fragment:
                field_id = f"about_{fragment}"
            else:
                tail = path.rstrip("/").split("/")[-1] or _slug_id(title)
                # сохраняем регистр сегмента пути (editorialTeam), иначе только slug
                safe_tail = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in tail)
                field_id = f"about_{safe_tail or _slug_id(title)}"
            if field_id in seen:
                # один и тот же якорь с разными подписями — оставляем первый
                continue
            seen.add(field_id)
            items.append(
                AboutMenuItem(
                    id=field_id,
                    title=title,
                    category=category,
                    page_url=page_url,
                    fragment=fragment,
                    weight=weight,
                    min_chars_filled=min_filled,
                    min_chars_weak=min_weak,
                )
            )
    return items


def content_for_about_item(doc: Any, item: AboutMenuItem) -> str:
    """Текст пункта about: секция по якорю или вся страница."""
    if item.fragment:
        text = section_body_text(doc, item.fragment)
        if text:
            return text
        # fallback: элемент с id может быть заголовком без обёртки
        nodes = doc.xpath(f"//*[@id='{item.fragment}']")
        if nodes:
            return norm_space(nodes[0].text_content() or "")
        return ""
    return page_main_text(doc)


def preview_value(value: str, limit: int = 180) -> str:
    text = norm_space(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"
