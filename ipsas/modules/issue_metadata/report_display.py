"""Данные для отображения статьи в отчёте (как в XML-валидации)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ipsas.modules.issue_metadata.lang import detect_lang
from ipsas.modules.issue_metadata.validators import (
    bibliography_item_looks_glued,
    split_glued_bibliography_item,
)


def _as_str_list(value: object) -> List[str]:
    if not isinstance(value, list):
        return []
    return [str(x).strip() for x in value if x and str(x).strip()]


def _dash(value: Optional[str]) -> str:
    text = (value or "").strip()
    return text if text else "—"


def _aff_map(jats_affs: Sequence[object]) -> Dict[str, List[Dict[str, object]]]:
    out: Dict[str, List[Dict[str, object]]] = {}
    for item in jats_affs:
        if not isinstance(item, dict):
            continue
        aff_id = str(item.get("id") or "").strip()
        if aff_id:
            out.setdefault(aff_id, []).append(item)
    return out


def _aff_names_for_rids(
    rids: Sequence[str],
    aff_by_id: Dict[str, List[Dict[str, object]]],
    *,
    lang: str,
) -> str:
    names: List[str] = []
    for rid in rids:
        for aff in aff_by_id.get(str(rid).strip()) or []:
            preferred = ""
            if lang == "ru":
                preferred = str(aff.get("name_ru") or "").strip()
            elif lang == "en":
                preferred = str(aff.get("name_en") or "").strip()
            if preferred:
                if preferred not in names:
                    names.append(preferred)
                continue
            name = str(aff.get("name") or "").strip()
            if not name:
                continue
            aff_lang = str(aff.get("lang") or "").strip().lower() or detect_lang(name) or "unk"
            if lang == "ru" and aff_lang in {"en"}:
                continue
            if lang == "en" and aff_lang in {"ru"}:
                continue
            if lang == "any" or aff_lang in {lang, "unk"} or lang not in {"ru", "en"}:
                if name not in names:
                    names.append(name)
    return "; ".join(names)


def _page_aff_by_index(page_affs: Sequence[object]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for item in page_affs:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except (TypeError, ValueError):
            continue
        name = str(item.get("name") or "").strip()
        if name:
            out[idx] = name
    return out


def build_authors_table(article: Dict[str, object]) -> List[Dict[str, object]]:
    """Таблица авторов: ФИО/аффилиация RUS+ENG (как в отчёте journal XML)."""
    authors_ru = _as_str_list(article.get("authors_ru"))
    authors_en = _as_str_list(article.get("authors_en"))
    authors_any = _as_str_list(article.get("authors"))
    affiliations_ru = _as_str_list(article.get("affiliations_ru"))
    affiliations_en = _as_str_list(article.get("affiliations_en"))
    organizations = _as_str_list(article.get("organizations") or article.get("affiliations"))

    try:
        authors_count = int(article.get("authors_count") or 0)
    except (TypeError, ValueError):
        authors_count = 0
    n = max(authors_count, len(authors_ru), len(authors_en), len(authors_any), 0)
    if n <= 0:
        return []

    jats_affs = article.get("jats_affiliations") or []
    aff_by_id = _aff_map(jats_affs if isinstance(jats_affs, list) else [])
    contrib_refs = article.get("contributor_affiliation_refs") or []
    if not isinstance(contrib_refs, list):
        contrib_refs = []
    page_aff_map = _page_aff_by_index(
        article.get("page_affiliations") if isinstance(article.get("page_affiliations"), list) else []
    )
    author_page_refs = article.get("author_affiliation_refs") or []
    if not isinstance(author_page_refs, list):
        author_page_refs = []

    rows: List[Dict[str, object]] = []
    for i in range(n):
        name_ru = authors_ru[i] if i < len(authors_ru) else ""
        name_en = authors_en[i] if i < len(authors_en) else ""
        if not name_ru and not name_en and i < len(authors_any):
            primary = authors_any[i]
            lang = detect_lang(primary)
            if lang == "en":
                name_en = primary
            else:
                name_ru = primary

        aff_ru = ""
        aff_en = ""
        author_has_aff_link = False
        if i < len(contrib_refs) and isinstance(contrib_refs[i], dict):
            rids = [
                str(x).strip()
                for x in (contrib_refs[i].get("rid") or [])
                if str(x).strip()
            ]
            author_has_aff_link = bool(rids)
            if rids and aff_by_id:
                aff_ru = _aff_names_for_rids(rids, aff_by_id, lang="ru")
                aff_en = _aff_names_for_rids(rids, aff_by_id, lang="en")
                # Если у aff нет языковой разметки — одно имя на обе колонки
                if not aff_ru and not aff_en:
                    shared = _aff_names_for_rids(rids, aff_by_id, lang="any")
                    aff_ru = shared
                    aff_en = shared

        if (not aff_ru or not aff_en) and i < len(author_page_refs):
            refs = author_page_refs[i]
            if isinstance(refs, list):
                page_names: List[str] = []
                for raw in refs:
                    try:
                        idx = int(raw)
                    except (TypeError, ValueError):
                        continue
                    name = page_aff_map.get(idx)
                    if name and name not in page_names:
                        page_names.append(name)
                joined = "; ".join(page_names)
                if joined:
                    author_has_aff_link = True
                    if not aff_ru:
                        aff_ru = joined
                    if not aff_en:
                        if not name_ru and name_en:
                            aff_en = joined
                        elif not aff_en and detect_lang(joined) == "en":
                            aff_en = joined

        # Только если организация одна на статью — копируем всем авторам.
        # Нельзя раздавать список организаций по индексу автора.
        if not aff_ru and len(affiliations_ru) == 1:
            aff_ru = affiliations_ru[0]
        if not aff_en and len(affiliations_en) == 1:
            aff_en = affiliations_en[0]
        if not aff_ru and len(organizations) == 1:
            aff_ru = organizations[0]

        problems: List[str] = []
        if name_ru and not name_en:
            problems.append("нет ФИО (ENG)")
        if name_en and not name_ru:
            problems.append("нет ФИО (RUS)")
        if (name_ru or name_en) and not aff_ru and not aff_en:
            if author_has_aff_link:
                problems.append("не удалось сопоставить организацию автора")
            else:
                problems.append("в метаданных не указана организация автора")
        else:
            if name_ru and not aff_ru:
                problems.append("нет аффилиации (RUS)")
            if name_en and not aff_en:
                problems.append("нет аффилиации (ENG)")

        rows.append(
            {
                "index": i + 1,
                "name_ru": _dash(name_ru),
                "name_en": _dash(name_en),
                "affiliation_ru": _dash(aff_ru),
                "affiliation_en": _dash(aff_en),
                "problems": problems,
            }
        )
    return rows


def _unique_aff_count(rows: Sequence[Dict[str, object]], key: str) -> int:
    seen: set[str] = set()
    for row in rows:
        value = str(row.get(key) or "").strip()
        if not value or value == "—":
            continue
        for part in re_split_orgs(value):
            seen.add(part)
    return len(seen)


def re_split_orgs(value: str) -> List[str]:
    parts = [p.strip() for p in value.replace("|", ";").split(";") if p.strip()]
    return parts or ([value.strip()] if value.strip() else [])


def _ref_counts_for_display(article: Dict[str, object]) -> Dict[str, int]:
    try:
        total = int(article.get("references_count") or 0)
    except (TypeError, ValueError):
        total = 0
    try:
        ru = int(article.get("references_ru_count") or 0)
    except (TypeError, ValueError):
        ru = 0
    try:
        en = int(article.get("references_en_count") or 0)
    except (TypeError, ValueError):
        en = 0
    try:
        unk = int(article.get("references_unk_count") or 0)
    except (TypeError, ValueError):
        unk = 0

    # JATS без xml:lang — не делим эвристикой, всё в UNK
    if article.get("references_lang_source") == "unspecified":
        items = article.get("references") or []
        n = len(items) if isinstance(items, list) and items else total
        if isinstance(items, list) and len(items) == 1 and bibliography_item_looks_glued(str(items[0])):
            parts = split_glued_bibliography_item(str(items[0]))
            if len(parts) >= 2:
                n = len(parts)
        return {"ru": 0, "en": 0, "unk": n, "total": n}

    if total and not (ru or en or unk):
        unk = total
    return {"ru": ru, "en": en, "unk": unk, "total": total or (ru + en + unk)}


def build_references_preview(article: Dict[str, object]) -> List[Dict[str, Any]]:
    """Первый/последний источник по языкам (как в XML-отчёте)."""
    counts = _ref_counts_for_display(article)
    preview: List[Dict[str, Any]] = []

    def add_row(lang: str, count: int, first: Optional[object], last: Optional[object]) -> None:
        if count <= 0 and not first:
            return
        first_s = str(first).strip() if first else ""
        last_s = str(last).strip() if last else first_s
        if not first_s and not last_s:
            return
        preview.append(
            {
                "lang": lang,
                "count": count or 1,
                "first": first_s or "—",
                "last": last_s or "—",
                "same": bool(first_s) and first_s == last_s,
            }
        )

    items = article.get("references") or []
    primary_first = article.get("reference_first")
    primary_last = article.get("reference_last")
    if isinstance(items, list) and items:
        primary_first = primary_first or items[0]
        primary_last = primary_last or items[-1]
        # Одна «запись» = несколько слипшихся источников — показать реальные первый/последний
        if len(items) == 1 and bibliography_item_looks_glued(str(items[0])):
            parts = split_glued_bibliography_item(str(items[0]))
            if len(parts) >= 2:
                primary_first = parts[0]
                primary_last = parts[-1]

    # Один список без xml:lang — одна строка UNK
    if article.get("references_lang_source") == "unspecified" or (
        counts["ru"] == 0
        and counts["en"] == 0
        and counts["unk"] > 0
        and article.get("references_lang_source") != "xml_lang"
        and article.get("references_mode") not in {"parallel_blocks", "parallel_citations"}
    ):
        add_row("UNK", counts["unk"] or counts["total"], primary_first, primary_last)
        return preview

    add_row(
        "RUS",
        counts["ru"],
        article.get("reference_ru_first"),
        article.get("reference_ru_last"),
    )
    add_row(
        "ENG",
        counts["en"],
        article.get("reference_en_first"),
        article.get("reference_en_last"),
    )
    add_row(
        "UNK",
        counts["unk"],
        article.get("reference_unk_first"),
        article.get("reference_unk_last"),
    )

    if not preview and primary_first:
        add_row("UNK", counts["total"] or 1, primary_first, primary_last)
    return preview


def build_references_status(article: Dict[str, object], counts: Dict[str, int]) -> Dict[str, str]:
    """Строка статуса источников в духе XML-отчёта."""
    if counts["total"] <= 0 and not (article.get("references") or []):
        return {"comparison": "❌ Источники отсутствуют"}
    parts: List[str] = []
    if counts["ru"]:
        parts.append(f"RUS={counts['ru']}")
    if counts["en"]:
        parts.append(f"ENG={counts['en']}")
    if counts["unk"]:
        parts.append(f"UNK={counts['unk']}")
    if not parts and counts["total"]:
        parts.append(f"total={counts['total']}")
    return {"comparison": f"✅ Источники есть ({', '.join(parts)})"}


def enrich_article_report_display(article: Dict[str, object]) -> None:
    """Заполнить поля UI-отчёта: авторы/аффилиации и превью источников."""
    authors_table = build_authors_table(article)
    article["authors_table"] = authors_table
    article["unique_affiliations_ru"] = _unique_aff_count(authors_table, "affiliation_ru")
    article["unique_affiliations_en"] = _unique_aff_count(authors_table, "affiliation_en")

    counts = _ref_counts_for_display(article)
    article["references_display_ru"] = counts["ru"]
    article["references_display_en"] = counts["en"]
    article["references_display_unk"] = counts["unk"]
    article["references_preview"] = build_references_preview(article)
    article["references_status"] = build_references_status(article, counts)
