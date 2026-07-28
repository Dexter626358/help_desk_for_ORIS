"""Слияние JATS-метаданных в структуру статьи выпуска."""

from __future__ import annotations

import re
from typing import Dict, Optional

from ipsas.modules.issue_metadata import validators as issue_validators


def abstract_stats(text: Optional[str]) -> Dict[str, Optional[object]]:
    if not text:
        return {"length": None, "first_10": None, "last_10": None}
    tokens = re.findall(r"[A-Za-zА-Яа-я0-9]+", text)
    first = " ".join(tokens[:10]) if tokens else None
    last = " ".join(tokens[-10:]) if tokens else None
    return {"length": len(tokens), "first_10": first, "last_10": last}


def merge_jats_into_article(
    article_data: Dict[str, object],
    issue_metadata: Dict[str, object],
    xml_parsed: Dict[str, object],
) -> None:
    """Слить JATS в статью: канонические данные статьи — из XML; HTML — для page_* (отображение)."""
    xml_warnings = xml_parsed.get("warnings")
    if isinstance(xml_warnings, list):
        src_warns = article_data.setdefault("source_warnings", [])
        if isinstance(src_warns, list):
            for warning in xml_warnings:
                if isinstance(warning, dict):
                    src_warns.append(warning)

    jmeta = xml_parsed.get("journal_meta")
    if isinstance(jmeta, dict):
        j_ru = jmeta.get("journal_title_ru")
        j_en = jmeta.get("journal_title")
        if not issue_metadata.get("journal_title_ru") and isinstance(j_ru, str) and j_ru.strip():
            issue_metadata["journal_title_ru"] = j_ru.strip()
        if (
            (not issue_metadata.get("journal_title"))
            or (
                issue_metadata.get("journal_title_ru")
                and issue_metadata.get("journal_title") == issue_metadata.get("journal_title_ru")
            )
        ):
            if isinstance(j_en, str) and j_en.strip():
                issue_metadata["journal_title"] = j_en.strip()

        if not issue_metadata.get("issn") and isinstance(jmeta.get("issn"), str) and jmeta.get("issn").strip():
            cand = jmeta["issn"].strip()
            if cand.lower() != str(issue_metadata.get("eissn") or "").strip().lower():
                issue_metadata["issn"] = cand
        if not issue_metadata.get("eissn") and isinstance(jmeta.get("eissn"), str) and jmeta.get("eissn").strip():
            cand = jmeta["eissn"].strip()
            if cand.lower() != str(issue_metadata.get("issn") or "").strip().lower():
                issue_metadata["eissn"] = cand

    # --- Названия: JATS канон, HTML → page_title_* ---
    page_title_ru = (article_data.get("title_ru") or "").strip() or None
    page_title_en = (article_data.get("title_en") or "").strip() or None
    article_data["page_title_ru"] = page_title_ru
    article_data["page_title_en"] = page_title_en
    jats_title_ru = xml_parsed.get("title_ru")
    jats_title_en = xml_parsed.get("title_en")
    if isinstance(jats_title_ru, str) and jats_title_ru.strip():
        article_data["jats_title_ru"] = jats_title_ru.strip()
        article_data["title_ru"] = jats_title_ru.strip()
        article_data["title_ru_from_jats_only"] = not bool(page_title_ru)
    if isinstance(jats_title_en, str) and jats_title_en.strip():
        article_data["jats_title_en"] = jats_title_en.strip()
        article_data["title_en"] = jats_title_en.strip()
        article_data["title_en_from_jats_only"] = not bool(page_title_en)

    # --- Аннотации / keywords: канон из JATS, page_* уже с HTML ---
    if xml_parsed.get("abstract_ru"):
        article_data["abstract_ru"] = xml_parsed["abstract_ru"]
        article_data["abstract_ru_stats"] = abstract_stats(xml_parsed["abstract_ru"])
        article_data["abstract_ru_from_jats_only"] = not bool(
            (article_data.get("page_abstract_ru") or "").strip()
        )
    if xml_parsed.get("abstract_en"):
        article_data["abstract_en"] = xml_parsed["abstract_en"]
        article_data["abstract_en_stats"] = abstract_stats(xml_parsed["abstract_en"])
        article_data["abstract_en_from_jats_only"] = not bool(
            (article_data.get("page_abstract_en") or "").strip()
        )
    if xml_parsed.get("keywords_ru"):
        article_data["keywords_ru"] = xml_parsed["keywords_ru"]
        article_data["keywords_ru_count"] = len(xml_parsed["keywords_ru"])
        article_data["keywords_ru_from_jats_only"] = not bool(article_data.get("page_keywords_ru"))
    if xml_parsed.get("keywords_en"):
        article_data["keywords_en"] = xml_parsed["keywords_en"]
        article_data["keywords_en_count"] = len(xml_parsed["keywords_en"])
        article_data["keywords_en_from_jats_only"] = not bool(article_data.get("page_keywords_en"))

    # --- Авторы: JATS канон ---
    page_authors_ru = list(article_data.get("authors_ru") or [])
    page_authors_en = list(article_data.get("authors_en") or [])
    article_data["page_authors_ru"] = page_authors_ru
    article_data["page_authors_en"] = page_authors_en
    jats_authors_ru = xml_parsed.get("authors_ru")
    jats_authors_en = xml_parsed.get("authors_en")
    jats_authors = xml_parsed.get("authors")
    if isinstance(jats_authors_ru, list) and jats_authors_ru:
        article_data["authors_ru"] = list(jats_authors_ru)
    if isinstance(jats_authors_en, list) and jats_authors_en:
        article_data["authors_en"] = list(jats_authors_en)
    if isinstance(jats_authors, list) and jats_authors:
        article_data["authors"] = list(jats_authors)
    elif article_data.get("authors_ru") or article_data.get("authors_en"):
        article_data["authors"] = list(article_data.get("authors_ru") or article_data.get("authors_en") or [])
    if isinstance(xml_parsed.get("orcids"), list) and xml_parsed["orcids"]:
        article_data["orcids"] = list(xml_parsed["orcids"])
    if isinstance(xml_parsed.get("emails"), list) and xml_parsed["emails"]:
        article_data["emails"] = list(xml_parsed["emails"])
    if xml_parsed.get("has_corresponding_author") is not None:
        article_data["has_corresponding_author"] = xml_parsed["has_corresponding_author"]
    # Пересчёт числа авторов после JATS: число contrib, не длина одного языкового списка
    if isinstance(xml_parsed.get("authors_count"), int) and int(xml_parsed["authors_count"]) > 0:
        article_data["authors_count"] = int(xml_parsed["authors_count"])
    elif isinstance(article_data.get("authors"), list) and article_data["authors"]:
        article_data["authors_count"] = len(article_data["authors"])
    elif isinstance(article_data.get("authors_ru"), list) and article_data["authors_ru"]:
        article_data["authors_count"] = len(article_data["authors_ru"])
    elif isinstance(article_data.get("authors_en"), list) and article_data["authors_en"]:
        article_data["authors_count"] = len(article_data["authors_en"])

    # Организации из JATS, если на странице пусто
    jats_affs = xml_parsed.get("jats_affiliations")
    if isinstance(jats_affs, list) and jats_affs:
        article_data["jats_affiliations"] = jats_affs
        jats_org_names = [
            str(a.get("name")).strip()
            for a in jats_affs
            if isinstance(a, dict) and str(a.get("name") or "").strip()
        ]
        if jats_org_names and not article_data.get("organizations"):
            article_data["organizations"] = jats_org_names
            article_data["organizations_count"] = len(jats_org_names)
            article_data["affiliations"] = list(jats_org_names)
    if isinstance(xml_parsed.get("contributor_affiliation_refs"), list):
        article_data["contributor_affiliation_refs"] = xml_parsed["contributor_affiliation_refs"]
    if isinstance(xml_parsed.get("broken_affiliation_refs"), list):
        article_data["broken_affiliation_refs"] = xml_parsed["broken_affiliation_refs"]

    if xml_parsed.get("identifiers"):
        from ipsas.modules.issue_metadata.validators import looks_like_edn

        idents = article_data.get("identifiers")
        if not isinstance(idents, dict):
            idents = {}
            article_data["identifiers"] = idents
        for key, val in xml_parsed["identifiers"].items():
            if val is None:
                continue
            if key == "edn":
                if looks_like_edn(str(val)):
                    idents["edn"] = val
                elif str(val).isdigit() and not idents.get("internal_id"):
                    idents["internal_id"] = str(val)
                else:
                    idents["invalid_edn"] = str(val)
                continue
            if key == "invalid_edn":
                if not idents.get("invalid_edn"):
                    idents["invalid_edn"] = str(val)
                continue
            if key == "internal_id":
                # JATS/URL: не затирать уже найденный internal_id с URL
                if not idents.get("internal_id"):
                    idents["internal_id"] = val
                continue
            # DOI и прочее — JATS предпочтительнее, если есть
            if key == "doi" and val:
                idents["doi"] = val
                continue
            if val is not None and (not idents.get(key)):
                idents[key] = val

    pages_sources = article_data.get("pages_sources")
    if not isinstance(pages_sources, dict):
        pages_sources = {}
    if xml_parsed.get("pages_jats"):
        pages_sources["jats"] = xml_parsed["pages_jats"]
    article_data["pages_sources"] = pages_sources
    # Итоговые страницы: JATS → HTML → …
    issue_validators.enrich_article_pages(article_data)

    if xml_parsed.get("article_type") is not None:
        article_data["article_type"] = xml_parsed["article_type"]

    pub_date = xml_parsed.get("publication_date")
    if isinstance(pub_date, str) and pub_date.strip():
        article_data["publication_date"] = pub_date.strip()
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", pub_date.strip())
        if m:
            article_data["publication_date_display"] = f"{m.group(3)}.{m.group(2)}.{m.group(1)}"

    article_data["data_source"] = "jats"

    refs_lang = xml_parsed.get("references_by_lang")
    ru = en = unk = None
    if isinstance(refs_lang, dict):
        ru = refs_lang.get("ru") if isinstance(refs_lang.get("ru"), dict) else None
        en = refs_lang.get("en") if isinstance(refs_lang.get("en"), dict) else None
        unk = refs_lang.get("unk") if isinstance(refs_lang.get("unk"), dict) else None

        html_refs = article_data.get("references")
        if not isinstance(html_refs, list):
            html_refs = []
        html_count = int(article_data.get("references_count") or 0)
        html_mode = str(article_data.get("references_mode") or "single_list")

        jats_items = refs_lang.get("items")
        total_refs = None
        if refs_lang.get("total_refs") is not None:
            try:
                total_refs = int(refs_lang.get("total_refs") or 0)
            except (TypeError, ValueError):
                total_refs = None
        if total_refs is None and isinstance(jats_items, list):
            total_refs = len(jats_items)

        # Библиография для анализа — предпочтительно из JATS (<ref>).
        if isinstance(jats_items, list) and jats_items:
            if html_refs:
                article_data["references_html"] = html_refs
                article_data["references_html_count"] = html_count or len(html_refs)
            article_data["references"] = list(jats_items)
            article_data["references_count"] = int(total_refs or len(jats_items))
            article_data["references_source"] = "jats"
            article_data["reference_first"] = jats_items[0]
            article_data["reference_last"] = jats_items[-1]
        elif total_refs and not html_count:
            article_data["references_count"] = total_refs
            article_data["references_source"] = "jats"
        else:
            article_data["references_source"] = article_data.get("references_source") or "html"

        # Режим: два блока на публичной странице важны для UI-проверки;
        # иначе берём режим JATS (single_list / parallel_citations).
        jats_mode = refs_lang.get("mode")
        if html_mode == "parallel_blocks":
            article_data["references_mode"] = "parallel_blocks"
        elif isinstance(jats_mode, str) and jats_mode.strip():
            article_data["references_mode"] = jats_mode.strip()

        if html_count and total_refs and abs(html_count - int(total_refs)) > 0:
            article_data["references_jats_count"] = int(total_refs)
            article_data["references_html_count"] = html_count
            if abs(html_count - int(total_refs)) >= 2:
                article_data["references_count_mismatch_html_jats"] = True

        # Язык источников из xml:lang (не перезаписывать эвристикой по алфавиту)
        if refs_lang.get("lang_from_attr"):
            article_data["references_lang_source"] = "xml_lang"
            if isinstance(ru, dict):
                article_data["references_ru_count"] = int(ru.get("count") or 0)
            if isinstance(en, dict):
                article_data["references_en_count"] = int(en.get("count") or 0)
            if isinstance(unk, dict):
                article_data["references_unk_count"] = int(unk.get("count") or 0)
        elif article_data.get("references_source") == "jats":
            # Без xml:lang — счётчики из JATS-эвристики, recompute может уточнить по items
            if isinstance(ru, dict):
                article_data["references_ru_count"] = int(ru.get("count") or 0)
            if isinstance(en, dict):
                article_data["references_en_count"] = int(en.get("count") or 0)
            if isinstance(unk, dict):
                article_data["references_unk_count"] = int(unk.get("count") or 0)

    if isinstance(ru, dict) and not article_data.get("reference_ru_first"):
        article_data["reference_ru_first"] = ru.get("first")
        article_data["reference_ru_last"] = ru.get("last")
    if isinstance(en, dict) and not article_data.get("reference_en_first"):
        article_data["reference_en_first"] = en.get("first")
        article_data["reference_en_last"] = en.get("last")
    if isinstance(unk, dict) and not article_data.get("reference_unk_first"):
        article_data["reference_unk_first"] = unk.get("first")
        article_data["reference_unk_last"] = unk.get("last")

