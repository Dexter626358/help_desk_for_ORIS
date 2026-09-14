"""Преобразование HTML-формы ↔ flat ENG JSON."""

from __future__ import annotations

from typing import Any

from werkzeug.datastructures import MultiDict


def metadata_to_form_defaults(data: dict[str, Any]) -> dict[str, Any]:
    """Значения для шаблона формы."""
    keywords = data.get("keywords_eng") or []
    if isinstance(keywords, list):
        keywords_text = "\n".join(str(k) for k in keywords)
    else:
        keywords_text = str(keywords)

    refs_raw = data.get("references_eng") or []
    if isinstance(refs_raw, list):
        references = [str(r) for r in refs_raw]
    elif isinstance(refs_raw, str) and refs_raw.strip():
        references = _split_lines(refs_raw)
    else:
        references = []

    dates = data.get("dates") if isinstance(data.get("dates"), dict) else {}
    authors = data.get("authors") if isinstance(data.get("authors"), list) else []

    return {
        "title_eng": str(data.get("title_eng") or ""),
        "title_ru": str(data.get("title_ru") or ""),
        "abstract_eng": str(data.get("abstract_eng") or ""),
        "abstract_ru": str(data.get("abstract_ru") or ""),
        "keywords_eng": keywords_text,
        "references_eng": references,
        "article_url": str(data.get("article_url") or ""),
        "date_received": str(dates.get("received") or ""),
        "date_revised": str(dates.get("revised") or ""),
        "date_accepted": str(dates.get("accepted") or ""),
        "authors": authors,
    }


def _split_lines(value: str) -> list[str]:
    return [line.strip() for line in (value or "").splitlines() if line.strip()]


def _collect_references(
    form: MultiDict[str, str],
    base: dict[str, Any],
) -> list[str]:
    """Собрать литературу из отдельных полей ref_value_{i} (или legacy textarea)."""
    base_refs = base.get("references_eng") or []
    if not isinstance(base_refs, list):
        base_refs = _split_lines(str(base_refs)) if base_refs else []

    count = len(base_refs)
    try:
        count = max(count, int(form.get("references_count") or count))
    except ValueError:
        pass

    # Indexed fields from the review UI
    has_indexed = any(form.get(f"ref_value_{i}") is not None for i in range(max(count, 1)))
    if has_indexed or form.get("references_count") is not None:
        refs: list[str] = []
        for i in range(count):
            raw = form.get(f"ref_value_{i}")
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                refs.append(text)
        return refs

    # Backward compatibility: one textarea, one line = one source
    return _split_lines(form.get("references_eng") or "")


def apply_form_to_metadata(
    base: dict[str, Any],
    form: MultiDict[str, str],
) -> dict[str, Any]:
    """Собрать обновлённый JSON из полей формы сверки."""
    data = dict(base)
    data["title_eng"] = (form.get("title_eng") or "").strip()
    data["abstract_eng"] = (form.get("abstract_eng") or "").strip()
    data["keywords_eng"] = _split_lines(form.get("keywords_eng") or "")
    data["references_eng"] = _collect_references(form, base)
    # article_url — справочно в шапке, из формы не меняем

    data["dates"] = {
        "received": (form.get("date_received") or "").strip() or None,
        "revised": (form.get("date_revised") or "").strip() or None,
        "accepted": (form.get("date_accepted") or "").strip() or None,
    }

    authors_in = base.get("authors") if isinstance(base.get("authors"), list) else []
    count = max(len(authors_in), 0)
    try:
        count = max(count, int(form.get("authors_count") or count))
    except ValueError:
        pass

    authors: list[dict[str, Any]] = []
    for i in range(count):
        prefix = f"author_{i}_"
        base_author = (
            authors_in[i] if i < len(authors_in) and isinstance(authors_in[i], dict) else {}
        )
        base_affs = (
            base_author.get("affiliations")
            if isinstance(base_author.get("affiliations"), list)
            else []
        )
        base_aff0 = base_affs[0] if base_affs and isinstance(base_affs[0], dict) else {}

        given = (form.get(prefix + "given_en") or "").strip()
        surname = (form.get(prefix + "surname_en") or "").strip()
        full = (form.get(prefix + "full_name_en") or "").strip()
        if not full and (given or surname):
            full = f"{given} {surname}".strip()
        org = (form.get(prefix + "org_en") or "").strip()
        addr = (form.get(prefix + "address_en") or "").strip()
        email = (form.get(prefix + "email") or "").strip() or None
        orcid = (form.get(prefix + "orcid") or "").strip() or None
        affiliations: list[dict[str, Any]] = []
        if org or addr or base_aff0.get("organization_ru"):
            affiliations.append(
                {
                    "organization_en": org or None,
                    "organization_ru": base_aff0.get("organization_ru"),
                    "address_en": addr or None,
                }
            )
        if not any([given, surname, full, org, addr, email, orcid]):
            if base_author:
                authors.append(dict(base_author))
            continue
        authors.append(
            {
                "full_name_en": full or None,
                "given_en": given or None,
                "surname_en": surname or None,
                "full_name_ru": base_author.get("full_name_ru"),
                "given_ru": base_author.get("given_ru"),
                "surname_ru": base_author.get("surname_ru"),
                "email": email,
                "orcid": orcid,
                "affiliations": affiliations,
            }
        )
    data["authors"] = authors
    return data
