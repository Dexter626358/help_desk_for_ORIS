"""Сборка update_payload по PLATFORM_JSON_MAPPING.md."""

from __future__ import annotations

from typing import Any

from ipsas.config.settings import get_settings


def build_update_payload(
    *,
    article_id: str,
    data: dict[str, Any],
    approved: bool = True,
) -> dict[str, Any]:
    """Плоский ENG JSON → структура, совместимая с PLATFORM_JSON_MAPPING."""
    changes: list[dict[str, Any]] = []

    title = str(data.get("title_eng") or "").strip()
    if title:
        changes.append(
            {
                "field": "article.titles.en",
                "value": title,
                "source": "eng_json",
            }
        )

    abstract = str(data.get("abstract_eng") or "").strip()
    if abstract:
        changes.append(
            {
                "field": "article.abstracts.en",
                "value": abstract,
                "source": "eng_json",
            }
        )

    keywords = data.get("keywords_eng") or []
    if isinstance(keywords, list) and keywords:
        changes.append(
            {
                "field": "article.keywords.en",
                "value": "; ".join(str(k).strip() for k in keywords if str(k).strip()),
                "source": "eng_json",
            }
        )

    refs = data.get("references_eng") or []
    if isinstance(refs, list):
        for idx, ref in enumerate(refs):
            text = str(ref).strip()
            if text:
                changes.append(
                    {
                        "field": f"references[{idx}].text.en",
                        "value": text,
                        "source": "eng_json",
                    }
                )

    authors = data.get("authors") if isinstance(data.get("authors"), list) else []
    for idx, author in enumerate(authors):
        if not isinstance(author, dict):
            continue
        # orcid — protected, на apply не уходит (см. PLATFORM_JSON_MAPPING)
        for key in ("given_en", "surname_en", "full_name_en", "email"):
            val = author.get(key)
            if val:
                changes.append(
                    {
                        "field": f"authors[{idx}].{key}",
                        "value": val,
                        "source": "eng_json",
                    }
                )
        affs = author.get("affiliations") if isinstance(author.get("affiliations"), list) else []
        for j, aff in enumerate(affs):
            if not isinstance(aff, dict):
                continue
            for akey in ("organization_en", "address_en"):
                aval = aff.get(akey)
                if aval:
                    changes.append(
                        {
                            "field": f"authors[{idx}].affiliations[{j}].{akey}",
                            "value": aval,
                            "source": "eng_json",
                        }
                    )

    dates = data.get("dates") if isinstance(data.get("dates"), dict) else {}
    # revised в auto-apply нет (только received/accepted)
    for dkey, field in (
        ("received", "dates.received"),
        ("accepted", "dates.accepted"),
    ):
        val = dates.get(dkey)
        if val:
            changes.append({"field": field, "value": val, "source": "eng_json"})

    settings = get_settings()
    apply_on = bool(settings.platform_apply_enabled)
    return {
        "article_id": article_id,
        "article_url": data.get("article_url"),
        "approved": approved,
        "platform_apply_enabled": apply_on,
        "dry_run": not apply_on,
        "changes": changes,
        "source_metadata": data,
        "note": (
            "Готово к отправке на платформу."
            if apply_on
            else "PLATFORM_APPLY_ENABLED выключен; payload сохранён локально (dry-run)."
        ),
    }


def summarize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    changes = payload.get("changes") if isinstance(payload.get("changes"), list) else []
    return {
        "article_id": payload.get("article_id"),
        "changes_count": len(changes),
        "fields": [c.get("field") for c in changes if isinstance(c, dict)],
        "approved": bool(payload.get("approved")),
        "dry_run": bool(payload.get("dry_run", True)),
        "platform_apply_enabled": bool(payload.get("platform_apply_enabled")),
        "note": payload.get("note"),
    }
