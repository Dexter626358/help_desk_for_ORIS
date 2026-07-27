"""Вспомогательные структуры для HTML-отчёта по выпуску."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, List, Optional

from ipsas.modules.issue_metadata.rules import (
    CATEGORIES,
    CATEGORY_TITLES,
    categories_for_report,
    resolve_category,
)


def group_findings(
    issue_warnings: Optional[List[Dict[str, object]]],
    articles: Optional[List[Dict[str, object]]],
) -> Dict[str, object]:
    """
    Сгруппировать одинаковые замечания и разложить по 7 категориям.

    Возвращает:
      {
        "errors": [{"text", "count", "articles", "issue_level", "category", "rule_id"}],
        "warnings": [...],
        "by_category": [
          {"id", "title", "description", "errors", "warnings",
           "error_count", "warning_count", "total"}
        ],
        "rules_catalog": [...],
      }
    """
    errors = _group_bucket(issue_warnings, articles, severity="error")
    warnings = _group_bucket(issue_warnings, articles, severity="warning")
    by_category = _build_by_category(errors, warnings)
    return {
        "errors": errors,
        "warnings": warnings,
        "by_category": by_category,
        "rules_catalog": categories_for_report(),
        "rules_total": sum(int(c.get("rules_count") or 0) for c in categories_for_report()),
    }


def _group_bucket(
    issue_warnings: Optional[List[Dict[str, object]]],
    articles: Optional[List[Dict[str, object]]],
    *,
    severity: str,
) -> List[Dict[str, object]]:
    grouped: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def add(
        text: str,
        article_no: Optional[int],
        issue_level: bool,
        *,
        category: Optional[str] = None,
        rule_id: Optional[str] = None,
        field: Optional[str] = None,
    ) -> None:
        key = text.strip()
        if not key:
            return
        cat = category or resolve_category(field=field, text=key, rule_id=rule_id)
        item = grouped.get(key)
        if item is None:
            item = {
                "text": key,
                "count": 0,
                "articles": [],
                "issue_level": False,
                "category": cat,
                "rule_id": rule_id,
                "field": field,
            }
            grouped[key] = item
        item["count"] = int(item["count"]) + 1
        if issue_level:
            item["issue_level"] = True
        if not item.get("category"):
            item["category"] = cat
        if rule_id and not item.get("rule_id"):
            item["rule_id"] = rule_id
        if article_no is not None and article_no not in item["articles"]:
            item["articles"].append(article_no)

    for w in issue_warnings or []:
        if not isinstance(w, dict):
            continue
        if (w.get("severity") or "warning") != severity:
            continue
        add(
            str(w.get("text") or ""),
            None,
            True,
            category=str(w["category"]) if w.get("category") else None,
            rule_id=str(w["rule_id"]) if w.get("rule_id") else None,
            field=str(w["field"]) if w.get("field") else None,
        )

    for idx, article in enumerate(articles or [], start=1):
        if not isinstance(article, dict):
            continue
        structured = article.get("issues")
        used_texts: set[str] = set()
        if isinstance(structured, list) and structured:
            for issue in structured:
                if not isinstance(issue, dict):
                    continue
                if (issue.get("severity") or "warning") != severity:
                    continue
                text = str(issue.get("text") or "")
                used_texts.add(text.strip())
                add(
                    text,
                    idx,
                    False,
                    category=str(issue["category"]) if issue.get("category") else None,
                    rule_id=str(issue["rule_id"]) if issue.get("rule_id") else None,
                    field=str(issue["field"]) if issue.get("field") else None,
                )
            continue

        # Fallback: старые строковые errors/problems без категории
        field_name = "errors" if severity == "error" else "problems"
        for msg in article.get(field_name) or []:
            text = str(msg)
            if text.strip() in used_texts:
                continue
            add(text, idx, False)

    return list(grouped.values())


def _build_by_category(
    errors: List[Dict[str, object]],
    warnings: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    by_cat: List[Dict[str, object]] = []
    for cat in CATEGORIES:
        cid = cat["id"]
        cat_errors = [g for g in errors if (g.get("category") or "texts") == cid]
        cat_warnings = [g for g in warnings if (g.get("category") or "texts") == cid]
        err_n = sum(int(g.get("count") or 1) for g in cat_errors)
        warn_n = sum(int(g.get("count") or 1) for g in cat_warnings)
        by_cat.append(
            {
                "id": cid,
                "title": cat["title"],
                "description": cat["description"],
                "errors": cat_errors,
                "warnings": cat_warnings,
                "error_count": err_n,
                "warning_count": warn_n,
                "total": err_n + warn_n,
            }
        )
    return by_cat


def category_title(category_id: Optional[str]) -> str:
    if not category_id:
        return CATEGORY_TITLES.get("texts", "Прочее")
    return CATEGORY_TITLES.get(str(category_id), str(category_id))
