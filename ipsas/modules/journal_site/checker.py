"""Проверка заполненности полей сайта журнала."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

from ipsas.modules.issue_metadata.http_client import HttpClient
from ipsas.modules.journal_site.criteria import (
    CATEGORY_TITLES,
    STANDARD_PAGES,
    CriterionSpec,
)
from ipsas.modules.journal_site.evaluate import (
    CriterionResult,
    JournalContext,
    PageBundle,
    detect_context,
    evaluate_all,
    summarize_results,
)
from ipsas.modules.journal_site.fields import AboutMenuItem
from ipsas.modules.journal_site.locale_fetch import LOCALE_LABELS, fetch_bytes_for_locale
from ipsas.modules.journal_site.parser import (
    content_for_about_item,
    extract_about_menu,
    journal_page_url,
    normalize_journal_base_url,
    page_main_text,
    parse_html,
    section_body_text,
)
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

FetchFn = Callable[[str, str], bytes]

LEVEL_LABELS = {
    "high": "хорошо заполнен",
    "medium": "средняя заполненность",
    "low": "низкая заполненность",
}


@dataclass(slots=True)
class LocaleReport:
    lang: str
    label: str
    locale_code: str
    journal_title: str
    fields: list[CriterionResult] = field(default_factory=list)
    pages: dict[str, dict[str, Any]] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)
    completeness_percent: float = 0.0
    quality_percent: float = 0.0
    level: str = "low"
    filled_count: int = 0
    weak_count: int = 0
    empty_count: int = 0
    missing_count: int = 0
    error_count: int = 0

    def by_category(self) -> list[dict[str, Any]]:
        groups: dict[str, list[CriterionResult]] = {}
        for item in self.fields:
            if item.status == "not_applicable":
                continue
            groups.setdefault(item.category, []).append(item)
        out: list[dict[str, Any]] = []
        for cat_id, title in CATEGORY_TITLES.items():
            items = groups.get(cat_id) or []
            if not items:
                continue
            # в сводку разделов — только base (required/conditional)
            base_items = [f for f in items if f.requirement in {"required", "conditional"}]
            use = base_items or items
            max_sum = sum(f.max_score for f in use) or 1
            score_sum = sum(f.score for f in use)
            pct = round(100.0 * score_sum / max_sum, 1)
            out.append(
                {
                    "id": cat_id,
                    "title": title,
                    "completeness_percent": pct,
                    "filled": sum(1 for f in use if f.status in {"complete", "partial"}),
                    "total": len(use),
                    "fields": [f.to_dict() for f in items],
                }
            )
        return out

    def by_requirement(self) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = {
            "required": [],
            "conditional": [],
            "optional": [],
        }
        for item in self.fields:
            groups.setdefault(item.requirement, []).append(item.to_dict())
        return groups

    def to_dict(self) -> dict[str, Any]:
        return {
            "lang": self.lang,
            "label": self.label,
            "locale_code": self.locale_code,
            "journal_title": self.journal_title,
            "completeness_percent": self.completeness_percent,
            "quality_percent": self.quality_percent,
            "level": self.level,
            "level_label": LEVEL_LABELS.get(self.level, self.level),
            "context": self.context,
            "summary": self.summary,
            "filled_count": self.filled_count,
            "weak_count": self.weak_count,
            "empty_count": self.empty_count,
            "missing_count": self.missing_count,
            "error_count": self.error_count,
            "fields": [f.to_dict() for f in self.fields],
            "pages": self.pages,
            "by_category": self.by_category(),
            "by_requirement": self.by_requirement(),
            "must_fix": self.summary.get("must_fix") or [],
            "recommendations": self.summary.get("recommendations") or [],
            "required_filled": self.summary.get("required_filled") or 0,
            "required_total": self.summary.get("required_total") or 0,
        }


@dataclass(slots=True)
class JournalSiteReport:
    journal_url: str
    base_url: str
    generated_at: str
    journal_title: str
    locales: dict[str, LocaleReport] = field(default_factory=dict)
    completeness_percent: float = 0.0
    completeness_ru: float = 0.0
    completeness_en: float = 0.0
    quality_percent: float = 0.0
    level: str = "low"

    def to_dict(self) -> dict[str, Any]:
        locales_dict = {k: v.to_dict() for k, v in self.locales.items()}
        primary = self.locales.get("ru") or next(iter(self.locales.values()), None)
        return {
            "journal_url": self.journal_url,
            "base_url": self.base_url,
            "generated_at": self.generated_at,
            "journal_title": self.journal_title,
            "completeness_percent": self.completeness_percent,
            "completeness_ru": self.completeness_ru,
            "completeness_en": self.completeness_en,
            "quality_percent": self.quality_percent,
            "level": self.level,
            "level_label": LEVEL_LABELS.get(self.level, self.level),
            "locales": locales_dict,
            "fields": [f.to_dict() for f in primary.fields] if primary else [],
            "pages": primary.pages if primary else {},
            "by_category": primary.by_category() if primary else [],
            "by_requirement": primary.by_requirement() if primary else {},
            "must_fix": primary.summary.get("must_fix") if primary else [],
            "recommendations": primary.summary.get("recommendations") if primary else [],
            "required_filled": primary.summary.get("required_filled") if primary else 0,
            "required_total": primary.summary.get("required_total") if primary else 0,
            "summary": primary.summary if primary else {},
            "filled_count": primary.filled_count if primary else 0,
            "weak_count": primary.weak_count if primary else 0,
            "empty_count": primary.empty_count if primary else 0,
            "missing_count": primary.missing_count if primary else 0,
            "error_count": primary.error_count if primary else 0,
        }


def _extract_known_fragments(doc: Any) -> dict[str, str]:
    ids = (
        "focusAndScope",
        "peerReviewProcess",
        "publicationFrequency",
        "openAccessPolicy",
        "onlineSubmissions",
        "authorGuidelines",
        "copyrightNotice",
        "privacyStatement",
    )
    out: dict[str, str] = {}
    for fid in ids:
        body = section_body_text(doc, fid)
        if body:
            out[fid] = body
    # custom ethics blocks
    for node in doc.xpath("//*[starts-with(@id,'custom-')]"):
        nid = node.get("id") or ""
        text = section_body_text(doc, nid)
        if text:
            out[nid] = text
    return out


class JournalSiteChecker:
    """Скачивает страницы журнала (RU + EN) и оценивает критерии заполненности."""

    def __init__(
        self,
        *,
        http: HttpClient | None = None,
        fetch: FetchFn | None = None,
        max_bytes: int = 2_500_000,
        languages: tuple[str, ...] = ("ru", "en"),
    ) -> None:
        self._http = http or HttpClient.from_env(max_bytes=max_bytes)
        self._fetch = fetch
        self._languages = languages

    def _get(self, url: str, lang: str) -> tuple[bytes, str]:
        if self._fetch is not None:
            return self._fetch(url, lang), lang
        return fetch_bytes_for_locale(self._http, url, lang=lang)

    def _fetch_page(
        self, url: str, lang: str
    ) -> tuple[Optional[Any], Optional[str], str]:
        try:
            raw, locale_code = self._get(url, lang)
            return parse_html(raw), None, locale_code
        except Exception as e:
            logger.warning("Не удалось загрузить %s [%s]: %s", url, lang, e)
            return None, str(e), lang

    def _build_bundle(self, base: str, lang: str) -> tuple[PageBundle, str, str]:
        bundle = PageBundle()
        used_locale = lang
        journal_title = ""

        for key, rel in STANDARD_PAGES:
            url = journal_page_url(base, *rel.split("/"))
            doc, err, code = self._fetch_page(url, lang)
            used_locale = code or used_locale
            bundle.urls[key] = url
            if doc is None:
                if err:
                    bundle.errors[key] = err
                continue
            bundle.docs[key] = doc
            bundle.texts[key] = page_main_text(doc)
            bundle.fragments.update(_extract_known_fragments(doc))

        # Дополнить корпус пунктами меню /about (custom-политики и т.п.)
        about_doc = bundle.docs.get("about")
        if about_doc is not None:
            menu: list[AboutMenuItem] = extract_about_menu(about_doc, base_url=base)
            for item in menu:
                # уже загруженная страница
                page_doc = None
                for key, doc in bundle.docs.items():
                    if bundle.urls.get(key) == item.page_url:
                        page_doc = doc
                        break
                if page_doc is None:
                    doc, err, _ = self._fetch_page(item.page_url, lang)
                    if doc is None:
                        continue
                    page_doc = doc
                    # кэш по хвосту пути
                    tail = item.page_url.rstrip("/").split("/")[-1]
                    if tail and tail not in bundle.docs:
                        bundle.docs[tail] = doc
                        bundle.texts[tail] = page_main_text(doc)
                        bundle.urls[tail] = item.page_url
                        bundle.fragments.update(_extract_known_fragments(doc))
                value = content_for_about_item(page_doc, item)
                if item.fragment and value:
                    bundle.fragments[item.fragment] = value

        home = bundle.docs.get("homepage")
        if home is not None:
            from ipsas.modules.journal_site.parser import extract_journal_title

            journal_title = extract_journal_title(home)

        return bundle, used_locale, journal_title

    def _finalize_locale(
        self,
        *,
        lang: str,
        locale_code: str,
        journal_title: str,
        results: list[CriterionResult],
        pages_meta: dict[str, dict[str, Any]],
        context: JournalContext,
    ) -> LocaleReport:
        summary = summarize_results(results)
        return LocaleReport(
            lang=lang,
            label=LOCALE_LABELS.get(lang, lang),
            locale_code=locale_code,
            journal_title=journal_title,
            fields=results,
            pages=pages_meta,
            context={
                "access_model": context.access_model,
                "flags": dict(context.flags),
                "notes": list(context.notes),
            },
            summary=summary,
            completeness_percent=float(summary["completeness_percent"]),
            quality_percent=float(summary["quality_percent"]),
            level=str(summary["level"]),
            filled_count=int(summary["complete_count"]),
            weak_count=int(summary["partial_count"]) + int(summary["formal_count"]),
            empty_count=0,
            missing_count=int(summary["missing_count"]),
            error_count=int(summary["error_count"]) + int(summary["conflict_count"]),
        )

    def _pages_meta(self, bundle: PageBundle) -> dict[str, dict[str, Any]]:
        meta: dict[str, dict[str, Any]] = {}
        for key, url in bundle.urls.items():
            meta[key] = {
                "url": url,
                "ok": key in bundle.docs,
                "error": bundle.errors.get(key),
                "locale": "",
            }
        return meta

    def _check_locale(
        self,
        base: str,
        lang: str,
        *,
        en_bundle: Optional[PageBundle] = None,
        prebuilt: Optional[tuple[PageBundle, str, str]] = None,
    ) -> tuple[LocaleReport, PageBundle]:
        if prebuilt is not None:
            bundle, used_locale, journal_title = prebuilt
        else:
            bundle, used_locale, journal_title = self._build_bundle(base, lang)

        ctx = detect_context(bundle, has_english_locale=True)
        results = evaluate_all(bundle, ctx, en_bundle=en_bundle)
        report = self._finalize_locale(
            lang=lang,
            locale_code=used_locale,
            journal_title=journal_title,
            results=results,
            pages_meta=self._pages_meta(bundle),
            context=ctx,
        )
        return report, bundle

    def check(self, journal_url: str) -> JournalSiteReport:
        base = normalize_journal_base_url(journal_url)
        locales: dict[str, LocaleReport] = {}

        bundles: dict[str, PageBundle] = {}
        built: dict[str, tuple[PageBundle, str, str]] = {}
        for lang in self._languages:
            built[lang] = self._build_bundle(base, lang)
            bundles[lang] = built[lang][0]

        en_bundle = bundles.get("en")
        for lang in self._languages:
            loc_report, _ = self._check_locale(
                base,
                lang,
                en_bundle=en_bundle,
                prebuilt=built[lang],
            )
            locales[lang] = loc_report

        ru = locales.get("ru")
        en = locales.get("en")
        pcts = [loc.completeness_percent for loc in locales.values()]
        overall = round(sum(pcts) / len(pcts), 1) if pcts else 0.0
        q_pcts = [loc.quality_percent for loc in locales.values()]
        quality = round(sum(q_pcts) / len(q_pcts), 1) if q_pcts else 0.0
        title = (ru.journal_title if ru and ru.journal_title else "") or (
            en.journal_title if en and en.journal_title else ""
        )

        # итоговый уровень — худший из локалей
        level_rank = {"high": 3, "medium": 2, "low": 1}
        level = "high"
        for loc in locales.values():
            if level_rank.get(loc.level, 0) < level_rank.get(level, 0):
                level = loc.level

        return JournalSiteReport(
            journal_url=journal_url,
            base_url=base,
            generated_at=datetime.now().strftime("%d.%m.%Y %H:%M"),
            journal_title=title,
            locales=locales,
            completeness_percent=overall,
            completeness_ru=ru.completeness_percent if ru else 0.0,
            completeness_en=en.completeness_percent if en else 0.0,
            quality_percent=quality,
            level=level,
        )


# Обратная совместимость типов
FieldResult = CriterionResult
FieldSpec = CriterionSpec
