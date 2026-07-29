"""Оценка критериев заполненности сайта журнала."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from ipsas.modules.journal_site.criteria import (
    COND_ACCEPTS_ADS,
    COND_ANIMAL_RESEARCH,
    COND_CLINICAL,
    COND_DELAYED_OA,
    COND_FEES_CHARGED,
    COND_HAS_EN,
    COND_HAS_SPONSORS,
    COND_HAS_WAIVER,
    COND_HUMAN_RESEARCH,
    COND_HYBRID,
    COND_ISSN_CHANGED,
    COND_MEDIA_REGISTERED,
    COND_NAME_CHANGED,
    COND_OPEN_ACCESS,
    COND_PATIENT_IMAGES,
    COND_PRINT_AND_ONLINE,
    COND_SUBSCRIPTION_LIKE,
    CRITERIA,
    PAGE_KEY_BY_PATH,
    PAGE_TITLES,
    AccessModel,
    CheckStatus,
    CriterionSpec,
)
from ipsas.modules.journal_site.parser import (
    extract_contact_fields,
    extract_contact_map,
    extract_contact_persons,
    extract_current_issue,
    extract_description,
    extract_editorial_members,
    extract_editorial_team_structure,
    extract_homepage_image,
    extract_homepage_meta,
    extract_issn,
    extract_journal_title,
    norm_space,
    page_main_text,
    preview_value,
    resolve_policy_section_body,
    section_body_text,
    _member_entry,
)

_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
_ISSN_RE = re.compile(r"\b\d{4}-\d{3}[\dXx]\b")
_PHONE_RE = re.compile(r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")

PLATFORM_EMAILS = frozenset(
    {
        "info@rcsi.science",
        "journals_support@rcsi.science",
        "support@rcsi.science",
    }
)

_AFFIL_MARKERS = (
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
)


@dataclass(slots=True)
class PageBundle:
    """Загруженные страницы одной локали."""

    docs: dict[str, Any] = field(default_factory=dict)
    texts: dict[str, str] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    urls: dict[str, str] = field(default_factory=dict)
    fragments: dict[str, str] = field(default_factory=dict)

    def text_for_page(self, page_path: str) -> str:
        key = PAGE_KEY_BY_PATH.get(page_path, "")
        parts: list[str] = []
        if key and self.texts.get(key):
            parts.append(self.texts[key])
        # политики/подача: фрагменты about, без главной (там аннотации выпуска)
        if page_path.endswith("editorialPolicies"):
            parts.append(self.texts.get("about", ""))
            for frag, body in self.fragments.items():
                if frag:
                    parts.append(body)
        if page_path.endswith("submissions"):
            parts.append(self.texts.get("about", ""))
        if page_path == "/index":
            parts.append(self.texts.get("homepage", ""))
        return norm_space(" ".join(p for p in parts if p))

    def combined_text(self) -> str:
        return norm_space(" ".join(self.texts.values()) + " " + " ".join(self.fragments.values()))


@dataclass(slots=True)
class JournalContext:
    access_model: AccessModel = "unknown"
    flags: dict[str, bool] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def is_applicable(self, condition: str | None) -> bool:
        if not condition:
            return True
        return bool(self.flags.get(condition))


@dataclass(slots=True)
class CriterionResult:
    id: str
    title: str
    category: str
    page: str
    page_label: str
    requirement: str
    applicable: bool
    status: CheckStatus
    score: int
    max_score: int
    critical: bool
    chars: int = 0
    value_preview: str = ""
    source_url: str = ""
    note: str = ""
    weight: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "category": self.category,
            "page": self.page,
            "page_label": self.page_label,
            "requirement": self.requirement,
            "applicable": self.applicable,
            "status": self.status,
            "score": self.score,
            "max_score": self.max_score,
            "critical": self.critical,
            "chars": self.chars,
            "value_preview": self.value_preview,
            "source_url": self.source_url,
            "note": self.note,
            "weight": self.weight,
        }


def _hit_count(text: str, keywords: tuple[str, ...]) -> int:
    low = text.lower()
    return sum(1 for kw in keywords if kw.lower() in low)


def _keyword_snippets(text: str, keywords: tuple[str, ...], window: int = 280) -> str:
    """Фрагменты вокруг ключевых слов — чтобы не оценивать всю страницу выпуска."""
    if not text or not keywords:
        return norm_space(text)
    low = text.lower()
    chunks: list[str] = []
    seen_idx: set[int] = set()
    for kw in keywords:
        start = 0
        needle = kw.lower()
        while True:
            idx = low.find(needle, start)
            if idx < 0:
                break
            # дедуп близких попаданий
            bucket = idx // 120
            if bucket not in seen_idx:
                seen_idx.add(bucket)
                chunks.append(text[max(0, idx - 60) : idx + len(kw) + window])
            start = idx + len(needle)
            if len(chunks) >= 6:
                break
        if len(chunks) >= 6:
            break
    return norm_space(" ".join(chunks))


def _pick_snippet(text: str, keywords: tuple[str, ...], limit: int = 180) -> str:
    low = text.lower()
    for kw in keywords:
        idx = low.find(kw.lower())
        if idx >= 0:
            start = max(0, idx - 40)
            return preview_value(text[start : start + limit + 40], limit=limit)
    return preview_value(text, limit=limit) if text else ""


def _status_from_text(
    text: str,
    spec: CriterionSpec,
    *,
    keyword_hits: int | None = None,
) -> tuple[CheckStatus, int, str]:
    body = norm_space(text)
    if spec.keywords:
        evidence = _keyword_snippets(body, spec.keywords)
        hits = keyword_hits if keyword_hits is not None else _hit_count(body, spec.keywords)
        if hits <= 0 or not evidence:
            return "missing", 0, ""
        n = len(evidence)
        preview = _pick_snippet(body, spec.keywords)
        if n >= spec.min_chars_complete and hits >= 1:
            return "complete", spec.max_score, preview
        if n >= spec.min_chars_partial or hits >= 2:
            return "partial", max(1, spec.max_score - 1), preview
        return "formal", 1, preview

    n = len(body)
    if n <= 0:
        return "missing", 0, ""
    preview = preview_value(body)
    if n >= spec.min_chars_complete:
        return "complete", spec.max_score, preview
    if n >= spec.min_chars_partial:
        return "partial", max(1, spec.max_score - 1), preview
    if n >= spec.min_chars_formal:
        return "formal", 1, preview
    return "missing", 0, preview


def _policy_section_body(
    bundle: PageBundle, spec: CriterionSpec
) -> tuple[str, str, str]:
    """Текст раздела, id якоря, URL страницы-источника."""
    page_key = PAGE_KEY_BY_PATH.get(spec.page, "")
    doc = bundle.docs.get(page_key)
    body, frag = resolve_policy_section_body(
        doc,
        fragment_ids=spec.ojs_fragments,
        title_aliases=spec.section_aliases,
    )
    source_url = bundle.urls.get(page_key, "")
    if frag and source_url and frag != "__page_h1__":
        source_url = source_url.split("#")[0] + f"#{frag}"

    if spec.id == "policy.personal_data" and not frag:
        sub_doc = bundle.docs.get("submissions")
        if sub_doc is not None:
            sub_nodes = sub_doc.xpath("//*[@id='privacyStatement']")
            if sub_nodes:
                body = section_body_text(sub_doc, "privacyStatement")
                frag = "privacyStatement"
                sub_url = bundle.urls.get("submissions", source_url)
                source_url = sub_url.split("#")[0] + "#privacyStatement"

    return body, frag, source_url


def _gather_criterion_text(bundle: PageBundle, spec: CriterionSpec) -> tuple[str, str]:
    """Текст и URL источника для критерия."""
    page_key = PAGE_KEY_BY_PATH.get(spec.page, "")
    source = bundle.urls.get(page_key, "")
    chunks: list[str] = []

    if spec.section_aliases or spec.ojs_fragments:
        sec_body, frag, sec_url = _policy_section_body(bundle, spec)
        if sec_body:
            chunks.append(sec_body)
            source = sec_url or source
        elif frag:
            source = sec_url or source

    for frag in spec.ojs_fragments:
        body = bundle.fragments.get(frag) or ""
        if not body and page_key in bundle.docs:
            body = section_body_text(bundle.docs[page_key], frag)
            if body:
                bundle.fragments[frag] = body
        if body and body not in chunks:
            chunks.append(body)
            source = (bundle.urls.get(page_key, "") + f"#{frag}") if page_key else source

    page_text = bundle.text_for_page(spec.page)
    if page_text:
        chunks.append(page_text)

    # Не подмешиваем главную с аннотациями статей — только целевые about-страницы
    if spec.category in {"ownership"} and len(norm_space(" ".join(chunks))) < 40:
        chunks.append(bundle.texts.get("journalSponsorship", ""))
        chunks.append(bundle.texts.get("about", ""))
        home = bundle.docs.get("homepage")
        if home is not None:
            meta = extract_homepage_meta(home)
            chunks.extend(meta.values())

    text = norm_space(" ".join(chunks))
    return text, source


def detect_access_model(text: str) -> AccessModel:
    low = text.lower()
    # «гибрид» без уточнения часто встречается в статьях (гибриды растений) — не используем
    if any(
        x in low
        for x in (
            "delayed open access",
            "отложенный открытый",
            "embargo period",
            "срок эмбарго",
        )
    ):
        return "delayed_open_access"
    if any(
        x in low
        for x in (
            "hybrid open access",
            "hybrid journal",
            "гибридный журнал",
            "гибридная модель",
            "hybrid model",
        )
    ):
        return "hybrid"
    if any(
        x in low
        for x in (
            "open access",
            "открытый доступ",
            "cc by",
            "creative commons",
            "свободный открытый доступ",
        )
    ):
        return "open_access"
    if any(
        x in low
        for x in (
            "subscription-based",
            "subscription journal",
            "подписной журнал",
            "доступ по подписке",
            "только по подписке",
        )
    ):
        return "subscription"
    # короткие подсказки с главной: «Open», «Открытый»
    hint = low.strip()
    if hint in {"open", "открытый", "открытый доступ"}:
        return "open_access"
    if hint in {"subscription", "подписка", "подписной"}:
        return "subscription"
    return "unknown"


def _policy_scope_text(bundle: PageBundle) -> str:
    parts = [
        bundle.fragments.get("focusAndScope", ""),
        bundle.fragments.get("openAccessPolicy", ""),
        bundle.texts.get("editorialPolicies", ""),
        bundle.texts.get("journalSponsorship", ""),
        bundle.texts.get("submissions", ""),
    ]
    home = bundle.docs.get("homepage")
    if home is not None:
        meta = extract_homepage_meta(home)
        parts.extend(meta.values())
        parts.append(extract_description(home)[:1500])
    return norm_space(" ".join(parts))


def detect_context(bundle: PageBundle, *, has_english_locale: bool = True) -> JournalContext:
    scope = _policy_scope_text(bundle)
    access_corpus = norm_space(
        " ".join(
            [
                bundle.fragments.get("openAccessPolicy", ""),
                bundle.texts.get("editorialPolicies", "")[:5000],
                bundle.texts.get("subscriptions", "")[:2000],
            ]
        )
    )
    home_meta: dict[str, str] = {}
    home_doc = bundle.docs.get("homepage")
    if home_doc is not None:
        home_meta = extract_homepage_meta(home_doc)
        if home_meta.get("access_hint"):
            access_corpus = home_meta["access_hint"] + " " + access_corpus

    access = detect_access_model(access_corpus)
    low = scope.lower()

    issn_text = home_meta.get("issn") or ""
    if not issn_text and home_doc is not None:
        issn_text = extract_issn(home_doc)
    issn_low = issn_text.lower()
    print_online = (
        ("print" in issn_low or "печат" in issn_low)
        and ("online" in issn_low or "электрон" in issn_low or "eissn" in issn_low or "e-issn" in issn_low)
    ) or len(_ISSN_RE.findall(issn_text)) >= 2

    fees_charged = any(
        x in low
        for x in (
            "article processing charge",
            "apc",
            "плата за публикац",
            "публикационный сбор",
            "publication fee",
        )
    ) and not any(
        x in low
        for x in (
            "плата не взимается",
            "не взимается",
            "no fee",
            "free of charge",
            "без оплаты",
            "бесплатн",
        )
    )
    has_waiver = any(x in low for x in ("waiver", "льгот", "освобожден", "скидк", "discount"))
    media = bool(home_meta.get("media_certificate")) or any(
        x in low for x in ("свидетельств", "роскомнадзор", "сми №", "media registration")
    )
    name_changed = any(x in low for x in ("ранее называл", "formerly titled", "previous title", "прежнее название"))
    issn_changed = any(x in low for x in ("прежний issn", "former issn", "continues as", "преемственность issn"))
    sponsor_text = (bundle.texts.get("journalSponsorship") or "").lower()
    has_sponsors = any(x in sponsor_text for x in ("спонсор", "sponsor", "supported by", "при поддерж", "источники поддержки"))
    # отраслевые флаги — только по focusAndScope / краткому описанию, не по этике целиком
    scope_core = norm_space(
        " ".join(
            [
                bundle.fragments.get("focusAndScope", ""),
                (extract_description(home_doc) if home_doc is not None else "")[:1200],
                home_meta.get("frequency", ""),
            ]
        )
    ).lower()
    human = any(
        x in scope_core
        for x in (
            "медицин",
            "medicin",
            "психолог",
            "psycholog",
            "human subject",
            "исследован с участием людей",
            "клиническ медицин",
        )
    )
    animal = any(
        x in scope_core
        for x in (
            "зоолог",
            "ветеринар",
            "veterinary",
            "animal research",
            "животновод",
            "зоотехн",
        )
    )
    clinical = any(x in scope_core for x in ("клиническ исследован", "clinical trial", "clinical study"))
    patient_img = any(x in scope_core for x in ("согласие пациента", "patient consent", "изображен пациента"))
    ads = any(x in low for x in ("рекламная политика", "advertising policy", "размещает реклам"))

    flags = {
        COND_OPEN_ACCESS: access == "open_access",
        COND_HYBRID: access == "hybrid",
        COND_DELAYED_OA: access == "delayed_open_access",
        COND_SUBSCRIPTION_LIKE: access in {"subscription", "hybrid", "delayed_open_access"},
        COND_FEES_CHARGED: fees_charged,
        COND_HAS_WAIVER: has_waiver,
        COND_MEDIA_REGISTERED: media,
        COND_NAME_CHANGED: name_changed,
        COND_ISSN_CHANGED: issn_changed,
        COND_HAS_SPONSORS: has_sponsors,
        COND_HUMAN_RESEARCH: human,
        COND_ANIMAL_RESEARCH: animal,
        COND_CLINICAL: clinical,
        COND_PATIENT_IMAGES: patient_img,
        COND_HAS_EN: has_english_locale,
        COND_ACCEPTS_ADS: ads,
        COND_PRINT_AND_ONLINE: print_online,
    }
    notes: list[str] = []
    if access != "unknown":
        notes.append(f"Модель доступа: {access}")
    return JournalContext(access_model=access, flags=flags, notes=notes)


def _eval_journal_title(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("homepage")
    title = extract_journal_title(doc) if doc is not None else ""
    url = bundle.urls.get("homepage", "")
    if doc is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    if len(title) >= 3:
        return _ok(spec, "complete", spec.max_score, title, url, title)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_issn(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("homepage")
    url = bundle.urls.get("homepage", "")
    if doc is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    value = extract_issn(doc)
    found = _ISSN_RE.findall(value)
    if len(found) >= 1:
        return _ok(spec, "complete", spec.max_score, value, url, value)
    if value:
        return _ok(spec, "formal", 1, value, url, value, note="ISSN-подобная строка без стандартного формата")
    return _ok(spec, "missing", 0, "", url, "")


def _eval_issn_both(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("homepage")
    url = bundle.urls.get("homepage", "")
    if doc is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    value = extract_issn(doc)
    found = _ISSN_RE.findall(value)
    low = value.lower()
    has_print = "print" in low or "печат" in low
    has_online = "online" in low or "электрон" in low or "eissn" in low or "e-issn" in low
    if len(found) >= 2 or (has_print and has_online and found):
        return _ok(spec, "complete", spec.max_score, value, url, value)
    if found:
        return _ok(spec, "partial", 2, value, url, value, note="Указан не полный набор print/online ISSN")
    return _ok(spec, "missing", 0, "", url, "")


def _eval_description(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("homepage")
    url = bundle.urls.get("homepage", "")
    if doc is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    value = extract_description(doc)
    status, score, preview = _status_from_text(value, spec)
    return _ok(spec, status, score, preview, url, value)


def _eval_language(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    home = bundle.docs.get("homepage")
    url = bundle.urls.get("homepage", "")
    if home is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    meta = extract_homepage_meta(home)
    labeled = meta.get("languages") or ""
    if labeled:
        return _ok(spec, "complete", spec.max_score, labeled, url, labeled)

    # Не ищем «русский/english» по всей главной с аннотациями статей —
    # только явные блоки about / политики.
    chunks = [
        bundle.fragments.get("languages", ""),
        bundle.texts.get("about", "")[:3000],
        bundle.texts.get("editorialPolicies", "")[:4000],
        bundle.texts.get("submissions", "")[:3000],
    ]
    text = norm_space(" ".join(chunks))
    explicit = (
        "язык публикации",
        "языки публикации",
        "language of publication",
        "publication language",
        "languages of the journal",
        "журнал публикует статьи на",
        "publishes articles in",
        "accepts manuscripts in",
    )
    if not any(x in text.lower() for x in explicit):
        return _ok(
            spec,
            "missing",
            0,
            "",
            url,
            "",
            note="Явная строка о языках публикации не найдена",
        )
    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, url, text)


def _eval_founder(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    home = bundle.docs.get("homepage")
    url = bundle.urls.get("journalSponsorship", "") or bundle.urls.get("homepage", "")
    if home is not None:
        meta = extract_homepage_meta(home)
        founder = meta.get("founder") or ""
        if founder:
            return _ok(
                spec,
                "complete",
                spec.max_score,
                founder,
                bundle.urls.get("homepage", ""),
                founder,
            )
        desc = extract_description(home)
        if "учредител" in desc.lower() or "founder" in desc.lower():
            status, score, preview = _status_from_text(desc, spec)
            if status != "missing":
                return _ok(spec, status, score, preview, bundle.urls.get("homepage", ""), desc)
    text, src = _gather_criterion_text(bundle, spec)
    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, src or url, text)


def _eval_responsibility(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    """Ответственная организация: издатель/учредитель достаточны."""
    home = bundle.docs.get("homepage")
    spons = bundle.texts.get("journalSponsorship") or ""
    url = bundle.urls.get("journalSponsorship", "") or bundle.urls.get("homepage", "")
    meta = extract_homepage_meta(home) if home is not None else {}
    publisher = meta.get("publisher") or ""
    founder = meta.get("founder") or ""
    if not publisher and "издател" in spons.lower():
        publisher = preview_value(spons, limit=120)
    if not founder and ("учредител" in spons.lower() or "founder" in spons.lower()):
        founder = preview_value(spons, limit=120)
    if publisher or founder:
        value = publisher or founder
        return _ok(
            spec,
            "complete",
            spec.max_score,
            value,
            url,
            spons or value,
            note="Как ответственная организация учтён издатель/учредитель",
        )
    text, src = _gather_criterion_text(bundle, spec)
    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, src or url, text)


def _eval_access_model(bundle: PageBundle, spec: CriterionSpec, ctx: JournalContext) -> CriterionResult:
    home = bundle.docs.get("homepage")
    meta = extract_homepage_meta(home) if home is not None else {}
    text, url = _gather_criterion_text(bundle, spec)
    hint = meta.get("access_hint") or ""
    if hint and ctx.access_model == "unknown":
        guessed = detect_access_model(hint)
        if guessed != "unknown":
            ctx.access_model = guessed
            ctx.flags[COND_OPEN_ACCESS] = guessed == "open_access"
            ctx.flags[COND_HYBRID] = guessed == "hybrid"
            ctx.flags[COND_DELAYED_OA] = guessed == "delayed_open_access"
            ctx.flags[COND_SUBSCRIPTION_LIKE] = guessed in {
                "subscription",
                "hybrid",
                "delayed_open_access",
            }
    if ctx.access_model != "unknown":
        preview = hint or ctx.access_model
        return _ok(
            spec,
            "complete",
            spec.max_score,
            preview,
            url or bundle.urls.get("homepage", "") or bundle.urls.get("editorialPolicies", ""),
            text or hint,
            note=f"Определено: {ctx.access_model}",
        )
    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, url, text)


def _eval_peer_review(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    text, url = _gather_criterion_text(bundle, spec)
    # оцениваем по фрагменту peer review, а не по всей странице политик
    focused = bundle.fragments.get("peerReviewProcess") or _keyword_snippets(
        text,
        ("реценз", "peer review", "reviewer", "рецензент"),
        window=500,
    )
    low = focused.lower() or text.lower()
    components = {
        "all_articles": any(
            x in low
            for x in (
                "все стать",
                "каждая статья",
                "each article",
                "all article",
                "all scientific",
                "все научные",
                "all materials submitted",
            )
        ),
        "type": any(
            x in low
            for x in (
                "double-blind",
                "single-blind",
                "blind review",
                "blind reviewing",
                "двойное",
                "двухэтапн",
                "two-stage",
                "two stage",
                "открытое реценз",
                "open peer",
            )
        ),
        "reviewer": any(
            x in low for x in ("рецензент", "reviewer", "independent expert", "эксперт", "relevant scholar")
        ),
        "decision": any(
            x in low
            for x in (
                "окончательн",
                "final decision",
                "принимает решение",
                "редактор принимает",
                "editorial decision",
                "decision of the editor",
                "editor-in-chief",
                "reject",
                "принятии к публикации",
            )
        ),
        "after": any(
            x in low
            for x in (
                "замечан",
                "revision",
                "доработк",
                "after review",
                "после реценз",
                "revise",
                "returned to the author",
                "comments that need",
                "list of the comments",
                "significant comments",
            )
        ),
    }
    hits = sum(1 for v in components.values() if v)
    preview = preview_value(focused or text)
    if hits >= 4 and len(focused) >= 80:
        return _ok(spec, "complete", spec.max_score, preview, url, focused)
    if hits >= 2:
        return _ok(
            spec,
            "partial",
            2,
            preview,
            url,
            focused or text,
            note=f"Найдено компонентов рецензирования: {hits}/5",
        )
    if hits >= 1 or len(focused) >= spec.min_chars_formal:
        return _ok(spec, "formal", 1, preview, url, focused or text)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_fees(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    body, frag, sec_url = _policy_section_body(bundle, spec)
    text, url = _gather_criterion_text(bundle, spec)
    url = sec_url or url
    if spec.section_aliases and not frag:
        return _ok(
            spec,
            "missing",
            0,
            "",
            url,
            "",
            note="Раздел «Плата за публикацию» не найден на странице редакционной политики",
        )
    if frag and not body.strip():
        return _ok(
            spec,
            "formal",
            1,
            "",
            url,
            "",
            note="Раздел о плате за публикацию без текста",
        )
    low = text.lower()
    explicit_free = any(
        x in low
        for x in (
            "плата не взимается",
            "не взимается",
            "no fee",
            "free of charge",
            "без оплаты",
            "публикационный сбор отсутствует",
        )
    )
    has_fee = any(x in low for x in ("apc", "плата", "fee", "charge", "сбор"))
    if explicit_free or (has_fee and len(text) >= 40):
        return _ok(spec, "complete", spec.max_score, preview_value(text), url, text)
    if has_fee:
        return _ok(spec, "partial", 2, preview_value(text), url, text)
    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, url, text)


def _find_eic_block(text: str) -> str:
    low = text.lower()
    markers = ("главный редактор", "editor-in-chief", "editor in chief", "editor–in–chief")
    for m in markers:
        idx = low.find(m)
        if idx >= 0:
            return norm_space(text[idx : idx + 400])
    return ""


def _team_structure(bundle: PageBundle):
    doc = bundle.docs.get("editorialTeam")
    if doc is None:
        return None
    return extract_editorial_team_structure(doc)


def _home_or_pages_eic(bundle: PageBundle) -> tuple[str, str]:
    url = bundle.urls.get("editorialTeam", "") or bundle.urls.get("homepage", "")
    structure = _team_structure(bundle)
    if structure is not None and structure.chief:
        chief = structure.chief[0]["raw"]
        return chief, bundle.urls.get("editorialTeam", url)

    home = bundle.docs.get("homepage")
    if home is not None:
        meta = extract_homepage_meta(home)
        if meta.get("editor_in_chief"):
            eic = meta["editor_in_chief"]
            # если на странице редакции есть список Editors — привязать к ФИО с главной
            if structure is not None:
                token = eic.split(",")[0].split()[-1].lower()[:12]
                if token:
                    for m in structure.board:
                        if token in m["raw"].lower():
                            return m["raw"], bundle.urls.get("editorialTeam", url)
            return eic, bundle.urls.get("homepage", url)
    contact_doc = bundle.docs.get("contact")
    if contact_doc is not None:
        fields = extract_contact_fields(contact_doc)
        principal = fields.get("contact_principal") or ""
        block = _find_eic_block(principal)
        if not block and (
            "главный редактор" in principal.lower() or "editor-in-chief" in principal.lower()
        ):
            block = principal
        if block:
            return block, bundle.urls.get("contact", url)
    team = bundle.texts.get("editorialTeam") or ""
    block = _find_eic_block(team)
    if block:
        return block, bundle.urls.get("editorialTeam", url)
    return "", url


def _deputies_from_bundle(bundle: PageBundle) -> list[dict[str, Any]]:
    structure = _team_structure(bundle)
    deputies: list[dict[str, Any]] = []
    if structure is not None:
        deputies.extend(structure.deputies)
    contact_doc = bundle.docs.get("contact")
    if contact_doc is not None:
        principal = extract_contact_fields(contact_doc).get("contact_principal") or ""
        low = principal.lower()
        if ("заместитель" in low and "редактор" in low) or "deputy editor" in low:
            e = _member_entry(principal)
            if e:
                deputies.append(e)
    return deputies


def _eval_deputy_editors(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    url = bundle.urls.get("editorialTeam", "") or bundle.urls.get("contact", "")
    deputies = _deputies_from_bundle(bundle)
    if deputies:
        preview = "; ".join(m["raw"] for m in deputies[:3])
        return _ok(
            spec,
            "complete",
            spec.max_score,
            preview_value(preview),
            url,
            preview,
            note=f"Заместителей: {len(deputies)}",
        )
    text = norm_space(
        " ".join(
            [
                bundle.texts.get("editorialTeam") or "",
                bundle.texts.get("contact") or "",
            ]
        )
    )
    low = text.lower()
    if ("заместитель" in low and "редактор" in low) or "deputy editor" in low:
        return _ok(
            spec,
            "formal",
            1,
            preview_value(text),
            url,
            text,
            note="Упоминание заместителя без явного ФИО в структурированном блоке",
        )
    if "editorialTeam" in bundle.errors and not bundle.texts.get("editorialTeam"):
        return _error_result(spec, url, bundle.errors["editorialTeam"])
    return _ok(spec, "missing", 0, "", url, "", note="Заместители главного редактора не указаны")

def _eval_editor_in_chief(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    block, url = _home_or_pages_eic(bundle)
    if not block:
        if "editorialTeam" in bundle.errors and not bundle.texts.get("editorialTeam"):
            return _error_result(spec, url, bundle.errors["editorialTeam"])
        return _ok(spec, "missing", 0, "", url, "")
    if re.search(r"[А-ЯЁA-Z][а-яёa-z\-']{1,}", block):
        return _ok(spec, "complete", spec.max_score, preview_value(block), url, block)
    return _ok(spec, "formal", 1, preview_value(block), url, block, note="Упоминание роли без ФИО")


def _eval_eic_affiliation(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    block, url = _home_or_pages_eic(bundle)
    team = bundle.texts.get("editorialTeam") or ""
    contact = bundle.texts.get("contact") or ""
    extra = norm_space(" ".join([block, team, contact]))
    hits = sum(1 for m in _AFFIL_MARKERS if m in extra.lower())
    home = bundle.docs.get("homepage")
    if hits >= 1 and block:
        return _ok(spec, "complete", spec.max_score, preview_value(extra), url, extra)
    if home is not None and block:
        meta = extract_homepage_meta(home)
        org = meta.get("founder") or meta.get("publisher") or ""
        if org:
            return _ok(
                spec,
                "partial",
                2,
                preview_value(block + "; " + org),
                url,
                block + " " + org,
                note="ФИО главного редактора найдено; аффилиация рядом не указана явно",
            )
    if hits >= 1:
        return _ok(spec, "partial", 2, preview_value(extra), url, extra)
    return _ok(spec, "missing", 0, "", url, "")


def _count_members_from_bundle(bundle: PageBundle) -> tuple[int, int]:
    doc = bundle.docs.get("editorialTeam")
    if doc is None:
        return 0, 0
    members = extract_editorial_members(doc)
    with_aff = sum(1 for m in members if m.get("has_affiliation"))
    return len(members), with_aff


def _eval_editorial_board(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    text = bundle.texts.get("editorialTeam") or ""
    url = bundle.urls.get("editorialTeam", "")
    if "editorialTeam" in bundle.errors and not text:
        return _error_result(spec, url, bundle.errors["editorialTeam"])
    count, _ = _count_members_from_bundle(bundle)
    preview = preview_value(text)
    if count >= 5:
        return _ok(spec, "complete", spec.max_score, preview, url, text, note=f"Участников редколлегии: {count}")
    if count >= 2:
        return _ok(spec, "partial", 2, preview, url, text, note=f"Участников редколлегии: {count}")
    if count == 1:
        return _ok(spec, "formal", 1, preview, url, text, note="Найден 1 участник редколлегии")
    # техредакторы/корректоры без блока editorialBoard не засчитываем
    if text and any(
        x in text.lower()
        for x in ("редактор", "editor", "layout", "copy editor", "корректор")
    ):
        return _ok(
            spec,
            "missing",
            0,
            "",
            url,
            text,
            note="На странице есть только техперсонал или редакторы без состава редколлегии",
        )
    if text:
        return _ok(spec, "formal", 1, preview, url, text)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_member_affiliations(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    text = bundle.texts.get("editorialTeam") or ""
    url = bundle.urls.get("editorialTeam", "")
    count, with_aff = _count_members_from_bundle(bundle)
    if count <= 0:
        return _ok(
            spec,
            "not_applicable",
            0,
            "",
            url,
            "",
            note="Нет блока редакционной коллегии — аффилиации членов не оцениваются",
            applicable=False,
        )
    ratio = with_aff / max(count, 1)
    note = f"Аффилиации у {with_aff} из {count} ({ratio:.0%})"
    if count >= 5 and ratio >= 0.7:
        return _ok(spec, "complete", spec.max_score, preview_value(text), url, text, note=note)
    if ratio >= 0.4 or with_aff >= 3:
        return _ok(spec, "partial", 2, preview_value(text), url, text, note=note)
    if with_aff >= 1:
        return _ok(spec, "formal", 1, preview_value(text), url, text, note=note)
    return _ok(spec, "missing", 0, "", url, text, note=note)


def _eval_contact_map(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("contact")
    url = bundle.urls.get("contact", "")
    if doc is None:
        err = bundle.errors.get("contact")
        if err:
            return _error_result(spec, url, err)
        return _ok(spec, "missing", 0, "", url, "", note="Страница контактов не загружена")
    info = extract_contact_map(doc)
    if info.get("src"):
        kind = info.get("kind") or "map"
        preview = preview_value(info["src"], limit=120)
        return _ok(
            spec,
            "complete",
            spec.max_score,
            preview,
            url,
            info["src"],
            note=f"Карта найдена ({kind})",
        )
    return _ok(spec, "missing", 0, "", url, "", note="Карта на странице контактов не найдена")


def _eval_contact_persons(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("contact")
    url = bundle.urls.get("contact", "")
    if doc is None:
        err = bundle.errors.get("contact")
        if err:
            return _error_result(spec, url, err)
        return _ok(spec, "missing", 0, "", url, "", note="Страница контактов не загружена")
    persons = extract_contact_persons(doc)
    if not persons:
        fields = extract_contact_fields(doc)
        principal = fields.get("contact_principal") or ""
        if principal and re.search(r"[A-ZА-ЯЁ][a-zа-яё\-']{1,}", principal):
            return _ok(
                spec,
                "formal",
                1,
                preview_value(principal),
                url,
                principal,
                note="Блок контакта найден, но данные о лицах не разобраны",
            )
        return _ok(
            spec,
            "missing",
            0,
            "",
            url,
            "",
            note="Контактные лица редакции не указаны (техподдержка платформы не засчитывается)",
        )
    with_contacts = sum(1 for p in persons if p.get("has_email") or p.get("has_phone"))
    preview = "; ".join(p["raw"] for p in persons[:3])
    note = f"Контактных лиц: {len(persons)}; с телефоном/e-mail: {with_contacts}"
    if len(persons) >= 1 and with_contacts >= 1:
        return _ok(spec, "complete", spec.max_score, preview_value(preview), url, preview, note=note)
    if persons:
        return _ok(spec, "partial", 2, preview_value(preview), url, preview, note=note)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_contact_email(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("contact")
    url = bundle.urls.get("contact", "")
    if doc is None:
        err = bundle.errors.get("contact")
        if err:
            return _error_result(spec, url, err)
        return _ok(spec, "missing", 0, "", url, "")
    fields = extract_contact_fields(doc)
    emails = [e.lower() for e in _EMAIL_RE.findall(page_main_text(doc))]
    journal_emails = [e for e in emails if e not in PLATFORM_EMAILS]
    platform_only = [e for e in emails if e in PLATFORM_EMAILS]
    if journal_emails:
        value = journal_emails[0]
        return _ok(spec, "complete", spec.max_score, value, url, value)
    if platform_only:
        return _ok(
            spec,
            "conflict",
            0,
            platform_only[0],
            url,
            platform_only[0],
            note="Найден только общий контакт платформы РЦНИ — не засчитывается",
        )
    if fields.get("contact_email"):
        em = fields["contact_email"].lower()
        if em in PLATFORM_EMAILS:
            return _ok(
                spec,
                "conflict",
                0,
                em,
                url,
                em,
                note="Найден только общий контакт платформы РЦНИ — не засчитывается",
            )
        return _ok(spec, "complete", spec.max_score, em, url, em)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_contact_address(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("contact")
    url = bundle.urls.get("contact", "")
    if doc is None:
        err = bundle.errors.get("contact")
        if err:
            return _error_result(spec, url, err)
        return _ok(spec, "missing", 0, "", url, "")
    fields = extract_contact_fields(doc)
    mailing = fields.get("mailing_address") or ""
    text = page_main_text(doc)
    low = text.lower()
    # общий адрес РЦНИ не засчитываем как достаточный сам по себе
    if "rcsi" in low and len(mailing) < 20 and "иркутск" not in low:
        if "journals.rcsi" in low or "рцни" in low:
            pass
    if len(mailing) >= 20:
        city_like = any(x in mailing.lower() for x in ("г.", "город", "city", "ул.", "street", "russia", "россия"))
        status: CheckStatus = "complete" if city_like or len(mailing) >= 40 else "partial"
        score = 3 if status == "complete" else 2
        return _ok(spec, status, score, preview_value(mailing), url, mailing)
    # fallback: any postal-looking chunk
    if _PHONE_RE.search(text) and any(x in low for x in ("ул.", "пр.", "address", "адрес")):
        return _ok(spec, "partial", 2, preview_value(text), url, text)
    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, url, text)


def _eval_cover(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("homepage")
    url = bundle.urls.get("homepage", "")
    if doc is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    img = extract_homepage_image(doc)
    if img:
        return _ok(spec, "complete", spec.max_score, preview_value(img, limit=120), url, img)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_current_issue(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    doc = bundle.docs.get("homepage")
    url = bundle.urls.get("homepage", "")
    if doc is None:
        return _error_result(spec, url, bundle.errors.get("homepage", "ошибка загрузки"))
    value = extract_current_issue(doc)
    text = norm_space(value)
    if len(text) >= 3:
        return _ok(spec, "complete", spec.max_score, preview_value(text), url, text)
    if text:
        return _ok(spec, "formal", 1, preview_value(text), url, text)
    return _ok(spec, "missing", 0, "", url, "")


def _eval_english_locale(
    bundle: PageBundle,
    spec: CriterionSpec,
    *,
    en_bundle: Optional[PageBundle],
) -> CriterionResult:
    if en_bundle is None:
        return _ok(
            spec,
            "not_applicable",
            0,
            "",
            "",
            "",
            note="Английская локаль не проверялась",
            applicable=False,
        )
    title = ""
    if en_bundle.docs.get("homepage") is not None:
        title = extract_journal_title(en_bundle.docs["homepage"])
    desc = ""
    if en_bundle.docs.get("homepage") is not None:
        desc = extract_description(en_bundle.docs["homepage"])
    policy = en_bundle.texts.get("editorialPolicies") or ""
    score_bits = sum(
        [
            1 if len(title) >= 3 else 0,
            1 if len(desc) >= 80 else 0,
            1 if len(policy) >= 120 else 0,
        ]
    )
    url = en_bundle.urls.get("homepage", "")
    preview = preview_value(title or desc)
    if score_bits >= 3:
        return _ok(spec, "complete", spec.max_score, preview, url, title)
    if score_bits >= 2:
        return _ok(spec, "partial", 2, preview, url, title)
    if score_bits >= 1:
        return _ok(spec, "formal", 1, preview, url, title)
    return _ok(spec, "missing", 0, "", url, "")


def _ok(
    spec: CriterionSpec,
    status: CheckStatus,
    score: int,
    preview: str,
    url: str,
    raw: str,
    *,
    note: str = "",
    applicable: bool = True,
) -> CriterionResult:
    return CriterionResult(
        id=spec.id,
        title=spec.title,
        category=spec.category,
        page=spec.page,
        page_label=PAGE_TITLES.get(spec.page, spec.page),
        requirement=spec.requirement,
        applicable=applicable,
        status=status,
        score=score if applicable and status != "not_applicable" else 0,
        max_score=spec.max_score,
        critical=spec.critical,
        chars=len(norm_space(raw)),
        value_preview=preview,
        source_url=url,
        note=note,
    )


def _eval_policy_section(bundle: PageBundle, spec: CriterionSpec) -> CriterionResult:
    page_key = PAGE_KEY_BY_PATH.get(spec.page, "")
    url = bundle.urls.get(page_key, "")
    body, frag, sec_url = _policy_section_body(bundle, spec)
    url = sec_url or url

    if page_key in bundle.errors and page_key not in bundle.docs:
        return _error_result(spec, url, bundle.errors[page_key])

    missing_note = (
        "Раздел не найден на странице «Для авторов»"
        if spec.page == "/about/submissions"
        else "Раздел не найден на странице редакционной политики"
    )

    if frag == "__page_h1__":
        h1_preview = preview_value(body) if body else spec.title
        if len(norm_space(body)) >= spec.min_chars_formal:
            return _ok(spec, "complete", spec.max_score, h1_preview, url, body)
        return _ok(
            spec,
            "complete",
            spec.max_score,
            h1_preview or spec.title,
            url,
            body,
            note="Страница приёма статей оформлена (заголовок раздела)",
        )

    if not frag and not body.strip():
        return _ok(
            spec,
            "missing",
            0,
            "",
            url,
            "",
            note=missing_note,
        )

    if frag and len(norm_space(body)) < spec.min_chars_formal:
        return _ok(
            spec,
            "formal",
            1,
            preview_value(body),
            url,
            body,
            note="Заголовок раздела есть, содержание не заполнено",
        )

    text, _ = _gather_criterion_text(bundle, spec)
    eval_text = body if body.strip() else text
    n = len(norm_space(eval_text))
    if frag and n >= spec.min_chars_complete:
        return _ok(
            spec,
            "complete",
            spec.max_score,
            preview_value(eval_text),
            url,
            eval_text,
        )
    status, score, preview = _status_from_text(eval_text, spec)
    if frag and status == "missing":
        preview = preview_value(eval_text)
        if n >= spec.min_chars_complete:
            status, score = "complete", spec.max_score
        elif n >= spec.min_chars_partial:
            status, score = "partial", max(1, spec.max_score - 1)
        elif n >= spec.min_chars_formal:
            status, score = "formal", 1
    elif frag and status == "partial" and n >= spec.min_chars_complete:
        status, score = "complete", spec.max_score
    return _ok(spec, status, score, preview, url, eval_text)


def _error_result(spec: CriterionSpec, url: str, note: str) -> CriterionResult:
    return CriterionResult(
        id=spec.id,
        title=spec.title,
        category=spec.category,
        page=spec.page,
        page_label=PAGE_TITLES.get(spec.page, spec.page),
        requirement=spec.requirement,
        applicable=True,
        status="error",
        score=0,
        max_score=spec.max_score,
        critical=spec.critical,
        source_url=url,
        note=note,
    )


def _not_applicable(spec: CriterionSpec, reason: str) -> CriterionResult:
    return CriterionResult(
        id=spec.id,
        title=spec.title,
        category=spec.category,
        page=spec.page,
        page_label=PAGE_TITLES.get(spec.page, spec.page),
        requirement=spec.requirement,
        applicable=False,
        status="not_applicable",
        score=0,
        max_score=spec.max_score,
        critical=spec.critical,
        note=reason,
    )


_CUSTOM = {
    "journal_title": _eval_journal_title,
    "issn": _eval_issn,
    "issn_both": _eval_issn_both,
    "description": _eval_description,
    "language": _eval_language,
    "founder": _eval_founder,
    "responsibility": _eval_responsibility,
    "peer_review": _eval_peer_review,
    "fees": _eval_fees,
    "policy_section": _eval_policy_section,
    "editor_in_chief": _eval_editor_in_chief,
    "eic_affiliation": _eval_eic_affiliation,
    "deputy_editors": _eval_deputy_editors,
    "editorial_board": _eval_editorial_board,
    "member_affiliations": _eval_member_affiliations,
    "contact_email": _eval_contact_email,
    "contact_address": _eval_contact_address,
    "contact_map": _eval_contact_map,
    "contact_persons": _eval_contact_persons,
    "cover": _eval_cover,
    "current_issue": _eval_current_issue,
}


def evaluate_criterion(
    bundle: PageBundle,
    spec: CriterionSpec,
    ctx: JournalContext,
    *,
    en_bundle: Optional[PageBundle] = None,
) -> CriterionResult:
    if spec.requirement == "conditional" and spec.condition and not ctx.is_applicable(spec.condition):
        reasons = {
            COND_OPEN_ACCESS: "Журнал не определён как полностью открытый доступ.",
            COND_HYBRID: "Журнал не определён как гибридный.",
            COND_DELAYED_OA: "Отложенный открытый доступ не выявлен.",
            COND_SUBSCRIPTION_LIKE: "Подписка не требуется для выявленной модели доступа.",
            COND_FEES_CHARGED: "Признаки платной публикации не выявлены.",
            COND_HAS_WAIVER: "Условия льготы не упоминаются.",
            COND_MEDIA_REGISTERED: "Признаки регистрации СМИ не выявлены.",
            COND_NAME_CHANGED: "Смена названия журнала не выявлена.",
            COND_ISSN_CHANGED: "Смена ISSN не выявлена.",
            COND_HAS_SPONSORS: "Спонсоры не упоминаются.",
            COND_HUMAN_RESEARCH: "Тематика не связана с исследованиями с участием людей.",
            COND_ANIMAL_RESEARCH: "Тематика журнала не связана с исследованиями на животных.",
            COND_CLINICAL: "Клинические исследования не выявлены в тематике.",
            COND_PATIENT_IMAGES: "Публикация изображений пациентов не выявлена.",
            COND_ACCEPTS_ADS: "Признаки размещения рекламы не выявлены.",
            COND_PRINT_AND_ONLINE: "Не выявлено наличие и печатной, и электронной версий.",
        }
        return _not_applicable(spec, reasons.get(spec.condition, "Условие неприменимо."))

    if spec.evaluator == "access_model":
        return _eval_access_model(bundle, spec, ctx)
    if spec.evaluator == "english_locale":
        return _eval_english_locale(bundle, spec, en_bundle=en_bundle)

    custom = _CUSTOM.get(spec.evaluator)
    if custom is not None:
        return custom(bundle, spec)

    text, url = _gather_criterion_text(bundle, spec)
    page_key = PAGE_KEY_BY_PATH.get(spec.page, "")
    if page_key and page_key in bundle.errors and page_key not in bundle.docs and not text:
        return _error_result(spec, url or bundle.urls.get(page_key, ""), bundle.errors[page_key])

    # для ownership ищем также на about/homepage
    if spec.category == "ownership" and len(text) < 40:
        text = norm_space(text + " " + bundle.combined_text())

    status, score, preview = _status_from_text(text, spec)
    return _ok(spec, status, score, preview, url, text)


def evaluate_all(
    bundle: PageBundle,
    ctx: JournalContext,
    *,
    en_bundle: Optional[PageBundle] = None,
) -> list[CriterionResult]:
    return [evaluate_criterion(bundle, spec, ctx, en_bundle=en_bundle) for spec in CRITERIA]


def is_problem_status(status: str) -> bool:
    return status in {"missing", "formal", "partial", "conflict", "error"}


def is_filled_enough(status: str) -> bool:
    return status in {"complete", "partial"}


def summarize_results(results: list[CriterionResult]) -> dict[str, Any]:
    """Базовая оценка только по required + applicable conditional; optional отдельно."""
    base = [
        r
        for r in results
        if r.applicable and r.requirement in {"required", "conditional"} and r.status != "not_applicable"
    ]
    optional = [r for r in results if r.requirement == "optional" and r.applicable]

    base_max = sum(r.max_score for r in base) or 1
    base_score = sum(r.score for r in base)
    opt_max = sum(r.max_score for r in optional) or 1
    opt_score = sum(r.score for r in optional)

    required_total = sum(1 for r in base if r.requirement == "required")
    required_ok = sum(1 for r in base if r.requirement == "required" and r.status == "complete")
    # «заполнены» для сводки — complete или partial
    required_filled = sum(
        1 for r in base if r.requirement == "required" and is_filled_enough(r.status)
    )

    must_fix = [
        r
        for r in base
        if r.status in {"missing", "formal", "conflict", "error"}
        or (r.status == "partial" and r.critical)
    ]
    recommendations = [
        r for r in optional if r.status in {"missing", "formal", "partial", "error"}
    ]

    completeness = round(100.0 * base_score / base_max, 1)
    quality = round(100.0 * opt_score / opt_max, 1)

    level = "high" if completeness >= 85 else ("medium" if completeness >= 55 else "low")
    # критические ограничения
    by_id = {r.id: r for r in results}

    def weak_or_missing(cid: str) -> bool:
        r = by_id.get(cid)
        return r is None or r.status in {"missing", "formal", "error", "conflict"}

    caps: list[str] = []
    if weak_or_missing("team.editor_in_chief"):
        level = "low"
        caps.append("team.editor_in_chief")
    if weak_or_missing("team.editorial_board"):
        level = "low"
        caps.append("team.editorial_board")

    medium_caps = (
        "policy.peer_review",
        "submission.guidelines",
        "contact.email",
        "contact.map",
        "contact.persons",
        "policy.ethics",
        "policy.license",
    )
    for cid in medium_caps:
        r = by_id.get(cid)
        if r is None:
            continue
        if r.status == "not_applicable":
            continue
        if weak_or_missing(cid):
            if level == "high":
                level = "medium"
            caps.append(cid)

    return {
        "completeness_percent": completeness,
        "quality_percent": quality,
        "level": level,
        "level_caps": caps,
        "required_total": required_total,
        "required_complete": required_ok,
        "required_filled": required_filled,
        "base_count": len(base),
        "base_score": base_score,
        "base_max_score": base_max,
        "must_fix": [r.to_dict() for r in must_fix],
        "recommendations": [r.to_dict() for r in recommendations],
        "complete_count": sum(1 for r in base if r.status == "complete"),
        "partial_count": sum(1 for r in base if r.status == "partial"),
        "formal_count": sum(1 for r in base if r.status == "formal"),
        "missing_count": sum(1 for r in base if r.status == "missing"),
        "conflict_count": sum(1 for r in base if r.status == "conflict"),
        "error_count": sum(1 for r in base if r.status == "error"),
        "not_applicable_count": sum(1 for r in results if r.status == "not_applicable"),
    }
