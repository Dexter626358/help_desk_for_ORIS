"""Сходство названий и авторов для кандидатов-дублей."""

from __future__ import annotations

import re

from rapidfuzz import fuzz

from ipsas.modules.article_duplicates.models import (
    MatchCandidate,
    QueueSubmission,
    XmlArticle,
)

# Порог по названию (без сильных авторов).
DEFAULT_TITLE_FLOOR = 0.58
# Сильное пересечение авторов без полного совпадения множества.
DEFAULT_AUTHOR_FLOOR = 0.75
# Минимальный title-сигнал при сильных авторах (не full match).
DEFAULT_TITLE_WITH_AUTHORS = 0.28
# Full-match: 2 автора — слабый доп. сигнал.
DEFAULT_FULL_MATCH_TITLE_2 = 0.20
DEFAULT_FULL_MATCH_ABSTRACT_2 = 0.25
# Full-match: 1 автор — заметный доп. сигнал.
DEFAULT_FULL_MATCH_TITLE_1 = 0.28
DEFAULT_FULL_MATCH_ABSTRACT_1 = 0.30
DEFAULT_TOP_K = 5
# Diversified top-k: сколько слотов под название / авторов.
DEFAULT_TITLE_SLOTS = 3
DEFAULT_AUTHOR_SLOTS = 2

# Обратная совместимость для вызовов с score_floor=
DEFAULT_SCORE_FLOOR = DEFAULT_TITLE_FLOOR

_AUTHOR_SPLIT_RE = re.compile(r"[,;]+")
_NON_WORD_RE = re.compile(r"[^\wа-яё]+", re.I)
_TITLE_STOPWORDS = frozenset(
    {
        "и",
        "в",
        "во",
        "на",
        "по",
        "для",
        "из",
        "к",
        "с",
        "со",
        "о",
        "об",
        "от",
        "до",
        "при",
        "про",
        "как",
        "the",
        "a",
        "an",
        "of",
        "in",
        "on",
        "for",
        "to",
        "and",
        "or",
        "with",
        "by",
    }
)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).casefold()


def _significant_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for raw in normalize_text(text).split():
        token = _NON_WORD_RE.sub("", raw)
        if len(token) < 3 or token in _TITLE_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


def author_surnames_ordered(authors: str) -> list[str]:
    """Фамилии авторов в порядке перечисления."""
    ordered: list[str] = []
    for part in _AUTHOR_SPLIT_RE.split(authors or ""):
        part = part.strip()
        if not part:
            continue
        first = part.split()[0] if part.split() else part
        key = _NON_WORD_RE.sub("", first).casefold()
        if len(key) >= 2:
            ordered.append(key)
    return ordered


def author_tokens(authors: str) -> set[str]:
    """Набор нормализованных фамилий (первое слово каждого автора)."""
    return set(author_surnames_ordered(authors))


def authors_order_match(reference: str, submission: str) -> bool:
    """Все фамилии эталона идут в том же порядке в начале списка рукописи."""
    left = author_surnames_ordered(reference)
    right = author_surnames_ordered(submission)
    if not left or not right or len(right) < len(left):
        return False
    return right[: len(left)] == left


def _is_truncated_text(value: str) -> bool:
    text = (value or "").strip()
    return text.endswith("…") or text.endswith("...")


def title_similarity(a: str, b: str) -> float:
    """
    Консервативное сходство названий.

    Берём минимум из WRatio, token_set и token_sort, чтобы общие слова
    и перестановки не завышали оценку по одному каналу.
    Дополнительно учитываем долю значимых токенов.
    Для обрезанных названий из списков OJS («…») — partial_ratio.
    """
    left = normalize_text(a)
    right = normalize_text(b)
    if not left or not right:
        return 0.0

    wratio = float(fuzz.WRatio(left, right)) / 100.0
    token_set = float(fuzz.token_set_ratio(left, right)) / 100.0
    token_sort = float(fuzz.token_sort_ratio(left, right)) / 100.0
    fuzzy = min(wratio, token_set, token_sort)
    if _is_truncated_text(a) or _is_truncated_text(b):
        partial = float(fuzz.partial_ratio(left, right)) / 100.0
        fuzzy = max(fuzzy, partial)

    left_toks = _significant_tokens(left)
    right_toks = _significant_tokens(right)
    if left_toks and right_toks:
        jaccard = len(left_toks & right_toks) / len(left_toks | right_toks)
        # Смесь: fuzzy не выше, чем позволяет пересечение значимых слов + запас.
        return 0.55 * fuzzy + 0.45 * max(fuzzy * 0.5, jaccard)
    return fuzzy * 0.85


def author_similarity(a: str, b: str) -> tuple[float, bool]:
    """
    Jaccard по фамилиям.

    Returns:
        (score, authors_full_match) — full_match, если все авторы эталона
        есть среди авторов рукописи. Если список рукописи обрезан («…»),
        достаточно что видимые фамилии ⊆ эталона (не менее 2).
    """
    left = author_tokens(a)
    right = author_tokens(b)
    if not left or not right:
        return 0.0, False
    inter = left & right
    union = left | right
    score = len(inter) / len(union) if union else 0.0
    full = bool(left) and left.issubset(right)
    if (
        not full
        and _is_truncated_text(b)
        and len(right) >= 3
        and right.issubset(left)
    ):
        # Очередь OJS часто показывает «Иванов, Петров, Сидоров,...»
        full = True
        score = max(score, len(inter) / len(right))
    return score, full


def abstract_similarity(a: str, b: str) -> float:
    """Сходство аннотаций: min(token_set, token_sort), без завышения set-only."""
    left = normalize_text(a)
    right = normalize_text(b)
    if not left or not right:
        return 0.0
    token_set = float(fuzz.token_set_ratio(left, right)) / 100.0
    token_sort = float(fuzz.token_sort_ratio(left, right)) / 100.0
    return min(token_set, token_sort)


def combine_score(
    title_score: float,
    author_score: float,
    *,
    authors_full_match: bool,
    abstract_score: float = 0.0,
    has_abstract: bool = False,
) -> float:
    """
    Итоговый score для сортировки.

    При наличии обеих аннотаций: title 60% / authors 25% / abstract 15%.
    Без аннотации вес перераспределяется на title/authors (не штраф).
    """
    if has_abstract:
        w_title, w_authors, w_abs = 0.60, 0.25, 0.15
        base = (
            w_title * title_score
            + w_authors * author_score
            + w_abs * abstract_score
        )
    else:
        # 0.60+0.25 → нормализация к 1.0
        base = (0.60 / 0.85) * title_score + (0.25 / 0.85) * author_score
    if authors_full_match:
        # Полные авторы поднимают кандидата, но не подменяют сходство названия.
        return max(base, 0.45 + 0.4 * author_score + 0.15 * title_score)
    return base


def qualifies(
    *,
    title_score: float,
    author_score: float,
    authors_full_match: bool,
    abstract_score: float = 0.0,
    ref_author_count: int = 0,
    title_floor: float = DEFAULT_TITLE_FLOOR,
    author_floor: float = DEFAULT_AUTHOR_FLOOR,
    title_with_authors: float = DEFAULT_TITLE_WITH_AUTHORS,
    full_match_title_2: float = DEFAULT_FULL_MATCH_TITLE_2,
    full_match_abstract_2: float = DEFAULT_FULL_MATCH_ABSTRACT_2,
    full_match_title_1: float = DEFAULT_FULL_MATCH_TITLE_1,
    full_match_abstract_1: float = DEFAULT_FULL_MATCH_ABSTRACT_1,
) -> bool:
    """Кандидат попадает в выдачу при сильном названии и/или авторах."""
    if authors_full_match:
        n = max(0, int(ref_author_count))
        if n >= 3:
            return True
        if n == 2:
            return (
                title_score >= full_match_title_2
                or abstract_score >= full_match_abstract_2
            )
        # 0–1 автор эталона: нужен доп. сигнал (частые фамилии).
        return (
            title_score >= full_match_title_1
            or abstract_score >= full_match_abstract_1
        )
    if title_score >= title_floor:
        return True
    if author_score >= author_floor and title_score >= title_with_authors:
        return True
    return False


def confidence_label(
    *,
    score: float,
    title_score: float,
    authors_full_match: bool,
) -> str:
    """high | medium | low — для сотрудника, не для порога отбора."""
    if score >= 0.85 and (title_score >= 0.90 or (authors_full_match and title_score >= 0.70)):
        return "high"
    if score >= 0.55 or title_score >= 0.58 or authors_full_match:
        return "medium"
    return "low"


def explain_match(
    xml: XmlArticle,
    submission: QueueSubmission,
    *,
    title_score: float,
    author_score: float,
    abstract_score: float,
    authors_full_match: bool,
) -> tuple[list[str], bool]:
    """Человекочитаемые причины совпадения + флаг порядка авторов."""
    reasons: list[str] = []
    order_ok = authors_order_match(xml.authors, submission.authors)
    ref_n = len(author_surnames_ordered(xml.authors))

    if title_score >= 0.97:
        reasons.append("Названия идентичны")
    elif title_score >= 0.85:
        reasons.append("Названия почти совпадают")
    elif title_score >= 0.58:
        reasons.append("Названия похожи")
    elif title_score >= 0.28:
        reasons.append("Слабое сходство названий")

    if authors_full_match and ref_n > 0:
        reasons.append(f"Совпали все {ref_n} авторов эталона")
        if order_ok:
            reasons.append("Порядок авторов совпадает")
        ref_raw = normalize_text(xml.authors)
        sub_raw = normalize_text(submission.authors)
        if ref_raw and sub_raw and ref_raw != sub_raw:
            reasons.append("Отличаются формы записи инициалов/имён")
    elif author_score >= 0.75:
        reasons.append("Сильное пересечение авторов")
    elif author_score >= 0.4:
        reasons.append("Частичное пересечение авторов")

    if abstract_score >= 0.85:
        reasons.append("Аннотации сильно похожи")
    elif abstract_score >= 0.45:
        reasons.append("Аннотации частично пересекаются")

    if not reasons:
        reasons.append("Слабые сигналы по метаданным")
    return reasons, order_ok


def score_pair(
    xml: XmlArticle,
    submission: QueueSubmission,
) -> MatchCandidate:
    title_a = xml.title_ru or xml.title
    title_b = submission.title
    t_score = title_similarity(title_a, title_b)
    if xml.title_en and xml.title_en != title_a:
        t_score = max(t_score, title_similarity(xml.title_en, title_b))

    a_score, full = author_similarity(xml.authors, submission.authors)
    if xml.title_en and submission.title_en:
        t_score = max(t_score, title_similarity(xml.title_en, submission.title_en))
    has_abstract = bool(xml.abstract and submission.abstract)
    abs_score = 0.0
    if has_abstract:
        abs_score = abstract_similarity(xml.abstract, submission.abstract)
    overall = combine_score(
        t_score,
        a_score,
        authors_full_match=full,
        abstract_score=abs_score,
        has_abstract=has_abstract,
    )
    reasons, order_ok = explain_match(
        xml,
        submission,
        title_score=t_score,
        author_score=a_score,
        abstract_score=abs_score,
        authors_full_match=full,
    )
    return MatchCandidate(
        submission=submission,
        score=overall,
        title_score=t_score,
        author_score=a_score,
        abstract_score=abs_score,
        authors_full_match=full,
        authors_order_match=order_ok,
        match_reasons=reasons,
        confidence=confidence_label(
            score=overall,
            title_score=t_score,
            authors_full_match=full,
        ),
    )


def annotate_uniqueness(candidates: list[MatchCandidate]) -> None:
    """Однозначность: единственный сильный кандидат или большой отрыв от 2-го."""
    if not candidates:
        return
    for cand in candidates:
        cand.uniqueness = ""
    ordered = sorted(candidates, key=lambda c: c.score, reverse=True)
    top = ordered[0]
    if len(ordered) == 1:
        if top.confidence == "high" or (
            top.score >= 0.85 and top.authors_full_match and top.title_score >= 0.85
        ):
            top.uniqueness = "unique"
        return
    gap = top.score - ordered[1].score
    if top.score >= 0.80 and gap >= 0.20 and (
        top.confidence == "high" or top.title_score >= 0.85
    ):
        top.uniqueness = "likely_unique"


def _merge_diversified(
    qualified: list[MatchCandidate],
    *,
    top_k: int,
    title_slots: int = DEFAULT_TITLE_SLOTS,
    author_slots: int = DEFAULT_AUTHOR_SLOTS,
) -> list[MatchCandidate]:
    """
    Top-k из двух пулов: лучшие по названию и лучшие по авторам.

    Снижает риск, что один признак вытеснит все слоты.
    """
    if top_k <= 0 or not qualified:
        return []

    by_title = sorted(
        qualified,
        key=lambda c: (c.title_score, c.score, c.author_score),
        reverse=True,
    )
    by_authors = sorted(
        qualified,
        key=lambda c: (
            c.author_score,
            c.authors_full_match,
            c.score,
            c.title_score,
        ),
        reverse=True,
    )

    n_title = min(max(0, title_slots), top_k)
    n_author = min(max(0, author_slots), max(0, top_k - n_title))
    picked: list[MatchCandidate] = []
    seen: set[int] = set()

    def _take(pool: list[MatchCandidate], limit: int) -> None:
        nonlocal picked
        for cand in pool:
            if len(picked) >= top_k or limit <= 0:
                return
            aid = cand.submission.article_id
            if aid in seen:
                continue
            seen.add(aid)
            picked.append(cand)
            limit -= 1

    _take(by_title, n_title)
    _take(by_authors, n_author)

    by_score = sorted(
        qualified,
        key=lambda c: (
            c.score,
            c.title_score,
            c.author_score,
            c.authors_full_match,
        ),
        reverse=True,
    )
    _take(by_score, top_k - len(picked))

    picked.sort(
        key=lambda c: (
            c.score,
            c.title_score,
            c.author_score,
            c.authors_full_match,
        ),
        reverse=True,
    )
    return picked


def rank_candidates(
    xml: XmlArticle,
    submissions: list[QueueSubmission],
    *,
    top_k: int = DEFAULT_TOP_K,
    score_floor: float = DEFAULT_TITLE_FLOOR,
    author_floor: float = DEFAULT_AUTHOR_FLOOR,
    title_with_authors: float = DEFAULT_TITLE_WITH_AUTHORS,
    title_slots: int = DEFAULT_TITLE_SLOTS,
    author_slots: int = DEFAULT_AUTHOR_SLOTS,
) -> list[MatchCandidate]:
    """До top_k кандидатов: фильтр qualifies + diversified merge."""
    ref_author_count = len(author_tokens(xml.authors))
    qualified: list[MatchCandidate] = []
    for sub in submissions:
        candidate = score_pair(xml, sub)
        if qualifies(
            title_score=candidate.title_score,
            author_score=candidate.author_score,
            authors_full_match=candidate.authors_full_match,
            abstract_score=candidate.abstract_score,
            ref_author_count=ref_author_count,
            title_floor=score_floor,
            author_floor=author_floor,
            title_with_authors=title_with_authors,
        ):
            qualified.append(candidate)
    picked = _merge_diversified(
        qualified,
        top_k=top_k,
        title_slots=title_slots,
        author_slots=author_slots,
    )
    annotate_uniqueness(picked)
    return picked
