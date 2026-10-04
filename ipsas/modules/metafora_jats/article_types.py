"""Сопоставление JATS ``article-type`` с типами Метафоры.

Неизвестный Метафоре тип на стороне Метафоры трактуется как «Другое».
Для обязательности авторов неизвестный тип = авторы не обязательны
(как у «Другое»).

Значения ниже — стартовый расширяемый набор; при появлении официального
словаря Метафоры дополнить ``AUTHORS_OPTIONAL_JATS_TYPES``.
"""

from __future__ import annotations

# JATS article-type, для которых авторы не обязательны
# (От редакции / Сообщение о ретракции / Другое).
AUTHORS_OPTIONAL_JATS_TYPES: frozenset[str] = frozenset(
    {
        "editorial",  # От редакции
        "retraction",  # Сообщение о ретракции
        "other",  # Другое
        # уточнить при появлении официального mapping:
        # "announcement",
        # "expression-of-concern",
    }
)

# Типы, для которых авторы точно обязательны (явный whitelist опционален;
# по умолчанию: всё, что не в AUTHORS_OPTIONAL и не «неизвестный→Другое»).
KNOWN_TYPES_REQUIRING_AUTHORS: frozenset[str] = frozenset(
    {
        "research-article",
        "review-article",
        "brief-report",
        "case-report",
        "letter",
        "article-commentary",
        "book-review",
        "conference",
        "discussion",
        "rapid-communication",
        "methods-article",
        "product-review",
    }
)

# Научные статьи: обязателен непустой список литературы (ref-list/ref).
TYPES_REQUIRING_REFERENCES: frozenset[str] = frozenset(
    {
        "research-article",
        "review-article",
    }
)

# Подписи для отчёта (JATS article-type → понятное название).
ARTICLE_TYPE_LABELS: dict[str, str] = {
    "research-article": "Научная статья",
    "review-article": "Обзор",
    "brief-report": "Краткое сообщение",
    "case-report": "Клинический случай",
    "letter": "Письмо в редакцию",
    "article-commentary": "Комментарий",
    "book-review": "Рецензия",
    "conference": "Материалы конференции",
    "discussion": "Дискуссия",
    "rapid-communication": "Экспресс-сообщение",
    "methods-article": "Методическая статья",
    "product-review": "Обзор продукта",
    "editorial": "От редакции",
    "retraction": "Сообщение о ретракции",
    "other": "Другое",
}


def label_for_article_type(article_type: str) -> str:
    """Человекочитаемая подпись типа; неизвестный → «Другое (… )»."""
    raw = (article_type or "").strip()
    if not raw:
        return ""
    known = ARTICLE_TYPE_LABELS.get(raw.casefold())
    if known:
        return known
    # как в Метафоре: неизвестный тип ≈ «Другое»
    return f"Другое ({raw})"


def authors_required_for_type(article_type: str) -> bool:
    """Нужны ли авторы для данного ``article-type``.

    - известный тип из whitelist → да;
    - тип из AUTHORS_OPTIONAL → нет;
    - неизвестный тип → нет (Метафора: «Другое»).
    """
    raw = (article_type or "").strip().casefold()
    if not raw:
        # Пустой тип — отдельная ошибка M001; авторов не требуем дополнительно.
        return False
    if raw in {t.casefold() for t in AUTHORS_OPTIONAL_JATS_TYPES}:
        return False
    if raw in {t.casefold() for t in KNOWN_TYPES_REQUIRING_AUTHORS}:
        return True
    # Неизвестный тип ≈ «Другое»
    return False


def references_required_for_type(article_type: str) -> bool:
    """Нужен ли список литературы для данного ``article-type``."""
    raw = (article_type or "").strip().casefold()
    return raw in {t.casefold() for t in TYPES_REQUIRING_REFERENCES}
