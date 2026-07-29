"""Каталог правил проверки метаданных выпуска.

Около 40–50 атомарных правил; в UI объединяются в 7 категорий.
Категории помогают отличать отсутствие полей, ошибки импорта и сбои анализа.
"""

from __future__ import annotations

from typing import Dict, List, Optional, TypedDict


class CategoryInfo(TypedDict):
    id: str
    title: str
    description: str


class RuleInfo(TypedDict, total=False):
    id: str
    category: str
    title: str
    default_severity: str
    field: str


CATEGORIES: List[CategoryInfo] = [
    {
        "id": "issue",
        "title": "Выпуск",
        "description": "Реквизиты выпуска: журнал, том/номер, год, ISSN, обложка, число статей.",
    },
    {
        "id": "identifiers",
        "title": "Идентификаторы",
        "description": "DOI, EDN, внутренний ID платформы и их корректное разделение.",
    },
    {
        "id": "texts",
        "title": "Названия и тексты",
        "description": "Названия, аннотации, ключевые слова: наличие, язык, нормализация, транслит.",
    },
    {
        "id": "authors_orgs",
        "title": "Авторы и организации",
        "description": "Авторы, ORCID, email, аффилиации и их отображение на публичной странице.",
    },
    {
        "id": "files",
        "title": "Файлы",
        "description": "PDF статьи и общий PDF выпуска, доступность ссылок.",
    },
    {
        "id": "bibliography",
        "title": "Библиография",
        "description": "Список литературы: наличие, структура, дубли, склейки, подозрительная нормализация.",
    },
    {
        "id": "consistency",
        "title": "Согласованность источников",
        "description": "Расхождения HTML / JATS / meta / DOI, порядок и пересечение страниц, чужой выпуск.",
    },
]

CATEGORY_IDS: List[str] = [c["id"] for c in CATEGORIES]
CATEGORY_TITLES: Dict[str, str] = {c["id"]: c["title"] for c in CATEGORIES}

# Поле замечания → категория (основной способ классификации)
FIELD_TO_CATEGORY: Dict[str, str] = {
    # Выпуск
    "journal_title": "issue",
    "issue_title": "issue",
    "year": "issue",
    "issue": "issue",
    "volume": "issue",
    "issue_serial": "issue",
    "issue_language": "issue",
    "cover": "issue",
    "article_count": "issue",
    "issn": "issue",
    "eissn": "issue",
    # Идентификаторы
    "doi": "identifiers",
    "edn": "identifiers",
    "article_id": "identifiers",
    "internal_id": "identifiers",
    # Названия и тексты
    "title": "texts",
    "title_ru": "texts",
    "title_en": "texts",
    "abstract": "texts",
    "abstract_ru": "texts",
    "abstract_en": "texts",
    "keywords": "texts",
    "keywords_ru": "texts",
    "keywords_en": "texts",
    "publication_date": "texts",
    # Авторы и организации
    "authors": "authors_orgs",
    "orcid": "authors_orgs",
    "email": "authors_orgs",
    "organizations": "authors_orgs",
    "ror": "authors_orgs",
    # Файлы
    "pdf": "files",
    "issue_pdf": "files",
    # Библиография
    "references": "bibliography",
    # Согласованность
    "pages": "consistency",
    "issue_membership": "consistency",
    "jats_xml": "consistency",
}

# Каталог атомарных правил (для отчёта и стабильных rule_id)
RULES: List[RuleInfo] = [
    # --- Выпуск ---
    {"id": "ISSUE_JOURNAL_TITLE", "category": "issue", "title": "Название журнала", "field": "journal_title"},
    {"id": "ISSUE_TITLE", "category": "issue", "title": "Заголовок выпуска", "field": "issue_title"},
    {"id": "ISSUE_YEAR", "category": "issue", "title": "Год выпуска", "field": "year"},
    {"id": "ISSUE_NUMBER", "category": "issue", "title": "Номер выпуска", "field": "issue"},
    {"id": "ISSUE_VOLUME", "category": "issue", "title": "Том (если модель журнала использует тома)", "field": "volume"},
    {"id": "ISSUE_SERIAL", "category": "issue", "title": "Дополнительный номер в скобках (если указан)", "field": "issue_serial"},
    {"id": "ISSUE_DATE", "category": "issue", "title": "Дата публикации выпуска (не проверяется)", "field": "publication_date"},
    {"id": "ISSUE_LANGUAGE", "category": "issue", "title": "Язык выпуска", "field": "issue_language"},
    {"id": "ISSUE_COVER", "category": "issue", "title": "Обложка выпуска", "field": "cover"},
    {"id": "ISSUE_ARTICLE_COUNT", "category": "issue", "title": "Число статей в карточке и по ссылкам", "field": "article_count"},
    {"id": "ISSUE_ISSN", "category": "issue", "title": "ISSN / eISSN формат и совпадение", "field": "issn"},
    # --- Идентификаторы ---
    {"id": "ID_DOI_PRESENT", "category": "identifiers", "title": "DOI присутствует", "field": "doi"},
    {"id": "ID_DOI_FORMAT", "category": "identifiers", "title": "DOI: синтаксис (//, пробелы, URL, хвост)", "field": "doi"},
    {"id": "ID_DOI_RESOLVE", "category": "identifiers", "title": "DOI resolve через doi.org (информативно, без замечаний)", "field": "doi"},
    {"id": "ID_DOI_META", "category": "identifiers", "title": "Метаданные DOI соответствуют статье", "field": "doi"},
    {"id": "ID_DOI_DUP", "category": "identifiers", "title": "DOI не дублируется между статьями", "field": "doi"},
    {"id": "ID_EDN_FORMAT", "category": "identifiers", "title": "EDN: 6 латинских букв, не OJS ID", "field": "edn"},
    {"id": "ID_INTERNAL_VS_EDN", "category": "identifiers", "title": "Внутренний ID не подменяется под EDN", "field": "edn"},
    {"id": "ID_ARTICLE_ID_DUP", "category": "identifiers", "title": "article_id не дублируется", "field": "article_id"},
    # --- Названия и тексты ---
    {"id": "TXT_TITLE_RU", "category": "texts", "title": "Название RU присутствует", "field": "title_ru"},
    {"id": "TXT_TITLE_EN", "category": "texts", "title": "Название EN присутствует", "field": "title_en"},
    {"id": "TXT_TITLE_LANG", "category": "texts", "title": "Язык полей названия", "field": "title"},
    {"id": "TXT_TITLE_TRANSLIT", "category": "texts", "title": "EN-название не является транслитерацией", "field": "title_en"},
    {"id": "TXT_TITLE_HTML", "category": "texts", "title": "Нет HTML-мусора и лишних пробелов в названии", "field": "title"},
    {"id": "TXT_TITLE_DUP", "category": "texts", "title": "Название не дублируется между статьями", "field": "title"},
    {"id": "TXT_ABSTRACT_RU", "category": "texts", "title": "Аннотация RU на публичной странице", "field": "abstract_ru"},
    {"id": "TXT_ABSTRACT_EN", "category": "texts", "title": "Аннотация EN на публичной странице", "field": "abstract_en"},
    {"id": "TXT_ABSTRACT_LANG", "category": "texts", "title": "Язык аннотаций", "field": "abstract"},
    {"id": "TXT_ABSTRACT_LEN", "category": "texts", "title": "Длина аннотации 30–1000 слов", "field": "abstract"},
    {"id": "TXT_ABSTRACT_QUALITY", "category": "texts", "title": "Аннотация без заголовка/утечек/обрыва/копии RU=EN", "field": "abstract"},
    {"id": "TXT_KW_RU", "category": "texts", "title": "Ключевые слова RU", "field": "keywords_ru"},
    {"id": "TXT_KW_EN", "category": "texts", "title": "Ключевые слова EN", "field": "keywords_en"},
    {"id": "TXT_KW_LANG", "category": "texts", "title": "Язык наборов ключевых слов", "field": "keywords"},
    {"id": "TXT_KW_QUALITY", "category": "texts", "title": "Ключевые слова: дубли, пустые, точка, один элемент", "field": "keywords"},
    # --- Авторы и организации ---
    {"id": "AU_PRESENT", "category": "authors_orgs", "title": "Есть хотя бы один автор", "field": "authors"},
    {"id": "AU_COUNT_RU_EN", "category": "authors_orgs", "title": "Число авторов RU = EN", "field": "authors"},
    {"id": "AU_DUP", "category": "authors_orgs", "title": "Нет дублей авторов", "field": "authors"},
    {"id": "AU_NAME_FORM", "category": "authors_orgs", "title": "Форма имени, инициалы, пунктуация", "field": "authors"},
    {"id": "AU_ORCID", "category": "authors_orgs", "title": "Формат ORCID", "field": "orcid"},
    {"id": "AU_EMAIL", "category": "authors_orgs", "title": "Формат email и отсутствие одного email у всех", "field": "email"},
    {"id": "ORG_PRESENT", "category": "authors_orgs", "title": "Организации присутствуют", "field": "organizations"},
    {"id": "ORG_PAGE_EMPTY", "category": "authors_orgs", "title": "Индекс аффилиации без названия на странице", "field": "organizations"},
    {"id": "ORG_RU_EN", "category": "authors_orgs", "title": "RU/EN формы организаций", "field": "organizations"},
    {"id": "ORG_MULTI", "category": "authors_orgs", "title": "Несколько организаций не слиты в одну строку", "field": "organizations"},
    {"id": "ORG_ROR", "category": "authors_orgs", "title": "ROR организации", "field": "ror"},
    # --- Файлы ---
    {"id": "FILE_ARTICLE_PDF", "category": "files", "title": "PDF статьи", "field": "pdf"},
    {"id": "FILE_ISSUE_PDF", "category": "files", "title": "PDF выпуска", "field": "issue_pdf"},
    {"id": "FILE_PDF_DUP", "category": "files", "title": "PDF URL не дублируется", "field": "pdf"},
    # --- Библиография ---
    {"id": "REF_PRESENT", "category": "bibliography", "title": "Список литературы присутствует", "field": "references"},
    {"id": "REF_NUMBERING", "category": "bibliography", "title": "Нумерация источников", "field": "references"},
    {"id": "REF_DUP", "category": "bibliography", "title": "Дубли записей", "field": "references"},
    {"id": "REF_GLUE_BREAK", "category": "bibliography", "title": "Склеенные / разорванные записи", "field": "references"},
    {"id": "REF_DOI", "category": "bibliography", "title": "DOI внутри источников", "field": "references"},
    {"id": "REF_SUSPICIOUS", "category": "bibliography", "title": "Подозрительная нормализация записей", "field": "references"},
    {"id": "REF_PARALLEL", "category": "bibliography", "title": "Равенство двух самостоятельных блоков Литература/References", "field": "references"},
    {"id": "REF_MIXED_SCRIPT", "category": "bibliography", "title": "Смешение кириллицы и латиницы в слове", "field": "references"},
    # --- Согласованность ---
    {"id": "CON_PAGES_PRESENT", "category": "consistency", "title": "Страницы указаны", "field": "pages"},
    {"id": "CON_PAGES_RANGE", "category": "consistency", "title": "Корректный диапазон страниц", "field": "pages"},
    {"id": "CON_PAGES_SOURCES", "category": "consistency", "title": "Согласованность источников страниц (отключено: канон JATS)", "field": "pages"},
    {"id": "CON_PAGES_DOI", "category": "consistency", "title": "Страницы из DOI без подтверждения", "field": "pages"},
    {"id": "CON_PAGES_ORDER", "category": "consistency", "title": "Порядок и пересечение страниц в выпуске", "field": "pages"},
    {"id": "CON_PAGES_GAP", "category": "consistency", "title": "Пропуски страниц между статьями", "field": "pages"},
    {"id": "CON_ISSUE_MEMBER", "category": "consistency", "title": "Статья принадлежит этому выпуску", "field": "issue_membership"},
    {"id": "CON_HTML_JATS_REFS", "category": "consistency", "title": "Число источников HTML ↔ JATS", "field": "references"},
    {"id": "CON_JATS_RECOVER", "category": "consistency", "title": "JATS восстановлен в recover-режиме", "field": "jats_xml"},
]

RULES_BY_ID: Dict[str, RuleInfo] = {r["id"]: r for r in RULES}


def resolve_category(
    *,
    field: Optional[str] = None,
    text: Optional[str] = None,
    rule_id: Optional[str] = None,
    default: str = "texts",
) -> str:
    """Определить категорию замечания."""
    if rule_id and rule_id in RULES_BY_ID:
        return str(RULES_BY_ID[rule_id]["category"])
    if field and field in FIELD_TO_CATEGORY:
        return FIELD_TO_CATEGORY[field]
    t = (text or "").lower()
    if any(x in t for x in ("doi", "edn", "article_id", "внутренний id")):
        return "identifiers"
    if any(x in t for x in ("pdf",)):
        return "files"
    if any(x in t for x in ("литератур", "библиограф", "источник", "references")):
        return "bibliography"
    if any(x in t for x in ("автор", "orcid", "email", "аффилиац", "организац", "ror")):
        return "authors_orgs"
    if any(
        x in t
        for x in (
            "страниц",
            "согласован",
            "jats",
            "пересечен",
            "пропуск страниц",
            "другого выпуска",
            "html —",
        )
    ):
        return "consistency"
    if any(x in t for x in ("выпуск", "том ", "issn", "обложк", "статей в", "номер выпуска", "год выпуска")):
        return "issue"
    return default


def resolve_rule_id(*, field: Optional[str] = None, text: Optional[str] = None) -> Optional[str]:
    """Эвристически подобрать rule_id по тексту (для отчёта/группировки)."""
    t = (text or "").lower()
    mapping = [
        ("двойной слеш", "ID_DOI_FORMAT"),
        ("doi отсутствует", "ID_DOI_PRESENT"),
        ("не найден doi", "ID_DOI_PRESENT"),
        ("не разрешается через doi.org", "ID_DOI_RESOLVE"),
        ("метаданные doi не соответствуют", "ID_DOI_META"),
        ("дублирующийся doi", "ID_DOI_DUP"),
        ("edn", "ID_EDN_FORMAT"),
        ("внутренний id", "ID_INTERNAL_VS_EDN"),
        ("транслитерац", "TXT_TITLE_TRANSLIT"),
        ("аннотация", "TXT_ABSTRACT_RU" if "ru" in t else "TXT_ABSTRACT_EN"),
        ("ключевые слова", "TXT_KW_EN" if " en" in t or "en" in t[-8:] else "TXT_KW_RU"),
        ("аффилиацию", "ORG_PAGE_EMPTY"),
        ("несоответствие метаданных организаций", "ORG_RU_EN"),
        ("организац", "ORG_PRESENT"),
        ("pdf выпуска", "FILE_ISSUE_PDF"),
        ("pdf файл статьи", "FILE_ARTICLE_PDF"),
        ("не найден pdf", "FILE_ARTICLE_PDF"),
        ("список литературы", "REF_PRESENT"),
        ("подозрительная запись библиографии", "REF_SUSPICIOUS"),
        ("склеенн", "REF_GLUE_BREAK"),
        ("разорван", "REF_GLUE_BREAK"),
        ("ошибка согласованности страниц", "CON_PAGES_SOURCES"),
        ("из doi без подтверждения", "CON_PAGES_DOI"),
        ("пересечение или нарушение последовательности", "CON_PAGES_ORDER"),
        ("пропуск страниц", "CON_PAGES_GAP"),
        ("другого выпуска", "CON_ISSUE_MEMBER"),
    ]
    for needle, rid in mapping:
        if needle in t:
            return rid
    if field:
        for rule in RULES:
            if rule.get("field") == field:
                return rule["id"]
    return None


def categories_for_report() -> List[Dict[str, object]]:
    """Список категорий с числом правил для блока «Правила проверки»."""
    counts: Dict[str, int] = {c: 0 for c in CATEGORY_IDS}
    for rule in RULES:
        cat = str(rule.get("category") or "")
        if cat in counts:
            counts[cat] += 1
    out: List[Dict[str, object]] = []
    for cat in CATEGORIES:
        out.append(
            {
                "id": cat["id"],
                "title": cat["title"],
                "description": cat["description"],
                "rules_count": counts.get(cat["id"], 0),
                "rules": [r for r in RULES if r.get("category") == cat["id"]],
            }
        )
    return out
