"""Чек-лист «Настройка журнала по умолчанию» (сотрудники Платформы).

Источник методики: чек-лист настройки журнала в связке с БАЗА.
Проверяется по экспорту OJS (.data): settings + plugins + sections.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CheckKind = Literal[
    "locale_flags",
    "setting_text",
    "setting_bool",
    "setting_present_or_empty_ok",
    "plugin",
    "plugin_conditional",
    "sections_articles",
    "sections_other",
    "reader_tools",
    "email_outgoing",
    "copyright_license",
    "submission_checklist",
    "library_mode",
    "issue_identification",
    "browse_plugin",
    "webfeed_plugin",
    "extra_generic_plugins",
    "dates_display",
    "custom_about",
    "board",
    "map_address",
    "manual",
]

Severity = Literal["required", "recommended", "info"]


@dataclass(frozen=True, slots=True)
class ChecklistItem:
    id: str
    section: str
    title: str
    kind: CheckKind
    severity: Severity = "required"
    # для setting_text / custom_about
    setting_keys: tuple[str, ...] = ()
    locales: tuple[str, ...] = ("ru", "en")
    min_chars: int = 1
    # для plugin*
    plugin_keys: tuple[str, ...] = ()
    expect_enabled: bool | None = None
    condition: str | None = None  # has_doi | in_scopus
    # подсказки
    doc_url: str = ""
    note_hint: str = ""


SECTION_TITLES: dict[str, str] = {
    "pages": "Управление страницами",
    "modules_gateway": "Управление модулями → Шлюзы",
    "modules_pubids": "Управление модулями → Публичные идентификаторы",
    "modules_generic": "Управление модулями → Основные модули",
    "modules_metrics": "Управление модулями → Метрики",
    "step1": "Шаг 1. Детали",
    "step2": "Шаг 2. Политика",
    "step3": "Шаг 3. Приём статей",
    "step4": "Шаг 4. Управление",
    "step5": "Шаг 5. Вид журнала",
}


# Плагины из чек-листа, которые могут быть включены (whitelist для «остальные выключены»)
ALLOWED_ENABLED_GENERIC: frozenset[str] = frozenset(
    {
        "browseplugin",
        "coinsplugin",
        "customblockmanagerplugin",
        "driverplugin",
        "pdfjsviewerplugin",
        "sehlplugin",
        "staticpagesplugin",
        "tinymceplugin",
        "webfeedplugin",
        "fundrefplugin",
        "acronplugin",
        # часто штатные / не из «остальных» в смысле чек-листа
        "customlocaleplugin",
        "referralplugin",  # может быть «распознавание» / referral — не штрафуем как лишний
    }
)


DEFAULT_SETUP_CHECKLIST: tuple[ChecklistItem, ...] = (
    # --- Управление страницами ---
    ChecklistItem(
        "pages.lang.ru",
        "pages",
        "Язык журнала: русский (интерфейс, отправка, формы)",
        "locale_flags",
        setting_keys=("supportedLocales", "supportedSubmissionLocales", "supportedFormLocales"),
        locales=("ru",),
        doc_url="https://docs.rfbr.ru/doc/yazyki-v-zhurnale-R01seFFrAg",
    ),
    ChecklistItem(
        "pages.lang.en",
        "pages",
        "Язык журнала: английский (интерфейс, отправка, формы)",
        "locale_flags",
        setting_keys=("supportedLocales", "supportedSubmissionLocales", "supportedFormLocales"),
        locales=("en",),
        doc_url="https://docs.rfbr.ru/doc/yazyki-v-zhurnale-R01seFFrAg",
    ),
    ChecklistItem(
        "pages.reader_tools",
        "pages",
        "Инструменты читателя: включены и настроены",
        "reader_tools",
        doc_url="https://docs.rfbr.ru/doc/instrumenty-chitatelya-sgqJF9QfIm",
    ),
    ChecklistItem(
        "pages.editorial_board",
        "pages",
        "Редакция журнала: корректно отображается (RU/EN)",
        "board",
        setting_keys=("boardCustomText", "boardEnabled"),
        doc_url="https://docs.rfbr.ru/doc/redkollegiya-kak-vnesti-informaciyu-KO0EAvM4Gd",
    ),
    ChecklistItem(
        "pages.section_articles",
        "pages",
        "Раздел «Статьи»: название/сокращение RU+EN; скрыт в «О журнале»; только редакторы",
        "sections_articles",
        doc_url="https://docs.rfbr.ru/doc/razdely-i-rubriki-zhurnala-8EwNvt3xjH",
    ),
    ChecklistItem(
        "pages.sections_other",
        "pages",
        "Созданы и настроены прочие разделы журнала",
        "sections_other",
        doc_url="https://docs.rfbr.ru/doc/razdely-i-rubriki-zhurnala-8EwNvt3xjH",
    ),
    # --- Шлюзы ---
    ChecklistItem(
        "modules.mets_gateway",
        "modules_gateway",
        "Плагин шлюза METS: выключен",
        "plugin",
        plugin_keys=("METSGatewayPlugin",),
        expect_enabled=False,
    ),
    ChecklistItem(
        "modules.recognition",
        "modules_gateway",
        "Плагин распознавания: включен",
        "plugin",
        plugin_keys=("ResolverPlugin", "referralplugin", "RecommendBySimilarityPlugin"),
        expect_enabled=True,
        note_hint="В экспорте ищем Resolver/Referral/RecommendBySimilarity.",
    ),
    # --- PubIds (в чек-листе дважды — объединяем) ---
    ChecklistItem(
        "modules.doi",
        "modules_pubids",
        "DOI: включен, настроен (doiPrefix), если журнал присваивает DOI",
        "plugin",
        plugin_keys=("DOIPubIdPlugin",),
        expect_enabled=True,
        doc_url="https://docs.rfbr.ru/doc/doi-i-edn-kak-nastroit-moduli-publichnyh-identifikatorov-GtLjeoHspG",
    ),
    ChecklistItem(
        "modules.edn",
        "modules_pubids",
        "EDN: включен",
        "plugin",
        plugin_keys=("EDNPubIdPlugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.urn",
        "modules_pubids",
        "URN: выключен",
        "plugin",
        plugin_keys=("URNPubIdPlugin",),
        expect_enabled=False,
    ),
    ChecklistItem(
        "modules.url_pubid",
        "modules_pubids",
        "URL (pubIds): выключен",
        "plugin",
        plugin_keys=("OpenUrlPubIdPlugin", "UrlPubIdPlugin", "PublicURLPlugin"),
        expect_enabled=False,
        note_hint="Если плагин отсутствует в экспорте — считаем выключенным.",
    ),
    # --- Основные модули ---
    ChecklistItem(
        "modules.browse",
        "modules_generic",
        "Плагин «Браузер»: включен",
        "browse_plugin",
        plugin_keys=("browseplugin",),
    ),
    ChecklistItem(
        "modules.coins",
        "modules_generic",
        "Плагин «COinS»: включен",
        "plugin",
        plugin_keys=("coinsplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.custom_blocks",
        "modules_generic",
        "Управление блоками пользователя: включен",
        "plugin",
        plugin_keys=("customblockmanagerplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.driver",
        "modules_generic",
        "DRIVER: включен",
        "plugin",
        plugin_keys=("driverplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.pdfjs",
        "modules_generic",
        "PDF-просмотрщик PDF.JS: включен",
        "plugin",
        plugin_keys=("pdfjsviewerplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.sehl",
        "modules_generic",
        "SEHL: включен",
        "plugin",
        plugin_keys=("sehlplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.static_pages",
        "modules_generic",
        "Статические страницы: включен",
        "plugin",
        plugin_keys=("staticpagesplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.tinymce",
        "modules_generic",
        "TinyMCE: включен",
        "plugin",
        plugin_keys=("tinymceplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.webfeed",
        "modules_generic",
        "Новостная лента выпуска: включен (страницы выпуска / текущий выпуск)",
        "webfeed_plugin",
        plugin_keys=("webfeedplugin",),
    ),
    ChecklistItem(
        "modules.fundref",
        "modules_generic",
        "FundRef: включен, база организаций обновлена",
        "plugin",
        plugin_keys=("fundrefplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "modules.acron",
        "modules_generic",
        "ACRON: включен (администратор)",
        "plugin",
        plugin_keys=("acronPlugin", "acronplugin"),
        expect_enabled=True,
        severity="recommended",
        note_hint="В экспорте журнала у ACRON часто нет enabled — достаточно наличия плагина.",
    ),
    ChecklistItem(
        "modules.extra_off",
        "modules_generic",
        "Остальные основные модули: выключены",
        "extra_generic_plugins",
        severity="recommended",
    ),
    # --- Метрики ---
    ChecklistItem(
        "metrics.dimensions",
        "modules_metrics",
        "Dimensions: включен, если у журнала есть DOI",
        "plugin_conditional",
        plugin_keys=("dimensionsplugin",),
        expect_enabled=True,
        condition="has_doi",
    ),
    ChecklistItem(
        "metrics.plumx",
        "modules_metrics",
        "PlumX: включен, если журнал в Scopus",
        "plugin_conditional",
        plugin_keys=("plumxplugin",),
        expect_enabled=True,
        condition="in_scopus",
    ),
    ChecklistItem(
        "metrics.citedby",
        "modules_metrics",
        "Cited-by: включен, если у журнала есть DOI",
        "plugin_conditional",
        plugin_keys=("crossrefcitedbyplugin",),
        expect_enabled=True,
        condition="has_doi",
    ),
    ChecklistItem(
        "metrics.alm",
        "modules_metrics",
        "ALM: включен",
        "plugin",
        plugin_keys=("almplugin",),
        expect_enabled=True,
    ),
    ChecklistItem(
        "metrics.altmetrics",
        "modules_metrics",
        "Altmetrics: выключен",
        "plugin",
        plugin_keys=("altmetricsplugin",),
        expect_enabled=False,
    ),
    ChecklistItem(
        "metrics.publons",
        "modules_metrics",
        "Publons: выключен",
        "plugin",
        plugin_keys=("PublonsPlugin", "publonsbadgeplugin", "PublonsReviewerConnectPlugin"),
        expect_enabled=False,
    ),
    # --- Шаг 1 ---
    ChecklistItem("step1.title", "step1", "1.1 Название (RU/EN)", "setting_text", setting_keys=("title",)),
    ChecklistItem("step1.initials", "step1", "1.1 Инициалы (RU/EN)", "setting_text", setting_keys=("initials",)),
    ChecklistItem(
        "step1.abbreviation",
        "step1",
        "1.1 Сокращённое название (RU/EN)",
        "setting_text",
        setting_keys=("abbreviation",),
    ),
    ChecklistItem("step1.issn", "step1", "1.1 ISSN (печатный и/или онлайн)", "setting_text", setting_keys=("printIssn", "onlineIssn"), locales=()),
    ChecklistItem("step1.elibrary", "step1", "1.1 ID eLibrary", "setting_text", setting_keys=("elibraryId",), locales=()),
    ChecklistItem(
        "step1.mailing",
        "step1",
        "1.1 Почтовый адрес и карта (RU/EN)",
        "map_address",
        setting_keys=("mailingAddress",),
    ),
    ChecklistItem(
        "step1.board",
        "step1",
        "1.2 Редакция: отображается по образцу",
        "board",
        setting_keys=("boardCustomText",),
    ),
    ChecklistItem(
        "step1.support",
        "step1",
        "1.3 Техническая поддержка: имя и e-mail",
        "setting_text",
        setting_keys=("supportName", "supportEmail"),
    ),
    ChecklistItem(
        "step1.email",
        "step1",
        "1.4 Исходящая почта: заполнена или глобальные настройки сайта",
        "email_outgoing",
    ),
    ChecklistItem(
        "step1.publisher",
        "step1",
        "1.5 Издатель",
        "setting_text",
        setting_keys=("publisherInstitution", "publisherNote"),
        locales=("ru", "en"),
    ),
    ChecklistItem(
        "step1.sponsors",
        "step1",
        "1.6 Спонсоры: не заполнено или заполнено корректно",
        "setting_present_or_empty_ok",
        setting_keys=("sponsors", "contributorNote"),
        severity="info",
    ),
    ChecklistItem(
        "step1.funding",
        "step1",
        "1.7 Финансовая поддержка: не заполнено или заполнено корректно",
        "setting_present_or_empty_ok",
        setting_keys=("contributors", "contributorNote"),
        severity="info",
    ),
    ChecklistItem(
        "step1.indexing_kw",
        "step1",
        "1.8 Индексация / ключевые слова (через запятую)",
        "setting_text",
        setting_keys=("searchKeywords",),
        min_chars=3,
    ),
    ChecklistItem(
        "step1.history",
        "step1",
        "1.9 История журнала: заполнена или пусто",
        "setting_present_or_empty_ok",
        setting_keys=("history",),
        severity="info",
    ),
    # --- Шаг 2 ---
    ChecklistItem(
        "step2.focus",
        "step2",
        "2.1 Предметная область и цели журнала",
        "custom_about",
        setting_keys=("focusAndScope",),
        note_hint="Ищем focusAndScope или customAbout (тематика / aims / focus).",
    ),
    ChecklistItem(
        "step2.review",
        "step2",
        "2.2 Принципы рецензирования",
        "setting_text",
        setting_keys=("reviewPolicy",),
        min_chars=40,
    ),
    # --- Шаг 3 ---
    ChecklistItem(
        "step3.guidelines",
        "step3",
        "3.2 Правила для авторов",
        "setting_text",
        setting_keys=("authorGuidelines",),
        min_chars=40,
    ),
    ChecklistItem(
        "step3.checklist",
        "step3",
        "3.2 Список требований к статьям (пункты)",
        "submission_checklist",
    ),
    ChecklistItem(
        "step3.copyright",
        "step3",
        "3.3 Условия использования: правообладатель и лицензия",
        "copyright_license",
        setting_keys=("copyrightNotice", "copyrightHolderType", "licenseURL"),
    ),
    ChecklistItem(
        "step3.submission_ack",
        "step3",
        "3.7 Уведомление о подаче рукописи",
        "setting_bool",
        setting_keys=("copySubmissionAckPrimaryContact", "copySubmissionAckSpecified"),
        note_hint="Должен быть включён copySubmissionAckPrimaryContact и/или указан адрес.",
    ),
    # --- Шаг 4 ---
    ChecklistItem(
        "step4.access",
        "step4",
        "4.1 Доступ и роли регистрации",
        "setting_bool",
        setting_keys=("allowRegAuthor", "allowRegReader", "allowRegReviewer", "disableUserReg"),
        note_hint="Ожидается: регистрация авторов/читателей/рецензентов не отключена глобально.",
    ),
    ChecklistItem(
        "step4.library",
        "step4",
        "4.1 Режим библиотеки: если включён — указан URL",
        "library_mode",
    ),
    ChecklistItem(
        "step4.issue_id",
        "step4",
        "4.2 Формат идентификации выпусков",
        "issue_identification",
    ),
    ChecklistItem(
        "step4.pagination",
        "step4",
        "4.3 Пагинация: включена",
        "setting_bool",
        setting_keys=("enablePageNumber",),
    ),
    # --- Шаг 5 ---
    ChecklistItem(
        "step5.home_header",
        "step5",
        "5.1 Верхний колонтитул главной: название",
        "setting_text",
        setting_keys=("homeHeaderTitle",),
    ),
    ChecklistItem(
        "step5.thumbnail",
        "step5",
        "5.1 Миниатюра загружена",
        "setting_text",
        setting_keys=("journalThumbnail",),
        min_chars=1,
    ),
    ChecklistItem(
        "step5.description",
        "step5",
        "5.2 Описание журнала на главной",
        "setting_text",
        setting_keys=("description",),
        min_chars=40,
    ),
    ChecklistItem(
        "step5.cover",
        "step5",
        "5.2 Обложка загружена",
        "setting_text",
        setting_keys=("homepageImage",),
    ),
    ChecklistItem(
        "step5.current_issue",
        "step5",
        "5.2 Текущий выпуск отображается",
        "setting_bool",
        setting_keys=("displayCurrentIssue",),
    ),
    ChecklistItem(
        "step5.additional",
        "step5",
        "5.2 Дополнительное содержание",
        "setting_text",
        setting_keys=("additionalHomeContent",),
        min_chars=10,
        severity="recommended",
    ),
    ChecklistItem(
        "step5.page_header",
        "step5",
        "5.3 Колонтитул всех страниц: название",
        "setting_text",
        setting_keys=("pageHeaderTitle",),
    ),
    ChecklistItem(
        "step5.dates",
        "step5",
        "5.9 Даты: выбраны к отображению",
        "dates_display",
    ),
)
