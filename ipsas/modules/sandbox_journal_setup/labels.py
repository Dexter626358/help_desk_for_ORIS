"""Русские подписи для отчёта настройки песочницы."""

from __future__ import annotations

# Технические ключи плагинов (lower) → как в интерфейсе / отчётах IPSAS
PLUGIN_LABEL_RU: dict[str, str] = {
    "doipubidplugin": "DOI",
    "ednpubidplugin": "EDN",
    "urnpubidplugin": "URN",
    "browseplugin": "Браузер",
    "coinsplugin": "COinS",
    "customblockmanagerplugin": "Управление блоками пользователя",
    "customlocaleplugin": "Модуль локализации",
    "driverplugin": "DRIVER",
    "pdfjsviewerplugin": "PDF-просмотрщик PDF.JS",
    "sehlplugin": "SEHL",
    "staticpagesplugin": "Статические страницы",
    "tinymceplugin": "TinyMCE",
    "webfeedplugin": "Новостная лента выпуска",
    "fundrefplugin": "FundRef",
    "acronplugin": "Acron",
    "referralplugin": "Модуль обратных ссылок",
    "xmlgalleyplugin": "XML-гранки",
    "dimensionsplugin": "Dimensions",
    "plumxplugin": "PlumX",
    "crossrefcitedbyplugin": "Cited-by",
    "almplugin": "ALM",
    "altmetricsplugin": "Altmetrics",
    "publonsplugin": "Publons",
    "publonsbadgeplugin": "Publons",
    "publonsreviewerconnectplugin": "Publons Reviewer Connect",
}

CITATION_PARSER_RU: dict[str, str] = {
    "26": "FreeCite",
    "27": "ParsCit",
    "28": "ParaCite",
    "29": "RegEx",
}


def plugin_label(key: str) -> str:
    """Человекочитаемое имя плагина."""
    raw = (key or "").strip()
    low = raw.casefold()
    if low in PLUGIN_LABEL_RU:
        return PLUGIN_LABEL_RU[low]
    # acronPlugin → acronplugin
    compact = low.replace("_", "")
    if compact in PLUGIN_LABEL_RU:
        return PLUGIN_LABEL_RU[compact]
    return raw or "плагин"
