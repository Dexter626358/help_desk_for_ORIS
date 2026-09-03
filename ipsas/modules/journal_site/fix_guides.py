"""Краткие решения «как исправить» по инструкциям настройки журнала (БАЗА)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True, slots=True)
class FixGuide:
    """Практическая подсказка для проверяющего / редакции."""

    path: str
    role: str = "Издатель"
    steps: tuple[str, ...] = ()
    doc_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "role": self.role,
            "steps": list(self.steps),
            "doc_url": self.doc_url,
        }


_DOC_5_STEPS = "https://docs.rfbr.ru/doc/nastrojka-sajta-zhurnala-na-platforme-xwmhtcejap"
_DOC_LANG = "https://docs.rfbr.ru/doc/yazyki-v-zhurnale-R01seFFrAg"
_DOC_BOARD = "https://docs.rfbr.ru/doc/redkollegiya-kak-vnesti-informaciyu-KO0EAvM4Gd"
_DOC_SECTIONS = "https://docs.rfbr.ru/doc/razdely-i-rubriki-zhurnala-8EwNvt3xjH"
_DOC_RT = "https://docs.rfbr.ru/doc/instrumenty-chitatelya-sgqJF9QfIm"
_DOC_MAP = "https://docs.rfbr.ru/doc/karta-instrukciya-po-nastrojke-HRdfuJ0SEJ"
_DOC_SMTP = "https://docs.rfbr.ru/doc/pochta-cherez-protokol-smtp-nastrojka-t1I6xCCthE"
_DOC_DOI = "https://docs.rfbr.ru/doc/doi-i-edn-kak-nastroit-moduli-publichnyh-identifikatorov-GtLjeoHspG"
_DOC_USERS = "https://docs.rfbr.ru/doc/polzovateli-i-roli-upravlenie"

_PATH_LANG = "Личный кабинет → Издатель → Управление страницами → Языки"
_PATH_BOARD_PAGE = "Личный кабинет → Издатель → Редакция журнала"
_PATH_SECTIONS = "Личный кабинет → Редактор/Издатель → Разделы"
_PATH_RT = "Личный кабинет → Издатель → Инструменты → Настройки инструментов читателя"
_PATH_SETUP = "Личный кабинет → Издатель → Настройка журнала"
_PATH_PLUGINS = "Личный кабинет → Издатель → Управление модулями"
_PATH_METRICS = "Личный кабинет → Издатель → Управление модулями → Метрики"

_FIX_BY_ID: dict[str, FixGuide] = {
    "pages.lang.ru": FixGuide(
        path=_PATH_LANG,
        steps=(
            "Включите русский язык для параметров «интерфейс», «отправка рукописи» и «формы».",
            "Нажмите «Сохранить».",
        ),
        doc_url=_DOC_LANG,
    ),
    "pages.lang.en": FixGuide(
        path=_PATH_LANG,
        steps=(
            "Включите английский язык для параметров «интерфейс», «отправка рукописи» и «формы».",
            "Русский и английский обязательны для корректной индексации.",
            "Нажмите «Сохранить».",
        ),
        doc_url=_DOC_LANG,
    ),
    "pages.reader_tools": FixGuide(
        path=_PATH_RT,
        steps=(
            "Включите инструменты читателя.",
            "Рекомендуется не включать пункт «Добавить комментарий» (риск спама).",
            "Сохраните настройки.",
        ),
        doc_url=_DOC_RT,
    ),
    "pages.editorial_board": FixGuide(
        path=_PATH_BOARD_PAGE,
        role="Издатель",
        steps=(
            "Выберите «Произвольный текст на странице».",
            "Заполните сведения о редколлегии на русском и английском языках.",
            "Сохраните изменения.",
        ),
        doc_url=_DOC_BOARD,
    ),
    "pages.section_articles": FixGuide(
        path=_PATH_SECTIONS,
        role="Редактор или Издатель",
        steps=(
            "Откройте раздел «Статьи» (или создайте его).",
            "Укажите название и сокращение на русском и английском.",
            "При необходимости включите скрытие раздела из «О журнале» и ограничение «только редакторы».",
            "Сохраните раздел.",
        ),
        doc_url=_DOC_SECTIONS,
    ),
    "pages.sections_other": FixGuide(
        path=_PATH_SECTIONS,
        role="Редактор или Издатель",
        steps=(
            "Создайте нужные разделы через «Создать раздел».",
            "Для каждого раздела укажите название на русском и английском.",
            "Сохраните изменения.",
        ),
        doc_url=_DOC_SECTIONS,
    ),
    "modules.doi": FixGuide(
        path=f"{_PATH_PLUGINS} → Публичные идентификаторы",
        steps=(
            "Включите плагин DOI.",
            "Укажите префикс DOI журнала.",
            "Сохраните настройки.",
        ),
        doc_url=_DOC_DOI,
    ),
    "modules.edn": FixGuide(
        path=f"{_PATH_PLUGINS} → Публичные идентификаторы",
        steps=("Включите модуль EDN и сохраните настройки.",),
        doc_url=_DOC_DOI,
    ),
    "modules.browse": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите плагин «Браузер» и сохраните.",),
    ),
    "modules.webfeed": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 4. Управление → Объявления",
        steps=(
            "Включите новостную ленту выпуска.",
            "Укажите отображение на страницах выпуска / текущего выпуска.",
            "Сохраните настройки.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-4-upravlenie",
    ),
    "modules.coins": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите плагин COinS и сохраните.",),
    ),
    "modules.custom_blocks": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите «Управление блоками пользователя» и сохраните.",),
    ),
    "modules.driver": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите плагин DRIVER и сохраните.",),
    ),
    "modules.pdfjs": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите PDF.JS и сохраните.",),
    ),
    "modules.sehl": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите плагин SEHL и сохраните.",),
    ),
    "modules.static_pages": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите плагин статических страниц и сохраните.",),
    ),
    "modules.tinymce": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите TinyMCE и сохраните.",),
    ),
    "modules.fundref": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Включите FundRef и сохраните.",),
    ),
    "modules.mets_gateway": FixGuide(
        path=f"{_PATH_PLUGINS} → Шлюзы",
        steps=("Выключите плагин шлюза METS и сохраните.",),
    ),
    "modules.urn": FixGuide(
        path=f"{_PATH_PLUGINS} → Публичные идентификаторы",
        steps=("Выключите модуль URN и сохраните.",),
    ),
    "modules.url_pubid": FixGuide(
        path=f"{_PATH_PLUGINS} → Публичные идентификаторы",
        steps=("Выключите публичный идентификатор URL и сохраните.",),
    ),
    "modules.extra_off": FixGuide(
        path=f"{_PATH_PLUGINS} → Основные модули",
        steps=("Выключите лишние модули, не входящие в эталон настройки БАЗА.",),
    ),
    "modules.acron": FixGuide(
        path="Уровень администратора сайта → плагины",
        role="Администратор сайта",
        steps=("Убедитесь, что ACRON включён на уровне сайта (не журнала).",),
    ),
    "modules.recognition": FixGuide(
        path=f"{_PATH_PLUGINS} → Шлюзы / основные модули",
        steps=("Включите плагин распознавания по инструкции и сохраните.",),
    ),
    "metrics.dimensions": FixGuide(
        path=_PATH_METRICS,
        steps=(
            "При наличии DOI включите метрику Dimensions.",
            "Сохраните настройки.",
        ),
    ),
    "metrics.plumx": FixGuide(
        path=_PATH_METRICS,
        steps=(
            "Если журнал индексируется в Scopus — включите PlumX.",
            "Сохраните настройки.",
        ),
    ),
    "metrics.citedby": FixGuide(
        path=_PATH_METRICS,
        steps=(
            "При наличии DOI включите Cited-by (нужен кабинет Crossref).",
            "Сохраните настройки.",
        ),
    ),
    "metrics.alm": FixGuide(
        path=_PATH_METRICS,
        steps=("Включите плагин ALM и сохраните.",),
    ),
    "metrics.altmetrics": FixGuide(
        path=_PATH_METRICS,
        steps=("Выключите метрику Altmetrics и сохраните.",),
    ),
    "metrics.publons": FixGuide(
        path=_PATH_METRICS,
        steps=("Выключите метрику Publons и сохраните.",),
    ),
    "step1.title": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Общая информация",
        steps=(
            "Заполните название журнала на русском и английском языках.",
            "Нажмите «Сохранить и продолжить».",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.initials": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Общая информация",
        steps=(
            "Укажите инициалы журнала на русском и английском (используются в теме писем).",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.abbreviation": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Общая информация",
        steps=(
            "Заполните сокращённое название на русском и английском.",
            "Если официального сокращения нет, используйте полное название.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.issn": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Общая информация",
        steps=(
            "Укажите хотя бы один ISSN (печатный и/или онлайн).",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.elibrary": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Общая информация",
        steps=("Укажите идентификатор журнала в eLIBRARY.RU и сохраните.",),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.mailing": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Почтовый адрес",
        steps=(
            "Заполните почтовый адрес на русском и английском.",
            "Рекомендуется вставить карту через исходный код (iframe Google/Яндекс).",
            "Сохраните шаг.",
        ),
        doc_url=_DOC_MAP,
    ),
    "step1.board": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Редакция",
        steps=(
            "Заполните контакты редакции (ФИО, e-mail, телефон, адрес) на русском и английском.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.support": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Техническая поддержка",
        steps=(
            "Заполните по шаблону: "
            "«Служба поддержки Национальной платформы научных журналов», "
            "e-mail journals_support@rcsi.science.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.email": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Настройка исходящей почты",
        steps=(
            "Проверьте исходящий адрес журнала.",
            "Для снижения риска попадания в «Спам» настройте SMTP.",
            "Сохраните шаг.",
        ),
        doc_url=_DOC_SMTP,
    ),
    "step1.publisher": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Издатель",
        steps=(
            "Укажите наименование издателя и сведения об издателе на русском и английском.",
            "В поле «Организация» укажите официальное английское название для Crossref.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step1.indexing_kw": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали → Индексация",
        steps=(
            "Добавьте ключевые слова на русском и английском, разделяя их запятыми.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step2.focus": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 2. Политика → Предметная область и цели",
        steps=(
            "Заполните предметную область и цели журнала на русском и английском.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-2-politika",
    ),
    "step2.review": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 2. Политика → Рецензирование",
        steps=(
            "Заполните «Принципы рецензирования» на русском и английском.",
            "Для уровня БАЗА достаточно принципов и любой радиокнопки варианта сопровождения.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-2-politika",
    ),
    "step3.guidelines": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 3. Приём статей → Правила для авторов",
        steps=(
            "Разместите правила для авторов на русском и английском.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-3-priem-statej",
    ),
    "step3.checklist": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 3. Приём статей → Список требований к статьям",
        steps=(
            "Добавьте пункты требований на русском и английском языках.",
            "Автор должен отметить все пункты, чтобы завершить подачу.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-3-priem-statej",
    ),
    "step3.copyright": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 3. Приём статей → Условия использования",
        steps=(
            "Заполните текст об авторских правах на русском и английском.",
            "Укажите правообладателя и сведения о лицензии.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-3-priem-statej",
    ),
    "step3.submission_ack": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 3. Приём статей → Уведомление о подаче рукописи",
        steps=(
            "Включите уведомление контактному лицу журнала "
            "либо укажите отдельный e-mail для копии.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-3-priem-statej",
    ),
    "step4.access": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 4. Управление → Безопасность и права доступа",
        steps=(
            "Проверьте, что регистрация пользователей не отключена глобально.",
            "Разрешите роли автора, читателя и рецензента при регистрации.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-4-upravlenie",
    ),
    "step4.library": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 4. Управление → Безопасность и права доступа",
        steps=(
            "Если включён режим библиотеки — укажите URL исходного сайта.",
            "Либо выключите режим библиотеки, если авторы должны подавать статьи на Платформе.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-4-upravlenie",
    ),
    "step4.issue_id": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 4. Управление → Планирование выпусков",
        steps=(
            "Задайте формат идентификации выпусков (том, номер и/или год).",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-4-upravlenie",
    ),
    "step4.pagination": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 4. Управление → Пагинация",
        steps=(
            "Включите пагинацию, если издание использует страничную нумерацию.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-4-upravlenie",
    ),
    "step5.home_header": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Верхний колонтитул главной",
        steps=(
            "В поле «Текст заголовка» укажите название журнала на русском и английском.",
            "Сохраните и проверьте отображение на главной.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.thumbnail": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Миниатюра",
        steps=(
            "Загрузите миниатюру либо выберите обложку последнего выпуска.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.description": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Описание журнала",
        steps=(
            "Заполните описание на русском и английском по образцу карточки журнала.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.cover": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Изображение на главной",
        steps=(
            "Загрузите изображение журнала либо используйте обложку последнего выпуска.",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.current_issue": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Текущий выпуск",
        steps=("Включите отображение текущего выпуска на главной и сохраните.",),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.additional": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Дополнительное содержание",
        steps=("При необходимости заполните дополнительное содержание и сохраните.",),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.page_header": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Верхний колонтитул (все страницы)",
        steps=(
            "Заполните название журнала для колонтитула всех страниц на русском и английском "
            "(аналогично п. 5.1).",
            "Сохраните шаг.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
    "step5.dates": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала → Даты",
        steps=(
            "Выберите даты, которые должны отображаться у выпуска и статей.",
            "Сохраните шаг. Не забудьте заполнить английскую вкладку.",
        ),
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
}

_SECTION_FALLBACK: dict[str, FixGuide] = {
    "pages": FixGuide(path="Личный кабинет → Издатель → Управление страницами"),
    "modules_gateway": FixGuide(path=f"{_PATH_PLUGINS} → Шлюзы"),
    "modules_pubids": FixGuide(path=f"{_PATH_PLUGINS} → Публичные идентификаторы"),
    "modules_generic": FixGuide(path=f"{_PATH_PLUGINS} → Основные модули"),
    "modules_metrics": FixGuide(path=_PATH_METRICS),
    "step1": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 1. Детали",
        doc_url=f"{_DOC_5_STEPS}#h-shag-1-detali",
    ),
    "step2": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 2. Политика",
        doc_url=f"{_DOC_5_STEPS}#h-shag-2-politika",
    ),
    "step3": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 3. Приём статей",
        doc_url=f"{_DOC_5_STEPS}#h-shag-3-priem-statej",
    ),
    "step4": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 4. Управление",
        doc_url=f"{_DOC_5_STEPS}#h-shag-4-upravlenie",
    ),
    "step5": FixGuide(
        path=f"{_PATH_SETUP} → Шаг 5. Вид журнала",
        doc_url=f"{_DOC_5_STEPS}#h-shag-5-vid-zhurnala",
    ),
}


def get_fix_guide(
    item_id: str,
    *,
    section: str = "",
    doc_url: str = "",
) -> Optional[FixGuide]:
    """Вернуть подсказку по id пункта или разделу."""
    guide = _FIX_BY_ID.get(item_id)
    if guide is None and section:
        guide = _SECTION_FALLBACK.get(section)
    if guide is None:
        return None
    # если у пункта есть своя ссылка из чек-листа — предпочесть её
    if doc_url and doc_url != guide.doc_url:
        return FixGuide(
            path=guide.path,
            role=guide.role,
            steps=guide.steps,
            doc_url=doc_url,
        )
    return guide


def attach_fix_guide(row: Mapping[str, Any]) -> dict[str, Any]:
    """Добавить поля fix_* в строку отчёта."""
    guide = get_fix_guide(
        str(row.get("id") or ""),
        section=str(row.get("section") or ""),
        doc_url=str(row.get("doc_url") or ""),
    )
    out = dict(row)
    if not guide:
        out.setdefault("fix_path", "")
        out.setdefault("fix_role", "")
        out.setdefault("fix_steps", [])
        out.setdefault("fix_doc_url", "")
        out.setdefault("fix_guide_text", "")
        return out
    steps = list(guide.steps)
    text_parts = [f"Где: {guide.path}"]
    if guide.role:
        text_parts.append(f"Роль: {guide.role}")
    if steps:
        text_parts.append("Как исправить: " + " ".join(f"{i}. {s}" for i, s in enumerate(steps, 1)))
    out.update(
        {
            "fix_path": guide.path,
            "fix_role": guide.role,
            "fix_steps": steps,
            "fix_doc_url": guide.doc_url,
            "fix_guide_text": " · ".join(text_parts),
        }
    )
    return out
