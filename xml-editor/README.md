# Редактор XML

> **Интегрировано в IPSAS.** Основной вход: меню «Исправление XML» → «Редактор XML»
> (`/services/xml-editor`), код — `ipsas/modules/xml_editor` и `ipsas/web/xml_editor.py`.

Этот каталог сохранён как автономный MVP / эталон. Для повседневной работы
запускайте основное приложение:

```bash
python run.py
```

Откройте в меню **Исправление XML → Редактор XML**.

Автономный запуск (опционально):

```bash
cd xml-editor
pip install -r requirements.txt
flask --app app run --debug
```

Пользователь загружает XML, правит поля статьи через форму (без ручного редактирования
тегов) и скачивает исправленный файл. Исходное XML-дерево сохраняется; меняются только
связанные элементы.

## Поддерживаемый формат XML

Изучен по парсеру IPSAS (`ipsas/modules/journal_xml`) и образцу `samples/sample.xml`:

| Блок | Путь |
|------|------|
| Статьи | `.//issue/articles/article` |
| Название | `./artTitles/artTitle[@lang='RUS'\|'ENG']` |
| Авторы | `./authors/author/individInfo[@lang=…]` |
| Аннотации | `./abstracts/abstract[@lang=…]` |
| Ключевые слова | `./keywords/kwdGroup[@lang=…]/keyword` |
| Литература | `./references/reference/refInfo/text` |
| DOI | `./doi` или `./codes/doi` |
| УДК | `./codes/udk` |
| Страницы | `./pages` (`1-10`) |

XPath вынесены в словарь `XML_PATHS` в `xml_editor/utils.py`.

### Ограничения первой версии

- Поддерживается основной journal XML; JATS (`<ref-list>`) не редактируется.
- Ключевые слова — одна строка через `;`.
- Литература редактируется целиком, без разбора на поля.
- Вложенная разметка внутри `artTitle`/`abstract` при сохранении из формы заменяется текстом поля.
- Нет БД, аккаунтов и внешних API.

## Установка

```bash
cd xml-editor
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

Linux / macOS:

```bash
source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

## Запуск

```bash
flask --app app run --debug
```

Или:

```bash
python app.py
```

По умолчанию: `http://127.0.0.1:5055` (для `python app.py`) или порт Flask (5000).

Секретный ключ:

```bash
set XML_EDITOR_SECRET_KEY=your-secret   # Windows
export XML_EDITOR_SECRET_KEY=your-secret
```

Очистка старых загрузок:

```bash
flask --app app cleanup-uploads
```

## Тесты

```bash
pytest
```

## Структура

```text
xml-editor/
├── app.py
├── config.py
├── xml_editor/          # parser, editor, validator, routes, utils
├── templates/
├── static/
├── uploads/             # временные сессии
├── samples/sample.xml
└── tests/
```

## Как добавить новое поле

1. Добавьте XPath в `XML_PATHS` (`xml_editor/utils.py`).
2. Прочитайте значение в `parser.parse_article`.
3. Запишите точечно в `editor.update_article_from_form` (не пересобирайте документ).
4. Добавьте поле в шаблон формы.
5. При необходимости — проверку в `validator.py` и тест.

Правило: если пользователь не заполнил новое поле и тега не было — **не создавайте** пустой элемент.
