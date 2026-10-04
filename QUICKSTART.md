# Быстрый старт IPSAS

## Установка и запуск

### 1. Виртуальное окружение

```bash
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Windows Command Prompt
.venv\Scripts\activate.bat
```

### 2. Зависимости

```bash
pip install -r requirements-dev.txt
# или только runtime:
# pip install -r requirements.txt
```

### 3. Конфиг

```bash
copy .env.example .env
```

При необходимости укажите учётные данные Платформы (`PLATFORM_*` / `RCSI_*`), песочницы (`SANDBOX_*`) и `ISSUE_FETCH_ALLOWED_HOSTS` (для песочницы — также `f23g45.rcsi.science`).

### 4. (Опционально) editable-пакет

```bash
pip install -e .
```

### 5. Запуск

```bash
python run.py
```

Откройте **http://localhost:5000** (или порт из `PORT` / `.env`).

Авторизация в IPSAS не требуется — сервисы открываются с дашборда. Доступ к Платформе / песочнице — через переменные окружения, не через логин в UI.

## Основные сервисы

| Сервис | URL | Кратко |
|--------|-----|--------|
| Валидатор XML | `/services/xml-validator` | XSD и метаданные journal XML |
| JATS для Метафоры | `/services/metafora-jats` | JATS XML или ZIP выпуска → обязательные поля для ИС «Метафора» |
| Редактор XML | `/services/xml-editor` | Правка загруженного journal XML |
| Список литературы | `/bibliography` (хаб) | Нумерация / очистка / формат |
| Добавить PDF в XML | `/services/pdf-matching` | ZIP с PDF → привязка к XML |
| Проверить опубликованный выпуск | `/services/issue-metadata` | Аудит выпуска по URL |
| CSV для загрузки PDF | `/services/issue-pdf-csv` | CSV к выпуску |
| Рисунки → доп. файлы | `/services/issue-supp-images` | images.zip → доп. файлы статей |
| Обновление ENG-метаданных | `/services/eng-metadata` | ZIP JSON+PDF → подготовка / apply |
| Архивация «Новые» по отправителю | `/services/archive-by-sender` | URL журнала → архив без письма |
| Настройка журнала в песочнице | `/services/sandbox-journal-setup` | URL ветки f23g45 → базовая настройка |
| Проверить сайт журнала | `/services/journal-site-check` | Файл `.data` / чек-лист БАЗА |

В отчёте «Проверить сайт журнала» в подробном блоке показываются только пункты, которые ещё нужно настроить или проверить (выполненные скрыты). Документация БАЗА: [modulbaza](https://docs.rcsi.science/s/modulbaza).

### JATS для Метафоры

Загрузка **одного** `.xml` статьи или **ZIP выпуска** с несколькими JATS XML внутри  
(`/services/metafora-jats`). Без сети и без XSD: проверяются обязательные поля для отправки в Метафору.

| Код | Что проверяется |
|-----|-----------------|
| M001 | Тип публикации (`article-type`) |
| M002 | Название |
| M003 | Дата публикации (формат; не позже сегодня+2 дня) |
| M004 | Страницы `fpage`+`lpage` или `elocation-id`; без длинного тире в диапазоне |
| M005 | DOI опционален; если есть — формат, без кириллицы |
| M006 | EDN опционален; если есть — 6 лат. букв/цифр |
| M007/M008 | Авторы (для научных типов) |
| M009 | Список литературы (`ref-list/ref`) для `research-article` / `review-article` |
| M010 | Аффилиации авторов — **справочно**, на отправку не влияет |

В отчёте: название (предпочтительно RU), тип по-русски, ссылка из `self-uri`, значения даты/страниц/DOI/EDN/авторов.  
Статьи с ошибками или справочными замечаниями раскрыты сразу; полностью OK — свёрнуты.

CLI:

```bash
python -m ipsas.cli validate article.xml
python -m ipsas.cli validate issue.zip
python -m ipsas.cli validate article.xml --json
```

Коды выхода: `0` — все OK, `1` — ошибки Метафоры, `2` — файл/ZIP не разобрать.

Архивация по отправителю и настройка песочницы по умолчанию в режиме **только проверка** (dry-run); для записи снимите галочку на форме (CLI песочницы: `--apply`).

CLI песочницы:

```bash
python setup_sandbox_journal.py "https://f23g45.rcsi.science/257/index"
python setup_sandbox_journal.py "https://f23g45.rcsi.science/257/index" --apply
```

## Остановка сервера

`Ctrl+C` в терминале.
