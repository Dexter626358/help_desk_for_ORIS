# Структура проекта IPSAS

**IPSAS** (Internal Publishing Support System; репозиторий исторически `Help_desk_for_ORIS`) — Flask-приложение без БД и авторизации: веб-инструменты для XML журналов, списков литературы, метаданных выпусков и связанных задач публикации.

| Роль | Точка входа |
|------|-------------|
| Локальный запуск | `run.py` → `create_app()` |
| Production (Gunicorn) | `wsgi:app` / `ipsas.web.wsgi:app` |
| CLI-отчёт | `python -m ipsas.cli report input.xml` |
| Конфиг пакета | `pyproject.toml` (+ тонкий `setup.py`) |

---

## Слои

```
HTTP (ipsas/web)
  → сценарии (ipsas/services)
  → домен (ipsas/modules/…)
  → общие (ipsas/common)
  → фоновые задачи (ipsas/jobs)
```

Старые пути (`journal_xml_analyzer`, `journal_xml_report/`, `reference_*`, `validator`, `issue_metadata_parser`, `web/issue_metadata_tasks`, …) — **compatibility shim**.

---

## Дерево каталогов

```
Help_desk_for_ORIS/
├── run.py / wsgi.py
├── report_generator.py         # CLI-обёртка → journal_xml
├── pyproject.toml / setup.py / requirements.txt
├── runtime.txt / Procfile / railway.json
├── README.md / QUICKSTART.md / DEPLOYMENT.md / RAILWAY_DEPLOY.md / STRUCTURE.md
├── schemas/                    # XSD (journal3.xsd)
├── ipsas/
│   ├── cli.py
│   ├── common/                 # validation, ssrf, zip_safe, xml_secure,
│   │                           # models (Finding), rate_limit, exceptions
│   ├── config/settings.py      # env, SECRET_KEY, лимиты, TTL, PLATFORM_*, SANDBOX_*
│   ├── jobs/issue_metadata.py  # файловое хранилище task JSON
│   ├── services/               # сценарии UI
│   ├── modules/                # доменная логика (+ shim’ы)
│   ├── utils/                  # logger, temp_files, download_names, …
│   └── web/                    # Flask: app, blueprints, templates, static
├── setup_sandbox_journal.py    # CLI базовой настройки журнала в песочнице
├── upload_issue_images.py      # CLI: рисунки выпуска → доп. файлы
├── tests/
└── temp/ / logs/ / data/       # runtime (на Railway — эфемерны)
```
---

## `ipsas/common/`

| Модуль | Назначение |
|--------|------------|
| `validation.py` | Файл / URL / email (не XSD, не JATS) |
| `ssrf.py` | Защита исходящих URL (`ISSUE_FETCH_ALLOWED_HOSTS`) |
| `zip_safe.py` | Безопасная распаковка ZIP |
| `xml_secure.py` | Безопасный lxml XMLParser |
| `models.py` | `Severity`, `Finding` |
| `rate_limit.py` | Concurrent + rate для POST `/services/*` |
| `exceptions.py` | `IpsasError` и наследники |

Shim: `ipsas.modules.validator` → `common.validation`.

---

## Домен: `ipsas/modules/`

### Journal XML — `parse → validate → analyze → render`

```
journal_xml/
├── models.py / parser.py / validators.py / text_utils.py
├── analyzer.py
├── editorial_letter.py   # письмо для редакции (.txt) после валидатора XML
└── report/renderer.py + standalone.py   # CSS из static/theme.css+app.css
```

API: `ipsas.modules.journal_xml`.  
Shim’ы: `journal_xml_analyzer.py`, `journal_xml_report/`, `xml_report_generator.py`.

### Библиография — `bibliography/`

`processor` / `cleaner` / `formatter`. Shim’ы: `reference_*.py`.

### PDF — `pdf_matching/`

`matcher`, `models`, `zip_names`, `service`. Shim: `pdf_matcher.py`.

### Аудит выпуска — `issue_metadata/`

```
orchestrator.py     # IssueMetadataParser
jats.py / merge.py / lang.py
parsers.py / validators.py / rules.py / models.py
findings.py         # warning_dict, мост к Finding
editorial_letter.py # текст письма для редакции (.txt)
report_summary.py / http_client.py
```

Shim: `issue_metadata_parser.py`.

### Сайт журнала — `journal_site/`

Проверка экспорта OJS `.data` по чек-листу настройки (уровень БАЗА).  
Документация: `docs_urls.py` → коллекция [modulbaza](https://docs.rcsi.science/s/modulbaza).

Ключевые модули: `data_check.py`, `default_setup_checklist.py`, `checklist_messages.py`,
`docs_urls.py`, `fix_guides.py` / `form_guides.py`, `editorial_letter.py`,
`criteria.py` / `evaluate.py` (режим публичного сайта, если используется).

В UI подробного отчёта показываются только незакрытые пункты (статус ≠ «выполнено»).

### ENG-метаданные — `eng_metadata/`

Разбор ZIP (JSON + PDF), сессия правки, подготовка payload, опциональная запись в OJS
(`platform_auth`, `platform_client`, `platform_update`). Флаг `PLATFORM_APPLY_ENABLED`.

### Архивация «Новые» — `archive_by_sender/`

Логика как у корневого `archive_submissions_by_sender.py`: очередь Unassigned →
проверка поля «Отправитель» → `unsuitableSubmission` с кнопкой «Пропустить».

`constants.py` (список ФИО по умолчанию), `parser.py`, `service.py`, `models.py`.

### Рисунки выпуска — `issue_supp_images/`

Сопоставление папок `{start}-{end}_images` из ZIP с интервалами страниц в issueToc,
загрузка в доп. файлы статей. CLI: `upload_issue_images.py`.

### Песочница — `sandbox_journal_setup/`

Базовая настройка ветки журнала на f23g45: GATE (Basic Auth) + OJS login,
языки RU/EN, раздел «Статьи», RT, DOI/EDN, метрики, generic whitelist,
browse (по разделам), setup 3–5 (в т.ч. шаблон содержания главной).

CLI: `setup_sandbox_journal.py "https://f23g45.rcsi.science/257/index"` (`--apply` для записи).
Web: `/services/sandbox-journal-setup`. Env: `SANDBOX_GATE_*` / `SANDBOX_OJS_*` (alias `USER1`/`USER2`).

### Прочее

| Модуль | Назначение |
|--------|------------|
| `xml_validator.py` / `xsd_validator.py` | XSD / синтаксис |
| `issue_pdf_csv_builder.py` | CSV для загрузки PDF |
| `xml_editor/` | Встроенный редактор journal XML |

---

## `ipsas/services/`

| Файл | Сценарий |
|------|----------|
| `validate_xml.py` | XSD + метаданные journal |
| `build_journal_report.py` | HTML-отчёт |
| `process_references.py` | Нумерация / clean / format |
| `match_issue_pdfs.py` | ZIP → PDF в XML |
| `audit_published_issue.py` | Парсинг выпуска + worker |
| `build_issue_pdf_csv.py` | CSV к выпуску |
| `check_journal_site.py` | Проверка `.data` / сайта журнала |
| `eng_metadata_review.py` | Сессия ENG-метаданных |
| `archive_by_sender.py` | Архивация «Новые» по отправителю |

---

## Web (`ipsas/web/`)

- `app.py` — фабрика, health `/health/live` `/health/ready`, rate guard, CSRF, error handlers  
  (тихие 404 для `/favicon.ico` и служебных путей без flash «Страница не найдена»)
- `file_ops.py` — upload/temp/download XML  
- `routes.py` — dashboard + заглушки «В разработке»  
- Blueprints: `xml_validation`, `xml_report`, `reference_*`, `pdf_matching`, `issue_pdf_csv`,
  `issue_metadata`, `journal_site_check`, `eng_metadata`, `archive_by_sender`,
  `issue_supp_images`, `sandbox_journal_setup`, `xml_editor`
- Пакет `issue_metadata/` — `routes.py` + `inflight.py`  
- Shim: `issue_metadata_tasks.py` → `ipsas.jobs.issue_metadata`  
- `templates/` + `static/css/{theme,app}.css`

Префикс маршрутов сервисов: `/services` (кроме `main`).

---

## Сервисы UI ↔ код

| Сервис (UI) | Service | Домен |
|-------------|---------|--------|
| Валидатор XML | `validate_xml` | `journal_xml` + `xml_validator` |
| HTML-отчёт XML | `build_journal_report` | `journal_xml.report` |
| Список литературы | `process_references` | `bibliography` |
| Добавить PDF в XML | `match_issue_pdfs` | `pdf_matching` |
| Проверить выпуск | `audit_published_issue` | `issue_metadata` + `web/issue_metadata` |
| CSV для PDF | `build_issue_pdf_csv` | `issue_pdf_csv_builder` |
| Проверить сайт журнала | `check_journal_site` | `journal_site` |
| ENG-метаданные | `eng_metadata_review` | `eng_metadata` |
| Архивация по отправителю | `archive_by_sender` | `archive_by_sender` |
| Рисунки → доп. файлы | (web `issue_supp_images`) | `issue_supp_images` |
| Настройка в песочнице | (web `sandbox_journal_setup`) | `sandbox_journal_setup` |
| Редактор XML | (web `xml_editor`) | `xml_editor` |

---

## Тесты (`tests/`)

| Группа | Файлы |
|--------|--------|
| Journal XML | `test_xml_report_generator.py` |
| Аудит выпуска | `test_issue_metadata_*.py`, `test_issue_metadata_web_refactor.py` |
| PDF / литература | `test_pdf_matching.py`, `test_reference_processor.py` |
| Сайт журнала | `test_journal_site_*.py` |
| ENG-метаданные | `test_eng_metadata.py` |
| Архивация «Новые» | `test_archive_by_sender.py` |
| Рисунки выпуска | `test_issue_supp_images.py` |
| Песочница | `test_sandbox_journal_setup.py` |
| Безопасность / prod | `test_security.py`, `test_production_hardening.py` |
| Общее | `test_validator.py`, `test_maintainability.py`, `test_download_names.py`, `test_operation_history_and_cleaner.py` |

```bash
pytest
```

---

## Runtime и production

| Каталог / тема | Замечание |
|----------------|-----------|
| `temp/` | Загрузки, отчёты, tasks; TTL; lock’и активных jobs не чистятся |
| `logs/` | Локально файл; в production — stdout (`LOG_TO_FILE=0`) |
| `data/` | Зарезервировано; на Railway без volume не постоянно |
| `SECRET_KEY` | Обязателен при `IPSAS_ENV=production` |
| Лимиты | `MAX_CONTENT_LENGTH`, `MAX_CONCURRENT_JOBS`, `RATE_LIMIT_PER_MINUTE` |
| SSRF | `ISSUE_FETCH_ALLOWED_HOSTS` (**обязателен** в production) |
| Платформа | `PLATFORM_*` / `RCSI_*`, `PLATFORM_APPLY_ENABLED` |
| Песочница | `SANDBOX_GATE_*` / `SANDBOX_OJS_*` (alias `USER1`/`USER2`), `SANDBOX_BASE_URL` |
