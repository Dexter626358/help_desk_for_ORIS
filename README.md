# Internal Publishing Support System (IPSAS)

Внутренний веб-сервис на Flask для обработки журнальных материалов: XML, библиографии, метаданных выпусков, PDF, CSV и операций на журнальной платформе (OJS).

**Поддерживаемая версия Python: 3.11** (см. `runtime.txt`; в pyproject.toml указано >=3.10).

## Назначение

- Валидация XML по XSD (`schemas/journal3.xsd` / пакетные ресурсы)
- Анализ XML журнала (метаданные RUS/ENG)
- Обработка списков литературы
- Парсер выпуска по URL (OJS/HTML, только чтение публичных страниц)
- Сопоставление PDF / CSV
- Проверка настроек сайта журнала по `.data` (чек-лист БАЗА: [docs.rcsi.science/s/modulbaza](https://docs.rcsi.science/s/modulbaza))
- Обновление ENG-метаданных статей (локальный разбор ZIP; запись на Платформу — опционально)
- Архивация ошибочно загруженных рукописей из «Новые» по отправителю
- Загрузка рисунков выпуска в доп. файлы статей (images.zip)
- Базовая настройка журнала в песочнице (f23g45: языки, раздел, плагины, setup 3–5)
- Встроенный XML-редактор (сессии во временном каталоге)

Без базы данных и без встроенной авторизации. История задач после перезапуска не сохраняется.

## Архитектура

Клиент → (Nginx) → Gunicorn (1 worker) → Flask `create_app` → модули / `TEMP_DIR` / исходящий HTTPS с SSRF-защитой.

Точки входа:

- WSGI: `ipsas.web.wsgi:app` (корневой [wsgi.py](wsgi.py) — совместимость)
- Локально: `ipsas-web` / [run.py](run.py) (dev-сервер Flask; **не** для production)

## Локальный запуск

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
copy .env.example .env
python run.py
```

Откройте **http://localhost:5000** (или порт из `PORT`).

## Production

Подробно: **[DEPLOYMENT.md](DEPLOYMENT.md)**.

```bash
export IPSAS_ENV=production
export SECRET_KEY="…"
export ISSUE_FETCH_ALLOWED_HOSTS="journals.rcsi.science"
gunicorn -c gunicorn.conf.py ipsas.web.wsgi:app
```

**Один worker** обязателен: лимит запросов и фоновый пул хранятся в памяти процесса.

## Docker

```bash
docker build -t ipsas:latest .
docker run --rm -p 8000:8000 \
  -e SECRET_KEY=… \
  -e IPSAS_ENV=production \
  -e ISSUE_FETCH_ALLOWED_HOSTS=journals.example.org \
  ipsas:latest
curl -s http://127.0.0.1:8000/health
```

Railway: сборка через Dockerfile (`railway.json`).

## Переменные окружения

См. [.env.example](.env.example) и таблицу в [DEPLOYMENT.md](DEPLOYMENT.md). Секреты только в окружении сервера.

Для сервисов, ходящих на Платформу под учётной записью редактора (ENG-метаданные apply, архивация «Новые», рисунки выпуска):

- `PLATFORM_USERNAME` / `PLATFORM_PASSWORD` (или `RCSI_USERNAME` / `RCSI_PASSWORD`)
- `PLATFORM_APPLY_ENABLED=1` — разрешить запись ENG-метаданных (по умолчанию выкл.)
- `ISSUE_FETCH_ALLOWED_HOSTS` должен включать хост платформы

Для **настройки журнала в песочнице** (`/services/sandbox-journal-setup`, CLI `setup_sandbox_journal.py`):

- `SANDBOX_BASE_URL` (например `https://f23g45.rcsi.science`)
- `SANDBOX_GATE_*` / `SANDBOX_USER1_*` — HTTP Basic (ворота сайта)
- `SANDBOX_OJS_*` / `SANDBOX_USER2_*` — логин OJS
- в `ISSUE_FETCH_ALLOWED_HOSTS` добавьте хост песочницы (`f23g45.rcsi.science`)

По умолчанию песочница и архивация работают в **dry-run**; запись — только после снятия галочки / флага `--apply`.
## Health

`GET /health` → `{"status":"ok","service":"ipsas"}`

`/health/ready` проверяет запись в temp и наличие `journal3.xsd`.

## Документация

| Файл | Содержание |
|------|------------|
| [QUICKSTART.md](QUICKSTART.md) | Быстрый старт и список сервисов UI |
| [STRUCTURE.md](STRUCTURE.md) | Слои кода, модули, blueprints |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Production, env, Docker, systemd |
| [RAILWAY_DEPLOY.md](RAILWAY_DEPLOY.md) | Railway |

## Тесты

```bash
pytest
ruff check ipsas tests
pip-audit -r requirements.txt
```

## Важно

- Запись на Платформу по умолчанию выключена (`PLATFORM_APPLY_ENABLED=0`); включайте осознанно.
- CSRF включён для POST-форм; `IPSAS_DISABLE_CSRF` в production запрещён.
- `FLASK_DEBUG` не связан с `LOG_LEVEL`; в production debug запрещён.
- Каталог `xml-editor/` — legacy; в production используется `ipsas.modules.xml_editor`.
