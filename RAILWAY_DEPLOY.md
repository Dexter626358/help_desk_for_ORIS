# Инструкция по деплою на Railway

Руководство по развёртыванию IPSAS (Help Desk for ORIS) на Railway.

Общая production-документация: [DEPLOYMENT.md](DEPLOYMENT.md).

## Что нужно в репозитории

- `Procfile` / `railway.json` — команда запуска gunicorn
- `runtime.txt` — версия Python
- `requirements.txt` — зависимости
- `wsgi.py` — WSGI-точка входа (`gunicorn wsgi:app`)

Авторизация и база данных **не используются**: сервисы открываются без логина.
Ограничьте публичный доступ при необходимости (VPN / IP allowlist / Basic Auth перед приложением).

## Шаги деплоя

### 1. Создать проект

1. Откройте [railway.app](https://railway.app)
2. **New Project** → **Deploy from GitHub repo**
3. Выберите репозиторий `help_desk_for_ORIS`

### 2. Переменные окружения

В **Settings → Variables**:

**Обязательно в production (`IPSAS_ENV=production` или `FLASK_ENV=production`):**
```
SECRET_KEY=<случайная длинная строка>
IPSAS_ENV=production
ISSUE_FETCH_ALLOWED_HOSTS=journals.rcsi.science
```

Без `SECRET_KEY` или `ISSUE_FETCH_ALLOWED_HOSTS` приложение в production **не стартует**.

**Рекомендуется:**
```
LOG_LEVEL=INFO
LOG_TO_FILE=0
FLASK_DEBUG=0
# SESSION_COOKIE_SECURE по умолчанию true в production
```

**Опционально:**
```
MAX_FILE_SIZE=10485760
MAX_CONTENT_LENGTH=10485760
TEMP_FILE_TTL_SECONDS=21600
REQUEST_TIMEOUT=30
ISSUE_PARSER_INFLIGHT_TTL_S=900
ISSUE_PARSER_TASK_TTL_S=7200
MAX_CONCURRENT_JOBS=4
MAX_JOB_QUEUE=8
RATE_LIMIT_PER_MINUTE=30
PORT=<Railway задаёт сам>
```

`temp/`, `logs/`, `data/` на эфемерном диске Railway **не переживают** redeploy. Логи — в stdout. Health: `/health`, `/health/live`, `/health/ready`.

PostgreSQL / `DATABASE_URI` **не нужны**.

### 3. Networking

1. **Settings → Networking → Public Networking** — включите
2. Сгенерируйте домен (`*.up.railway.app`) или подключите свой
3. Healthcheck: путь `/health` (см. `railway.json`)

### 4. Запуск

Railway собирает **Dockerfile** (см. `railway.json`). Команда внутри образа:

```text
gunicorn -c gunicorn.conf.py ipsas.web.wsgi:app
```

- Приложение слушает `$PORT`
- **Один worker обязателен:** rate limit / concurrency (`RequestGuard`) и фоновый пул — in-memory
- Парсер выпуска: только **POST**; фоновые задачи — через ограниченный `ThreadPoolExecutor`
- После redeploy незавершённые running-задачи помечаются как interrupted
- Не задавайте вручную устаревшую команду `gunicorn run:app … --workers 2`

### 5. Проверка

После деплоя:

- https://\<ваш-домен\>/health → `{"status":"ok","service":"ipsas"}`
- https://\<ваш-домен\>/dashboard — список сервисов
- **Валидатор XML** — `/services/xml-validator` (схема XSD + метаданные; старый `/services/xml-report` редиректит сюда)
- Любой POST-сервис из браузера (CSRF включён; токен подставляется формами)

Если сайт не открывается из браузера (`ERR_CONNECTION_TIMED_OUT`), а curl/VPN работает — это сетевая фильтрация до `*.railway.app`, а не ошибка приложения. Помогает VPN или свой домен.

## Локальный запуск

```bash
pip install -r requirements.txt
python run.py
```

Откроется http://127.0.0.1:5000

## Частые проблемы

| Симптом | Что проверить |
|---------|----------------|
| Build OK, сайт не открывается | Public Networking, домен, VPN/провайдер |
| Crash при старте | `SECRET_KEY`, `IPSAS_ENV=production`, логи gunicorn |
| 502 / healthcheck fail | Логи gunicorn, пути `/health`, `/health/live`, `/health/ready` |
| 400 на POST-формах | CSRF: обновите страницу и повторите из браузера |
| Парсер выпуска «задача не найдена» | Права на `temp/`, TTL задач, free disk, redeploy сбросил temp |
| Большой XML не грузится | `MAX_FILE_SIZE` / `MAX_CONTENT_LENGTH` |

## Структура сервисов

- Валидация XML (XSD)
- Анализ XML журнала (метаданные RUS/ENG, источники, авторы)
- Обработка списков литературы
- Парсер выпуска по URL
- CSV PDF выпуска
- Проверка настроек сайта журнала (`.data`)
