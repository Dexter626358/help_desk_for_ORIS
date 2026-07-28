# Инструкция по деплою на Railway

Руководство по развёртыванию IPSAS (Help Desk for ORIS) на Railway.

## Что нужно в репозитории

- `Procfile` / `railway.json` — команда запуска gunicorn
- `runtime.txt` — версия Python
- `requirements.txt` — зависимости
- `run.py` / `wsgi.py` — WSGI-точка входа (`gunicorn wsgi:app` или `run:app`)

Авторизация и база данных **не используются**: сервисы открываются без логина.

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
```

**Рекомендуется:**
```
LOG_LEVEL=INFO
LOG_TO_FILE=0
```

**Опционально:**
```
MAX_FILE_SIZE=10485760
MAX_CONTENT_LENGTH=10485760
TEMP_FILE_TTL_SECONDS=21600
ISSUE_PARSER_INFLIGHT_TTL_S=900
ISSUE_PARSER_TASK_TTL_S=7200
ISSUE_FETCH_ALLOWED_HOSTS=journals.rcsi.science
MAX_CONCURRENT_JOBS=4
RATE_LIMIT_PER_MINUTE=30
PORT=<Railway задаёт сам>
```

`temp/`, `logs/`, `data/` на эфемерном диске Railway **не переживают** redeploy. Логи — в stdout. Health: `/health/live`, `/health/ready`.

PostgreSQL / `DATABASE_URI` **не нужны**.

### 3. Networking

1. **Settings → Networking → Public Networking** — включите
2. Сгенерируйте домен (`*.up.railway.app`) или подключите свой
3. Healthcheck: путь `/health` (см. `railway.json`)

### 4. Запуск

Start command (уже в `Procfile` / `railway.json`):

```text
gunicorn run:app --bind 0.0.0.0:$PORT --workers 2 --threads 2 --timeout 120
```

- Приложение слушает `$PORT` (обычно 8080 на Railway)
- Фоновые задачи парсера выпуска хранятся в файлах (`temp/issue_metadata_tasks/`), поэтому работают при нескольких workers
- Временные XML/HTML очищаются по TTL (`TEMP_FILE_TTL_SECONDS`, по умолчанию 6 часов)

### 5. Проверка

После деплоя:

- https://<ваш-домен>/health → `{"status":"ok"}`
- https://<ваш-домен>/dashboard — список сервисов
- **Валидатор XML** — `/services/xml-validator` (схема XSD + метаданные; старый `/services/xml-report` редиректит сюда)

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
| 502 / healthcheck fail | Логи gunicorn, пути `/health/live` и `/health/ready` |
| Парсер выпуска «задача не найдена» | Права на `temp/`, TTL задач, свободное место |
| Большой XML не грузится | `MAX_FILE_SIZE` |

## Структура сервисов

- Валидация XML (XSD)
- Анализ XML журнала (метаданные RUS/ENG, источники, авторы)
- Обработка списков литературы
- Парсер выпуска по URL
- CSV PDF выпуска
