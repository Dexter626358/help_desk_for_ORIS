# Развёртывание IPSAS (production)

Внутренний Flask-сервис без БД и без встроенной авторизации.  
Защита доступа — на периметре (VPN / IP allowlist / Basic Auth в Nginx).

## Ограничения архитектуры

- Задачи и история операций хранятся в `TEMP_DIR` / памяти процесса и **не переживают перезапуск**.
- Rate limit и лимит одновременных POST (`RequestGuard`) — **in-memory** → **один Gunicorn worker**.
- Запись метаданных на Платформу (dry-run/apply) **не реализована**: сервис только читает URL и обрабатывает локальные файлы.
- Standalone каталог `xml-editor/` **не** входит в этот production-пакет.

## Требования

- Python **3.11** (см. `runtime.txt`: 3.11.9; минимум в pyproject — 3.10+)
- Исходящий HTTPS к журналам OJS / НППНИ (если используете парсер выпуска по URL)
- Каталоги с правом записи: `TEMP_DIR`, `DATA_DIR`, при `LOG_TO_FILE=true` — `LOGS_DIR`

## Секреты

1. Сгенерируйте длинный `SECRET_KEY`.
2. Положите в `/etc/ipsas/ipsas.env` или Docker secrets / переменные оркестратора.
3. **Не** коммитьте `.env` с реальными значениями.
4. Пример переменных: [`.env.example`](.env.example).

В production без `SECRET_KEY` приложение **не стартует**.

## Переменные окружения

| Переменная | Обязательно в prod | Описание |
|------------|--------------------|----------|
| `IPSAS_ENV` / `FLASK_ENV` | да (`production`) | Режим |
| `SECRET_KEY` | да | Секрет сессий/CSRF |
| `LOG_LEVEL` | нет | `INFO` / `WARNING` / … |
| `LOG_TO_FILE` | нет | В Docker обычно `false` (stdout) |
| `FLASK_DEBUG` | нет | Только локально; в prod запрещён |
| `TEMP_DIR` | рекомендуется | Временные файлы |
| `DATA_DIR` | нет | Runtime-данные |
| `LOGS_DIR` | нет | Файловые логи |
| `MAX_CONTENT_LENGTH` | нет | Лимит тела запроса (байт) |
| `REQUEST_TIMEOUT` | нет | Таймаут исходящих HTTP (с) |
| `ISSUE_FETCH_ALLOWED_HOSTS` | **да** | Allowlist хостов через запятую |
| `SESSION_COOKIE_SECURE` | да за HTTPS | Secure-cookie (по умолчанию true в prod) |
| `TRUST_PROXY_HEADERS` | только за доверенным proxy | Доверять `X-Forwarded-For` |
| `MAX_CONCURRENT_JOBS` | нет (`4`) | Лимит POST + workers фонового пула |
| `MAX_JOB_QUEUE` | нет (`8`) | Очередь фоновых задач парсера |
| `GUNICORN_WORKERS` | держите `1` | Не увеличивать без выноса rate-limit |
| `PORT` | нет | Порт bind |

## Health

```http
GET /health
```

```json
{"status": "ok", "service": "ipsas"}
```

`/health/ready` дополнительно проверяет запись в `TEMP_DIR` и наличие `journal3.xsd`.

## Запуск без Docker (systemd)

```bash
cd /opt/ipsas
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
sudo cp deploy/ipsas.service.example /etc/systemd/system/ipsas.service
# отредактируйте EnvironmentFile=/etc/ipsas/ipsas.env
sudo systemctl daemon-reload
sudo systemctl enable --now ipsas
```

Команда:

```bash
gunicorn -c gunicorn.conf.py ipsas.web.wsgi:app
```

Nginx: [`deploy/nginx.example.conf`](deploy/nginx.example.conf).

## Docker

```bash
docker build -t ipsas:latest .
docker run --rm -p 8000:8000 \
  -e SECRET_KEY="$(openssl rand -hex 32)" \
  -e IPSAS_ENV=production \
  -e ISSUE_FETCH_ALLOWED_HOSTS="journals.example.org" \
  -v ipsas-temp:/var/lib/ipsas/temp \
  ipsas:latest
```

Проверка:

```bash
curl -s http://127.0.0.1:8000/health
```

## Обновление

1. Остановить сервис / контейнер.
2. Сохранить `.env` / volume с temp (при необходимости).
3. Обновить код / образ.
4. `pip install -r requirements.txt` (без Docker).
5. Запустить; проверить `/health`.
6. Прогнать smoke: главная страница, один upload XML.

## Откат

1. Остановить текущую версию.
2. Вернуть предыдущий git tag / Docker image tag.
3. Восстановить прежний `ipsas.env`.
4. Запустить и проверить `/health`.

Незавершённые фоновые разборы выпусков после отката/рестарта будут потеряны — это ожидаемо.

## Сетевые доступы

- Входящий: только из внутренней сети / VPN (Nginx allow / Basic Auth).
- Исходящий: HTTPS к разрешённым хостам OJS (`ISSUE_FETCH_ALLOWED_HOSTS`).
- SSRF: localhost, private IP, link-local и редиректы на них блокируются.

## Railway (legacy)

См. также [RAILWAY_DEPLOY.md](RAILWAY_DEPLOY.md). Стартовая команда унифицирована: `gunicorn wsgi:app … --workers 1`.
