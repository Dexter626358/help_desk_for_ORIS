# Развёртывание IPSAS (production)

Внутренний Flask-сервис без БД и без встроенной авторизации.

Основной способ — **Docker Compose за Nginx**. Пошаговая инструкция для сервера:
[`deploy/SERVER_DEPLOY.md`](deploy/SERVER_DEPLOY.md). Этот документ — сводка требований
и ограничений.

## Текущее развёртывание

| Параметр | Значение |
|----------|----------|
| Хост | `jourmatproc` (Ubuntu 24.04 LTS) |
| Адрес | http://172.20.254.133/ — приватный, доступен только из внутренней сети / VPN |
| Каталог | `/opt/ipsas` |
| Схема | Nginx :80 → `127.0.0.1:8000` → контейнер `ipsas` |
| Docker / Compose | 29.8.2 / 2.40.3 |
| Авторизация | нет |
| TLS | нет, только HTTP |

**Обновление:**

```bash
cd /opt/ipsas
git pull
docker compose up -d --build
./deploy/smoke-test.sh
```

> ⚠️ Пароля и TLS в этой схеме нет. Единственная защита — приватность адреса: сервис не
> маршрутизируется в интернет. Если появится публичный доступ или доступ из доверенной, но
> не своей сети, добавьте HTTPS и Basic Auth в Nginx (примеры есть в
> [`deploy/nginx.example.conf`](deploy/nginx.example.conf)).

## Ограничения архитектуры

- Задачи и история операций хранятся в `TEMP_DIR` / памяти процесса и **не переживают перезапуск**.
- Rate limit и лимит одновременных POST (`RequestGuard`) — **in-memory** → **один Gunicorn worker**.
- Запись ENG-метаданных на Платформу по умолчанию **выключена** (`PLATFORM_APPLY_ENABLED=0`).
  При включении нужны учётные данные редактора (`PLATFORM_*` / `RCSI_*`) и allowlist хостов.
- Архивация «Новые» по отправителю и загрузка рисунков выпуска ходят на Платформу под этими
  учётными данными (в UI по умолчанию dry-run).
- Настройка журнала в **песочнице** использует отдельные `SANDBOX_*` (ворота + OJS);
  хост песочницы должен быть в `ISSUE_FETCH_ALLOWED_HOSTS` (например `f23g45.rcsi.science`).
- Standalone каталог `xml-editor/` **не** входит в этот production-пакет.

## Требования

### Docker Compose (основной путь)

- Docker Engine **29.x** и плагин Docker Compose **v2**
- Nginx — как обратный прокси (в текущей схеме без TLS и Basic Auth)
- Python на хосте **не нужен**: он внутри образа

### Без Docker (systemd)

- Python **3.11** (см. `runtime.txt`: 3.11.9; минимум в pyproject — 3.10+)

### Общее

- Исходящий HTTPS к журналам OJS / НППНИ (если используете парсер выпуска по URL)
- Каталоги с правом записи: `TEMP_DIR`, `DATA_DIR`, при `LOG_TO_FILE=true` — `LOGS_DIR`

## Секреты

1. Сгенерируйте длинный `SECRET_KEY`.
2. Положите его в конфиг окружения сервиса:
   - **Docker Compose** — `/opt/ipsas/.env`, шаблон
     [`deploy/ipsas.env.production.example`](deploy/ipsas.env.production.example);
   - **systemd** — `/etc/ipsas/ipsas.env` (путь задаётся в `EnvironmentFile`).
3. Ограничьте доступ: `chmod 600` на файл с секретами.
4. **Не** коммитьте `.env` с реальными значениями — он в `.gitignore`.
5. Примеры переменных: [`.env.example`](.env.example) (разработка) и
   [`deploy/ipsas.env.production.example`](deploy/ipsas.env.production.example) (production).

В production без `SECRET_KEY` приложение **не стартует**.

Ротация `SECRET_KEY` возможна, но аннулирует все открытые сессии — пользователям придётся
заново инициализировать формы.

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
| `SESSION_COOKIE_SECURE` | `false`, если сервис работает по HTTP | Secure-cookie (по умолчанию true в prod). **На HTTP обязательно `false`**: иначе браузер не примет cookie и POST-формы будут отклонены по CSRF |
| `TRUST_PROXY_HEADERS` | **да**, когда сервис за Nginx | Доверять `X-Forwarded-For`. За обратным прокси обязателен: без него все запросы приходят с адреса Nginx и попадают в общий лимит 30 запросов/мин |
| `GUNICORN_BIND` | нет | Адрес bind Gunicorn внутри контейнера (`0.0.0.0:8000`) |
| `MAX_CONCURRENT_JOBS` | нет (`4`) | Лимит POST + workers фонового пула |
| `MAX_JOB_QUEUE` | нет (`8`) | Очередь фоновых задач парсера |
| `PLATFORM_APPLY_ENABLED` | нет (`0`) | `1` — разрешить запись ENG-метаданных в OJS |
| `PLATFORM_BASE_URL` | нет | Базовый URL платформы |
| `PLATFORM_USERNAME` / `PLATFORM_PASSWORD` | для apply / архивации / рисунков | Учётка редактора (или `RCSI_*`) |
| `PLATFORM_COOKIE_FILE` | нет | Файл cookie-сессии |
| `PLATFORM_REQUEST_DELAY` | нет (`0.35`) | Пауза между запросами к платформе (с) |
| `SANDBOX_BASE_URL` | для песочницы | Базовый URL (напр. `https://f23g45.rcsi.science`) |
| `SANDBOX_GATE_USERNAME` / `SANDBOX_GATE_PASSWORD` | для песочницы | HTTP Basic ворот (или `SANDBOX_USER1_*`) |
| `SANDBOX_OJS_USERNAME` / `SANDBOX_OJS_PASSWORD` | для песочницы | Логин OJS (или `SANDBOX_USER2_*`) |
| `SANDBOX_COOKIE_FILE` | нет | Cookie-сессия песочницы |
| `SANDBOX_REQUEST_DELAY` | нет (`0.35`) | Пауза между запросами к песочнице (с) |
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

## Docker Compose (основной путь)

```bash
cp deploy/ipsas.env.production.example .env
# заполните SECRET_KEY и ISSUE_FETCH_ALLOWED_HOSTS, затем:
chmod 600 .env
docker compose up -d --build
docker compose ps
```

Конфигурация — [`docker-compose.yml`](docker-compose.yml). Принятые решения:

- порт публикуется **только на loopback** (`127.0.0.1:8000:8000`): наружу сервис выходит
  через Nginx, напрямую с хоста он недоступен;
- `TEMP_DIR` и `DATA_DIR` — **именованные тома** (`ipsas-temp`, `ipsas-data`); host bind
  mount сломает права непривилегированного пользователя внутри контейнера;
- `SECRET_KEY` и `ISSUE_FETCH_ALLOWED_HOSTS` приходят из `.env`; compose падает на старте,
  если они пустые;
- образ требует **пересборки**: после `git pull` всегда `up -d --build`, иначе compose
  поднимет предыдущую версию образа без всякой ошибки.

Полная инструкция, включая Nginx и диагностику: [`deploy/SERVER_DEPLOY.md`](deploy/SERVER_DEPLOY.md).

## Nginx

Обратный прокси перед приложением: [`deploy/nginx.example.conf`](deploy/nginx.example.conf).
Текущий вариант — только `proxy_pass`, без TLS и без Basic Auth; примеры с HTTPS и паролем
лежат в комментариях того же файла.

Обслуживание: `sudo nginx -t && sudo systemctl reload nginx`. Если после правки видна
стандартная страница `Welcome to nginx!`, значит активен `/etc/nginx/sites-enabled/default` —
удалите его или отключите, симлинк `ipsas.conf` должен быть единственным.

## Docker вручную (для отладки)

```bash
docker build -t ipsas:latest .
docker run --rm -p 127.0.0.1:8000:8000 \
  -e SECRET_KEY="$(openssl rand -hex 32)" \
  -e IPSAS_ENV=production \
  -e ISSUE_FETCH_ALLOWED_HOSTS="journals.example.org" \
  -v ipsas-temp:/var/lib/ipsas/temp \
  ipsas:latest
```

Порт публикуется на `127.0.0.1`, а не на все интерфейсы: `-p 8000:8000` открыл бы сервис
напрямую, в обход Nginx и его настроек доступа.

Проверка:

```bash
curl -s http://127.0.0.1:8000/health
```

## Запуск без Docker (systemd) — альтернатива

Не используется в текущем развёртывании, сохранён для хостов без Docker:

```bash
cd /opt/ipsas
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
sudo cp deploy/ipsas.service.example /etc/systemd/system/ipsas.service
# отредактируйте EnvironmentFile=/etc/ipsas/ipsas.env
sudo systemctl daemon-reload
sudo systemctl enable --now ipsas
```

Команда запуска:

```bash
gunicorn -c gunicorn.conf.py ipsas.web.wsgi:app
```

## Обновление

### Docker Compose

```bash
cd /opt/ipsas
git pull
docker compose up -d --build      # без --build поднимется предыдущая версия образа
./deploy/smoke-test.sh            # 17 проверок, включая /services/*
```

`.env` и именованные тома `git pull` не затрагивает. Адрес проверки в smoke-тесте — первый
позиционный аргумент, по умолчанию `http://127.0.0.1:8000`; против внешнего адреса:

```bash
bash deploy/smoke-test.sh http://127.0.0.1/
```

### systemd (без Docker)

1. `sudo systemctl stop ipsas`
2. Обновить код, `.venv/bin/pip install -r requirements.txt`
3. `sudo systemctl start ipsas`, проверить `/health`
4. Прогнать smoke-тест

## Откат

### Docker Compose

```bash
cd /opt/ipsas
git checkout <предыдущий-коммит>
docker compose up -d --build
```

### systemd

1. Остановить текущую версию.
2. Вернуть предыдущий git tag.
3. Восстановить прежний `/etc/ipsas/ipsas.env`.
4. Запустить и проверить `/health`.

Незавершённые фоновые разборы выпусков после отката/рестарта будут потеряны — это ожидаемо.

## Сетевые доступы

- Входящий: **пароля и IP allowlist в текущем развёртывании нет**. Единственная защита —
  приватность адреса `172.20.254.133`: сервис не маршрутизируется в интернет. Любой, кто
  достигнет этого адреса из внутренней сети, получает доступ ко всему сервису, включая
  операции от имени редактора. При изменении модели доступа добавьте Basic Auth или allowlist
  в Nginx.
- Исходящий: HTTPS к разрешённым хостам OJS (`ISSUE_FETCH_ALLOWED_HOSTS`).
- Для ENG-apply, архивации «Новые», рисунков выпуска исходящие запросы идут с cookie/логином
  редактора (`PLATFORM_*`) — ограничьте доступ к инстансу IPSAS так же жёстко, как к Платформе.
- Для настройки песочницы — отдельные `SANDBOX_*` и хост в allowlist (напр. `f23g45.rcsi.science`).
- SSRF: localhost, private IP, link-local и редиректы на них блокируются.

## Railway (legacy)

См. также [RAILWAY_DEPLOY.md](RAILWAY_DEPLOY.md). Стартовая команда унифицирована: `gunicorn wsgi:app … --workers 1`.
