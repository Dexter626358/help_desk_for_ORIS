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

Одна команда (создаёт venv, ставит зависимости, готовит `.env`, поднимает сервер,
дожидается ответа `/health` и печатает адрес):

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\local-start.ps1
```

Откройте **http://localhost:5000** (или порт из `PORT`). Остановить — `Ctrl+C`.
Скрипт идемпотентен: повторный запуск не переустанавливает зависимости.
Ключи: `-Install` (переустановить), `-NoRun` (только подготовить), `-Port 5050`,
`-SkipChecks` (без HTTP-проверки).

Вручную — то же самое по шагам:

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
copy .env.example .env
python run.py
```

Проверить, что всё поднялось:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\smoke-test.ps1
```

## Production

Обновление сервера — две команды:

```bash
cd /opt/ipsas
git pull
docker compose up -d --build
```

`--build` обязателен: код находится внутри образа, и без пересборки compose
запустит старую версию без всякой ошибки.

Пайплайн GitLab гоняет тесты на каждом пуше — `.gitlab-ci.yml`.
Первый запуск (nginx, `.env`, проверка, откат) — пошаговая инструкция
**[deploy/SERVER_DEPLOY.md](deploy/SERVER_DEPLOY.md)**.
Сводка по настройкам и ограничениям — **[DEPLOYMENT.md](DEPLOYMENT.md)**.

Без Docker (systemd):

```bash
export IPSAS_ENV=production
export SECRET_KEY="…"
export ISSUE_FETCH_ALLOWED_HOSTS="journals.rcsi.science"
gunicorn -c gunicorn.conf.py ipsas.web.wsgi:app
```

**Один worker** обязателен: лимит запросов и фоновый пул хранятся в памяти процесса.

## Docker

Обычно Docker не нужен отдельно — сервис поднимает `docker compose` (см. `## Production`).
Прямой запуск образа пригодится для отладки:

```bash
docker build -t ipsas:latest .
docker run --rm -p 127.0.0.1:8000:8000 \
  -e SECRET_KEY=… \
  -e IPSAS_ENV=production \
  -e ISSUE_FETCH_ALLOWED_HOSTS=journals.example.org \
  ipsas:latest
curl -s http://127.0.0.1:8000/health
```

Порт публикуется на `127.0.0.1`, а не на все интерфейсы: наружу сервис должен выходить
через Nginx, `-p 8000:8000` открыл бы его напрямую в обход прокси.

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
| [STRUCTURE.md](STRUCTURE.md) | Слои кода, модули, blueprints, дерево каталогов |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Production: схема развёртывания, env, Docker Compose, Nginx, systemd |
| [deploy/SERVER_DEPLOY.md](deploy/SERVER_DEPLOY.md) | **Пошаговое развёртывание на сервере с нуля** (Docker Compose + nginx) |
| [RAILWAY_DEPLOY.md](RAILWAY_DEPLOY.md) | Railway |
| [specs/001-speckit-baseline/spec.md](specs/001-speckit-baseline/spec.md) | Внедрение Spec-Driven Development |
| [specs/002-server-deployment/spec.md](specs/002-server-deployment/spec.md) | Развёртывание на внутреннем сервере |

### Скрипты развёртывания

| Скрипт | Назначение |
|--------|-----------|
| `deploy/local-start.ps1` | Локальный запуск одной командой (Windows) |
| `deploy/smoke-test.ps1` | Проверка запущенного экземпляра (Windows) |
| `deploy/smoke-test.sh` | То же для Linux: `bash deploy/smoke-test.sh http://127.0.0.1:8000` |
| `docker-compose.yml` | Запуск на сервере: `docker compose up -d --build` |
| `deploy/ipsas.env.production.example` | Шаблон `.env` для сервера |
| `deploy/ipsas.service.example` | systemd-юнит (вариант без Docker) |
| `deploy/nginx.example.conf` | Конфигурация reverse-proxy |

## Работа по Spec Kit

Проект развивается по Spec-Driven Development: перед изменением кода описывается
спецификация в `specs/NNN-<slug>/`. Правила проекта зафиксированы в конституции
`.specify/memory/constitution.md` — принципы слоистости, совместимости shim'ов, dry-run
для операций записи, безопасности внешних данных, quality gate и единственного worker.

### Установка (один раз)

```bash
python -m pip install --upgrade uv
uv tool update-shell                                  # добавить uv в PATH
uv tool install specify-cli --from git+https://github.com/github/spec-kit.git
```

Скрипты Spec Kit в этом репозитории написаны под PowerShell.

### Рабочий цикл

Слэш-команды определены в [`.opencode/commands/`](.opencode/commands/):

| Команда | Что делает |
|---------|-----------|
| `/speckit.specify` | Описание намерения: пользовательские истории, требования, критерии приёмки |
| `/speckit.clarify` | Уточнение неоднозначных требований — до написания плана |
| `/speckit.plan` | Технический план: контекст, проверка по конституции, структура изменений |
| `/speckit.checklist` | Чек-лист «готовность к реализации» |
| `/speckit.tasks` | Декомпозиция плана на задачи с зависимостями |
| `/speckit.analyze` | Согласованность артефактов и проверка по конституции |
| `/speckit.implement` | Выполнение задач |
| `/speckit.converge` | Сведение правок и финальная сверка с конституцией |

Готовые спецификации: [`specs/001-speckit-baseline/`](specs/001-speckit-baseline/spec.md) —
внедрение процесса, [`specs/002-server-deployment/`](specs/002-server-deployment/spec.md) —
развёртывание на внутреннем сервере.

Полезные скрипты:

```powershell
Set-ExecutionPolicy -Scope Process Bypass   # требуется для скриптов .specify
.\.specify\scripts\powershell\check-prerequisites.ps1 -RequireSpec -RequireTasks
```

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
