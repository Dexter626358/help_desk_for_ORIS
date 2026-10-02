# Implementation Plan: Внедрение Spec-Driven Development в IPSAS

**Branch**: `001-speckit-baseline` | **Date**: 2026-09-28 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-speckit-baseline/spec.md`

## Summary

Внедрить в репозиторий IPSAS инфраструктуру GitHub Spec Kit (`.specify/` + slash-команды для
opencode), заполнить конституцию принципами, выведенными из фактического кода, и оформить
baseline-спецификацию/план/задачи. Дополнительно устранить нестабильность quality gate:
зафиксировать правила ruff в `pyproject.toml` и числовой baseline замечаний. Код приложения не
меняется, зависимости не добавляются.

Технический подход: инфраструктура SDD добавляется файлами в корень репозитория и полностью
изолирована от рантайма (нет импортов из `ipsas/`, нет новых записей в `requirements*.txt`).
Конституция пишется по шаблону Spec Kit без незаполненных плейсхолдеров, каждое утверждение
привязано к существующему модулю. Развёртывание проверяется фактическим запуском, а не
документированием.

## Technical Context

**Language/Version**: Python 3.11 (`runtime.txt`); `pyproject.toml` — `>=3.10`; фактически
проверено на 3.12.10. Скрипты Spec Kit — PowerShell 5.1+.

**Primary Dependencies**: Flask 3.x (<3.2), Werkzeug 3.x (<3.2), Flask-WTF 1.x, WTForms 3.x,
lxml 6.x, pypdf 3.9–6.x, gunicorn 21.2–23.x. Dev: pytest 8.x, ruff 0.6–0.16, pip-audit 2.x.
Инфраструктура SDD: `uv` (>=0.12) + `specify-cli` из `github/spec-kit` (установлен
`1.0.13.dev0`).

**Storage**: файлы в `TEMP_DIR` (`temp/`, `temp/jobs`) + память процесса. Без БД. Каталоги
`data/` и `logs/` создаются конструктором `Settings`.

**Testing**: pytest (`testpaths = ["tests"]`, `pythonpath = ["."]`), 263 теста; ruff;
`pip-audit -r requirements.txt`; ручная проверка маршрутов через HTTP-запросы.

**Target Platform**: dev — Windows/PowerShell; production — Linux (gunicorn 1 worker, nginx,
systemd/Docker/Railway).

**Project Type**: web-service (Flask, Jinja2, без SPA и без сборки фронтенда).

**Performance Goals**: фоновые задачи ограничены `MAX_CONCURRENT_JOBS=4` и
`MAX_JOB_QUEUE=8`; rate limit `RATE_LIMIT_PER_MINUTE=30`; время ответа на страницу сервиса —
в пределах обычного dev-сервера; полный прогон `pytest` < 60 с.

**Constraints**: без БД; без встроенной авторизации; один worker gunicorn; `SECRET_KEY`
обязателен в production; `ISSUE_FETCH_ALLOWED_HOSTS` обязателен в production; CSRF включён;
`PLATFORM_APPLY_ENABLED=0` по умолчанию; операции записи — только dry-run без явного opt-in.

**Scale/Scope**: 155 Python-файлов в `ipsas/`, 29 тестовых файлов, 12 сервисов UI, 13
blueprint'ов, 1 XSD (`schemas/journal3.xsd`). Объём фичи — инфраструктура и документация,
изменений в приложении нет.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Принцип | Проверка | Статус |
|---------|----------|--------|
| I. Слоистость | Фича не добавляет ни одного импорта в `ipsas/`; слой приложения не тронут | PASS |
| II. Shim-совместимость | Публичные пути `ipsas.*`, `python -m ipsas.cli`, `ipsas-web` и имена env не меняются | PASS |
| III. Dry-run для записей | Новых операций записи во внешние системы нет | PASS |
| IV. Безопасность внешних данных | Новых точек входа извне нет; секреты не добавляются; `SECRET_KEY`/CSRF/SSRF инварианты не меняются | PASS |
| V. Тесты и quality gate | 263 теста остаются зелёными; ruff-конфигурация фиксируется явно, baseline фиксируется числом | PASS (с обязательным действием T007) |
| VI. Один worker | Не влияет; в плане нет шагов, меняющих число воркеров | PASS |
| NFR-001 (изоляция SDD) | `.specify/` и `.opencode/` не импортируются приложением; `requirements*.txt` без изменений | PASS |

Нарушений, требующих записи в Complexity Tracking, нет: фича не создаёт новых архитектурных
исключений. Единственная не-тривиальная часть — фиксация правил ruff, которая меняет
`pyproject.toml`, но не рантайм.

## Project Structure

### Documentation (this feature)

```text
specs/001-speckit-baseline/
├── spec.md              # этот набор требований (/speckit.specify)
├── plan.md              # этот план (/speckit.plan)
├── tasks.md             # задачи по фазам (/speckit.tasks)
└── quickstart.md        # команды развёртывания и проверки SDD (/speckit.plan)
```

`research.md`, `data-model.md`, `contracts/` не создаются: у фичи нет новых сущностей данных и
нет внешних контрактов — она меняет только процесс и инфраструктуру репозитория.

### Source Code (repository root)

```text
# Инфраструктура SDD (новая, изолированная от рантайма)
.specify/
├── memory/
│   ├── constitution.md          # заполняется: 6 принципов + ограничения + DoD + governance
│   └── .constitution-template.json
├── templates/                   # spec/plan/tasks/checklist/constitution templates
├── scripts/powershell/          # common.ps1, create-new-feature.ps1, setup-plan.ps1,
│                                # setup-tasks.ps1, check-prerequisites.ps1, resolve-template.ps1
├── workflows/speckit/           # workflow.yml + registry
├── integration.json / init-options.json
└── .gitignore                   # feature.json, extensions/*/local-config.yml

.opencode/
└── commands/                    # speckit.constitution|specify|clarify|plan|checklist|
                                 # tasks|analyze|implement|converge|taskstoissues

# Baseline-фича
specs/001-speckit-baseline/

# Единственная правка существующего файла
pyproject.toml                   # + [tool.ruff] — фиксирует правила линтера

# Не изменяются
ipsas/  tests/  schemas/  requirements.txt  requirements-dev.txt  README.md
STRUCTURE.md  QUICKSTART.md  DEPLOYMENT.md  .env.example
```

**Structure Decision**: инфраструктура SDD размещается в корне репозитория рядом с кодом, а не
в подкаталоге. Причина: команды Spec Kit резолвятся относительно корня репозитория
(`Get-RepoRoot` в `.specify/scripts/powershell/common.ps1`), каталог фич `specs/` обязан быть в
корне для навигации ревьюеров, а изоляция от рантайма обеспечивается тем, что ни один файл
`.specify/`/`.opencode/` не импортируется пакетом `ipsas` и не попадает в сборку
(`[tool.setuptools.packages.find] include = ["ipsas*"]`).

## Порядок реализации

| Фаза | Содержание | Обратимость |
|------|-----------|-------------|
| 0 | Инвентаризация репозитория: слои, маршруты, security-инварианты, baseline чисел | read-only |
| 1 | Установка `uv` и `specify-cli`; `specify init --here --force --non-interactive --integration opencode --script ps` | откат — удалить `.specify/`, `.opencode/` |
| 2 | Заполнение `constitution.md` по шаблону | откат — восстановить шаблон |
| 3 | Создание `specs/001-speckit-baseline/` скриптом `create-new-feature.ps1`, заполнение `spec.md`, `plan.md`, `tasks.md` | откат — удалить каталог фичи |
| 4 | Локальный запуск: venv, зависимости, `.env`, dev-сервер, проверка 14 маршрутов | безопасно, только локальные файлы |
| 5 | Quality gate: зафиксировать `[tool.ruff]`, замерить baseline, сверить с конституцией | требует ревью: меняет поведение линта |
| 6 | Обновление документации процесса (`README.md`/`STRUCTURE.md` — раздел про SDD) | откат — revert коммита |

## Проверка результата

1. `.venv` создан, `pytest` — 263 passed.
2. `python run.py` поднимает сервер; `GET /health` → `{"status":"ok","service":"ipsas"}`;
   `GET /health/ready` → 200; 12 маршрутов `/services/*` → 200
   (`/services/xml-editor` — 308 на канонический `/services/xml-editor/`, затем 200).
3. `ruff check ipsas tests` выдаёт число, совпадающее с записанным в конституции.
4. `constitution.md` не содержит `[ALL_CAPS]`-плейсхолдеров.
5. `git status` показывает только ожидаемые новые пути и `pyproject.toml`.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

Нарушений нет. Пункт 5 «Quality gate» потребовал явного действия (фиксация `[tool.ruff]`), но не
нарушает принцип, а исполняет его: без конфигурации правил критерий приёмки неисполним.
