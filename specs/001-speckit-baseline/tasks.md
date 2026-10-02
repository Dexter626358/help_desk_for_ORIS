---

description: "Task list for 001-speckit-baseline: внедрение Spec-Driven Development в IPSAS"
---

# Tasks: Внедрение Spec-Driven Development в IPSAS

**Input**: Design documents from `/specs/001-speckit-baseline/`

**Prerequisites**: plan.md, spec.md (оба готовы). `research.md`, `data-model.md`, `contracts/`
не создаются — фича не вводит новых сущностей и контрактов.

**Tests**: задачи проверки включены там, где спецификация требует измеримого результата
(SR-001 … SC-003). Автотесты приложения не добавляются: код приложения не меняется.

**Organization**: Задачи сгруппированы по user stories. Легенда: `[x]` — выполнено и проверено,
`[ ]` — осталось.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: можно выполнять параллельно (разные файлы, нет зависимостей)
- **[Story]**: user story, к которому относится задача (US1 … US6)

---

## Phase 0: Инвентаризация (Blocking)

**Purpose**: Зафиксировать фактическое состояние репозитория, на которое опираются
конституция и baseline. Код не изменяется.

- [x] T001 [P] Клонировать репозиторий `git@gl-pub.rcsi.science:other-web-applications/help_desk_for_oris.git` в `C:\Work\ipsas\help_desk_for_oris`
- [x] T002 [P] Проверить SSH-доступ к `gl-pub.rcsi.science`; при `Host key verification failed` задать глобально `git config --global core.sshCommand "C:/Windows/System32/OpenSSH/ssh.exe"` (MSYS-ssh не читает `known_hosts` при кириллице в пути пользователя)
- [x] T003 Изучить `README.md`, `STRUCTURE.md`, `QUICKSTART.md`, `DEPLOYMENT.md`, `pyproject.toml`, `requirements*.txt`, `.env.example`
- [x] T004 [P] Снять карту слоёв: `ipsas/{web,services,modules,common,jobs,config,utils}` и 13 blueprint'ов
- [x] T005 [P] Снять инвентарь маршрутов `/services/*` и точек входа (`run.py`, `wsgi.py`, `ipsas-web`, `python -m ipsas.cli`)
- [x] T006 [P] Снять инвентарь security-инвариантов: `common/ssrf.py`, `common/zip_safe.py`, `common/xml_secure.py`, CSRF в `web/app.py`, rate limit, production-проверки в `config/settings.py`
- [x] T007 [P] Зафиксировать baseline чисел: `pytest` = 263 passed; `ruff` = 1300 (default-набор `ruff 0.16.9`) и 53 (`E4,E7,E9,F`)

**Checkpoint**: Инвентаризация завершена, конституция может опираться на проверенные факты.

---

## Phase 1: Setup — инфраструктура Spec Kit

**Purpose**: Фазы 2–6 блокируются до неё.

- [x] T008 [P] Установить `uv` (`python -m pip install --upgrade uv`), путь инструментов добавить через `uv tool update-shell`
- [x] T009 Установить `uv tool install specify-cli --from git+https://github.com/github/spec-kit.git`
- [x] T010 Развернуть Spec Kit в корень репозитория: `specify init --here --force --non-interactive --integration opencode --script ps --ignore-agent-tools`
- [x] T011 [P] Проверить результат: `.specify/{templates,scripts/powershell,workflows,memory}`, `.opencode/commands/speckit.*` (10 команд)
- [x] T012 [P] Убедиться, что `specs/` и `.specify/` не исключены `.gitignore`

**Checkpoint**: `.specify/` и `.opencode/commands/` в репозитории, `specify --version` = 1.0.13.dev0.

---

## Phase 2: Foundational — конституция и baseline-фича

**Purpose**: Блокирует все user stories. Документы, на которые ссылается каждая история.

- [x] T013 Заполнить `.specify/memory/constitution.md` по шаблону: 6 принципов (I слоистость, II shim-совместимость, III dry-run для записей, IV безопасность внешних данных, V тесты и quality gate, VI один worker) со ссылками на реальные модули
- [x] T014 [P] Заполнить секцию «Технологические ограничения»: стек с диапазонами версий, отсутствие БД, стиль имён, логирование, иерархия ошибок, обязательность обновления документации
- [x] T015 [P] Заполнить секцию «Рабочий процесс и критерий готовности» (Definition of Done из 6 пунктов) и «Governance» (SemVer, амортизация долга)
- [x] T016 Проверить конституцию: 0 незаполненных плейсхолдеров `[ALL_CAPS]`, даты в ISO-формате, версия `1.0.0` / ratified 2026-09-28
- [x] T017 Создать каталог фичи: `.specify/scripts/powershell/create-new-feature.ps1 -ShortName "speckit-baseline" ...` (требует `Set-ExecutionPolicy -Scope Process Bypass`) → `specs/001-speckit-baseline/`
- [x] T018 Заполнить `specs/001-speckit-baseline/spec.md`: 6 user stories с приоритетами, edge cases, 16 FR + 5 NFR, 5 сущностей, 8 SC, допущения
- [x] T019 Заполнить `specs/001-speckit-baseline/plan.md`: technical context, Constitution Check (7 строк PASS), структура, 7 фаз реализации
- [x] T020 Заполнить `specs/001-speckit-baseline/tasks.md` (этот файл)

**Checkpoint**: Конституция и baseline-артефакты готовы и согласованы.

---

## Phase 3: User Story 1 — Локальный запуск (Priority: P1) MVP

**Goal**: Чистый клон поднимается и проверяется по инструкции.

**Independent Test**: `pytest` + `GET /health` + 12 маршрутов `/services/*` на поднятом dev-сервере.

### Verification for User Story 1

- [x] T021 [P] [US1] Создать venv: `uv venv .venv --python 3.12` в `C:\Work\ipsas\help_desk_for_oris`
- [x] T022 [US1] Установить зависимости: `uv pip install --python .venv\Scripts\python.exe -r requirements-dev.txt` (46 пакетов, без изменения `requirements*.txt`)
- [x] T023 [P] [US1] Создать `.env` из `.env.example` (в `.gitignore`; `config/settings.py` читает его своим парсером, `python-dotenv` не нужен)
- [x] T024 [US1] Прогнать `pytest` → 263 passed
- [x] T025 [US1] Поднять dev-сервер: `python run.py` в фоне, логи в `devserver.out.log` / `devserver.err.log`
- [x] T026 [US1] Проверить `GET /health` → `{"service":"ipsas","status":"ok"}` и `GET /health/ready` → 200
- [x] T027 [US1] Проверить 12 маршрутов `/services/*` → 200 (`/services/xml-editor` отдаёт 308 на канонический `/services/xml-editor/`, затем 200)

**Checkpoint**: SC-001, SC-002, SC-003 выполнены. Локальное развёртывание подтверждено фактическим запуском.

---

## Phase 4: User Story 2 — Описание фичи через Spec Kit (Priority: P1)

**Goal**: Намерение превращается в структуру `specs/<NNN>-<slug>/` со связанными артефактами.

**Independent Test**: `create-new-feature.ps1 -DryRun` на вымышленной фиче + проверка созданного `spec.md` на отсутствие плейсхолдеров.

### Verification for User Story 2

- [x] T028 [P] [US2] Проверить резолв шаблона: `.specify/scripts/powershell/resolve-template.ps1 constitution-template -Json`
- [x] T029 [P] [US2] Прогнать `create-new-feature.ps1 -DryRun` для проверки генерации имени ветки и путей без создания файлов
- [x] T030 [US2] Проверить, что `Save-FeatureJson` создаёт `.specify/feature.json` (машинное состояние, в `.gitignore`)
- [x] T031 [P] [US2] Зафиксировать в `plan.md` последовательность команд `/speckit.specify` → `/speckit.clarify` → `/speckit.plan` → `/speckit.checklist` → `/speckit.tasks` → `/speckit.analyze` → `/speckit.implement`
- [ ] T032 [US2] Прогнать сценарий на тестовой фиче: `/speckit.specify` → `/speckit.plan` → `/speckit.tasks`, убедиться в связности артефактов, затем удалить тестовый каталог `specs/002-*`

**Checkpoint**: сквозной прогон SDD-цикла на тестовой фиче (следующая рабочая сессия).

---

## Phase 5: User Story 3 — Проверка по конституции (Priority: P1)

**Goal**: Нарушения принципов фиксируются автоматически, а не на словах.

**Independent Test**: `check-prerequisites.ps1` + ручное нарушение принципа в плане → отчёт `/speckit.analyze`.

### Verification for User Story 3

- [x] T033 [P] [US3] Проверить `.specify/scripts/powershell/check-prerequisites.ps1`:
  `plan.md` — жёсткое требование (выход 1, если отсутствует, строки 104-109); `spec.md`
  проверяется только с `-RequireSpec`, `tasks.md` — с `-RequireTasks`; поле `AVAILABLE_DOCS`
  содержит лишь необязательные документы (`research.md`, `data-model.md`, `contracts/`,
  `quickstart.md`) и по контракту не перечисляет `spec.md`/`plan.md`
- [x] T034 [P] [US3] Подтвердить прохождение gate: `check-prerequisites.ps1 -Json -RequireSpec -RequireTasks -IncludeTasks` → без ошибок, `FEATURE_DIR` = `specs/001-speckit-baseline`
- [ ] T035 [US3] Проверить на тестовой фиче: план с логикой в маршруте отклоняется `/speckit.analyze` с цитатой принципа
- [ ] T036 [US3] Добавить в `tests/` проверку ссылок конституции на существующие пути репозитория (регрессия против «принцип ссылается на удалённый модуль»)

**Checkpoint**: нарушения принципов видны автоматически (следующая рабочая сессия).

---

## Phase 6: User Story 4 — Правила проекта в машиночитаемом виде (Priority: P2)

**Goal**: Ревьюер получает правила из одного файла.

**Independent Test**: `specs/001-speckit-baseline/spec.md` (User Story 4) ↔ `.specify/memory/constitution.md` — все шесть принципов покрыты и привязаны к модулям.

- [x] T037 [P] [US4] Принцип I — слои: `STRUCTURE.md:14-24`, `ipsas/modules/issue_metadata/http_client.py`, `__all__` в `ipsas/modules/*/__init__.py`
- [x] T038 [P] [US4] Принцип II — shim: `ipsas/modules/validator.py:1-5`, `ipsas/modules/journal_xml_report/__init__.py:1-4`, отсутствие `DeprecationWarning` в `ipsas/`
- [x] T039 [P] [US4] Принцип III — dry-run: `PLATFORM_APPLY_ENABLED` в `config/settings.py:163-177`, разделы «Переменные окружения» и «Важно» в `README.md` (ссылка дана по названию раздела, а не по номеру строки: нумерация сдвигается при правках README)
- [x] T040 [P] [US4] Принцип IV — безопасность: `common/ssrf.py`, `common/zip_safe.py`, `common/xml_secure.py`, CSRF-инвариант `web/app.py:39-40`, `request_id` в `web/app.py:249-258`
- [x] T041 [P] [US4] Принцип V — quality gate: `pyproject.toml` (отсутствие `[tool.ruff]`), 263 теста, `requirements-dev.txt`
- [x] T042 [P] [US4] Принцип VI — один worker: `gunicorn.conf.py`, `config/settings.py:214-216`, раздел «Production» в `README.md`
- [x] T043 [US4] Проверить отсутствие плейсхолдеров в `constitution.md`

**Checkpoint**: US4 выполнен.

---

## Phase 7: User Story 5 — Стабильный quality gate (Priority: P2)

**Goal**: `ruff check ipsas tests` даёт одинаковый результат на любой версии ruff из `>=0.6.0,<1`.

**Independent Test**: установить два разных ruff и сравнить вывод.

- [x] T044 [P] [US5] Зафиксировать текущее поведение: без `[tool.ruff]` — 1300 замечаний на `ruff 0.16.9`; на `--select E4,E7,E9,F` — 53
- [x] T045 [P] [US5] Зафиксировать числа в конституции (принцип V) как baseline
- [ ] T046 [US5] **Требует решения команды**: добавить `[tool.ruff]` в `pyproject.toml` — выбрать `select`, `line-length`, `exclude`; учесть, что конфигурация должна читаться всеми версиями из диапазона `>=0.6.0,<1`
- [ ] T047 [US5] После T046: прогнать `ruff check ipsas tests`, записать фактическое число в конституцию вместо 1300/53
- [ ] T048 [P] [US5] Синхронизировать `requirements-dev.txt` и `pyproject.toml` при изменении диапазона ruff

**Checkpoint**: quality gate воспроизводим. Блокирует SC-004.

---

## Phase 8: User Story 6 — Спецификации в репозитории (Priority: P3)

**Goal**: Спецификации коммитятся в тот же репозиторий и GitLab.

**Independent Test**: `git check-ignore specs/001-speckit-baseline/spec.md` не выводит ничего; `git add -n specs .specify .opencode` показывает файлы.

- [x] T049 [P] [US6] Проверить `.gitignore`: `specs/`, `.specify/`, `.opencode/` не исключены
- [x] T050 [P] [US6] Проверить, что `.specify/.gitignore` исключает только per-checkout
  состояние (`feature.json`, `extensions/*/local-config.yml`), а `integration.json` и
  `init-options.json` коммитятся намеренно: они детерминированно описывают конфигурацию
  Spec Kit в этом репозитории (integration `opencode`, script `ps`), и по ним
  восстанавливается установка на другой машине
- [x] T051 [P] [US6] Оценить конфликт с правилом `*.xml` в `.gitignore` (глобально исключает XML, кроме `schemas/`, `ipsas/resources/schemas/`, `tests/fixtures/`) — на артефакты SDD не влияет
- [x] T052 [US6] `git add` и коммит артефактов SDD (`.specify/`, `.opencode/`, `specs/`) —
  выполнено по явному указанию пользователя вместе с правками документации
- [x] T053 [P] [US6] Обновить `README.md`: раздел «Работа по Spec Kit» (установка `uv` +
  `specify`, команды `/speckit.*`, место спецификаций) — см. также фичу
  [`002-server-deployment`](../002-server-deployment/spec.md), T053
- [x] T054 [P] [US6] Обновить `STRUCTURE.md`: `deploy/`, `docker-compose.yml`, `Dockerfile`,
  `.gitlab-ci.yml`, `.gitattributes` в дереве каталогов; разделы «Развёртывание (`deploy/`)»
  и «Spec Kit» (выполнено в фиче `002-server-deployment`, T050–T051)
- [ ] T055 [P] [US6] Создать `specs/001-speckit-baseline/quickstart.md` с командами развёртывания и проверки SDD

**Checkpoint**: артефакты видны команде, документация описывает процесс.

---

## Phase 9: Polish & Cross-Cutting

- [ ] T056 [P] Прогнать `pip-audit -r requirements.txt`, зафиксировать результат и решить, блокирующий ли он
- [x] T057 [P] Проверить CLI-контракт из принципа II: `python -m ipsas.cli --help` → подкоманды
  `report` (HTML-отчёт по journal XML) и `version`
- [ ] T058 [P] Проверить идемпотентность `specify init --here --force`: повторный прогон не теряет заполненную конституцию (NFR-004)
- [ ] T059 [P] Проверить, что dev-сервер возвращает 500 с `request_id` при искусственной ошибке, а не стек-трейс
- [ ] T060 [P] Остановить фоновый dev-сервер и удалить `devserver.out.log` / `devserver.err.log`,
  когда развёртывание больше не нужно (журналы `*.log` уже исключены `.gitignore`, поэтому
  на состояние репозитория не влияют)
- [ ] T061 Зафиксировать в конституции версию 1.0.1 (PATCH) после принятия решений по T046 и T053–T055
- [ ] T062 Прогнать `pytest` повторно после всех правок; число тестов должно остаться ≥ 263

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 0 (Инвентаризация)**: без зависимостей
- **Phase 1 (Setup)**: зависит от Phase 0
- **Phase 2 (Foundational)**: зависит от Phase 1 — **блокирует все user stories**
- **Phases 3–8 (User Stories)**: зависят от Phase 2; US1 и US4 уже выполнены и независимы
- **Phase 9 (Polish)**: зависит от завершения выбранных историй

### User Story Dependencies

- **US1 (P1)**: только Phase 2 — выполнена
- **US2 (P1)**: только Phase 2 — частично (T032 ждёт ручного прогона)
- **US3 (P1)**: зависит от US4 (текст конституции) — T033–T034 выполнены, T035–T036 ждут
- **US4 (P2)**: только Phase 2 — выполнена
- **US5 (P2)**: независима; требует решения команды по T046
- **US6 (P3)**: независима; T052–T054 выполнены по решению пользователя, остальное ждёт

### Critical Path

`Phase 0 → Phase 1 → Phase 2 → US1` — выполнен полностью. MVP достигнут: приложение
поднимается и проверяется, SDD-инфраструктура на месте.

## Parallel Opportunities

- T002, T004, T005, T006, T007 (Phase 0) — независимы
- T011, T012 (Phase 1) — независимы
- T014, T015 (Phase 2) — независимы, оба правят один файл → **выполнять последовательно**
- T021, T023 (US1) — независимы
- T028, T029, T031 (US2) — независимы
- T033, T034 (US3) — независимы
- T037–T042 (US4) — все пишут один файл → **последовательно**
- T044, T045 (US5) — независимы
- T049, T050, T051 (US6) — независимы; T053–T055 правят разные файлы → параллельно
- T056, T057, T058, T060 — независимы

## Implementation Strategy

1. **MVP (достигнут)**: Phase 0 → Phase 1 → Phase 2 → US1. Приложение поднимается,
   инфраструктура SDD на месте, конституция и baseline-артефакты написаны.
2. **Следующий инкремент**: US2 + US3 (T032, T035, T036) — сквозной прогон SDD-цикла и
   автоматическая проверка по конституции.
3. **Затем**: US5 (T046–T048) после решения команды по конфигурации ruff.
4. **Затем**: US6 (T052–T055) — коммит артефактов и документация процесса.
5. **Финал**: Phase 9 — аудит CLI-контракта, идемпотентности, `pip-audit`, чистка логов.

## Notes

- T021–T027, T033–T034, T037–T045, T049–T051 уже выполнены и проверены фактически; их
  результаты — источник чисел в конституции и baseline.
- T046 меняет поведение линтера для всей команды и требует отдельного обсуждения; выполнять
  только после явного решения, не «по ходу».
- T052 создаёт коммит — выполняется только по явному указанию пользователя.
- Все логи, отчёты и артефакты сценариев пишутся в `temp/`, `logs/` и `specs/`, которые
  исключены `.gitignore`; секреты в артефакты не попадают (принцип IV).
- Развёртывание сервиса (11 файлов: `docker-compose.yml`, `deploy/*`, `.gitlab-ci.yml`,
  `.gitattributes`) было выполнено **без** спецификации — это отклонение от процесса,
  зафиксированное в фиче [`002-server-deployment`](../002-server-deployment/spec.md),
  где спецификация, план и задачи написаны задним числом по фактическому результату.
  С этого момента фичи оформляются артефактами до кода.
