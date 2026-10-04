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
| Валидатор XML | `/services/xml-validator` | XSD и метаданные |
| JATS для Метафоры | `/services/metafora-jats` | JATS XML или ZIP выпуска → обязательные данные для Метафоры |
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

Архивация по отправителю и настройка песочницы по умолчанию в режиме **только проверка** (dry-run); для записи снимите галочку на форме (CLI песочницы: `--apply`).

CLI песочницы:

```bash
python setup_sandbox_journal.py "https://f23g45.rcsi.science/257/index"
python setup_sandbox_journal.py "https://f23g45.rcsi.science/257/index" --apply
```

## Остановка сервера

`Ctrl+C` в терминале.

## Развёртывание на сервере

Запуск из `venv` — только для разработки. Продакшен поднимается через Docker Compose
за Nginx:

```bash
cp deploy/ipsas.env.production.example .env
# заполнить SECRET_KEY и ISSUE_FETCH_ALLOWED_HOSTS
chmod 600 .env
docker compose up -d --build
curl -s http://127.0.0.1:8000/health
```

Обновление развёртывания:

```bash
git pull
docker compose up -d --build      # без --build поднимется старый образ
bash deploy/smoke-test.sh http://127.0.0.1/
```

Подробности, включая настройку Nginx и разбор ошибок: [`DEPLOYMENT.md`](DEPLOYMENT.md)
(сводка) и [`deploy/SERVER_DEPLOY.md`](deploy/SERVER_DEPLOY.md) (пошагово).

## Работа по Spec Kit

Изменения проекта планируются в [`specs/`](specs/001-speckit-baseline/spec.md) до написания
кода. Установка и команды — в [README.md](README.md#работа-по-spec-kit).
