# Разделы журнала из CSV

Сервис создаёт недостающие разделы в OJS (`manager/createSection`) по CSV
и/или выстраивает существующие разделы по алфавиту (`manager/moveSection`).

| | |
|--|--|
| Web | `/services/journal-sections` |
| CLI | `sync_journal_sections.py` |
| Домен | `ipsas/modules/journal_sections/` |
| Сценарий | `ipsas/services/journal_sections.py` |

---

## Где работает

| Среда | Пример ссылки | Учётки в `.env` |
|-------|---------------|-----------------|
| Песочница | `https://f23g45.rcsi.science/2079-7877/manager/sections` | `SANDBOX_GATE_*`, `SANDBOX_OJS_*` |
| Платформа | `https://journals.rcsi.science/2079-7877/manager/sections` | `PLATFORM_*` / `RCSI_*` |

Подойдёт и корень журнала (`…/2079-7877`), и страница разделов (`…/manager/sections`).
Хост выбирает режим входа автоматически.

---

## Формат CSV

Разделитель `;`, кодировка UTF-8 (с BOM допустим).

```
number;title_ru;short_title_ru;title_en;short_title_en
1;Гидрология;Гидр;Hydrology;Hydr
```

Обязательны все пять колонок и непустые `title_*` / `short_title_*`.
Пустые строки пропускаются; полностью пустой файл — ошибка.

Рабочий файл `journal_sections.csv` в корне репозитория **в git не хранится**
(см. `.gitignore`). Пример для тестов: `tests/fixtures/journal_sections.csv`.

---

## Режимы

1. **Только CSV** — сверка с `manager/sections` по русскому названию; отсутствующие создаются
   с `title`/`abbrev` для `ru_RU` и `en_US`.
2. **Только сортировка** (`--sort-alpha` / галочка) — CSV не нужен; разделы A→Я по RU.
3. **CSV + сортировка** — сначала create missing, затем сортировка.

Флаги:

- dry-run (по умолчанию) — без записи на платформу;
- `--apply` / снятие «Только проверка» — запись.

**Исключение при сортировке:** раздел «Статьи» / Articles остаётся на своём месте;
остальные выстраиваются вокруг него.

Существующие разделы **не обновляются** (EN/abbrev не правятся, лишние не удаляются).

---

## CLI

```bash
# проверка CSV (пример из репозитория)
python sync_journal_sections.py ^
  --journal-url https://journals.rcsi.science/2079-7877/manager/sections ^
  --csv tests/fixtures/journal_sections.csv --dry-run

# создать недостающие + сортировка (свой CSV)
python sync_journal_sections.py ^
  --journal-url https://f23g45.rcsi.science/2079-7877 ^
  --csv my_sections.csv --apply --sort-alpha

# только сортировка на платформе
python sync_journal_sections.py ^
  --journal-url https://journals.rcsi.science/2079-7877/manager/sections ^
  --sort-alpha --apply
```

---

## Связанные файлы

```
ipsas/modules/journal_sections/
  csv_io.py      # разбор CSV
  sections.py    # list / create / sort (moveSection)
  service.py     # оркестрация
  models.py
ipsas/services/journal_sections.py
ipsas/web/journal_sections.py
sync_journal_sections.py
tests/test_journal_sections.py
tests/fixtures/journal_sections.csv
```
