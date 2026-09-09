# Итоговый отчёт: аудит сервиса проверки настроек журнала

**Дата:** 2026-09-08 21:51  
**Выборка:** 141 файлов `ras_settings/*.data`  
**Согласованные правила:** DOI опционально; copyright без licenseURL → partial; access → подсказка; extra_off → отмечать.

## 1. Краткий вывод о надёжности

Сервис **в целом работоспособен** на выборке РАН: падений нет, базовые поля и плагины оцениваются близко к независимому эталону.  
Точность по сопоставимым решениям (TP/TN/FP/FN): **accuracy=95.3%**, precision=86.7%, recall=82.8%, F1=84.7%.

Главные зоны риска: расхождения по `step3.submission_ack`, `step1.mailing`, эвристика `modules.extra_off`, severity у copyright/раздела «Статьи».  
Журналов с нулём расхождений: **0/141**.

## 2. Описание выборки

- Источник: экспорт OJS `.data` (JSON) журналов РАН.
- Всего: **141** журналов.
- Независимый аудит + прогон сервиса `execute()` без изменения исходных файлов.

## 3. Распределение по заполненности (indep completeness)

| Диапазон | Журналов |
|---|---:|
| <75% | 1 |
| 75–84% | 86 |
| 85–91% | 53 |
| ≥92% | 1 |

Средняя indep completeness: **84.1%**  
Медиана: **84.0%**

## 4. Распределение по готовности (indep)

| Уровень | Журналов |
|---|---:|
| ready | 0 |
| mostly_ready | 102 |
| needs_revision | 33 |
| not_ready | 6 |
| manual_review | 0 |

## 5. Общие метрики качества сервиса

| Метрика | Значение |
|---|---:|
| Сопоставимых решений | 7995 |
| TP | 1044 |
| TN | 6574 |
| FP | 160 |
| FN | 217 |
| severity_mismatch | 77 |
| cannot_compare | 670 |
| Accuracy | 95.3% |
| Precision | 86.7% |
| Recall | 82.8% |
| F1 | 84.7% |
| FPR | 2.4% |
| FNR | 17.2% |
| Журналы без расхождений | 0 |
| Ср. разница indep−service score | 1.07 |
| Медиана разницы | 1.10 |
| Макс. |разница| | 6.80 |
| Ошибочно «готовее» сервиса | 25 |
| Ошибочно «неготовее» сервиса | 15 |

## 6. Наиболее точные проверки

- `metrics.alm`: accuracy=1.0, FP=0, FN=0
- `metrics.altmetrics`: accuracy=1.0, FP=0, FN=0
- `metrics.citedby`: accuracy=1.0, FP=0, FN=0
- `metrics.dimensions`: accuracy=1.0, FP=0, FN=0
- `metrics.publons`: accuracy=1.0, FP=0, FN=0
- `modules.acron`: accuracy=1.0, FP=0, FN=0
- `modules.browse`: accuracy=1.0, FP=0, FN=0
- `modules.coins`: accuracy=1.0, FP=0, FN=0
- `modules.custom_blocks`: accuracy=1.0, FP=0, FN=0
- `modules.doi`: accuracy=1.0, FP=0, FN=0

## 7. Наиболее проблемные проверки

- `modules.extra_off`: FP=135, FN=1, sev_mismatch=0, accuracy=0.0355
- `step3.submission_ack`: FP=0, FN=125, sev_mismatch=0, accuracy=0.1135
- `step1.mailing`: FP=0, FN=91, sev_mismatch=1, accuracy=0.35
- `pages.reader_tools`: FP=17, FN=0, sev_mismatch=0, accuracy=0.8794
- `step1.email`: FP=7, FN=0, sev_mismatch=0, accuracy=0.9504
- `step3.checklist`: FP=1, FN=0, sev_mismatch=8, accuracy=0.9925
- `metrics.alm`: FP=0, FN=0, sev_mismatch=0, accuracy=1.0
- `metrics.altmetrics`: FP=0, FN=0, sev_mismatch=0, accuracy=1.0
- `metrics.citedby`: FP=0, FN=0, sev_mismatch=0, accuracy=1.0
- `metrics.dimensions`: FP=0, FN=0, sev_mismatch=0, accuracy=1.0

## 8. Ложные положительные (сервис ругает зря / строже эталона)

- `modules.extra_off`: 135
- `pages.reader_tools`: 17
- `step1.email`: 7
- `step3.checklist`: 1

## 9. Ложные отрицательные (сервис пропускает)

- `step3.submission_ack`: 125
- `step1.mailing`: 91
- `modules.extra_off`: 1

## 10. Ошибки итоговой оценки

Средняя разница indep−service: 1.07 п.п. (медиана 1.10). Расхождений готовности: сервису «готовее» — 25, «неготовее» — 15.

## 11. Систематические причины расхождений

1. **Разный порог severity** (copyright licenseURL, раздел «Статьи»).
2. **Условные/ручные пункты** (webfeed, plumx/Scopus) → `cannot_compare`.
3. **Эвристика лишних модулей** (`modules.extra_off`) — отмечать, но whitelist для RAS неполный.
4. **Почтовый адрес / submission_ack** — сервис иногда мягче независимого эталона.
5. **DOI** после согласования: опционален; сравнение с старыми ожиданиями «всегда fail» больше не применяется.

## 12. Ручная проверка

См. `manual_review.csv` ({len(picked)} журналов): низкая/средняя/высокая заполненность, макс. mismatches, пограничные оценки, расхождение readiness, много cannot_determine.

## 13–14. Рекомендации и приоритеты

### P0
- Не считать журнал «готовым», если есть critical fail по title/ISSN/языкам/политике (уже в indep модели).

### P1
- Добить FN по `step3.submission_ack` и `step1.mailing` (сервис пропускает чаще эталона).

### P2
- Уточнить whitelist `modules.extra_off` под платформенные плагины RAS (пункт **отмечать**, но снизить ложный шум).
- Выровнять severity copyright (только license → partial) — **уже внесено в сервис**.

### P3
- DOI опционально с текстом «если журнал присваивает DOI» — **уже внесено**.
- access как подсказка — **уже внесено**.

## 15. Регрессионные тесты

См. `regression_tests.json` ({len(regressions)} сценариев): DOI optional/enabled-no-prefix, focusScopeDesc, copyright license-only, empty/null/HTML/spaces, partial RU, access hint, extra_off mark, checklist empty.

## Артефакты

| Файл | Содержание |
|---|---|
| `detected_schema.md` | Схема JSON |
| `validation_rules.json` | Правила v1.0-agreed |
| `journal_audit.jsonl` | Полный аудит по журналам |
| `journal_summary.csv` | Сводка |
| `rule_metrics.csv` | Метрики по правилам |
| `mismatches.csv` | Все расхождения |
| `manual_review.csv` | Выборка для ручной проверки |
| `regression_tests.json` | Регрессии |
| `final_report.md` | Этот отчёт |
