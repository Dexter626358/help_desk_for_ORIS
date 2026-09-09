# Предварительный отчёт: независимый аудит vs сервис (8 журналов)

**Дата:** 2026-09-08  
**Статус:** этап разведки — **массовый прогон 141 не запускался** (ждём подтверждения правил).  
**Артефакты:** `validation_rules.json`, `preview/preview_summary.json`, `preview/journal_audit_preview.json`, `scripts/independent_audit.py`.

## 1. Краткий вывод (по превью)

Сервис в целом **согласуется** с независимой проверкой по большинству пунктов (много `true_negative` / `true_positive`), но уже на 8 журналах видны **систематические расхождения**:

| Класс сравнения | Кол-во (8×~62 правил) |
|---|---:|
| true_negative | 369 |
| true_positive | 53 |
| false_positive | 15 |
| false_negative | 14 |
| severity_mismatch | 13 |
| cannot_compare | 32 |

Оценки готовности близки (часто `mostly_ready`), но **не совпадают 1:1** с сервисным `%` из‑за другой модели весов и трактовки `not_applicable` / `cannot_determine`.

## 2. Выборка превью (стратификация)

| Файл | Журнал | Indep % | Indep readiness | Service % | Service readiness | FP | FN |
|---|---|---:|---|---:|---|---:|---:|
| 0869-7698.data | Вестник ДВО РАН | 80.5 | not_ready | 76.8 | needs_revision | 2 | 2 |
| 0367-6765.data | Известия РАН. Сер. физ. | 81.1 | needs_revision | 80.4 | mostly_ready | 2 | 2 |
| 0016-7770.data | Геология рудных месторождений | 81.7 | mostly_ready | 78.6 | needs_revision | 2 | 2 |
| 2686-7400.data | Доклады РАН. Физика… | 83.9 | mostly_ready | 82.1 | mostly_ready | 2 | 2 |
| 0869-8139.data | РФЖ им. Сеченова | 84.4 | mostly_ready | 82.1 | mostly_ready | 2 | 2 |
| 0203-0306.data | Вулканология и сейсмология | 84.7 | mostly_ready | 82.1 | mostly_ready | 2 | 2 |
| 2949-124X.data | Российская история | 87.8 | mostly_ready | 83.9 | mostly_ready | 2 | 1 |
| 0235-0106.data | Расплавы | 90.3 | mostly_ready | 87.5 | mostly_ready | 1 | 1 |

## 3. Предложенная модель оценки (к согласованию)

- critical ×5, error ×3, warning ×1, info ×0  
- `not_applicable` исключается из знаменателя  
- `cannot_determine` **не** считается fail  
- `partial` = 0.5 веса  

Готовность: `ready` / `mostly_ready` / `needs_revision` / `not_ready` / `manual_review`.

## 4. Ключевые расхождения (систематика превью)

### False positive (сервис ругает, независимый эталон — нет проблемы / иначе)

1. **`step4.access`** (7/8) — сервис часто `warn/partial` «проверьте роли»; независимый чаще не считает это fail при типовых флагах.  
2. **`modules.extra_off`** (7/8) — эвристика «лишних» модулей расходится: в RAS много штатных плагинов платформы.  
3. **`pages.reader_tools`** — местами сервис строже/иначе, чем независимая эвристика по block-plugin.

### False negative (сервис пропускает / смягчает)

1. **`step3.submission_ack`** (8/8 в топе) — независимый видит проблему чаще.  
2. **`step1.mailing`** (6/8) — адрес/карта: сервис иногда ok/partial, независимый — fail/partial строже по RU+EN.

### Severity mismatch

1. **`step3.copyright`** — оба видят проблему (часто нет `licenseURL`), но статусы `fail` vs `partial` расходятся.  
2. **`pages.section_articles`** — названия есть, не хватает `editorRestriction`/`hideAbout`: partial vs fail.

### cannot_compare

- `modules.webfeed`, `metrics.plumx`, часть manual/условных метрик.

## 5. Уже учтённые исправления сервиса

- `focusScopeDesc` в предметной области — в превью **не** всплывает как массовый FP (после фикса).

## 6. Вопросы на подтверждение перед прогоном 141

Ответьте «да/нет/уточнить» по каждому пункту:

1. **DOI:** если плагин выключен → `not_applicable` (как в независимом аудите), а не обязательный fail?  
2. **Copyright:** отсутствие только `licenseURL` при заполненном тексте → `partial` или `fail`?  
3. **step4.access:** оставляем как обязательный warn сервиса или понижаем до info/ручного?  
4. **modules.extra_off:** исключить из метрик качества / сузить whitelist под RAS?  
5. **Модель весов** critical/error/warning — принимаем как эталон для `completeness_score`?  
6. **FundRef org DB / webfeed pages / Scopus** — оставляем `cannot_determine` без штрафа?

## 7. Следующий шаг после подтверждения

Полный прогон 141 → каталог результатов:

`journal_audit.jsonl`, `journal_summary.csv`, `rule_metrics.csv`, `mismatches.csv`, `manual_review.csv`, `regression_tests.json`, `final_report.md`.
