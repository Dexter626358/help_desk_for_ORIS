# Поиск дублей статей

Сервис сопоставляет статьи **journal XML** или **опубликованного выпуска**
с рукописями **активных очередей** редактора OJS (без Архива) и предлагает
сотруднику до пяти кандидатов на каждую статью.
Окончательное решение и архивация — только вручную.

| | |
|--|--|
| Web | `/services/article-duplicates` |
| CLI | `find_article_duplicates.py` |
| Домен | `ipsas/modules/article_duplicates/` |
| Сценарий | `ipsas/services/article_duplicates.py` |

---

## Входы

Эталон (взаимоисключающие варианты):

1. **Journal XML** + URL журнала / ISSN  
2. **Ссылка на опубликованный выпуск**, например  
   `https://journals.rcsi.science/0044-4502/issue/view/30862`  
   (ISSN журнала берётся из URL; отдельное поле журнала необязательно)

Эталон из выпуска: названия, авторы и аннотации со страниц статей (HTML), без XML.

Учётки: `PLATFORM_USERNAME` / `PLATFORM_PASSWORD` (или `RCSI_*`).

---

## Корпус рукописей

Один раз за запуск выгружаются три активные очереди:

1. Новые — `submissionsUnassigned`
2. На рецензии — `submissionsInReview`
3. В редакции — `submissionsInEditing`

**Архив не включается** — там уже отработанные статьи.

Дубликаты id схлопываются (приоритет у более «ранней» очереди).

Поиск через форму `/editor` в v1 **не используется**: OJS keyword-search плохо
ловит сильно отредактированные заглавия и пары «те же авторы — другое название».

---

## Сходство

| Признак | Метод |
|---------|--------|
| Название | min(WRatio, token_set, token_sort) + Jaccard значимых слов |
| Авторы | Jaccard по нормализованным фамилиям |
| Аннотация | min(token_set, token_sort), только если текст есть с обеих сторон |

Итоговый score при наличии аннотаций: название 60% / авторы 25% / аннотация 15%.
Без аннотации вес перераспределяется на название и авторов (отсутствие данных
не штрафуется).

В выдачу попадает кандидат, если выполняется хотя бы одно:

- сходство названия ≥ ~0.58;
- или полное совпадение авторов эталона ⊆ рукописи **с учётом числа авторов**:
  - ≥3 авторов — без доп. сигнала по названию;
  - 2 автора — слабый title/abstract;
  - 1 автор — заметный title/abstract;
- или сильные авторы (≥ ~0.75) **и** хотя бы слабый сигнал по названию (≥ ~0.28).

Правила:

- до **5** кандидатов: diversified merge (слоты по названию и по авторам);
- один общий автор при чужом названии в выдачу не попадает;
- нейросети и автоудаление отсутствуют.

---

## Режимы

В отчёте у каждого кандидата есть кнопка **«Архивировать»**.

## Архивация

Напротив кандидата — кнопка **«Архивировать»** (одна рукопись за раз) без письма
(`unsuitableSubmission` + `send[skip]`). После подтверждения статус пишется
рядом с кандидатом, список выпуска остаётся на экране.

- **Published** не архивируются.
- Фильтр по отправителю не применяется — решение сотрудника достаточно.

---

## CLI

```bash
python find_article_duplicates.py ^
  --journal-url https://journals.rcsi.science/0320-7919 ^
  --xml issue.xml

python find_article_duplicates.py ^
  --issue-url https://journals.rcsi.science/0044-4502/issue/view/30862

python find_article_duplicates.py ^
  --journal-url https://journals.rcsi.science/0320-7919 ^
  --xml issue.xml --archive-ids 125,231 --dry-run

python find_article_duplicates.py ^
  --journal-url https://journals.rcsi.science/0320-7919 ^
  --xml issue.xml --archive-ids 125 --apply-archive
```

---

## Связанные файлы

```
ipsas/modules/article_duplicates/
  xml_source.py    # эталоны из journal XML
  issue_source.py  # эталоны из URL выпуска (HTML)
  queues.py        # активные очереди редактора (без Архива)
  similarity.py    # RapidFuzz + авторы
  service.py       # find + archive
  models.py
ipsas/services/article_duplicates.py
ipsas/web/article_duplicates.py
find_article_duplicates.py
tests/test_article_duplicates.py
```
