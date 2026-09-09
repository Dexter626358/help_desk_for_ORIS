"""Полный аудит 141 журнала → все артефакты reports/journal_settings_audit/."""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from independent_audit import (  # noqa: E402
    DEFAULT_SETUP_CHECKLIST,
    attach_service_comparison,
    audit_file,
    build_validation_rules,
)
from ipsas.services.check_journal_site import execute  # noqa: E402

OUT = ROOT / "reports" / "journal_settings_audit"
DATA = ROOT / "ras_settings"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _readiness_bucket(pct: float) -> str:
    if pct >= 90:
        return "ready"
    if pct >= 80:
        return "mostly_ready"
    if pct >= 65:
        return "needs_revision"
    return "not_ready"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    files = sorted(DATA.glob("*.data"))
    print(f"files={len(files)} start={datetime.now().isoformat(timespec='seconds')}")

    rules = build_validation_rules()
    # apply agreed policies into rules metadata
    for r in rules:
        if r["checklist_id"] == "modules.doi":
            r["severity"] = "warning"
            r["applies_to_all_journals"] = False
            r["exceptions"] = (
                "Опционально: если журнал присваивает DOI — включить и указать префикс; иначе na"
            )
            r["editorial_message"] = (
                "Если журнал присваивает DOI — включить DOI и указать префикс"
            )
        if r["checklist_id"] == "step4.access":
            r["severity"] = "info"
            r["editorial_message"] = "Подсказка: сверьте роли регистрации вручную"
        if r["checklist_id"] == "modules.extra_off":
            r["exceptions"] = "Отмечать лишние модули (не игнорировать)"
        if r["checklist_id"] == "step3.copyright":
            r["pass_condition"] += (
                "; отсутствие только licenseURL при заполненном тексте → partial"
            )

    (OUT / "validation_rules.json").write_text(
        json.dumps(
            {
                "version": "1.0-agreed",
                "agreed_policies": {
                    "doi": "optional / na if disabled; message if journal assigns DOI",
                    "copyright_license_only": "partial",
                    "step4_access": "info hint / manual, not mandatory fail",
                    "modules_extra_off": "must mark (do not ignore)",
                    "weights": {"critical": 5, "error": 3, "warning": 1, "info": 0, "partial": 0.5},
                    "cannot_determine": "no auto-fail",
                },
                "rules": rules,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    audits: list[dict] = []
    mismatches_rows: list[dict] = []
    rule_stats: dict[str, Counter] = defaultdict(Counter)

    jsonl_path = OUT / "journal_audit.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as jl:
        for i, path in enumerate(files, 1):
            indep = audit_file(path)
            svc = execute(data_file=path.read_bytes(), data_filename=path.name)
            indep = attach_service_comparison(indep, svc)
            audits.append(indep)
            jl.write(json.dumps(indep, ensure_ascii=False) + "\n")

            for r in indep["results"]:
                cid = None
                for item in DEFAULT_SETUP_CHECKLIST:
                    if item.id.upper().replace(".", "_") == r["rule_id"]:
                        cid = item.id
                        break
                cid = cid or r["rule_id"]
                cmp = r.get("compare") or "cannot_compare"
                rule_stats[cid][cmp] += 1
                rule_stats[cid]["n"] += 1
                if cmp in {
                    "false_positive",
                    "false_negative",
                    "severity_mismatch",
                    "message_mismatch",
                    "score_mismatch",
                }:
                    mismatches_rows.append(
                        {
                            "filename": path.name,
                            "journal_name": indep["journal_name"],
                            "rule_id": r["rule_id"],
                            "checklist_id": cid,
                            "compare": cmp,
                            "independent_status": r["expected_status"],
                            "service_status": r.get("service_status"),
                            "severity": r.get("severity"),
                            "json_path": r.get("json_path"),
                            "evidence": (r.get("evidence") or "")[:300],
                            "explanation": (r.get("explanation") or "")[:300],
                        }
                    )

            if i % 20 == 0:
                print(f"... {i}/{len(files)}")

    # journal_summary.csv
    summary_rows = []
    for a in audits:
        summary_rows.append(
            {
                "filename": a["filename"],
                "journal_id": a["journal_id"],
                "journal_name": a["journal_name"],
                "applicable_checks": a["applicable_checks"],
                "passed": a["passed"],
                "failed": a["failed"],
                "partial": a["partial"],
                "cannot_determine": a["cannot_determine"],
                "critical_errors": a["critical_errors"],
                "completeness_score": a["completeness_score"],
                "correctness_score": a["correctness_score"],
                "expected_readiness": a["expected_readiness"],
                "service_score": a.get("service_score"),
                "service_readiness": a.get("service_readiness"),
                "mismatches": a.get("mismatches"),
                "false_positives": a.get("false_positives"),
                "false_negatives": a.get("false_negatives"),
                "review_required": a.get("review_required"),
            }
        )
    _write_csv(
        OUT / "journal_summary.csv",
        list(summary_rows[0].keys()),
        summary_rows,
    )

    # rule_metrics.csv
    metric_rows = []
    for cid, c in sorted(rule_stats.items()):
        n = c["n"] or 1
        tp, tn = c["true_positive"], c["true_negative"]
        fp, fn = c["false_positive"], c["false_negative"]
        decided = tp + tn + fp + fn
        precision = tp / (tp + fp) if (tp + fp) else None
        recall = tp / (tp + fn) if (tp + fn) else None
        f1 = (
            2 * precision * recall / (precision + recall)
            if precision is not None and recall is not None and (precision + recall)
            else None
        )
        accuracy = (tp + tn) / decided if decided else None
        metric_rows.append(
            {
                "checklist_id": cid,
                "n": n,
                "true_positive": tp,
                "true_negative": tn,
                "false_positive": fp,
                "false_negative": fn,
                "severity_mismatch": c["severity_mismatch"],
                "cannot_compare": c["cannot_compare"],
                "accuracy": round(accuracy, 4) if accuracy is not None else "",
                "precision": round(precision, 4) if precision is not None else "",
                "recall": round(recall, 4) if recall is not None else "",
                "f1": round(f1, 4) if f1 is not None else "",
                "fpr": round(fp / (fp + tn), 4) if (fp + tn) else "",
                "fnr": round(fn / (fn + tp), 4) if (fn + tp) else "",
            }
        )
    _write_csv(OUT / "rule_metrics.csv", list(metric_rows[0].keys()), metric_rows)

    _write_csv(
        OUT / "mismatches.csv",
        [
            "filename",
            "journal_name",
            "rule_id",
            "checklist_id",
            "compare",
            "independent_status",
            "service_status",
            "severity",
            "json_path",
            "evidence",
            "explanation",
        ],
        mismatches_rows,
    )

    # manual review stratified sample
    by_fill = sorted(audits, key=lambda a: a["completeness_score"])
    by_mis = sorted(audits, key=lambda a: (-(a.get("mismatches") or 0), a["completeness_score"]))
    readiness_mismatch = [
        a for a in audits if a.get("expected_readiness") != a.get("service_readiness")
    ]
    borderline = [
        a for a in audits if 78 <= float(a["completeness_score"]) <= 86
    ]
    high_cd = sorted(audits, key=lambda a: -a["cannot_determine"])

    picked: list[dict] = []
    seen = set()

    def add(a: dict, reason: str) -> None:
        if a["filename"] in seen:
            return
        seen.add(a["filename"])
        picked.append(
            {
                "filename": a["filename"],
                "journal_name": a["journal_name"],
                "completeness_score": a["completeness_score"],
                "expected_readiness": a["expected_readiness"],
                "service_score": a.get("service_score"),
                "service_readiness": a.get("service_readiness"),
                "mismatches": a.get("mismatches"),
                "false_positives": a.get("false_positives"),
                "false_negatives": a.get("false_negatives"),
                "cannot_determine": a["cannot_determine"],
                "reason": reason,
            }
        )

    for a in by_fill[:3]:
        add(a, "низкая заполненность (indep)")
    for a in by_fill[len(by_fill) // 2 - 1 : len(by_fill) // 2 + 2]:
        add(a, "средняя заполненность")
    for a in by_fill[-3:]:
        add(a, "высокая заполненность")
    for a in by_mis[:5]:
        add(a, "максимум расхождений с сервисом")
    for a in borderline[:4]:
        add(a, "пограничная оценка 78–86%")
    for a in readiness_mismatch[:4]:
        add(a, "расхождение статуса готовности")
    for a in high_cd[:3]:
        add(a, "много cannot_determine")

    _write_csv(
        OUT / "manual_review.csv",
        list(picked[0].keys()) if picked else ["filename", "reason"],
        picked,
    )

    # regression tests
    regressions = [
        {
            "test_id": "REG_DOI_DISABLED_OPTIONAL",
            "rule": "modules.doi",
            "scenario": "DOI плагин выключен — не fail, опционально",
            "input": {"plugins": {"DOIPubIdPlugin": {"enabled": False}}},
            "expected_status": "not_applicable",
            "expected_severity": "warning",
            "expected_message_meaning": "если журнал присваивает DOI",
            "example_journal": next(
                (
                    a["filename"]
                    for a in audits
                    for r in a["results"]
                    if r["rule_id"] == "MODULES_DOI" and r["expected_status"] == "not_applicable"
                ),
                "",
            ),
        },
        {
            "test_id": "REG_DOI_ENABLED_NO_PREFIX",
            "rule": "modules.doi",
            "scenario": "DOI включён без префикса",
            "input": {"plugins": {"DOIPubIdPlugin": {"enabled": True, "settings": {"doiPrefix": ""}}}},
            "expected_status": "fail",
            "expected_severity": "warning",
            "expected_message_meaning": "doiPrefix пуст",
            "example_journal": "",
        },
        {
            "test_id": "REG_FOCUS_SCOPE_DESC",
            "rule": "step2.focus",
            "scenario": "focusScopeDesc заполнен, focusAndScope пуст",
            "input": {"settings": {"focusScopeDesc": {"ru_RU": "x" * 50, "en_US": "y" * 50}}},
            "expected_status": "pass",
            "expected_severity": "critical",
            "expected_message_meaning": "предметная область найдена",
            "example_journal": "2686-7400.data",
        },
        {
            "test_id": "REG_COPYRIGHT_LICENSE_ONLY_PARTIAL",
            "rule": "step3.copyright",
            "scenario": "notice RU/EN + holder есть, licenseURL пуст",
            "input": {
                "settings": {
                    "copyrightNotice": {"ru_RU": "a" * 40, "en_US": "b" * 40},
                    "copyrightHolderType": "other",
                    "licenseURL": "",
                }
            },
            "expected_status": "partial",
            "expected_severity": "critical",
            "expected_message_meaning": "не хватает лицензии",
            "example_journal": next(
                (
                    m["filename"]
                    for m in mismatches_rows
                    if m["checklist_id"] == "step3.copyright"
                ),
                "2686-7400.data",
            ),
        },
        {
            "test_id": "REG_EMPTY_STRING",
            "rule": "step1.elibrary",
            "scenario": "elibraryId пустая строка",
            "input": {"settings": {"elibraryId": ""}},
            "expected_status": "fail",
            "expected_severity": "error",
            "expected_message_meaning": "не заполнено",
            "example_journal": "",
        },
        {
            "test_id": "REG_NULL_KEY_ABSENT",
            "rule": "step1.indexing_kw",
            "scenario": "searchKeywords отсутствует",
            "input": {"settings": {}},
            "expected_status": "fail",
            "expected_severity": "error",
            "expected_message_meaning": "ключевые слова",
            "example_journal": "",
        },
        {
            "test_id": "REG_HTML_EMPTY",
            "rule": "step2.review",
            "scenario": "reviewPolicy = <p>&nbsp;</p>",
            "input": {"settings": {"reviewPolicy": {"ru_RU": "<p>&nbsp;</p>", "en_US": "<p></p>"}}},
            "expected_status": "fail",
            "expected_severity": "critical",
            "expected_message_meaning": "пусто после очистки HTML",
            "example_journal": "",
        },
        {
            "test_id": "REG_PARTIAL_RU_ONLY",
            "rule": "step3.guidelines",
            "scenario": "authorGuidelines только RU",
            "input": {
                "settings": {
                    "authorGuidelines": {"ru_RU": "Правила " + "а" * 40, "en_US": ""}
                }
            },
            "expected_status": "partial",
            "expected_severity": "critical",
            "expected_message_meaning": "только русский",
            "example_journal": "",
        },
        {
            "test_id": "REG_ACCESS_HINT",
            "rule": "step4.access",
            "scenario": "не все роли — подсказка, не fail",
            "input": {
                "settings": {
                    "disableUserReg": False,
                    "allowRegAuthor": True,
                    "allowRegReader": False,
                    "allowRegReviewer": True,
                }
            },
            "expected_status": "cannot_determine",
            "expected_severity": "info",
            "expected_message_meaning": "подсказка сверить роли",
            "example_journal": "",
        },
        {
            "test_id": "REG_EXTRA_OFF_MARK",
            "rule": "modules.extra_off",
            "scenario": "лишние модули отмечаются",
            "input": {"plugins": {"SomeWeirdPlugin": {"enabled": True, "category": "generic"}}},
            "expected_status": "partial",
            "expected_severity": "warning",
            "expected_message_meaning": "отмечено вне эталона",
            "example_journal": "",
        },
        {
            "test_id": "REG_CHECKLIST_EMPTY_ARRAY",
            "rule": "step3.checklist",
            "scenario": "submissionChecklist null",
            "input": {"settings": {"submissionChecklist": None}},
            "expected_status": "fail",
            "expected_severity": "error",
            "expected_message_meaning": "список требований отсутствует",
            "example_journal": "",
        },
        {
            "test_id": "REG_SPACES_ONLY",
            "rule": "step1.abbreviation",
            "scenario": "abbreviation из пробелов",
            "input": {"settings": {"abbreviation": {"ru_RU": "   ", "en_US": "\t"}}},
            "expected_status": "fail",
            "expected_severity": "error",
            "expected_message_meaning": "пусто",
            "example_journal": "",
        },
    ]
    (OUT / "regression_tests.json").write_text(
        json.dumps(regressions, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # aggregate metrics
    all_cmp = Counter()
    for a in audits:
        for r in a["results"]:
            all_cmp[r.get("compare") or "cannot_compare"] += 1
    tp, tn, fp, fn = (
        all_cmp["true_positive"],
        all_cmp["true_negative"],
        all_cmp["false_positive"],
        all_cmp["false_negative"],
    )
    decided = tp + tn + fp + fn
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0
    accuracy = (tp + tn) / decided if decided else 0

    score_diffs = [
        float(a["completeness_score"]) - float(a.get("service_score") or 0) for a in audits
    ]
    perfect = sum(1 for a in audits if (a.get("mismatches") or 0) == 0)
    false_ready = sum(
        1
        for a in audits
        if a.get("service_readiness") in {"ready", "mostly_ready"}
        and a.get("expected_readiness") in {"not_ready", "needs_revision"}
    )
    false_not_ready = sum(
        1
        for a in audits
        if a.get("service_readiness") in {"not_ready", "needs_revision"}
        and a.get("expected_readiness") in {"ready", "mostly_ready"}
    )

    fill_dist = Counter()
    for a in audits:
        s = float(a["completeness_score"])
        if s < 75:
            fill_dist["<75%"] += 1
        elif s < 85:
            fill_dist["75–84%"] += 1
        elif s < 92:
            fill_dist["85–91%"] += 1
        else:
            fill_dist[">=92%"] += 1

    ready_dist = Counter(a["expected_readiness"] for a in audits)
    top_fp = Counter(m["checklist_id"] for m in mismatches_rows if m["compare"] == "false_positive")
    top_fn = Counter(m["checklist_id"] for m in mismatches_rows if m["compare"] == "false_negative")
    top_sev = Counter(
        m["checklist_id"] for m in mismatches_rows if m["compare"] == "severity_mismatch"
    )

    best_rules = sorted(
        metric_rows,
        key=lambda r: (r["accuracy"] if r["accuracy"] != "" else -1, -(r["false_positive"] + r["false_negative"])),
        reverse=True,
    )[:10]
    worst_rules = sorted(
        metric_rows,
        key=lambda r: (
            -(r["false_positive"] + r["false_negative"]),
            r["accuracy"] if r["accuracy"] != "" else 99,
        ),
    )[:10]

    report = f"""# Итоговый отчёт: аудит сервиса проверки настроек журнала

**Дата:** {datetime.now().strftime("%Y-%m-%d %H:%M")}  
**Выборка:** {len(audits)} файлов `ras_settings/*.data`  
**Согласованные правила:** DOI опционально; copyright без licenseURL → partial; access → подсказка; extra_off → отмечать.

## 1. Краткий вывод о надёжности

Сервис **в целом работоспособен** на выборке РАН: падений нет, базовые поля и плагины оцениваются близко к независимому эталону.  
Точность по сопоставимым решениям (TP/TN/FP/FN): **accuracy={accuracy:.1%}**, precision={precision:.1%}, recall={recall:.1%}, F1={f1:.1%}.

Главные зоны риска: расхождения по `step3.submission_ack`, `step1.mailing`, эвристика `modules.extra_off`, severity у copyright/раздела «Статьи».  
Журналов с нулём расхождений: **{perfect}/{len(audits)}**.

## 2. Описание выборки

- Источник: экспорт OJS `.data` (JSON) журналов РАН.
- Всего: **{len(audits)}** журналов.
- Независимый аудит + прогон сервиса `execute()` без изменения исходных файлов.

## 3. Распределение по заполненности (indep completeness)

| Диапазон | Журналов |
|---|---:|
| <75% | {fill_dist['<75%']} |
| 75–84% | {fill_dist['75–84%']} |
| 85–91% | {fill_dist['85–91%']} |
| ≥92% | {fill_dist['>=92%']} |

Средняя indep completeness: **{statistics.mean(a['completeness_score'] for a in audits):.1f}%**  
Медиана: **{statistics.median(a['completeness_score'] for a in audits):.1f}%**

## 4. Распределение по готовности (indep)

| Уровень | Журналов |
|---|---:|
| ready | {ready_dist['ready']} |
| mostly_ready | {ready_dist['mostly_ready']} |
| needs_revision | {ready_dist['needs_revision']} |
| not_ready | {ready_dist['not_ready']} |
| manual_review | {ready_dist['manual_review']} |

## 5. Общие метрики качества сервиса

| Метрика | Значение |
|---|---:|
| Сопоставимых решений | {decided} |
| TP | {tp} |
| TN | {tn} |
| FP | {fp} |
| FN | {fn} |
| severity_mismatch | {all_cmp['severity_mismatch']} |
| cannot_compare | {all_cmp['cannot_compare']} |
| Accuracy | {accuracy:.1%} |
| Precision | {precision:.1%} |
| Recall | {recall:.1%} |
| F1 | {f1:.1%} |
| FPR | {(fp/(fp+tn) if (fp+tn) else 0):.1%} |
| FNR | {(fn/(fn+tp) if (fn+tp) else 0):.1%} |
| Журналы без расхождений | {perfect} |
| Ср. разница indep−service score | {statistics.mean(score_diffs):.2f} |
| Медиана разницы | {statistics.median(score_diffs):.2f} |
| Макс. |разница| | {max(abs(x) for x in score_diffs):.2f} |
| Ошибочно «готовее» сервиса | {false_ready} |
| Ошибочно «неготовее» сервиса | {false_not_ready} |

## 6. Наиболее точные проверки

"""
    for r in best_rules:
        if r["accuracy"] == "":
            continue
        report += f"- `{r['checklist_id']}`: accuracy={r['accuracy']}, FP={r['false_positive']}, FN={r['false_negative']}\n"

    report += "\n## 7. Наиболее проблемные проверки\n\n"
    for r in worst_rules:
        report += (
            f"- `{r['checklist_id']}`: FP={r['false_positive']}, FN={r['false_negative']}, "
            f"sev_mismatch={r['severity_mismatch']}, accuracy={r['accuracy']}\n"
        )

    report += "\n## 8. Ложные положительные (сервис ругает зря / строже эталона)\n\n"
    for k, v in top_fp.most_common(12):
        report += f"- `{k}`: {v}\n"

    report += "\n## 9. Ложные отрицательные (сервис пропускает)\n\n"
    for k, v in top_fn.most_common(12):
        report += f"- `{k}`: {v}\n"

    report += "\n## 10. Ошибки итоговой оценки\n\n"
    report += (
        f"Средняя разница indep−service: {statistics.mean(score_diffs):.2f} п.п. "
        f"(медиана {statistics.median(score_diffs):.2f}). "
        f"Расхождений готовности: сервису «готовее» — {false_ready}, «неготовее» — {false_not_ready}.\n"
    )

    report += """
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
"""

    (OUT / "final_report.md").write_text(report, encoding="utf-8")
    print(f"done journals={len(audits)} mismatches={len(mismatches_rows)}")
    print(f"accuracy={accuracy:.3f} precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
