"""Предварительный аудит: схема + правила + 8 журналов. Массовый прогон — отдельно."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from independent_audit import (  # noqa: E402
    attach_service_comparison,
    audit_file,
    build_validation_rules,
)
from ipsas.services.check_journal_site import execute  # noqa: E402

OUT = ROOT / "reports" / "journal_settings_audit"
PREVIEW = OUT / "preview"
DATA = ROOT / "ras_settings"

PICKS = [
    "0869-7698.data",  # низкая заполненность
    "0367-6765.data",
    "0016-7770.data",
    "2686-7400.data",  # середина
    "0869-8139.data",
    "0203-0306.data",
    "2949-124X.data",  # высокая
    "0235-0106.data",
]


def main() -> None:
    PREVIEW.mkdir(parents=True, exist_ok=True)
    rules = build_validation_rules()
    (OUT / "validation_rules.json").write_text(
        json.dumps(
            {
                "version": "0.1-preview",
                "source": "DEFAULT_SETUP_CHECKLIST + independent clarifications",
                "scoring_model_proposal": {
                    "critical": 5,
                    "error": 3,
                    "warning": 1,
                    "info": 0,
                    "not_applicable_excluded": True,
                    "cannot_determine_not_auto_fail": True,
                    "partial_weight": 0.5,
                    "note": "Модель предложена для согласования до массового прогона",
                },
                "rules": rules,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    rows = []
    for name in PICKS:
        path = DATA / name
        indep = audit_file(path)
        svc = execute(data_file=path.read_bytes(), data_filename=name)
        indep = attach_service_comparison(indep, svc)
        rows.append(indep)
        print(
            f"{name}: indep={indep['completeness_score']}%/{indep['expected_readiness']} "
            f"svc={indep['service_score']}%/{indep['service_readiness']} "
            f"FP={indep['false_positives']} FN={indep['false_negatives']} mis={indep['mismatches']}"
        )

    (PREVIEW / "journal_audit_preview.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # aggregate compare counts
    from collections import Counter

    cmp_c = Counter()
    by_rule = Counter()
    for row in rows:
        for r in row["results"]:
            cmp_c[r["compare"]] += 1
            if r["compare"] in {"false_positive", "false_negative", "severity_mismatch"}:
                by_rule[f"{r['rule_id']}:{r['compare']}"] += 1

    summary = {
        "n_journals": len(rows),
        "compare_counts": dict(cmp_c),
        "top_rule_mismatches": by_rule.most_common(25),
        "journals": [
            {
                "filename": r["filename"],
                "journal_name": r["journal_name"],
                "completeness_score": r["completeness_score"],
                "expected_readiness": r["expected_readiness"],
                "service_score": r["service_score"],
                "service_readiness": r["service_readiness"],
                "false_positives": r["false_positives"],
                "false_negatives": r["false_negatives"],
                "mismatches": r["mismatches"],
                "failed": r["failed"],
                "partial": r["partial"],
                "cannot_determine": r["cannot_determine"],
                "not_applicable": r["not_applicable"],
            }
            for r in rows
        ],
    }
    (PREVIEW / "preview_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary["compare_counts"], ensure_ascii=False, indent=2))
    print("top mismatches:")
    for k, v in summary["top_rule_mismatches"][:15]:
        print(f"  {v}  {k}")


if __name__ == "__main__":
    main()
