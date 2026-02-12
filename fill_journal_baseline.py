#!/usr/bin/env python3
"""
Скрипт для сбора статистики по архиву журнала (длины аннотаций, количество источников)
и заполнения ipsas/config/journal_baseline_stats.json.

Использование (из корня проекта):
  python fill_journal_baseline.py [URL архива]
  python fill_journal_baseline.py --limit 3   # только первые 3 выпуска (для проверки)

По умолчанию: https://journals.rcsi.science/0320-9652/issue/archive
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# корень проекта
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from lxml import html


ARCHIVE_URL_DEFAULT = "https://journals.rcsi.science/0320-9652/issue/archive"
STATS_FILE = PROJECT_ROOT / "ipsas" / "config" / "journal_baseline_stats.json"
USER_AGENT = "IPSAS-Journal-Baseline/1.0"


def fetch_html(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def extract_issue_urls(archive_url: str) -> list[str]:
    """Скачать страницу архива и извлечь ссылки на выпуски (issue/view/ID)."""
    data = fetch_html(archive_url)
    root = html.fromstring(data)
    base = re.match(r"^(https?://[^/]+)", archive_url)
    base_url = base.group(1) if base else archive_url
    urls = []
    for a in root.xpath("//a[contains(@href, '/issue/view/')]"):
        href = (a.get("href") or "").strip()
        if not href or "/issue/view/" not in href:
            continue
        if href.startswith("/"):
            full = base_url + href
        elif href.startswith("http"):
            full = href
        else:
            full = base_url + "/" + href
        if full not in urls:
            urls.append(full)
    return urls


def compute_stats(values: list[int | float]) -> dict:
    """Посчитать min, max, mean, median, p10, p90 по списку чисел."""
    values = [v for v in values if v is not None]
    n = len(values)
    if n == 0:
        return {"n": 0, "min": None, "max": None, "mean": None, "median": None, "p10": None, "p90": None}
    sorted_v = sorted(values)
    mean = sum(values) / n
    median = sorted_v[n // 2] if n % 2 else (sorted_v[n // 2 - 1] + sorted_v[n // 2]) / 2.0
    p10_idx = min(int(n * 0.10), n - 1)
    p90_idx = min(int(n * 0.90), n - 1)
    return {
        "n": n,
        "min": sorted_v[0],
        "max": sorted_v[-1],
        "mean": round(mean, 2),
        "median": round(median, 2),
        "p10": sorted_v[p10_idx],
        "p90": sorted_v[p90_idx],
    }


def main() -> None:
    argv = sys.argv[1:]
    limit = None
    rest = []
    i = 0
    while i < len(argv):
        if argv[i] == "--limit" and i + 1 < len(argv):
            try:
                limit = int(argv[i + 1])
                i += 2
                continue
            except ValueError:
                pass
        rest.append(argv[i])
        i += 1
    archive_url = rest[0] if rest else ARCHIVE_URL_DEFAULT
    print(f"Архив: {archive_url}")

    print("Извлечение ссылок на выпуски...")
    issue_urls = extract_issue_urls(archive_url)
    if limit:
        issue_urls = issue_urls[:limit]
        print(f"Ограничение: первые {limit} выпусков")
    print(f"Выпусков к обработке: {len(issue_urls)}")

    from ipsas.modules.issue_metadata_parser import IssueMetadataParser

    parser = IssueMetadataParser()
    len_ru: list[int] = []
    len_en: list[int] = []
    ref_counts: list[int] = []
    issue_metadata_sample = None
    issues_processed = 0

    for i, issue_url in enumerate(issue_urls, 1):
        print(f"[{i}/{len(issue_urls)}] {issue_url}")
        try:
            result = parser.parse_issue_url(issue_url)
            issue = result.get("issue") or {}
            articles = result.get("articles") or []
            if not issue_metadata_sample and issue:
                issue_metadata_sample = issue
            issues_processed += 1
            for art in articles:
                ru_stats = art.get("abstract_ru_stats") or {}
                en_stats = art.get("abstract_en_stats") or {}
                lr = ru_stats.get("length")
                le = en_stats.get("length")
                rc = art.get("references_count")
                if lr is not None:
                    len_ru.append(int(lr))
                if le is not None:
                    len_en.append(int(le))
                if rc is not None:
                    ref_counts.append(int(rc))
        except Exception as e:
            print(f"  Ошибка: {e}")
        time.sleep(0.5)

    print(f"Обработано выпусков: {issues_processed}")
    print(f"Аннотаций RU: {len(len_ru)}, EN: {len(len_en)}, источников: {len(ref_counts)}")

    stats_ru = compute_stats(len_ru)
    stats_en = compute_stats(len_en)
    stats_ref = compute_stats(ref_counts)

    # Определяем ISSN и названия из первого выпуска или по умолчанию
    issn_print = (issue_metadata_sample or {}).get("issn") or "0320-9652"
    issn_online = (issue_metadata_sample or {}).get("eissn") or "3034-5227"
    journal_title = (issue_metadata_sample or {}).get("journal_title") or "Inland Water Biology"
    journal_title_ru = (issue_metadata_sample or {}).get("journal_title_ru") or "Биология внутренних вод"

    entry = {
        "issn_print": issn_print,
        "issn_online": issn_online,
        "journal_title": journal_title,
        "journal_title_ru": journal_title_ru,
        "updated": time.strftime("%Y-%m-%d"),
        "articles_count": len(len_ru) or len(len_en) or len(ref_counts),
        "issues_count": issues_processed,
        "abstract_ru": stats_ru,
        "abstract_en": stats_en,
        "references_count": stats_ref,
    }

    if STATS_FILE.exists():
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}

    # Ключ по печатному ISSN; дублируем запись по онлайн ISSN для быстрого поиска
    data[issn_print] = entry
    if issn_online and issn_online != issn_print:
        data[issn_online] = entry

    STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Записано в {STATS_FILE}")


if __name__ == "__main__":
    main()
