#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тонкая CLI/совместимая обёртка над ipsas.modules.journal_xml_report.

Предпочтительный импорт в коде приложения:
    from ipsas.modules.journal_xml_report import ...
"""

from __future__ import annotations

import sys
from pathlib import Path

from ipsas.modules.journal_xml_report import (  # noqa: F401
    collect_article_issues,
    extract_first_last_words,
    format_article_title,
    generate_html_content,
    generate_html_report,
    get_articles_info,
    get_first_last_references,
    get_issue_info,
    safe_strip,
    validate_author_data,
    validate_keywords_data,
    validate_references_data,
    _format_article_title,
    _safe_strip,
)

# Windows консоль часто использует cp1251/cp866 и падает на эмодзи/символах.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def main() -> int:
    """CLI: python report_generator.py <xml> [html]."""
    if len(sys.argv) < 2:
        print("Использование: python report_generator.py <путь_к_xml_файлу> [путь_к_html_файлу]")
        return 1

    xml_file = Path(sys.argv[1])
    if not xml_file.exists():
        print(f"Ошибка: XML файл не найден: {xml_file}")
        return 1

    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    try:
        html_file = generate_html_report(xml_file, output_file)
        print(f"✅ HTML отчет успешно создан: {html_file}")
        return 0
    except Exception as e:
        print(f"❌ Ошибка при генерации отчета: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
