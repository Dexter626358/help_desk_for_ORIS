"""
Улучшенный модуль для сопоставления PDF файлов со статьями в XML.

Compatibility shim: implementation lives in ``ipsas.modules.pdf_matching``.

Ключевые улучшения:
1. Более надёжное извлечение DOI (учёт переносов строк, обрезанных DOI)
2. Улучшенное извлечение названий и авторов из PDF
3. Многоуровневая стратегия матчинга с приоритетами
4. Расширенная диагностика и логирование
5. Автоматическая подстройка порогов на основе данных
6. Поддержка частичных совпадений DOI
"""

from ipsas.modules.pdf_matching import (
    ArticleInfo,
    MatchMethod,
    MatchResult,
    PDFEntry,
    PDFMatcher,
    PDFMetadata,
    process_archive,
)

__all__ = [
    "PDFMatcher",
    "process_archive",
    "MatchMethod",
    "PDFEntry",
    "ArticleInfo",
    "PDFMetadata",
    "MatchResult",
]

if __name__ == "__main__":
    import sys

    from ipsas.modules.pdf_matching.service import process_archive as _process

    if len(sys.argv) < 2:
        print("Usage: python -m ipsas.modules.pdf_matcher <path_to_zip>")
        sys.exit(1)

    result = _process(sys.argv[1])

    print("\n" + "=" * 80)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    print(f"Успешно: {result['success']}")
    print(f"Обработано статей: {result['total_articles']}")
    print(f"Сопоставлено: {result['matched_articles']}")
    print(f"Не сопоставлено: {result['unmatched_articles']}")
    print(f"\nОбработанный XML: {result['output_xml']}")
