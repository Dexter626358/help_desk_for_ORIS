"""High-level PDF matching service helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from ipsas.modules.pdf_matching.matcher import PDFMatcher


def process_archive(
    zip_path: str,
    extract_dir: str = "./extracted",
    adaptive: bool = True,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Удобная функция для обработки одного архива.
    
    Args:
        zip_path: Путь к ZIP архиву
        extract_dir: Директория для извлечения
        adaptive: Использовать адаптивные пороги
        verbose: Подробное логирование
    
    Returns:
        Словарь с результатами
    """
    matcher = PDFMatcher(adaptive_thresholds=adaptive, verbose=verbose)
    
    zip_p = Path(zip_path)
    extract_p = Path(extract_dir) / zip_p.stem
    
    return matcher.process_zip(zip_p, extract_p)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m ipsas.modules.pdf_matching.service <path_to_zip>")
        sys.exit(1)

    result = process_archive(sys.argv[1])

    print("\n" + "=" * 80)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    print(f"Успешно: {result['success']}")
    print(f"Обработано статей: {result['total_articles']}")
    print(f"Сопоставлено: {result['matched_articles']}")
    print(f"Не сопоставлено: {result['unmatched_articles']}")
    print(f"\nОбработанный XML: {result['output_xml']}")
