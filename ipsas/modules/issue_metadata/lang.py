"""Эвристика языка текста (ru/en) для issue metadata."""

from __future__ import annotations

import re
from typing import Optional


def detect_lang(text: Optional[str]) -> Optional[str]:
    """Грубая эвристика языка: ru / en / None (не текст).

    Не считает «английским» строки без букв (числа, DOI, формулы).
    Транслитерацию отделяет `looks_like_transliteration` в validators.
    """
    if not text or not str(text).strip():
        return None
    s = str(text)
    cyrillic = len(re.findall(r"[А-Яа-яЁё]", s))
    latin = len(re.findall(r"[A-Za-z]", s))
    total = cyrillic + latin
    if total == 0:
        return None
    if cyrillic / total >= 0.8:
        return "ru"
    if latin / total >= 0.8:
        return "en"
    if cyrillic > latin:
        return "ru"
    if latin > cyrillic:
        return "en"
    return None
