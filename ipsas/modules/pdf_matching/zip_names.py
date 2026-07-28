"""Helpers for ZIP member filename decoding."""

from __future__ import annotations

import re


def _decode_zip_filename(name: str) -> str:
    """
    Исправить кодировку имени файла из ZIP (кириллица: CP866/UTF-8, прочитаны как CP437 и т.д.).
    Возвращает строку в корректной Unicode для записи в UTF-8 XML (например, Гудимова_web.pdf).
    """
    if not name or not name.strip():
        return name
    # Пары (ошибочная кодировка при чтении, реальная кодировка в архиве)
    for wrong_enc, right_enc in (
        ("cp437", "cp866"),   # архив с CP866 (DOS), прочитан как CP437
        ("cp437", "utf-8"),   # архив с UTF-8, прочитан как CP437
        ("latin-1", "cp866"),
        ("cp1252", "cp866"),
    ):
        try:
            fixed = name.encode(wrong_enc).decode(right_enc)
            if re.search(r"[А-Яа-яЁё]", fixed):
                return fixed
        except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
            continue
    return name
