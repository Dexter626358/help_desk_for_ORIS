"""Чтение CSV с разделами журнала."""

from __future__ import annotations

import csv
from pathlib import Path

from ipsas.modules.journal_sections.models import SectionSpec

REQUIRED_COLUMNS = (
    "number",
    "title_ru",
    "short_title_ru",
    "title_en",
    "short_title_en",
)


def _cell(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def load_sections_csv(path: Path) -> list[SectionSpec]:
    """Прочитать CSV (``;``, utf-8-sig) → список разделов по ``number``."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"CSV не найден: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        delimiter = ";" if sample.count(";") >= sample.count(",") else ","
        reader = csv.DictReader(fh, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError("CSV пуст или без заголовка")
        headers = {(h or "").strip().casefold(): (h or "").strip() for h in reader.fieldnames}
        missing = [c for c in REQUIRED_COLUMNS if c not in headers]
        if missing:
            raise ValueError(
                "В CSV нет колонок: "
                + ", ".join(missing)
                + f". Ожидаются: {', '.join(REQUIRED_COLUMNS)}"
            )

        # нормализуем ключи строк к каноническим именам
        key_map = {c: headers[c] for c in REQUIRED_COLUMNS}
        specs: list[SectionSpec] = []
        for idx, raw in enumerate(reader, start=2):
            row = {canon: _cell(raw, src) for canon, src in key_map.items()}
            if not any(row.values()):
                continue
            title_ru = row["title_ru"]
            if not title_ru:
                raise ValueError(f"Строка {idx}: пустой title_ru")
            short_ru = row["short_title_ru"]
            title_en = row["title_en"]
            short_en = row["short_title_en"]
            if not short_ru or not title_en or not short_en:
                raise ValueError(
                    f"Строка {idx}: нужны short_title_ru, title_en, short_title_en "
                    f"(раздел «{title_ru}»)"
                )
            number_raw = row["number"] or str(len(specs) + 1)
            try:
                number = int(number_raw)
            except ValueError as exc:
                raise ValueError(f"Строка {idx}: некорректный number={number_raw!r}") from exc
            specs.append(
                SectionSpec(
                    number=number,
                    title_ru=title_ru,
                    short_title_ru=short_ru,
                    title_en=title_en,
                    short_title_en=short_en,
                )
            )

    if not specs:
        raise ValueError("В CSV нет строк с разделами")
    specs.sort(key=lambda s: s.number)
    return specs
