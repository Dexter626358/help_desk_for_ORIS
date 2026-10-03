"""Распаковка ZIP и группировка пар PDF/JSON."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

from ipsas.common.zip_safe import ZipLimits, extract_zip_safely
from ipsas.modules.eng_metadata.models import ArticlePair

ARTICLE_ID_RE = re.compile(r"article_(\d+)", re.I)
# Префикс страниц в имени: 3-21__article_288752, 100–110_article_1
PAGE_PREFIX_RE = re.compile(
    r"^(?P<start>\d{1,5})\s*[-–—]\s*(?P<end>\d{1,5})",
)

ENG_ZIP_LIMITS = ZipLimits(
    max_members=400,
    max_uncompressed_bytes=400 * 1024 * 1024,
    max_single_file_bytes=80 * 1024 * 1024,
    allowed_suffixes=(".pdf", ".json"),
)


def unpack_eng_archive(zip_path: Path, extract_to: Path) -> list[tuple[Path, str]]:
    """Распаковать ZIP с PDF/JSON. Raises UnsafeZipError / ValueError."""
    extract_to.mkdir(parents=True, exist_ok=True)
    written = extract_zip_safely(
        zip_path,
        extract_to,
        limits=ENG_ZIP_LIMITS,
        suffixes=(".pdf", ".json"),
    )
    if not written:
        raise ValueError("В архиве нет файлов .pdf / .json")
    return written


def _article_id_from_stem(stem: str) -> str | None:
    match = ARTICLE_ID_RE.search(stem)
    return match.group(1) if match else None


def parse_pages_from_stem(stem: str) -> tuple[int, int] | None:
    """Достать интервал страниц из начала имени файла."""
    raw = (stem or "").strip()
    if not raw:
        return None
    m = PAGE_PREFIX_RE.match(raw)
    if not m:
        return None
    start, end = int(m.group("start")), int(m.group("end"))
    if start > end:
        start, end = end, start
    return start, end


def _pair_sort_key(pair: ArticlePair) -> tuple[int, int, int, str]:
    """Сортировка: страницы по возрастанию, без префикса — в конец."""
    start = pair.page_start if pair.page_start is not None else 10**9
    end = pair.page_end if pair.page_end is not None else 10**9
    try:
        aid = int(pair.article_id) if pair.article_id else 10**9
    except ValueError:
        aid = 10**9
    return (start, end, aid, pair.stem.casefold())


def build_article_pairs(extract_dir: Path) -> list[ArticlePair]:
    """Сгруппировать файлы по stem в пары PDF+JSON.

    Порядок: по номерам страниц из префикса имени
    (``3-21__article_288752``) по возрастанию.
    """
    by_stem: dict[str, dict[str, str]] = defaultdict(dict)
    for path in sorted(extract_dir.rglob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in {".pdf", ".json"}:
            continue
        # arc relative name for display
        rel = path.relative_to(extract_dir).as_posix()
        # flatten: use basename stem for pairing (files in root or subfolders)
        key = Path(rel).name.rsplit(".", 1)[0]
        by_stem[key][suffix] = rel

    pairs: list[ArticlePair] = []
    for stem, files in by_stem.items():
        article_id = _article_id_from_stem(stem) or ""
        pages = parse_pages_from_stem(stem)
        page_start = pages[0] if pages else None
        page_end = pages[1] if pages else None
        pages_label = f"{page_start}-{page_end}" if pages else ""
        pair = ArticlePair(
            stem=stem,
            article_id=article_id,
            pdf_name=files.get(".pdf"),
            json_name=files.get(".json"),
            page_start=page_start,
            page_end=page_end,
            pages_label=pages_label,
        )
        if not pair.pdf_name:
            pair.issues.append("Нет PDF для этой статьи")
        if not pair.json_name:
            pair.issues.append("Нет JSON для этой статьи")
        if not article_id:
            pair.issues.append("Не удалось извлечь article_id из имени файла")
        pairs.append(pair)
    pairs.sort(key=_pair_sort_key)
    return pairs
