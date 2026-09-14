"""Распаковка ZIP и группировка пар PDF/JSON."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

from ipsas.common.zip_safe import ZipLimits, extract_zip_safely
from ipsas.modules.eng_metadata.models import ArticlePair

ARTICLE_ID_RE = re.compile(r"article_(\d+)", re.I)

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


def build_article_pairs(extract_dir: Path) -> list[ArticlePair]:
    """Сгруппировать файлы по stem (без расширения) в пары PDF+JSON."""
    by_stem: dict[str, dict[str, str]] = defaultdict(dict)
    for path in sorted(extract_dir.rglob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in {".pdf", ".json"}:
            continue
        # arc relative name for display
        rel = path.relative_to(extract_dir).as_posix()
        stem = Path(rel).stem
        # flatten: use basename stem for pairing (files in root or subfolders)
        key = Path(rel).name.rsplit(".", 1)[0]
        by_stem[key][suffix] = rel

    pairs: list[ArticlePair] = []
    for stem in sorted(by_stem.keys()):
        files = by_stem[stem]
        article_id = _article_id_from_stem(stem) or ""
        pair = ArticlePair(
            stem=stem,
            article_id=article_id,
            pdf_name=files.get(".pdf"),
            json_name=files.get(".json"),
        )
        if not pair.pdf_name:
            pair.issues.append("Нет PDF для этой статьи")
        if not pair.json_name:
            pair.issues.append("Нет JSON для этой статьи")
        if not article_id:
            pair.issues.append("Не удалось извлечь article_id из имени файла")
        pairs.append(pair)
    return pairs
