"""Разбор ZIP с папками ``{start}-{end}_images``."""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

from ipsas.modules.issue_supp_images.models import ImageBundle, ImageFile, PageRange

FOLDER_RE = re.compile(
    r"^(?P<start>\d{1,5})\s*[-–—]\s*(?P<end>\d{1,5})[_ ]?images?$",
    re.I,
)
IMAGE_SUFFIXES = {
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".gif",
    ".webp",
    ".bmp",
}


def parse_page_range_token(value: str) -> PageRange | None:
    text = (value or "").strip().replace("–", "-").replace("—", "-")
    m = re.fullmatch(r"(\d{1,5})\s*-\s*(\d{1,5})", text)
    if not m:
        return None
    start, end = int(m.group(1)), int(m.group(2))
    if start > end:
        start, end = end, start
    return PageRange(start=start, end=end)


def parse_folder_name(name: str) -> PageRange | None:
    raw = (name or "").strip().strip("/\\")
    if not raw:
        return None
    raw = raw.replace("\\", "/").rstrip("/").split("/")[-1]
    m = FOLDER_RE.match(raw)
    if not m:
        return None
    start, end = int(m.group("start")), int(m.group("end"))
    if start > end:
        start, end = end, start
    return PageRange(start=start, end=end)


def parse_images_archive(zip_path: Path, extract_dir: Path) -> list[ImageBundle]:
    """Распаковать ZIP и сгруппировать файлы по интервалам страниц."""
    zip_path = Path(zip_path)
    extract_dir = Path(extract_dir)
    if not zip_path.is_file():
        raise ValueError(f"Архив не найден: {zip_path}")
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Файл не является ZIP: {zip_path}")

    extract_dir.mkdir(parents=True, exist_ok=True)
    bundles: dict[str, ImageBundle] = {}

    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            member = info.filename.replace("\\", "/")
            if member.startswith("/") or ".." in member.split("/"):
                continue
            parts = [p for p in member.split("/") if p]
            if len(parts) < 2:
                continue
            folder = parts[-2]
            page_range = parse_folder_name(folder)
            if page_range is None:
                continue
            fname = parts[-1]
            if Path(fname).suffix.lower() not in IMAGE_SUFFIXES:
                continue
            target = extract_dir / page_range.key / fname
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, target.open("wb") as dst:
                dst.write(src.read())
            key = page_range.key
            if key not in bundles:
                bundles[key] = ImageBundle(
                    page_range=page_range,
                    folder_name=folder,
                    files=[],
                )
            bundles[key].files.append(
                ImageFile(
                    path=target,
                    original_name=fname,
                    folder_key=key,
                )
            )

    result = list(bundles.values())
    for bundle in result:
        bundle.files.sort(key=lambda f: f.original_name.lower())
    result.sort(key=lambda b: (b.page_range.start, b.page_range.end))
    return result
