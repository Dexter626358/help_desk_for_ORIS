"""Разбор ZIP с папками ``{start}-{end}_images``."""

from __future__ import annotations

import json
import logging
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ipsas.modules.issue_supp_images.models import ImageBundle, ImageFile, PageRange

logger = logging.getLogger(__name__)

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
CAPTIONS_FILENAME = "figure_captions.json"


@dataclass(frozen=True)
class FigureCaption:
    """Подписи рисунка из figure_captions.json."""

    ru: str = ""
    en: str = ""

    @property
    def has_any(self) -> bool:
        return bool(self.ru or self.en)


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


def _normalize_caption_entry(value: Any) -> FigureCaption:
    """Поддержать и legacy-строку, и объект ``{\"ru\": …, \"en\": …}``."""
    if value is None:
        return FigureCaption()
    if isinstance(value, str):
        # Старый формат: одна строка → русская подпись (как раньше писали в ru_RU)
        text = value.strip()
        return FigureCaption(ru=text) if text else FigureCaption()
    if isinstance(value, dict):
        ru = str(
            value.get("ru")
            or value.get("RU")
            or value.get("ru_RU")
            or ""
        ).strip()
        en = str(
            value.get("en")
            or value.get("EN")
            or value.get("en_US")
            or ""
        ).strip()
        return FigureCaption(ru=ru, en=en)
    text = str(value).strip()
    return FigureCaption(ru=text) if text else FigureCaption()


def _load_captions_json(raw: bytes) -> dict[str, FigureCaption]:
    """Разобрать figure_captions.json → stem файла → подписи ru/en."""
    text: str | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        logger.warning("figure_captions.json: не удалось декодировать")
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("figure_captions.json: невалидный JSON (%s)", exc)
        return {}
    if not isinstance(data, dict):
        logger.warning(
            "figure_captions.json: ожидался объект "
            '{ "Fig. 1": {"ru": "...", "en": "..."} }'
        )
        return {}
    result: dict[str, FigureCaption] = {}
    for key, value in data.items():
        if key is None:
            continue
        stem = str(key).strip()
        if not stem:
            continue
        caption = _normalize_caption_entry(value)
        if caption.has_any:
            result[stem] = caption
    return result


def _caption_for_filename(
    captions: dict[str, FigureCaption],
    filename: str,
) -> FigureCaption:
    """Подобрать подпись по имени файла (Fig. 1.jpeg → ключ «Fig. 1»)."""
    if not captions:
        return FigureCaption()
    stem = Path(filename).stem.strip()
    if stem in captions:
        return captions[stem]
    low = {k.casefold(): v for k, v in captions.items()}
    return low.get(stem.casefold(), FigureCaption())


def parse_images_archive(zip_path: Path, extract_dir: Path) -> list[ImageBundle]:
    """Распаковать ZIP и сгруппировать файлы по интервалам страниц.

    В каждой папке ``{start}-{end}_images`` может быть
    ``figure_captions.json``:

    - новый формат: ``{\"Fig. 1\": {\"ru\": \"…\", \"en\": \"…\"}}``
      (``ru`` / ``en`` опциональны);
    - legacy: ``{\"Fig. 1\": \"одна подпись\"}`` → считается русской.

    Подписи пишутся в title доп. файла на платформе; иначе — имя файла.
    """
    zip_path = Path(zip_path)
    extract_dir = Path(extract_dir)
    if not zip_path.is_file():
        raise ValueError(f"Архив не найден: {zip_path}")
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Файл не является ZIP: {zip_path}")

    extract_dir.mkdir(parents=True, exist_ok=True)
    captions_by_folder: dict[str, dict[str, FigureCaption]] = {}
    image_members: list[tuple[zipfile.ZipInfo, PageRange, str, str]] = []

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
            key = page_range.key

            if fname.casefold() == CAPTIONS_FILENAME:
                captions_by_folder[key] = _load_captions_json(zf.read(info))
                continue

            if Path(fname).suffix.lower() not in IMAGE_SUFFIXES:
                continue
            image_members.append((info, page_range, folder, fname))

        bundles: dict[str, ImageBundle] = {}
        for info, page_range, folder, fname in image_members:
            key = page_range.key
            target = extract_dir / key / fname
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, target.open("wb") as dst:
                dst.write(src.read())
            caption = _caption_for_filename(
                captions_by_folder.get(key, {}), fname
            )
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
                    title_ru=caption.ru,
                    title_en=caption.en,
                    title=caption.ru,
                )
            )

    result = list(bundles.values())
    for bundle in result:
        bundle.files.sort(key=lambda f: f.original_name.lower())
    result.sort(key=lambda b: (b.page_range.start, b.page_range.end))
    return result
