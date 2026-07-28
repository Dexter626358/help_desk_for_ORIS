"""Безопасная распаковка ZIP (path traversal, лимиты размера/числа файлов)."""

from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional


class UnsafeZipError(ValueError):
    """Архив отклонён политикой безопасности."""


@dataclass(frozen=True)
class ZipLimits:
    max_members: int = 500
    max_uncompressed_bytes: int = 200 * 1024 * 1024  # 200 MiB
    max_compression_ratio: float = 100.0
    max_single_file_bytes: int = 100 * 1024 * 1024
    allowed_suffixes: tuple[str, ...] = (".xml", ".pdf")


def _is_symlink_member(info: zipfile.ZipInfo) -> bool:
    # Unix symlink: external_attr high bits == 0xA000
    return ((info.external_attr >> 16) & 0xF000) == 0xA000


def safe_member_path(extract_to: Path, member_name: str) -> Path:
    """Путь внутри extract_to без traversal / абсолютных путей."""
    raw = (member_name or "").replace("\\", "/")
    if not raw or raw.endswith("/"):
        raise UnsafeZipError(f"Недопустимое имя в ZIP: {member_name!r}")
    if raw.startswith("/") or (len(raw) > 1 and raw[1] == ":"):
        raise UnsafeZipError(f"Абсолютный путь в ZIP запрещён: {member_name}")
    parts = [p for p in Path(raw).parts if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise UnsafeZipError(f"Path traversal в ZIP запрещён: {member_name}")
    if not parts:
        raise UnsafeZipError(f"Пустое имя в ZIP: {member_name!r}")
    target = (extract_to.joinpath(*parts)).resolve()
    try:
        target.relative_to(extract_to.resolve())
    except ValueError as e:
        raise UnsafeZipError(f"Выход за пределы каталога: {member_name}") from e
    return target


def validate_zip_limits(zf: zipfile.ZipFile, limits: ZipLimits) -> None:
    members = [m for m in zf.infolist() if not m.is_dir()]
    if len(members) > limits.max_members:
        raise UnsafeZipError(
            f"Слишком много файлов в ZIP: {len(members)} > {limits.max_members}"
        )
    total_uncomp = 0
    for info in members:
        if _is_symlink_member(info):
            raise UnsafeZipError(f"Символические ссылки в ZIP запрещены: {info.filename}")
        if info.file_size > limits.max_single_file_bytes:
            raise UnsafeZipError(
                f"Файл слишком большой: {info.filename} ({info.file_size} байт)"
            )
        total_uncomp += int(info.file_size)
        if info.compress_size > 0:
            ratio = info.file_size / max(info.compress_size, 1)
            if ratio > limits.max_compression_ratio and info.file_size > 1024 * 1024:
                raise UnsafeZipError(
                    f"Подозрительный коэффициент сжатия у {info.filename}: {ratio:.0f}x"
                )
    if total_uncomp > limits.max_uncompressed_bytes:
        raise UnsafeZipError(
            f"Суммарный размер после распаковки слишком велик: {total_uncomp} байт"
        )


def extract_zip_safely(
    zip_path: Path,
    extract_to: Path,
    *,
    limits: Optional[ZipLimits] = None,
    name_transform: Optional[Callable[[str], str]] = None,
    suffixes: Optional[Iterable[str]] = None,
) -> list[tuple[Path, str]]:
    """
    Распаковать ZIP с проверками.

    Returns:
        Список (путь_на_диске, arcname_после_transform).
    """
    limits = limits or ZipLimits()
    allowed = tuple(s.lower() for s in (suffixes if suffixes is not None else limits.allowed_suffixes))
    if not zip_path.exists() or not zipfile.is_zipfile(zip_path):
        raise UnsafeZipError(f"Некорректный ZIP: {zip_path}")

    extract_to.mkdir(parents=True, exist_ok=True)
    written: list[tuple[Path, str]] = []
    total_written = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        validate_zip_limits(zf, limits)
        for member in zf.infolist():
            if member.is_dir():
                continue
            arc = member.filename
            if name_transform is not None:
                arc = name_transform(arc)
            suffix = Path(arc).suffix.lower()
            if allowed and suffix not in allowed:
                continue
            target = safe_member_path(extract_to, arc)
            if _is_symlink_member(member):
                raise UnsafeZipError(f"Символические ссылки запрещены: {member.filename}")
            data = zf.read(member)
            if len(data) > limits.max_single_file_bytes:
                raise UnsafeZipError(f"Файл превышает лимит после чтения: {arc}")
            total_written += len(data)
            if total_written > limits.max_uncompressed_bytes:
                raise UnsafeZipError("Превышен лимит суммарной распаковки")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            written.append((target, arc.replace("\\", "/")))
    return written
