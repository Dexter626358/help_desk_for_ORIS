"""Сервис: проверка JATS XML (файл или ZIP выпуска) для Метафоры."""

from __future__ import annotations

import logging
import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from ipsas.common.zip_safe import UnsafeZipError, ZipLimits, extract_zip_safely
from ipsas.modules.metafora_jats.models import ValidationReport
from ipsas.modules.metafora_jats.validator import validate_jats_bytes, validate_jats_file

logger = logging.getLogger(__name__)

METAFORA_ZIP_LIMITS = ZipLimits(
    max_members=400,
    max_uncompressed_bytes=400 * 1024 * 1024,
    max_single_file_bytes=80 * 1024 * 1024,
    allowed_suffixes=(".xml",),
)


@dataclass
class MetaforaJatsResult:
    """Результат одной статьи или пакета из ZIP."""

    source_name: str
    reports: list[ValidationReport] = field(default_factory=list)
    is_batch: bool = False
    batch_error: str = ""

    @property
    def report(self) -> ValidationReport | None:
        return self.reports[0] if self.reports else None

    @property
    def ok(self) -> bool:
        if self.batch_error:
            return False
        if not self.reports:
            return False
        return all(r.valid_for_metafora for r in self.reports)

    @property
    def article_count(self) -> int:
        return len(self.reports)

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.reports if r.valid_for_metafora)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.reports if not r.valid_for_metafora)

    def to_dict(self) -> dict:
        return {
            "source_name": self.source_name,
            "is_batch": self.is_batch,
            "batch_error": self.batch_error,
            "article_count": self.article_count,
            "ok_count": self.ok_count,
            "fail_count": self.fail_count,
            "valid_for_metafora": self.ok,
            "reports": [r.to_dict() for r in self.reports],
        }


def _skip_zip_member(arcname: str) -> bool:
    name = arcname.replace("\\", "/")
    base = Path(name).name
    if not base or base.startswith("."):
        return True
    if "__macosx" in name.casefold():
        return True
    return False


def _validate_zip_bytes(zip_bytes: bytes, *, source_name: str) -> MetaforaJatsResult:
    tmp_root = Path(tempfile.mkdtemp(prefix="metafora_jats_zip_"))
    zip_path = tmp_root / "upload.zip"
    extract_dir = tmp_root / "extract"
    try:
        zip_path.write_bytes(zip_bytes)
        if not zipfile.is_zipfile(zip_path):
            return MetaforaJatsResult(
                source_name=source_name,
                is_batch=True,
                batch_error="Файл не является ZIP-архивом.",
            )
        written = extract_zip_safely(
            zip_path,
            extract_dir,
            limits=METAFORA_ZIP_LIMITS,
            suffixes=(".xml",),
        )
        xml_members = [
            (path, arc) for path, arc in written if not _skip_zip_member(arc)
        ]
        xml_members.sort(key=lambda x: x[1].casefold())
        if not xml_members:
            return MetaforaJatsResult(
                source_name=source_name,
                is_batch=True,
                batch_error="В архиве нет XML-файлов статей.",
            )
        reports: list[ValidationReport] = []
        for path, arc in xml_members:
            report = validate_jats_file(path)
            report.filename = arc.replace("\\", "/")
            reports.append(report)
        return MetaforaJatsResult(
            source_name=source_name,
            reports=reports,
            is_batch=True,
        )
    except UnsafeZipError as exc:
        logger.warning("metafora zip rejected: %s", exc)
        return MetaforaJatsResult(
            source_name=source_name,
            is_batch=True,
            batch_error=str(exc),
        )
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def execute(
    *,
    xml_path: Path | None = None,
    xml_bytes: bytes | None = None,
    filename: str = "",
) -> MetaforaJatsResult:
    """Проверить один XML или ZIP с JATS."""
    name = filename or (xml_path.name if xml_path else "article.xml")
    low = name.casefold()

    if xml_bytes is not None:
        if low.endswith(".zip"):
            return _validate_zip_bytes(xml_bytes, source_name=name)
        report = validate_jats_bytes(xml_bytes, filename=name)
        return MetaforaJatsResult(
            source_name=name,
            reports=[report],
            is_batch=False,
        )

    if xml_path is None:
        raise ValueError("Укажите xml_path или xml_bytes")

    path = Path(xml_path)
    disp = filename or path.name
    if path.suffix.casefold() == ".zip" or disp.casefold().endswith(".zip"):
        return _validate_zip_bytes(path.read_bytes(), source_name=disp)

    report = validate_jats_file(path)
    report.filename = disp
    return MetaforaJatsResult(
        source_name=disp,
        reports=[report],
        is_batch=False,
    )

