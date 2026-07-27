"""Имена скачиваемых файлов выпуска: issn_год_том_номер / issn_год_номер."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping, Optional

_TEMP_PREFIX_RE = re.compile(
    r"^\d{8}_\d{6}_[0-9a-f]{8}_(.+)$",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"(?:19|20)\d{2}")
_ISSN_RE = re.compile(r"[\dXx]{4}-?[\dXx]{4}", re.IGNORECASE)


def normalize_issn_token(value: Any) -> str:
    """ISSN для имени файла: цифры и дефис, без пробелов."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    m = _ISSN_RE.search(raw.replace(" ", ""))
    if m:
        token = m.group(0).upper().replace("X", "X")
        if "-" not in token and len(token) == 8:
            token = f"{token[:4]}-{token[4:]}"
        return token
    cleaned = re.sub(r"[^0-9Xx\-]", "", raw)
    return cleaned.upper() if cleaned else ""


def extract_year_token(value: Any) -> str:
    """Год YYYY из строки (dateUni, pub-date и т.п.)."""
    m = _YEAR_RE.search(str(value or ""))
    return m.group(0) if m else ""


def normalize_issue_part(value: Any) -> str:
    """Том / номер для имени файла (без пробелов и опасных символов)."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    cleaned = re.sub(r"[\\/:*?\"<>|\s]+", "-", raw)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-_.")
    return cleaned


def build_issue_download_basename(
    issn: Any = None,
    year: Any = None,
    volume: Any = None,
    number: Any = None,
    *,
    eissn: Any = None,
) -> str:
    """
    Базовое имя: ``issn_год_том_номер`` или ``issn_год_номер``.

    Если тома нет — ``issn_год_номер``. Пустые части пропускаются.
    """
    issn_tok = normalize_issn_token(issn) or normalize_issn_token(eissn)
    year_tok = extract_year_token(year)
    vol_tok = normalize_issue_part(volume)
    num_tok = normalize_issue_part(number)

    parts: list[str] = []
    if issn_tok:
        parts.append(issn_tok)
    if year_tok:
        parts.append(year_tok)
    if vol_tok and num_tok:
        parts.extend([vol_tok, num_tok])
    elif num_tok:
        parts.append(num_tok)
    elif vol_tok:
        parts.append(vol_tok)
    return "_".join(parts)


def with_report_stem(base: str, *, fallback: str = "report") -> str:
    """Добавить суффикс ``_report`` к базовому имени (без расширения)."""
    stem = (base or "").strip().rstrip("._-")
    if not stem:
        return fallback if fallback.endswith("report") else f"{fallback}_report"
    if stem.lower() == "report" or stem.lower().endswith("_report"):
        return stem
    return f"{stem}_report"


def build_issue_download_filename(
    issn: Any = None,
    year: Any = None,
    volume: Any = None,
    number: Any = None,
    *,
    eissn: Any = None,
    extension: str = ".xml",
    fallback: str = "output",
) -> str:
    """Полное имя файла: ``issn_год_том_номер_report.ext``."""
    base = with_report_stem(
        build_issue_download_basename(issn, year, volume, number, eissn=eissn),
        fallback=fallback,
    )
    ext = extension if extension.startswith(".") else f".{extension}"
    return f"{base}{ext}"


def basename_from_issue_meta(issue: Optional[Mapping[str, Any]]) -> str:
    """Имя из словаря метаданных выпуска (issue_metadata / OJS)."""
    if not isinstance(issue, Mapping):
        return ""
    number = (
        issue.get("issue")
        or issue.get("number")
        or issue.get("issue_serial")
        or issue.get("issue_number")
    )
    return build_issue_download_basename(
        issn=issue.get("issn"),
        year=issue.get("year") or issue.get("date_uni") or issue.get("pub_date"),
        volume=issue.get("volume"),
        number=number,
        eissn=issue.get("eissn"),
    )


def basename_from_journal_xml(xml_path: Path | str) -> str:
    """Имя из journal XML (ORIS/Elpub): issn, dateUni, volume, number."""
    from ipsas.modules.journal_xml_report.parsing import get_issue_info

    path = Path(xml_path)
    if not path.is_file():
        return ""
    try:
        info = get_issue_info(path)
    except Exception:
        return ""
    if not isinstance(info, dict):
        return ""
    return build_issue_download_basename(
        issn=info.get("issn"),
        year=info.get("date_uni") or info.get("year"),
        volume=info.get("volume"),
        number=info.get("number"),
        eissn=info.get("eissn"),
    )


def attachment_filename_from_xml(
    xml_path: Path | str,
    *,
    extension: str = ".xml",
    fallback: str = "output",
) -> str:
    """Content-Disposition имя по содержимому journal XML (с суффиксом ``_report``)."""
    base = with_report_stem(basename_from_journal_xml(xml_path), fallback=fallback)
    ext = extension if extension.startswith(".") else f".{extension}"
    return f"{base}{ext}"


def strip_temp_prefix(filename: str) -> str:
    """Снять префикс ``YYYYMMDD_HHMMSS_uuid8_`` у temp-имени."""
    name = Path(filename).name
    m = _TEMP_PREFIX_RE.match(name)
    return m.group(1) if m else name


def content_disposition_attachment(filename: str) -> str:
    """Заголовок Content-Disposition для скачивания."""
    safe = Path(filename).name.replace('"', "").replace("\r", "").replace("\n", "")
    if not safe:
        safe = "download"
    return f'attachment; filename="{safe}"'
