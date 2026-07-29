"""Сценарий: XSD-валидация + анализ метаданных journal XML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ipsas.config.settings import get_settings
from ipsas.modules.journal_xml.analyzer import analyze_journal_xml
from ipsas.modules.journal_xml.editorial_letter import build_xml_editorial_letter
from ipsas.modules.xml_validator import XMLValidator
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidateXmlResult:
    schema_result: dict[str, Any] | None
    schema_label: str
    report: dict[str, Any] | None
    metadata_error: str | None
    check_schema: bool
    check_metadata: bool


def build_xml_editorial_letter_text(
    *,
    report: dict[str, Any] | None = None,
    schema_result: dict[str, Any] | None = None,
    metadata_error: str | None = None,
    source_file: str = "",
    generated_at: str | None = None,
) -> str:
    """Текст письма редакции по уже посчитанному результату валидации XML."""
    return build_xml_editorial_letter(
        report=report,
        schema_result=schema_result,
        metadata_error=metadata_error,
        source_file=source_file,
        generated_at=generated_at,
    )


def execute(
    xml_path: Path,
    *,
    schema_name: str = "",
    check_schema: bool = True,
    check_metadata: bool = True,
) -> ValidateXmlResult:
    """Проверить XML по схеме и/или собрать отчёт метаданных."""
    if not check_schema and not check_metadata:
        check_schema = True
        check_metadata = True

    settings = get_settings()
    schema_result: dict[str, Any] | None = None
    schema_label = "не выполнялась"

    if check_schema:
        validator = XMLValidator()
        name = (schema_name or "").strip()
        if name:
            schema_path = settings.schemas_dir / name
            if not schema_path.exists():
                raise FileNotFoundError(f"Схема {name} не найдена")
            validator.load_schema(schema_path)
            schema_result = validator.validate_xml_file(xml_path)
            schema_label = schema_path.name
        else:
            schema_result = validator.validate_xml_file(xml_path)
            schema_label = "только синтаксис"

    report: Optional[dict[str, Any]] = None
    metadata_error: str | None = None
    if check_metadata:
        try:
            report = analyze_journal_xml(xml_path)
        except ValueError as e:
            metadata_error = str(e)
        except Exception as e:
            logger.error("Ошибка анализа метаданных: %s", e, exc_info=True)
            metadata_error = f"Не удалось проанализировать метаданные: {e}"

    return ValidateXmlResult(
        schema_result=schema_result,
        schema_label=schema_label,
        report=report,
        metadata_error=metadata_error,
        check_schema=check_schema,
        check_metadata=check_metadata,
    )
