"""CLI IPSAS: отчёты и служебные команды.

Примеры::

    python -m ipsas.cli report input.xml -o report.html
    python -m ipsas.cli validate article.xml
    python -m ipsas.cli validate issue.zip
    python -m ipsas.cli validate article.xml --json
    python -m ipsas.cli version
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def cmd_report(args: argparse.Namespace) -> int:
    from ipsas.modules.journal_xml.report.renderer import generate_html_report

    xml_path = Path(args.input)
    output = Path(args.output) if args.output else xml_path.with_suffix(".html")
    result = generate_html_report(xml_path, output)
    print(result)
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Проверка JATS XML или ZIP выпуска для Метафоры.

    Exit codes:
      0 — обязательные проверки пройдены (все статьи);
      1 — есть ERROR;
      2 — XML/ZIP не разобрать / системная ошибка.
    """
    from ipsas.services.validate_metafora_jats import execute

    path = Path(args.input)
    if not path.is_file():
        print(f"Файл не найден: {path}", file=sys.stderr)
        return 2
    try:
        result = execute(xml_path=path)
    except Exception as exc:  # noqa: BLE001
        print(f"Ошибка обработки: {exc}", file=sys.stderr)
        return 2

    if result.batch_error and not result.reports:
        if args.json:
            print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        else:
            print(result.batch_error, file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(f"Источник: {result.source_name}")
        if result.is_batch:
            print(
                f"Статей: {result.article_count} · "
                f"OK: {result.ok_count} · с ошибками: {result.fail_count}"
            )
        for report in result.reports:
            status = (
                "OK"
                if report.valid_for_metafora
                else "ОШИБКИ"
            )
            title = (report.article_title or "").strip()
            if title:
                print(f"\n[{status}] {title}")
                print(f"  файл: {report.filename}")
            else:
                print(f"\n[{status}] {report.filename}")
            if report.publication_type_label or report.publication_type:
                type_line = report.publication_type_label or report.publication_type
                if (
                    report.publication_type
                    and report.publication_type_label
                    and report.publication_type not in report.publication_type_label
                ):
                    type_line = (
                        f"{report.publication_type_label} "
                        f"({report.publication_type})"
                    )
                print(f"  тип: {type_line}")
            if report.article_url:
                print(f"  ссылка: {report.article_url}")
            print(
                f"  ошибок: {report.error_count} · "
                f"предупреждений: {report.warning_count} · "
                f"справочно: {report.info_count} · "
                f"пройдено: {report.passed_count}"
            )
            for iss in report.issues:
                if iss.severity == "info":
                    print(f"  (справка) [{iss.code}] {iss.message}")
            for iss in report.issues:
                if iss.severity == "error":
                    print(f"  [{iss.code}] {iss.message}")
                    if iss.value:
                        print(f"           значение: {iss.value}")
                elif iss.severity == "warning":
                    print(f"  (предупр.) [{iss.code}] {iss.message}")

    if not result.reports:
        return 2
    if (
        not result.is_batch
        and len(result.reports) == 1
        and not result.reports[0].parse_ok
        and any(i.code == "PARSE" for i in result.reports[0].issues)
    ):
        return 2
    if not result.ok:
        return 1
    return 0


def cmd_version(_: argparse.Namespace) -> int:
    import ipsas

    print(getattr(ipsas, "__version__", "0.1.0"))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ipsas", description="IPSAS CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    report = sub.add_parser("report", help="HTML-отчёт по journal XML")
    report.add_argument("input", help="Путь к XML")
    report.add_argument("-o", "--output", help="Путь к HTML")
    report.set_defaults(func=cmd_report)

    validate = sub.add_parser(
        "validate", help="Проверка JATS XML или ZIP выпуска для Метафоры"
    )
    validate.add_argument("input", help="Путь к JATS XML или ZIP с XML")
    validate.add_argument(
        "--json",
        action="store_true",
        help="Вывести отчёт в JSON",
    )
    validate.set_defaults(func=cmd_validate)

    ver = sub.add_parser("version", help="Версия пакета")
    ver.set_defaults(func=cmd_version)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
