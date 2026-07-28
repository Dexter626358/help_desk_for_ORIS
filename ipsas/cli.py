"""CLI IPSAS: отчёты и служебные команды.

Примеры::

    python -m ipsas.cli report input.xml -o report.html
    python -m ipsas.cli version
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def cmd_report(args: argparse.Namespace) -> int:
    from ipsas.modules.journal_xml.report.renderer import generate_html_report

    xml_path = Path(args.input)
    output = Path(args.output) if args.output else xml_path.with_suffix(".html")
    result = generate_html_report(xml_path, output)
    print(result)
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

    ver = sub.add_parser("version", help="Версия пакета")
    ver.set_defaults(func=cmd_version)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
