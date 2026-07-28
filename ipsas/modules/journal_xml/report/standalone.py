"""Standalone HTML-отчёт в стиле web-интерфейса (на базе analyze_journal_xml).

CSS берётся из static/css (theme + app) — один источник с UI.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from ipsas.config.settings import get_settings
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

_STANDALONE_CHROME = """
/* Standalone report chrome (единый источник токенов — theme.css / app.css) */
body {
  margin: 0;
  padding: 1.25rem;
  font-family: var(--font, "Segoe UI", system-ui, sans-serif);
  color: var(--text-primary, var(--color-text, #0f172a));
  line-height: 1.5;
  background: var(--content-background, #eef4fb);
}
.page { max-width: 1100px; margin: 0 auto; }
.report-footer {
  margin-top: 1.5rem;
  color: var(--text-muted, var(--color-muted, #64748b));
  font-size: 0.85rem;
  text-align: center;
}
"""


@lru_cache(maxsize=1)
def load_standalone_css() -> str:
    """Собрать CSS для скачиваемого HTML из файлов UI."""
    settings = get_settings()
    css_dir = settings.base_dir / "ipsas" / "web" / "static" / "css"
    parts: list[str] = []
    for name in ("theme.css", "app.css"):
        path = css_dir / name
        if path.exists():
            parts.append(path.read_text(encoding="utf-8"))
        else:
            logger.warning("CSS для standalone не найден: %s", path)
    parts.append(_STANDALONE_CHROME)
    return "\n".join(parts)


def _jinja_env(*, templates_dir: Optional[Path] = None) -> Environment:
    root = Path(templates_dir) if templates_dir is not None else get_settings().templates_dir
    return Environment(
        loader=FileSystemLoader(str(root)),
        autoescape=select_autoescape(["html", "xml"]),
    )


def render_web_style_html_report(
    report: dict[str, Any],
    *,
    filename: str,
    templates_dir: Optional[Path] = None,
) -> str:
    """Собрать self-contained HTML из тех же данных/разметки, что и web-отчёт."""
    template = _jinja_env(templates_dir=templates_dir).get_template("xml_report_download.html")
    # CSS из наших static-файлов — доверенный источник.
    css = Markup(load_standalone_css())
    return template.render(
        report=report,
        filename=filename,
        download_mode=True,
        xml_filename=None,
        standalone_css=css,
    )


def write_web_style_html_report(
    report: dict[str, Any],
    output_path: Path,
    *,
    filename: str,
    templates_dir: Optional[Path] = None,
) -> Path:
    """Сохранить HTML-отчёт на диск."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = render_web_style_html_report(
        report, filename=filename, templates_dir=templates_dir
    )
    output_path.write_text(html, encoding="utf-8")
    logger.info("HTML-отчёт (web-стиль) сохранён: %s", output_path.name)
    return output_path
