"""Разбор URL журнала и HTML страниц OJS «Новые» / отклонение."""

from __future__ import annotations

import re
import urllib.parse
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlparse

from ipsas.modules.archive_by_sender.models import SubmissionInfo

SUBMISSION_ROW_RE = re.compile(
    r"<td>(\d+)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>"
    r'\s*<td><a href="([^"]+)"[^>]*>([^<]+)</a></td>',
    re.I | re.S,
)
SENDER_RE = re.compile(
    r'<td class="label">Отправитель</td>\s*<td[^>]*>\s*(.*?)(?:<a|</td>)',
    re.I | re.S,
)
RESULT_RANGE_RE = re.compile(
    r"(\d+)\s*[-–]\s*(\d+)\s*(?:из|of)\s*(\d+)\s*(?:результат|result)",
    re.I,
)
SEARCH_FORM_RE = re.compile(
    r'<form[^>]*id="searchForm"[^>]*>(.*?)</form>',
    re.I | re.S,
)
HIDDEN_INPUT_RE = re.compile(
    r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"',
    re.I,
)
_BARE_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,80}$")
_ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dXx]$")


class EmailFormParser(HTMLParser):
    """Разбирает форму id=emailForm на странице unsuitableSubmission."""

    def __init__(self) -> None:
        super().__init__()
        self.in_form = False
        self.form_action = ""
        self.fields: dict[str, str] = {}
        self.skip_field = ""
        self.skip_value = ""
        self._textarea_name: str | None = None
        self._textarea_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        ad = {k: (v or "") for k, v in attrs}
        if tag == "form" and ad.get("id") == "emailForm":
            self.in_form = True
            self.form_action = ad.get("action", "")
            return
        if not self.in_form:
            return
        if tag == "input":
            name = ad.get("name")
            if not name:
                return
            typ = (ad.get("type") or "text").lower()
            if typ == "submit" and name.startswith("send"):
                if "[skip]" in name:
                    self.skip_field = name
                    self.skip_value = ad.get("value") or "Пропустить"
            elif typ not in {"submit", "button", "image", "file"}:
                self.fields[name] = ad.get("value") or ""
        elif tag == "textarea":
            self._textarea_name = ad.get("name")
            self._textarea_parts = []

    def handle_data(self, data: str) -> None:
        if self._textarea_name is not None:
            self._textarea_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self.in_form:
            self.in_form = False
        if tag == "textarea" and self._textarea_name:
            self.fields[self._textarea_name] = "".join(self._textarea_parts)
            self._textarea_name = None
            self._textarea_parts = []


def clean_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).replace("\xa0", " ").strip()


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def sender_matches(sender: str, allowed_senders: list[str]) -> bool:
    normalized = normalize_name(sender)
    allowed = {normalize_name(name) for name in allowed_senders if name.strip()}
    return normalized in allowed


def parse_journal_ref(value: str) -> str:
    """Извлечь slug/ISSN журнала из URL или голого идентификатора."""
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Укажите ссылку на журнал или ISSN")

    if _ISSN_RE.fullmatch(raw) or _BARE_SLUG_RE.fullmatch(raw):
        return raw

    url = raw if "://" in raw else f"https://{raw}"
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        raise ValueError(
            "Не удалось извлечь журнал из ссылки "
            "(ожидается …/ISSN/… или голый ISSN)"
        )
    slug = parts[0]
    if not (_ISSN_RE.fullmatch(slug) or _BARE_SLUG_RE.fullmatch(slug)):
        raise ValueError(f"Некорректный идентификатор журнала: {slug!r}")
    return slug


def unassigned_url(base_url: str, journal: str) -> str:
    return f"{base_url.rstrip('/')}/{journal}/editor/submissions/submissionsUnassigned"


def unassigned_page_url(
    base_url: str,
    journal: str,
    page: int,
    params: dict[str, str] | None = None,
) -> str:
    query = dict(params or {})
    if page > 1:
        query["submissionsPage"] = str(page)
    elif "submissionsPage" in query:
        query.pop("submissionsPage")
    base = unassigned_url(base_url, journal)
    if not query:
        return base
    return f"{base}?{urllib.parse.urlencode(query)}"


def submission_url(base_url: str, journal: str, article_id: int) -> str:
    return f"{base_url.rstrip('/')}/{journal}/editor/submission/{article_id}"


def unsuitable_url(base_url: str, journal: str, article_id: int) -> str:
    return (
        f"{base_url.rstrip('/')}/{journal}/editor/unsuitableSubmission"
        f"?articleId={article_id}"
    )


def parse_result_range(html: str) -> tuple[int, int, int] | None:
    match = RESULT_RANGE_RE.search(html)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def extract_search_params(html: str) -> dict[str, str]:
    form_match = SEARCH_FORM_RE.search(html)
    if not form_match:
        return {}
    params: dict[str, str] = {}
    for name, value in HIDDEN_INPUT_RE.findall(form_match.group(1)):
        params[name] = unescape(value)
    return params


def extract_page_numbers(html: str) -> list[int]:
    return [int(page) for page in re.findall(r"submissionsPage=(\d+)", html, re.I)]


def is_login_page(html: str) -> bool:
    return "signinForm" in html or "loginUsername" in html


def parse_submissions(html: str) -> list[SubmissionInfo]:
    items: list[SubmissionInfo] = []
    for match in SUBMISSION_ROW_RE.finditer(html):
        article_id = int(match.group(1))
        items.append(
            SubmissionInfo(
                article_id=article_id,
                submit_date=clean_text(match.group(2)),
                section=clean_text(match.group(3)),
                authors=clean_text(match.group(4)),
                title=clean_text(match.group(6)),
                url=clean_text(match.group(5)),
            )
        )
    return items


def extract_sender(html: str) -> str:
    match = SENDER_RE.search(html)
    if not match:
        return ""
    return clean_text(match.group(1))


def parse_email_form(html: str) -> EmailFormParser:
    parser = EmailFormParser()
    parser.feed(html)
    if not parser.form_action:
        raise RuntimeError("Форма emailForm не найдена на странице отклонения")
    if not parser.skip_field:
        raise RuntimeError("Кнопка «Пропустить» (send[skip]) не найдена")
    return parser


def build_skip_payload(form: EmailFormParser) -> dict[str, str]:
    payload = dict(form.fields)
    payload[form.skip_field] = form.skip_value
    return payload


def submission_in_unassigned(html: str, article_id: int) -> bool:
    return f"/editor/submission/{article_id}" in html
