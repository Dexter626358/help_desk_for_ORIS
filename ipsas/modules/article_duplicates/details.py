"""Догрузка полного названия и полей со страницы рукописи OJS."""

from __future__ import annotations

import logging
import re
import time
from html import unescape
from typing import Protocol
from urllib.parse import urlparse

from ipsas.modules.archive_by_sender.parser import is_login_page
from ipsas.modules.article_duplicates.models import QueueSubmission
from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError

logger = logging.getLogger(__name__)

_LABEL_VALUE_RE = re.compile(
    r'<td[^>]*class="label"[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>',
    re.I | re.S,
)
_H3_TITLE_RE = re.compile(
    r"<h3[^>]*>\s*(.*?)\s*</h3>",
    re.I | re.S,
)
_INPUT_RE = re.compile(
    r"<input\b([^>]*)>",
    re.I | re.S,
)
_TEXTAREA_RE = re.compile(
    r'<textarea\b([^>]*)>(.*?)</textarea>',
    re.I | re.S,
)
_ATTR_RE = re.compile(
    r"""([^\s=]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))""",
    re.I,
)
_SUBMISSION_URL_RE = re.compile(
    r"^(?P<head>https?://[^/]+/[^/]+)/editor/"
    r"(?:submission(?:Editing|Review)?)/(?P<aid>\d+)",
    re.I,
)


class PlatformHttpClient(Protocol):
    base_url: str

    def get_text(self, url: str, encoding: str = "utf-8") -> str: ...


def _clean_text(value: str) -> str:
    # Сначала entities (&lt;p&gt; → <p>), затем снятие тегов.
    text = unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).replace("\xa0", " ").strip()


def _normalize_label(label: str) -> str:
    return _clean_text(label).casefold().rstrip(":")


def _attrs(tag_attrs: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for match in _ATTR_RE.finditer(tag_attrs or ""):
        key = (match.group(1) or "").casefold()
        val = match.group(2) or match.group(3) or match.group(4) or ""
        out[key] = unescape(val)
    return out


def _normalize_keywords(raw: str) -> str:
    text = _clean_text(raw)
    if not text:
        return ""
    # OJS subject часто через запятую или точку с запятой.
    parts = [
        p.strip(" ;,")
        for p in re.split(r"[;\n,]+", text)
        if p.strip(" ;,")
    ]
    return "; ".join(parts) if parts else text


def parse_submission_detail(html: str) -> dict[str, str]:
    """Извлечь поля со страницы editor/submission/{id}."""
    fields: dict[str, str] = {}
    for match in _LABEL_VALUE_RE.finditer(html):
        label = _normalize_label(match.group(1))
        value = _clean_text(match.group(2))
        if not label or not value:
            continue
        fields[label] = value

    title = ""
    for key in (
        "название",
        "title",
        "название статьи",
        "article title",
    ):
        if key in fields:
            title = fields[key]
            break
    if not title:
        h3 = _H3_TITLE_RE.search(html)
        if h3:
            title = _clean_text(h3.group(1))

    title_en = ""
    for key in (
        "title",
        "английское название",
        "название (eng)",
        "название (en)",
        "article title",
    ):
        if key in fields and fields[key] != title:
            # «Title» на RU-странице часто = EN
            if key == "title" or "eng" in key or key.endswith("(en)"):
                title_en = fields[key]
                break

    authors = ""
    for key in ("авторы", "authors", "author"):
        if key in fields:
            authors = fields[key]
            break

    section = ""
    for key in ("раздел", "section"):
        if key in fields:
            section = fields[key]
            break

    sender = ""
    for key in ("отправитель", "submitter"):
        if key in fields:
            sender = fields[key]
            break

    status = ""
    for key in ("статус", "status"):
        if key in fields:
            status = fields[key]
            break

    submit_date = ""
    for key in (
        "дата подачи",
        "date submitted",
        "submitted",
        "дата",
    ):
        if key in fields:
            submit_date = fields[key]
            break

    abstract = ""
    for key in (
        "аннотация",
        "abstract",
        "резюме",
        "summary",
    ):
        if key in fields:
            abstract = fields[key]
            break

    keywords = ""
    for key in (
        "ключевые слова",
        "keywords",
        "subjects",
        "subject",
        "индексирование",
        "indexing",
    ):
        if key in fields:
            keywords = _normalize_keywords(fields[key])
            break

    return {
        "title": title,
        "title_en": title_en,
        "authors": authors,
        "section": section,
        "sender": sender,
        "ojs_status": status,
        "submit_date": submit_date,
        "abstract": abstract,
        "keywords": keywords,
    }


def parse_view_metadata_fields(html: str) -> dict[str, str]:
    """Поля формы viewMetadata: title/abstract/subject по локалям."""
    fields: dict[str, str] = {}
    for match in _INPUT_RE.finditer(html or ""):
        attrs = _attrs(match.group(1))
        name = attrs.get("name", "")
        if not name:
            continue
        typ = (attrs.get("type") or "text").casefold()
        if typ in {"submit", "button", "image", "file", "checkbox", "radio"}:
            continue
        fields[name] = attrs.get("value") or ""
    for match in _TEXTAREA_RE.finditer(html or ""):
        attrs = _attrs(match.group(1))
        name = attrs.get("name", "")
        if not name:
            continue
        fields[name] = _clean_text(match.group(2) or "")
    return fields


def _locale_value(
    fields: dict[str, str],
    prefix: str,
    locales: tuple[str, ...],
) -> str:
    prefix_l = prefix.casefold()
    for locale in locales:
        for key in (f"{prefix}[{locale}]", f"{prefix}[{locale.lower()}]"):
            val = (fields.get(key) or "").strip()
            if val:
                return val
    for key, val in fields.items():
        if not key.casefold().startswith(prefix_l + "["):
            continue
        text = (val or "").strip()
        if not text:
            continue
        key_l = key.casefold()
        for locale in locales:
            if locale.casefold() in key_l:
                return text
    return ""


def metadata_fields_to_detail(fields: dict[str, str]) -> dict[str, str]:
    """Свести title/abstract/keywords из полей viewMetadata."""
    title_ru = _locale_value(fields, "title", ("ru_RU", "ru"))
    title_en = _locale_value(fields, "title", ("en_US", "en"))
    abstract_ru = _locale_value(fields, "abstract", ("ru_RU", "ru"))
    abstract_en = _locale_value(fields, "abstract", ("en_US", "en"))
    keywords_ru = _normalize_keywords(
        _locale_value(fields, "subject", ("ru_RU", "ru"))
        or (fields.get("keywords") or "")
    )
    keywords_en = _normalize_keywords(
        _locale_value(fields, "subject", ("en_US", "en"))
    )
    return {
        "title": title_ru or title_en,
        "title_en": title_en if title_en and title_en != title_ru else "",
        "abstract": abstract_ru or abstract_en,
        "keywords": keywords_ru,
        "keywords_en": keywords_en,
    }


def view_metadata_url(submission_url: str) -> str | None:
    """URL editor/viewMetadata/{id} из URL submission."""
    raw = (submission_url or "").strip()
    if not raw:
        return None
    match = _SUBMISSION_URL_RE.match(raw)
    if match:
        return f"{match.group('head')}/editor/viewMetadata/{match.group('aid')}"
    parsed = urlparse(raw)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 3 and parts[-1].isdigit():
        kind = parts[-2].casefold()
        if kind in {"submission", "submissionediting", "submissionreview"}:
            journal = parts[0]
            aid = parts[-1]
            return (
                f"{parsed.scheme}://{parsed.netloc}/{journal}"
                f"/editor/viewMetadata/{aid}"
            )
    return None


def enrich_submission_from_detail(
    auth: PlatformHttpClient,
    submission: QueueSubmission,
) -> QueueSubmission:
    """Обновить рукопись данными со страницы submission (+ viewMetadata)."""
    url = submission.url
    if not url:
        return submission
    html = auth.get_text(url)
    if is_login_page(html):
        raise PlatformAuthError(f"Нет доступа к {url}")

    detail = parse_submission_detail(html)
    _apply_submission_detail(submission, detail)

    meta_url = view_metadata_url(url)
    if meta_url:
        try:
            meta_html = auth.get_text(meta_url)
            if not is_login_page(meta_html):
                meta = metadata_fields_to_detail(
                    parse_view_metadata_fields(meta_html)
                )
                _apply_metadata_detail(submission, meta)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "viewMetadata id=%s: %s", submission.article_id, exc
            )

    submission.detail_loaded = True
    return submission


def _apply_submission_detail(
    submission: QueueSubmission,
    detail: dict[str, str],
) -> None:
    """Записать поля со страницы submission."""
    if detail.get("title") and (
        len(detail["title"]) > len(submission.title)
        or submission.title.endswith("…")
        or submission.title.endswith("...")
        or not submission.title
    ):
        submission.title = detail["title"]
    if detail.get("title_en"):
        submission.title_en = detail["title_en"]
    if detail.get("authors"):
        submission.authors = detail["authors"]
    if detail.get("section"):
        submission.section = detail["section"]
    if detail.get("sender"):
        submission.sender = detail["sender"]
    if detail.get("ojs_status"):
        submission.ojs_status = detail["ojs_status"]
    if detail.get("submit_date"):
        submission.submit_date = detail["submit_date"]
    if detail.get("abstract"):
        submission.abstract = detail["abstract"]
    if detail.get("keywords"):
        submission.keywords = detail["keywords"]


def _apply_metadata_detail(
    submission: QueueSubmission,
    detail: dict[str, str],
) -> None:
    """Дополнить/уточнить поля из viewMetadata (источник title EN / abstract / KW)."""
    if detail.get("title") and (
        not submission.title
        or submission.title.endswith("…")
        or submission.title.endswith("...")
        or len(detail["title"]) > len(submission.title)
    ):
        submission.title = detail["title"]
    if detail.get("title_en"):
        submission.title_en = detail["title_en"]
    if detail.get("abstract"):
        submission.abstract = detail["abstract"]
    if detail.get("keywords"):
        submission.keywords = detail["keywords"]
    if detail.get("keywords_en"):
        submission.keywords_en = detail["keywords_en"]


def enrich_candidates(
    auth: PlatformHttpClient,
    submissions: list[QueueSubmission],
    *,
    delay: float = 0.0,
) -> None:
    """Догрузить детали для уникальных рукописей (in-place)."""
    seen: set[int] = set()
    for sub in submissions:
        if sub.article_id in seen or sub.detail_loaded:
            continue
        seen.add(sub.article_id)
        try:
            enrich_submission_from_detail(auth, sub)
            logger.info(
                "Детали id=%s: «%s» abstract=%s kw=%s",
                sub.article_id,
                (sub.title or "")[:80],
                "yes" if sub.abstract else "no",
                "yes" if (sub.keywords or sub.keywords_en) else "no",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Не удалось догрузить детали id=%s: %s",
                sub.article_id,
                exc,
            )
        if delay > 0:
            time.sleep(delay)
