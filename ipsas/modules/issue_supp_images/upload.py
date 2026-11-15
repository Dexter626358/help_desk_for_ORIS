"""Загрузка доп. файла (supp) и сохранение метаданных / видимости."""

from __future__ import annotations

import logging
import mimetypes
import re
import time
from typing import Protocol

from ipsas.modules.eng_metadata.platform_auth import PlatformAuthError
from ipsas.modules.issue_supp_images.models import ImageFile, UploadResult

logger = logging.getLogger(__name__)

SUPP_TYPE_FIGURE_MATERIALS = (
    "author.submit.suppFile.figureResearchMaterials"
)
EDIT_SUPP_RE = re.compile(r"/editor/editSuppFile/(\d+)/(\d+)", re.I)


class PlatformHttpClient(Protocol):
    base_url: str

    def get_text(self, url: str, encoding: str = "utf-8") -> str: ...

    def request(
        self,
        url: str,
        *,
        data: dict[str, str] | None = None,
        method: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes]: ...

    def request_multipart(
        self,
        url: str,
        *,
        fields: dict[str, str],
        files: dict[str, tuple[str, bytes, str]],
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes]: ...


def _is_login(html: str) -> bool:
    return "signinForm" in html or "loginUsername" in html


def _guess_content_type(filename: str) -> str:
    ctype, _ = mimetypes.guess_type(filename)
    return ctype or "application/octet-stream"


def _titles_for_image(image: ImageFile) -> tuple[str, str]:
    """Подписи ru/en для title доп. файла.

    Returns:
        ``(title_ru, title_en)`` — пустая строка, если подписи нет.
        Если обеих нет — RU = имя файла (как раньше).
    """
    title_ru = image.resolved_title_ru
    title_en = image.resolved_title_en
    if not title_ru and not title_en:
        title_ru = (image.original_name or "").strip() or "figure"
    return title_ru, title_en


def parse_edit_supp_redirect(final_url: str, html: str) -> tuple[int, int]:
    for source in (final_url, html):
        m = EDIT_SUPP_RE.search(source)
        if m:
            return int(m.group(1)), int(m.group(2))
    raise RuntimeError(
        "После загрузки не удалось определить editSuppFile "
        f"(url={final_url!r})"
    )


def discover_hidden_locales(html: str, article_id: int, file_id: int) -> list[str]:
    """Локали, для которых сейчас скрыто (кнопка «Показывать»)."""
    locales: list[str] = []
    block_re = re.compile(
        rf'id="suppFileDisplay_{file_id}"[\s\S]*?(?=<div id="suppFileDisplay_|\Z)',
        re.I,
    )
    block_m = block_re.search(html)
    scope = block_m.group(0) if block_m else html
    lang_blocks = re.findall(
        r'<div class="suppFIleDisplayLanguage\s+([^"]+)"[^>]*>([\s\S]*?)</div>',
        scope,
        re.I,
    )
    for locale, inner in lang_blocks:
        locale = locale.strip()
        if "hidden.png" not in inner:
            continue
        if not re.search(
            rf"suppFileDisplayChange\(\s*{article_id}\s*,\s*{file_id}\s*,\s*'{re.escape(locale)}'\s*\)",
            inner,
        ):
            continue
        if locale not in locales:
            locales.append(locale)
    return locales


def upload_supp_file(
    auth: PlatformHttpClient,
    *,
    journal: str,
    article_id: int,
    image: ImageFile,
    delay: float = 0.35,
) -> tuple[int, int]:
    """Загрузить файл как доп. → (article_id, supp_file_id)."""
    base = auth.base_url.rstrip("/")
    edit_url = f"{base}/{journal}/editor/submissionEditing/{article_id}"
    upload_url = f"{base}/{journal}/editor/uploadLayoutFile"
    edit_html = auth.get_text(edit_url)
    if _is_login(edit_html):
        raise PlatformAuthError(f"Нет доступа к {edit_url}")

    content = image.path.read_bytes()
    if not content:
        raise RuntimeError(f"Пустой файл: {image.path}")

    status, final_url, body = auth.request_multipart(
        upload_url,
        fields={
            "from": "submissionEditing",
            "articleId": str(article_id),
            "layoutFileType": "supp",
        },
        files={
            "layoutFile": (
                image.original_name,
                content,
                _guess_content_type(image.original_name),
            ),
        },
        headers={
            "Referer": edit_url,
            "Origin": base,
        },
    )
    text = body.decode("utf-8", "replace")
    if status >= 400:
        raise RuntimeError(f"uploadLayoutFile HTTP {status}")
    if _is_login(text) or "/login" in (final_url or ""):
        raise PlatformAuthError("Сессия потеряна при uploadLayoutFile")
    aid, fid = parse_edit_supp_redirect(final_url, text)
    if delay > 0:
        time.sleep(delay)
    return aid, fid


def save_supp_metadata(
    auth: PlatformHttpClient,
    *,
    journal: str,
    article_id: int,
    file_id: int,
    title: str = "",
    title_ru: str = "",
    title_en: str = "",
    supp_type: str = SUPP_TYPE_FIGURE_MATERIALS,
    form_locale: str = "ru_RU",
    delay: float = 0.35,
) -> None:
    """Сохранить метаданные доп. файла.

    ``title_ru`` / ``title_en`` пишутся в ``title[ru_RU]`` / ``title[en_US]``.
    Устаревший аргумент ``title`` используется как RU, если ``title_ru`` пуст.
    """
    base = auth.base_url.rstrip("/")
    edit_url = f"{base}/{journal}/editor/editSuppFile/{article_id}/{file_id}"
    save_url = f"{base}/{journal}/editor/saveSuppFile/{file_id}"
    html = auth.get_text(edit_url)
    if _is_login(html):
        raise PlatformAuthError(f"Нет доступа к {edit_url}")

    ru = (title_ru or title or "").strip()
    en = (title_en or "").strip()
    if not ru and not en:
        raise ValueError("Нужна хотя бы одна подпись title_ru/title_en/title")
    # Активная вкладка формы — локаль, для которой есть текст (приоритет RU)
    active_locale = form_locale
    if ru:
        active_locale = "ru_RU"
    elif en:
        active_locale = "en_US"

    payload: dict[str, str] = {
        "articleId": str(article_id),
        "from": "",
        "formLocale": active_locale,
        "type": supp_type,
        "dateCreated": "",
        "language": "",
    }
    for locale in ("ru_RU", "en_US"):
        payload[f"typeOther[{locale}]"] = ""
        payload[f"creator[{locale}]"] = ""
        payload[f"publisher[{locale}]"] = ""
        payload[f"source[{locale}]"] = ""
        payload[f"sponsor[{locale}]"] = ""
        payload[f"subject[{locale}]"] = ""
        payload[f"description[{locale}]"] = ""
    if ru:
        payload["title[ru_RU]"] = ru
    if en:
        payload["title[en_US]"] = en

    m = re.search(r'name="dateCreated"[^>]*value="([^"]*)"', html, re.I)
    if m:
        payload["dateCreated"] = m.group(1)

    status, final_url, body = auth.request(
        save_url,
        data=payload,
        headers={
            "Referer": edit_url,
            "Origin": base,
        },
    )
    text = body.decode("utf-8", "replace")
    if status >= 400:
        raise RuntimeError(f"saveSuppFile HTTP {status}")
    if _is_login(text):
        raise PlatformAuthError("Сессия потеряна при saveSuppFile")
    if "saveSuppFile" in (final_url or "") and "error" in text.casefold():
        raise RuntimeError("saveSuppFile вернул ошибку")
    if delay > 0:
        time.sleep(delay)


def enable_supp_visibility(
    auth: PlatformHttpClient,
    *,
    journal: str,
    article_id: int,
    file_id: int,
    delay: float = 0.2,
) -> list[str]:
    """Включить показ для скрытых языков. Возвращает список включённых локалей."""
    base = auth.base_url.rstrip("/")
    editing_url = f"{base}/{journal}/editor/submissionEditing/{article_id}"
    html = auth.get_text(editing_url)
    if _is_login(html):
        raise PlatformAuthError(f"Нет доступа к {editing_url}")

    locales = discover_hidden_locales(html, article_id, file_id)
    enabled: list[str] = []
    visible_url = f"{base}/{journal}/editor/visibleSuppFile"
    for locale in locales:
        status, _, body = auth.request(
            visible_url,
            data={
                "articleId": str(article_id),
                "fileId": str(file_id),
                "locale": locale,
            },
            headers={
                "Referer": editing_url,
                "Origin": base,
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
        )
        text = body.decode("utf-8", "replace")
        if status >= 400:
            logger.warning(
                "visibleSuppFile article=%s file=%s locale=%s HTTP %s",
                article_id,
                file_id,
                locale,
                status,
            )
            continue
        if "true" in text.casefold() or text.strip() == "" or "result" in text:
            enabled.append(locale)
        if delay > 0:
            time.sleep(delay)
    return enabled


def upload_image_to_article(
    auth: PlatformHttpClient,
    *,
    journal: str,
    article_id: int,
    pages_label: str,
    image: ImageFile,
    dry_run: bool = False,
    delay: float = 0.35,
) -> UploadResult:
    title_ru, title_en = _titles_for_image(image)
    if dry_run:
        title_bits = []
        if title_ru:
            title_bits.append(f"ru={title_ru!r}")
        if title_en:
            title_bits.append(f"en={title_en!r}")
        return UploadResult(
            article_id=article_id,
            pages=pages_label,
            filename=image.original_name,
            status="dry_run",
            message=(
                f"будет загружен как доп. файл, "
                f"{', '.join(title_bits)}, "
                f"type={SUPP_TYPE_FIGURE_MATERIALS}"
            ),
        )
    try:
        aid, fid = upload_supp_file(
            auth,
            journal=journal,
            article_id=article_id,
            image=image,
            delay=delay,
        )
        save_supp_metadata(
            auth,
            journal=journal,
            article_id=aid,
            file_id=fid,
            title_ru=title_ru,
            title_en=title_en,
            delay=delay,
        )
        locales = enable_supp_visibility(
            auth,
            journal=journal,
            article_id=aid,
            file_id=fid,
            delay=min(delay, 0.25),
        )
        return UploadResult(
            article_id=aid,
            pages=pages_label,
            filename=image.original_name,
            status="uploaded",
            message=f"supp_file_id={fid}; показан для: {', '.join(locales) or '—'}",
            supp_file_id=fid,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Ошибка загрузки article=%s file=%s: %s",
            article_id,
            image.original_name,
            exc,
        )
        return UploadResult(
            article_id=article_id,
            pages=pages_label,
            filename=image.original_name,
            status="error",
            message=str(exc),
        )
