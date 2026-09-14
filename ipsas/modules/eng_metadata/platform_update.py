from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

from lxml import html as lxml_html

from ipsas.modules.eng_metadata.platform_client import PlatformClient, PlatformClientError

logger = logging.getLogger(__name__)

METADATA_EDIT_PATH = "/editor/viewMetadata/{article_id}"
METADATA_SAVE_HINT = "saveMetadata"
AUTHOR_EDIT_PATH = "/editor/editMetadata/{article_id}"
AUTHOR_SAVE_HINT = "editMetadataSave"
SCHEDULING_EDIT_PATH = "/editor/submissionEditing/{article_id}"
SCHEDULING_SAVE_HINT = "updateScheduling"

FIELD_MAP: dict[str, list[str]] = {
    "article.titles.en": ["title[en_US]"],
    "article.abstracts.en": ["abstract[en_US]"],
    "article.keywords.en": ["subject[en_US]"],
    "article.funding.en": ["sponsor[en_US]"],
    "article.sections.en": [],
}

DATE_FIELD_MAP: dict[str, str] = {
    "dates.received": "dateSubmitted",
    "dates.accepted": "dateAccepted",
}

CITATIONS_EN_FIELD = "citations"
CITATIONS_RU_FIELD = "localeCitations[ru_RU]"

SKIP_APPLY_FIELDS = frozenset({"dates.revised"})
SKIP_APPLY_SUFFIXES = (".orcid",)


@dataclass
class Change:
    field: str
    value: Any


@dataclass
class ApplyResult:
    article_id: int | str
    ok: bool
    applied_fields: list[str]
    errors: list[str]
    message: str
    dry_run: bool = False


def changes_from_payload(payload: dict[str, Any]) -> list[Change]:
    raw = payload.get("changes") if isinstance(payload.get("changes"), list) else []
    out: list[Change] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        if not field:
            continue
        if field in SKIP_APPLY_FIELDS or field.endswith(SKIP_APPLY_SUFFIXES):
            continue
        out.append(Change(field=field, value=item.get("value")))
    return out


class UpdateService:
    """Dry-run / apply / verify updates via discovered Platform HTML forms."""

    def __init__(self, client: PlatformClient, issn: str) -> None:
        self.client = client
        self.issn = issn
        self.base = client.settings.base_url.rstrip("/")

    def discover_edit_url(self, article_id: int) -> tuple[str, dict[str, str]] | None:
        """Return (action_url, fields) for the main article metadata form."""
        path = METADATA_EDIT_PATH.format(article_id=article_id)
        url = f"{self.base}/{self.issn}{path}"
        try:
            html = self.client.get_text(url)
        except Exception as exc:  # noqa: BLE001
            logger.debug("viewMetadata недоступен (%s): %s", url, exc)
            return None

        forms = self._parse_forms(html, url)
        for form in forms:
            if form["method"] != "post":
                continue
            action = urljoin(url, form["action"] or url)
            if METADATA_SAVE_HINT in action.casefold() or self._looks_like_metadata_form(form):
                logger.info("Найдена форма метаданных: %s", action)
                return action, dict(form["fields"])
        return None

    @staticmethod
    def _looks_like_metadata_form(form: dict[str, Any]) -> bool:
        names = " ".join(form["fields"].keys()).casefold()
        return any(k in names for k in ("title[", "abstract[", "subject["))

    @staticmethod
    def _parse_forms(html: str, base_url: str) -> list[dict[str, Any]]:
        del base_url  # reserved for absolute actions via urljoin at call sites
        root = lxml_html.fromstring(html)
        forms: list[dict[str, Any]] = []
        for form in root.xpath(".//form"):
            fields: dict[str, str] = {}
            for el in form.xpath(".//input|.//textarea|.//select"):
                name = el.get("name")
                if not name:
                    continue
                tag = (el.tag or "").lower()
                if tag == "input":
                    typ = (el.get("type") or "text").lower()
                    if typ in {"submit", "button", "image", "file"}:
                        continue
                    if typ in {"checkbox", "radio"} and el.get("checked") is None:
                        continue
                    fields[name] = el.get("value") or ""
                elif tag == "textarea":
                    fields[name] = "".join(el.itertext()) or ""
                elif tag == "select":
                    opt = None
                    for candidate in el.xpath("./option"):
                        if candidate.get("selected") is not None:
                            opt = candidate
                            break
                    if opt is None:
                        opts = el.xpath("./option")
                        opt = opts[0] if opts else None
                    fields[name] = (opt.get("value") if opt is not None else "") or ""
            forms.append(
                {
                    "action": form.get("action") or "",
                    "method": (form.get("method") or "get").lower(),
                    "id": form.get("id") or "",
                    "fields": fields,
                }
            )
        return forms

    def apply_payload(
        self,
        payload: dict[str, Any],
        *,
        allow_apply: bool,
    ) -> ApplyResult:
        article_id_raw = payload.get("article_id")
        try:
            article_id = int(article_id_raw)
        except (TypeError, ValueError):
            return ApplyResult(
                article_id=article_id_raw or "",
                ok=False,
                applied_fields=[],
                errors=[f"Некорректный article_id: {article_id_raw!r}"],
                message="ошибка",
            )

        if not allow_apply:
            return ApplyResult(
                article_id=article_id,
                ok=True,
                applied_fields=[],
                errors=[],
                message="dry-run: POST не отправлялся",
                dry_run=True,
            )

        if not payload.get("approved"):
            return ApplyResult(
                article_id=article_id,
                ok=False,
                applied_fields=[],
                errors=["Обновление запрещено: update_payload.approved != true"],
                message="отклонено",
            )

        changes = changes_from_payload(payload)
        if not changes:
            return ApplyResult(
                article_id=article_id,
                ok=False,
                applied_fields=[],
                errors=["В payload нет изменений для отправки"],
                message="пусто",
            )

        discovered = self.discover_edit_url(article_id)
        if discovered is None:
            return ApplyResult(
                article_id=article_id,
                ok=False,
                applied_fields=[],
                errors=[
                    "Не найдена HTML-форма редактирования метаданных "
                    f"(ожидался {METADATA_EDIT_PATH.format(article_id=article_id)})."
                ],
                message="форма не найдена",
            )

        action_url, fields = discovered
        main_changes = [
            c
            for c in changes
            if c.field in FIELD_MAP or c.field.startswith("article.")
        ]
        ref_changes = [c for c in changes if c.field.startswith("references[")]
        author_changes = [
            c
            for c in changes
            if c.field.startswith("authors[") or c.field.startswith("affiliations[")
        ]
        date_changes = [c for c in changes if c.field in DATE_FIELD_MAP]

        applied: list[str] = []
        errors: list[str] = []

        author_ids = self._author_ids_from_fields(fields)
        for field_name, value in self._coalesce_author_changes(author_changes):
            try:
                ok, detail = self._apply_author_or_affiliation_change(
                    article_id, author_ids, field_name, value
                )
            except PlatformClientError as exc:
                ok, detail = False, str(exc)
            if ok:
                applied.extend(
                    self._applied_labels_for_author_op(
                        field_name, value, author_changes
                    )
                )
            else:
                errors.append(detail)

        if author_changes:
            rediscovered = self.discover_edit_url(article_id)
            if rediscovered is not None:
                action_url, fields = rediscovered

        main_applied, main_missing = self._patch_main_fields(fields, main_changes)
        ref_applied, ref_missing = self._patch_citations_en(fields, ref_changes)
        for field in main_missing + ref_missing:
            errors.append(f"Нет поля формы для: {field}")

        if main_applied or ref_applied:
            status, final_url, body = self.client.post_form(
                action_url, fields, referer=action_url
            )
            if status >= 400:
                return ApplyResult(
                    article_id=article_id,
                    ok=False,
                    applied_fields=applied,
                    errors=[f"HTTP {status} при saveMetadata ({final_url})"],
                    message="HTTP ошибка",
                )
            form_error = self._extract_form_errors(body)
            if form_error:
                errors.append(f"saveMetadata отклонён формой: {form_error}")
            else:
                applied.extend(main_applied)
                applied.extend(ref_applied)

        if date_changes:
            ok, detail, date_applied = self._apply_scheduling_dates(
                article_id, date_changes
            )
            if ok:
                applied.extend(date_applied)
            else:
                errors.append(detail)

        if not applied and errors:
            return ApplyResult(
                article_id=article_id,
                ok=False,
                applied_fields=[],
                errors=errors,
                message="не применено",
            )

        return ApplyResult(
            article_id=article_id,
            ok=not errors,
            applied_fields=applied,
            errors=errors,
            message="отправлено на платформу" if not errors else "отправлено с предупреждениями",
        )

    def _patch_main_fields(
        self,
        fields: dict[str, str],
        changes: list[Any],
    ) -> tuple[list[str], list[str]]:
        applied: list[str] = []
        missing: list[str] = []
        for change in changes:
            if change.field.startswith("authors[") or change.field.startswith(
                "affiliations["
            ) or change.field.startswith("references["):
                continue
            form_names = FIELD_MAP.get(change.field, [])
            form_names = [n for n in form_names if n in fields]
            if not form_names:
                missing.append(change.field)
                continue
            value = change.value
            if isinstance(value, list):
                value = "; ".join(str(v) for v in value)
            elif value is None:
                value = ""
            else:
                value = str(value)
            for name in form_names:
                fields[name] = value
            applied.append(change.field)
        return applied, missing

    def _patch_citations_en(
        self,
        fields: dict[str, str],
        ref_changes: list[Any],
    ) -> tuple[list[str], list[str]]:
        """Записывает EN-литературу в textarea ``citations``, RU не трогает."""
        if not ref_changes:
            return [], []
        if CITATIONS_EN_FIELD not in fields:
            return [], [CITATIONS_EN_FIELD]

        by_idx: dict[int, str] = {}
        applied: list[str] = []
        for change in ref_changes:
            m = re.match(r"references\[(\d+)\]\.text\.en$", change.field)
            if not m:
                continue
            idx = int(m.group(1))
            by_idx[idx] = "" if change.value is None else str(change.value).strip()
            applied.append(change.field)

        if not by_idx:
            return [], []

        max_idx = max(by_idx)
        lines = [by_idx[i] for i in range(max_idx + 1) if by_idx.get(i)]
        fields[CITATIONS_EN_FIELD] = "\r\n".join(lines)
        return applied, []

    @staticmethod
    def _author_ids_from_fields(fields: dict[str, str]) -> dict[int, str]:
        result: dict[int, str] = {}
        for name, value in fields.items():
            m = re.match(r"authors\[(\d+)\]\[authorId\]$", name)
            if m and value:
                result[int(m.group(1))] = value
        return result

    @staticmethod
    def _join_en_affiliation_parts(parts: list[Any]) -> str | None:
        """Склеивает несколько EN-аффилиаций в одно поле OJS через «; ».

        На платформе у автора одно поле affiliation[en_US] / address[en_US];
        в RU уже принято писать несколько организаций через «; ».
        """
        cleaned: list[str] = []
        for part in parts:
            if part is None:
                continue
            text = str(part).strip()
            if text:
                cleaned.append(text)
        if not cleaned:
            return None
        return "; ".join(cleaned)

    @staticmethod
    def _coalesce_author_changes(
        changes: list[Any],
    ) -> list[tuple[str, Any]]:
        """Сводит поля одного автора к ОДНОМУ POST editMetadataSave.

        Имя + organization + address вместе: иначе повторный POST только с
        affiliation/address или раздельный surname_en затирает инициалы.

        Несколько affiliations[j] склеиваются в одну строку через «; »,
        иначе остаётся только последняя (одно поле формы OJS).
        """
        by_idx: dict[int, dict[str, Any]] = {}
        order: list[int] = []
        other: list[tuple[str, Any]] = []

        def _bucket(idx: int) -> dict[str, Any]:
            if idx not in by_idx:
                by_idx[idx] = {
                    "name": {},
                    "organization_en": [],
                    "address_en": [],
                    "email": None,
                }
                order.append(idx)
            return by_idx[idx]

        for change in changes:
            field = change.field
            m_name = re.match(
                r"^authors\[(\d+)\]\.(full_name_en|given_en|surname_en|names\.en)$",
                field,
            )
            m_aff = re.match(
                r"^authors\[(\d+)\]\.affiliations\[\d+\]\.(organization_en|address_en|en)$",
                field,
            )
            m_email = re.match(r"^authors\[(\d+)\]\.email$", field)
            if m_name:
                idx = int(m_name.group(1))
                bucket = _bucket(idx)
                key = m_name.group(2)
                if key == "names.en" and isinstance(change.value, dict):
                    bucket["name"].update(change.value)
                else:
                    bucket["name"][key] = change.value
            elif m_aff:
                idx = int(m_aff.group(1))
                bucket = _bucket(idx)
                kind = m_aff.group(2)
                if kind in {"organization_en", "en"}:
                    bucket["organization_en"].append(change.value)
                else:
                    bucket["address_en"].append(change.value)
            elif m_email:
                idx = int(m_email.group(1))
                bucket = _bucket(idx)
                bucket["email"] = change.value
            else:
                other.append((field, change.value))

        coalesced: list[tuple[str, Any]] = []
        for idx in order:
            bucket = by_idx[idx]
            name_payload: dict[str, Any] | None = (
                dict(bucket["name"]) if bucket["name"] else None
            )
            coalesced.append(
                (
                    f"authors[{idx}].bundle",
                    {
                        "name_en": name_payload,
                        "affiliation_en": UpdateService._join_en_affiliation_parts(
                            bucket["organization_en"]
                        ),
                        "address_en": UpdateService._join_en_affiliation_parts(
                            bucket["address_en"]
                        ),
                        "email": bucket["email"],
                        "source_fields": [
                            c.field
                            for c in changes
                            if c.field.startswith(f"authors[{idx}].")
                        ],
                    },
                )
            )
        coalesced.extend(other)
        return coalesced

    @staticmethod
    def _applied_labels_for_author_op(
        field_name: str,
        value: Any,
        original_changes: list[Any],
    ) -> list[str]:
        if field_name.endswith(".bundle") and isinstance(value, dict):
            labels = list(value.get("source_fields") or [])
            return labels or [field_name]
        return [field_name]

    def _apply_author_or_affiliation_change(
        self,
        article_id: int,
        author_ids: dict[int, str],
        field: str,
        value: Any,
    ) -> tuple[bool, str]:
        m_bundle = re.match(r"authors\[(\d+)\]\.bundle$", field)
        if m_bundle and isinstance(value, dict):
            idx = int(m_bundle.group(1))
            author_id = author_ids.get(idx)
            if not author_id:
                return False, f"Не найден authorId для индекса {idx} в форме метаданных"
            name_en = value.get("name_en")
            if isinstance(name_en, dict) and not name_en:
                name_en = None
            return self._post_author_modal(
                article_id,
                author_id,
                idx,
                name_en=name_en,
                affiliation_en=value.get("affiliation_en"),
                address_en=value.get("address_en"),
                email=value.get("email"),
            )

        m_name = re.match(
            r"authors\[(\d+)\]\.(?:names\.en|full_name_en|given_en|surname_en)$",
            field,
        )
        m_aff = re.match(
            r"(?:affiliations\[(\d+)\]\.text\.en|"
            r"authors\[(\d+)\]\.affiliations(?:\[(\d+)\])?\."
            r"(?:en|organization_en|address_en))$",
            field,
        )
        m_email = re.match(r"authors\[(\d+)\]\.email$", field)
        if m_name:
            idx = int(m_name.group(1))
            author_id = author_ids.get(idx)
            if not author_id:
                return False, f"Не найден authorId для индекса {idx} в форме метаданных"
            # Важно: не подставлять пустые sibling-поля — иначе OJS затирает имя/отчество.
            name_payload: Any = value
            if field.endswith("given_en"):
                name_payload = {"given_en": value}
            elif field.endswith("surname_en"):
                name_payload = {"surname_en": value}
            elif field.endswith("full_name_en"):
                name_payload = value
            return self._post_author_modal(
                article_id, author_id, idx, name_en=name_payload, affiliation_en=None
            )
        if m_aff:
            idx = int(m_aff.group(1) or m_aff.group(2))
            author_id = author_ids.get(idx)
            if not author_id:
                return (
                    False,
                    f"Аффилиация автора[{idx}]: нет сопоставленного authorId "
                    "(на Платформе EN-аффилиация редактируется в карточке автора)",
                )
            if field.endswith("address_en"):
                return self._post_author_modal(
                    article_id,
                    author_id,
                    idx,
                    name_en=None,
                    affiliation_en=None,
                    address_en=value,
                )
            return self._post_author_modal(
                article_id, author_id, idx, name_en=None, affiliation_en=value
            )
        if m_email:
            idx = int(m_email.group(1))
            author_id = author_ids.get(idx)
            if not author_id:
                return False, f"Не найден authorId для индекса {idx} в форме метаданных"
            return self._post_author_modal(
                article_id,
                author_id,
                idx,
                name_en=None,
                affiliation_en=None,
                email=value,
            )
        return False, f"Неподдерживаемое поле для apply: {field}"

    def _post_author_modal(
        self,
        article_id: int,
        author_id: str,
        index: int,
        *,
        name_en: Any | None,
        affiliation_en: Any | None,
        address_en: Any | None = None,
        email: Any | None = None,
    ) -> tuple[bool, str]:
        get_url = (
            f"{self.base}/{self.issn}"
            f"{AUTHOR_EDIT_PATH.format(article_id=article_id)}"
            f"?type=author&id={author_id}&index={index}"
        )
        html = self.client.get_text(get_url)
        forms = self._parse_forms(html, get_url)
        usable = [
            f
            for f in forms
            if f["method"] == "post"
            and (
                AUTHOR_SAVE_HINT in urljoin(get_url, f["action"] or "").casefold()
                or any(k.startswith("authors[") for k in f["fields"])
            )
        ]
        if not usable:
            return False, f"Не найдена форма editMetadataSave для автора {author_id}"

        form = usable[0]
        action = urljoin(get_url, form["action"] or get_url)
        fields = dict(form["fields"])
        prefix = f"authors[{index}]"

        if name_en is not None:
            parts = self._split_en_name(name_en)
            if not parts:
                return (
                    False,
                    f"Не удалось разобрать EN-имя автора[{index}] для полей "
                    f"{prefix}[firstName|middleName|lastName][en_US]",
                )
            first, middle, last = parts
            mapping = {
                f"{prefix}[firstName][en_US]": first,
                f"{prefix}[middleName][en_US]": middle,
                f"{prefix}[lastName][en_US]": last,
            }
            # если передали только одно поле имени — не затираем остальные пустым
            if isinstance(name_en, dict):
                has_given = "given_en" in name_en or "first_name" in name_en or "given" in name_en
                has_surname = (
                    "surname_en" in name_en or "last_name" in name_en or "surname" in name_en
                )
                has_full = "full_name_en" in name_en or "full_name" in name_en
                if has_given and not has_surname and not has_full:
                    mapping = {
                        f"{prefix}[firstName][en_US]": first,
                        f"{prefix}[middleName][en_US]": middle,
                    }
                elif has_surname and not has_given and not has_full:
                    mapping = {f"{prefix}[lastName][en_US]": last}
            for key, val in mapping.items():
                if key not in fields:
                    return False, f"В форме автора нет поля {key}"
                # пустым firstName/middleName не затираем уже заполненные поля —
                # иначе OJS отклоняет весь saveMetadata («нужно имя автора»)
                if not str(val).strip():
                    if "firstName" in key or "middleName" in key:
                        continue
                fields[key] = val

        if affiliation_en is not None:
            key = f"{prefix}[affiliation][en_US]"
            if key not in fields:
                return False, f"В форме автора нет поля {key}"
            fields[key] = str(affiliation_en)

        if address_en is not None:
            key = f"{prefix}[address][en_US]"
            if key not in fields:
                return False, f"В форме автора нет поля {key}"
            fields[key] = str(address_en)

        if email is not None and str(email).strip():
            key = f"{prefix}[email]"
            if key not in fields:
                return False, f"В форме автора нет поля {key}"
            fields[key] = str(email).strip()

        status, final_url, _body = self.client.post_form(action, fields, referer=get_url)
        if status >= 400:
            return False, f"HTTP {status} при editMetadataSave ({final_url})"
        return True, "ok"

    def _apply_scheduling_dates(
        self,
        article_id: int,
        changes: list[Any],
    ) -> tuple[bool, str, list[str]]:
        """Пишет Received/Accepted в форму updateScheduling."""
        get_url = (
            f"{self.base}/{self.issn}"
            f"{SCHEDULING_EDIT_PATH.format(article_id=article_id)}"
        )
        html = self.client.get_text(get_url)
        forms = self._parse_forms(html, get_url)
        usable = [
            f
            for f in forms
            if f["method"] == "post"
            and (
                SCHEDULING_SAVE_HINT in urljoin(get_url, f["action"] or "").casefold()
                or "dateSubmitted" in f["fields"]
            )
        ]
        if not usable:
            return (
                False,
                f"Не найдена форма updateScheduling для статьи {article_id}",
                [],
            )

        form = usable[0]
        action = urljoin(get_url, form["action"] or get_url)
        fields = dict(form["fields"])
        applied: list[str] = []
        for change in changes:
            form_key = DATE_FIELD_MAP.get(change.field)
            if not form_key:
                continue
            if form_key not in fields:
                return (
                    False,
                    f"В форме scheduling нет поля {form_key}",
                    applied,
                )
            value = str(change.value or "").strip()
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                return (
                    False,
                    f"Дата {change.field} должна быть ISO YYYY-MM-DD, получено {value!r}",
                    applied,
                )
            fields[form_key] = value
            applied.append(change.field)

        if not applied:
            return True, "ok", []

        status, final_url, _body = self.client.post_form(
            action, fields, referer=get_url
        )
        if status >= 400:
            return False, f"HTTP {status} при updateScheduling ({final_url})", applied
        return True, "ok", applied

    @staticmethod
    def _extract_form_errors(body: bytes | str | None) -> str | None:
        """Достаёт текст pkp_form_error из HTML-ответа saveMetadata."""
        if not body:
            return None
        text = body.decode("utf-8", "replace") if isinstance(body, bytes) else body
        if "pkp_form_error" not in text and "formErrors" not in text:
            return None
        messages: list[str] = []
        for m in re.finditer(
            r'<ul class="pkp_form_error_list">(.*?)</ul>',
            text,
            re.I | re.S,
        ):
            for li in re.finditer(r"<li\b[^>]*>(.*?)</li>", m.group(1), re.I | re.S):
                raw = re.sub(r"<[^>]+>", " ", li.group(1))
                msg = " ".join(raw.split())
                if msg:
                    messages.append(msg)
        if messages:
            # берём уникальные короткие сообщения без JS-onclick мусора
            clean = []
            for msg in messages:
                if "document.getElementsByName" in msg:
                    # «Необходимо указать имя...» часто спрятано в ссылке
                    if "имя" in msg.lower() or "name" in msg.lower():
                        clean.append("Необходимо указать имя каждого автора")
                    continue
                if msg not in clean:
                    clean.append(msg)
            if clean:
                return "; ".join(clean[:5])
        if "pkp_form_error" in text:
            return "форма вернула ошибки валидации"
        return None

    @staticmethod
    def _split_en_name(value: Any) -> tuple[str, str, str] | None:
        if isinstance(value, dict):
            given = str(
                value.get("given_en")
                or value.get("first_name")
                or value.get("given")
                or ""
            ).strip()
            middle = str(value.get("middle_name") or value.get("middle_en") or "").strip()
            last = str(
                value.get("surname_en")
                or value.get("last_name")
                or value.get("surname")
                or ""
            ).strip()
            full = str(value.get("full_name_en") or value.get("full_name") or "").strip()
            # Если given пуст, а full есть — разбираем full (иначе surname_en
            # в bundle затирает firstName пустой строкой → OJS отклоняет всю форму).
            if full and not given:
                return UpdateService._split_en_name(full)
            # given «M. E.» → first=M., middle=E. при наличии фамилии
            if given and last and not middle:
                g_tokens = given.split()
                if len(g_tokens) >= 2:
                    given, middle = g_tokens[0], " ".join(g_tokens[1:])
            if given or last or middle:
                return given, middle, last
            return None
        if not isinstance(value, str) or not value.strip():
            return None
        text = " ".join(value.replace(",", " ").split())
        tokens = text.split()
        if len(tokens) == 1:
            return "", "", tokens[0]
        if len(tokens) == 2:
            return tokens[0], "", tokens[1]
        return tokens[0], " ".join(tokens[1:-1]), tokens[-1]
