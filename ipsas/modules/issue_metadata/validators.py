"""Валидация выделенных метаданных выпуска и статей.

Цель: отделить проверки (business rules) от загрузки/парсинга.
Статьи: `errors` (критично) и `problems` (предупреждения) — строки для UI.
Выпуск: `warnings` — dict с severity.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple


_ISSN_PATTERN = re.compile(r"^\d{4}-?\d{3}[\dXx]$")
_DOI_PATTERN = re.compile(r"^10\.\d{4,9}/\S+$")
# EDN: ровно 6 латинских букв, без цифр (числовой OJS ID — не EDN)
_EDN_PATTERN = re.compile(r"^[A-Za-z]{6}$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_YEAR_PATTERN = re.compile(r"^\d{4}$")
_PAGES_FROM_DOI_RE = re.compile(r"-(\d{1,5})-(\d{1,5})$")
_PAGE_RANGE_RE = re.compile(
    r"(?P<start>\d{1,5})\s*(?P<dash>[-–—−‐‑-])\s*(?P<end>\d{1,5})"
)
_ELOCATOR_RE = re.compile(r"^(?P<eloc>[eE]\d{3,}|\d{1,5})$")
_CORRECT_PAGE_DASH = "–"  # en-dash
_MIXED_SCRIPT_WORD_RE = re.compile(r"[А-ЯЁа-яё][A-Za-z]|[A-Za-z][А-ЯЁа-яё]")
_TRANSLIT_MARKERS_RE = re.compile(
    r"(?:kh|zh|shch|tsch|\bsh\b|\bts\b|\bya\b|\byu\b|iy[ae]|yy|eni[eya]|ani[eya]|ost'|iya|"
    r"poluchen|issledovan|primenen|razrabot|sostav|svoystv|dlya|vliyani|"
    r"obrabot|formirovan|povyshen|snizhen|blochn|modul)",
    re.IGNORECASE,
)
_EN_FUNCTION_WORDS_RE = re.compile(
    r"\b(?:the|and|of|for|with|from|into|using|based|study|analysis|effect|"
    r"method|methods|properties|influence|development|formation|research|"
    r"investigation|application|production|obtaining|use|new|on|in|to|a|an)\b",
    re.IGNORECASE,
)
_ORCID_RE = re.compile(
    r"^(?:https?://orcid\.org/)?\d{4}-\d{4}-\d{4}-\d{3}[\dX]$",
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}|\n|\r")
_AFFIL_MARK_IN_NAME_RE = re.compile(r"(?:\d|[\u00b9\u00b2\u00b3])")
_ABSTRACT_HEADING_RE = re.compile(
    r"^\s*(?:annotation|abstract|аннотация|реферат)\b[:.\s-]*",
    re.IGNORECASE,
)
_ABSTRACT_LEAK_RE = re.compile(
    r"(?i)\b(?:keywords?|ключевые\s+слова|список\s+литературы|references?|bibliography|"
    r"автор(?:ы)?\s*:|authors?\s*:)",
)
_MULTI_ORG_RE = re.compile(r"(?:\s+[;/|]\s+)|(?:\s+и\s+(?:ФГБОУ|ФГАОУ|ФГБУ|университет|институт))", re.IGNORECASE)
_ROR_RE = re.compile(r"(?:https?://)?(?:www\.)?ror\.org/[0-9a-z]+", re.IGNORECASE)
_MIN_ABSTRACT_WORDS = 30
_MAX_ABSTRACT_WORDS = 1000
_REF_NUM_PREFIX_RE = re.compile(r"^\s*(?:\[(\d+)\]|(\d+)[.)]\s+)")
_REF_DOI_RE = re.compile(r"(?:doi:\s*|https?://(?:dx\.)?doi\.org/)?(10\.\d{4,9}/\S+)", re.IGNORECASE)
_REF_TILDE_RE = re.compile(r"[A-Za-z]~[A-Za-z]|~[A-Za-z]|[A-Za-z]~")
_REF_STUCK_NAME_RE = re.compile(r"\b([A-Z][a-z]{2,})([A-Z][a-z]{2,})\b")
# Инициалы рядом со склейкой: «PerezRey A.» / «J.F. PerezRey» (не бренд ExoPass, AlfaBuild)
_REF_STUCK_INITIALS = r"[A-Z](?:\.[\u00A0\u2009\s]*[A-Z])*\."
_REF_STUCK_AUTHOR_AFTER_RE = re.compile(rf"^\s*,?\s*{_REF_STUCK_INITIALS}")
_REF_STUCK_AUTHOR_BEFORE_RE = re.compile(rf"{_REF_STUCK_INITIALS}\s*$")
_REF_STUCK_NAME_ALLOW = frozenset({"McDonald", "MacLeod", "DeVries"})
_REF_STUCK_PARTICLE_PREFIXES = frozenset({"mc", "mac", "de", "la", "le", "van", "von"})
_REF_ODD_PAGES_RE = re.compile(
    r"(?:pp?\.?|с\.|стр\.?)\s*(\d{1,5})\s*[-–—]\s*(\d{1,5})",
    re.IGNORECASE,
)
_REF_GLUE_RE = re.compile(
    # URL одной записи сразу переходит в нумерацию следующей: «...doi.org/10.x 2. Author»
    r"https?://\S+\s+\d{1,3}\.\s*[A-ZА-ЯЁ]"
)
# Две записи в одной строке: год первой + номер/автор второй
_REF_GLUE_NEXT_NUM_RE = re.compile(
    r"\b(?:19|20)\d{2}\b\s*[.;]?\s*(?:\[\d+\]|\d{1,3}[.)])\s+[A-ZА-ЯЁ]"
)
# Редкая склейка без номера: «... 2020. Ivanov I. Title. 2021.»
_REF_GLUE_TWO_PUBS_RE = re.compile(
    r"\b(?:19|20)\d{2}\b\s*[.]\s+[A-ZА-ЯЁ][a-zа-яё'-]{1,}\s+[A-ZА-ЯЁ](?:\.|[a-zа-яё])"
    r".{10,120}?\b(?:19|20)\d{2}\b"
)
# ГОСТ/РФ: страницы слиплись с номером следующей записи («772 с.2. ВЕНТЦЕЛЬ», «С. 66–73.4. ЛАРЮШИН»)
_REF_GLUE_GOST_RE = re.compile(
    r"(?:"
    r"\d{1,5}\s*[сcСC]\.\s*\d{1,3}\.\s*[A-ZА-ЯЁ]"
    r"|"
    r"[сcСC]\.\s*\d{1,5}\s*[–—\-]\s*\d{1,5}\.\s*\d{1,3}\.\s*[A-ZА-ЯЁ]"
    r")"
)
# Граница следующей записи после «с.» или после диапазона страниц «66–73.»
# (без lookbehind переменной длины — в Python он запрещён)
_REF_GLUE_SPLIT_AT_RE = re.compile(
    r"(?:[сcСC]\.|[–—\-]\d{1,5}\.)(?=\d{1,3}\.\s+[A-ZА-ЯЁ])"
)
_REF_BROKEN_RE = re.compile(
    r"(?:"
    r"https?://\s*$"  # оборванный схемой
    r"|"
    r"https?://(?:dx\.)?doi\.org/?$"
    r"|"
    r"\bdoi:\s*$"
    r"|"
    r"[,:]\s*$"  # обрыв на запятой/двоеточии
    r"|"
    r"https?://\S+/$"  # URL оборван на /
    r")",
    re.IGNORECASE,
)
# Начало «выводов/аннотации», ошибочно попавших в ref-list
_REF_PROSE_START_RE = re.compile(
    r"^\s*(?:\d+[.)]\s+|\[\d+\]\s*)?(?:"
    r"На\s+основе|Установлено|Показано|Разработан[аоы]?|Выявлено|"
    r"Рассмотрен[аоы]?|Предложен[аоы]?|Получен[аоы]?|В\s+работе|"
    r"Целью\s+|Актуальность|Сделан[аоы]?\s+вывод|"
    r"It\s+is\s+shown|It\s+was\s+(?:found|shown)|We\s+(?:show|propose|develop)|"
    r"On\s+the\s+basis\s+of|The\s+(?:paper|article)\s+"
    r")",
    re.IGNORECASE,
)
_REF_CITATION_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
_REF_CITATION_BIBLIO_MARK_RE = re.compile(
    r"(?:"
    r"//"  # ГОСТ: название // журнал
    r"|\b(?:vol\.|том\b|т\.\s*\d|no\.|№\s*\d|iss(?:ue)?\.?|"
    r"pp?\.?\s*\d|с\.\s*\d|стр\.?\s*\d|pages?\b)"
    r"|\bdoi\b|10\.\d{4,9}/"
    r"|(?:https?://|www\.)"
    r")",
    re.IGNORECASE,
)
_MIN_REF_ABSTRACT_OVERLAP = 72


def validate_issn(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _ISSN_PATTERN.match(s):
        return f"ISSN не соответствует формату XXXX-XXXX: «{s[:20]}{'…' if len(s) > 20 else ''}»"
    return None


def validate_doi(value: Optional[str]) -> Optional[str]:
    """Синтаксическая проверка DOI. Возвращает текст ошибки или None."""
    analysis = analyze_doi(value)
    if not analysis.get("present"):
        return "Не найден DOI статьи"
    msgs = analysis.get("format_errors") or []
    if msgs:
        return str(msgs[0])
    return None


def analyze_doi(value: Optional[str]) -> Dict[str, object]:
    """
    Этапы 1–2 проверки DOI:
    - присутствует;
    - формат синтаксически корректен.
    """
    result: Dict[str, object] = {
        "present": False,
        "format_ok": False,
        "normalized": None,
        "format_errors": [],
    }
    if value is None or not str(value).strip():
        result["format_errors"] = ["Не найден DOI статьи"]
        return result

    raw = str(value).strip()
    result["present"] = True
    errors: List[str] = []

    s = raw
    # URL целиком / префикс doi:
    m_url = re.search(r"(?:https?://(?:dx\.)?doi\.org/|doi:\s*)(10\.\S+)", s, re.IGNORECASE)
    if m_url:
        errors.append("DOI задан как URL или с префиксом doi: — ожидается значение вида 10.xxxx/…")
        s = m_url.group(1).rstrip(".,;")

    if " " in s or "\t" in s:
        errors.append("DOI содержит пробелы")
    if "//" in s or re.search(r"10\.\d+\s*//", s.lower()):
        errors.append("DOI содержит двойной слеш «//» (некорректный формат)")
    if re.search(r"[.,;]+$", s):
        errors.append("DOI заканчивается точкой или запятой")
        s = s.rstrip(".,;")
    if "doi:" in s.lower():
        errors.append("Внутри значения DOI содержится «doi:»")

    s_l = s.lower()
    if not _DOI_PATTERN.match(s_l):
        errors.append(
            f"DOI не соответствует формату 10.XXXX/...: «{raw[:40]}{'…' if len(raw) > 40 else ''}»"
        )
    elif len(s_l) < 15:
        errors.append("DOI подозрительно короткий")

    result["normalized"] = s
    result["format_ok"] = not errors
    result["format_errors"] = errors
    return result


def resolve_doi_via_doi_org(
    doi: str,
    *,
    timeout_s: float = 12.0,
    title_hint: Optional[str] = None,
) -> Dict[str, object]:
    """
    Этапы 3–4: разрешение через doi.org и грубое сравнение метаданных.
    Не бросает наружу сетевые исключения — возвращает статус.
    """
    import json
    import urllib.error
    import urllib.parse
    import urllib.request

    out: Dict[str, object] = {
        "resolved": False,
        "metadata_match": None,
        "final_url": None,
        "title": None,
        "error": None,
    }
    doi_clean = str(doi).strip()
    if not doi_clean:
        out["error"] = "пустой DOI"
        return out

    url = "https://doi.org/" + urllib.parse.quote(doi_clean)
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.citationstyles.csl+json, application/json;q=0.9, */*;q=0.1",
            "User-Agent": "IPSAS-Issue-Metadata-Parser/1.0 (mailto:support@local)",
        },
        method="GET",
    )
    try:
        from ipsas.common.ssrf import urlopen_safe

        with urlopen_safe(req, timeout=timeout_s) as resp:
            out["final_url"] = resp.geturl()
            ctype = (resp.headers.get("Content-Type") or "").lower()
            body = resp.read(2_000_000)
            out["resolved"] = 200 <= getattr(resp, "status", 200) < 400
            if "json" in ctype or body[:1] in (b"{", b"["):
                try:
                    data = json.loads(body.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    data = None
                if isinstance(data, dict):
                    title = data.get("title")
                    if isinstance(title, list):
                        title = title[0] if title else None
                    out["title"] = title
                    if title_hint and title:
                        a = re.sub(r"\W+", " ", str(title_hint).lower()).strip()
                        b = re.sub(r"\W+", " ", str(title).lower()).strip()
                        if a and b:
                            # совпадение по существенной доле токенов
                            ta, tb = set(a.split()), set(b.split())
                            overlap = len(ta & tb) / max(1, min(len(ta), len(tb)))
                            out["metadata_match"] = overlap >= 0.45
                        else:
                            out["metadata_match"] = None
                    else:
                        out["metadata_match"] = None if not title_hint else False
            elif out["resolved"]:
                # HTML landing — разрешение успешно, метаданные не сверили
                out["metadata_match"] = None
    except urllib.error.HTTPError as e:
        out["error"] = f"HTTP {e.code}"
        out["resolved"] = False
    except Exception as e:  # noqa: BLE001 — сеть/таймаут не должны валить парсинг
        out["error"] = str(e)[:120]
        out["resolved"] = False
    return out


def build_doi_check(
    article: Dict[str, object],
    *,
    resolve: bool = False,
    timeout_s: float = 12.0,
) -> Dict[str, object]:
    """Собрать четырёхэтапный статус DOI для статьи."""
    identifiers = article.get("identifiers") or {}
    raw = identifiers.get("doi") if isinstance(identifiers, dict) else None
    analysis = analyze_doi(str(raw) if raw is not None else None)
    check: Dict[str, object] = {
        "present": bool(analysis.get("present")),
        "format_ok": bool(analysis.get("format_ok")),
        "resolved": None,
        "metadata_match": None,
        "normalized": analysis.get("normalized"),
        "format_errors": list(analysis.get("format_errors") or []),
        "resolve_error": None,
        "final_url": None,
        "crossref_title": None,
    }
    if resolve and check["present"] and check["format_ok"] and check.get("normalized"):
        title_hint = article.get("title_en") or article.get("title_ru")
        resolved = resolve_doi_via_doi_org(
            str(check["normalized"]),
            timeout_s=timeout_s,
            title_hint=str(title_hint) if title_hint else None,
        )
        check["resolved"] = resolved.get("resolved")
        check["metadata_match"] = resolved.get("metadata_match")
        check["resolve_error"] = resolved.get("error")
        check["final_url"] = resolved.get("final_url")
        check["crossref_title"] = resolved.get("title")
    article["doi_check"] = check
    return check


def validate_edn(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if s.isdigit():
        return f"Значение «{s}» похоже на внутренний ID статьи, а не на EDN"
    if not _EDN_PATTERN.match(s):
        return f"EDN должен быть 6-символьным кодом с буквами: «{s[:15]}{'…' if len(s) > 15 else ''}»"
    return None


def looks_like_edn(value: Optional[str]) -> bool:
    """True только для правдоподобного EDN (не числовой article id)."""
    if not value or not str(value).strip():
        return False
    s = str(value).strip()
    return bool(_EDN_PATTERN.match(s)) and not s.isdigit()


def validate_date(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if not _DATE_PATTERN.match(s):
        return f"Дата не в формате ГГГГ-ММ-ДД: «{s[:20]}»"
    parts = s.split("-")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return f"Некорректная дата: «{s}»"
    return None


def validate_year(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if not _YEAR_PATTERN.match(s):
        return f"Год не в формате ГГГГ: «{s[:15]}»"
    y = int(s)
    if not (1900 <= y <= 2100):
        return f"Год вне допустимого диапазона: «{s}»"
    return None


def validate_volume_issue(value: Optional[object], name: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list) and value:
        value = value[0]
    s = str(value).strip()
    if not s:
        return None
    if not re.match(r"^\d{1,5}$", s):
        return f"{name} должен быть числом: «{s[:15]}»"
    n = int(s)
    if n < 1 or n > 99999:
        return f"{name} вне допустимого диапазона (1–99999): «{s}»"
    return None


def validate_journal_title(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if len(s) < 2:
        return "Название журнала слишком короткое"
    if len(s) > 500:
        return "Название журнала слишком длинное"
    return None


def validate_author_name(name: str) -> Optional[str]:
    if not name or not name.strip():
        return "Пустое имя автора"
    s = name.strip()
    if len(s) < 3:
        return f"Слишком короткое имя автора: «{s}»"
    if len(s) > 150:
        return f"Слишком длинное имя автора: «{s[:30]}…»"
    if " " not in s and "." not in s:
        return f"Имя автора должно содержать пробел или инициалы: «{s[:30]}»"
    return None


def validate_affiliation(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if len(s) < 2:
        return "Название организации слишком короткое"
    if len(s) > 1000:
        return "Название организации слишком длинное"
    return None


def validate_edn(value: Optional[str]) -> Optional[str]:
    if not value or not value.strip():
        return None
    s = value.strip()
    if s.isdigit():
        return f"Значение «{s}» — внутренний ID платформы, а не EDN"
    if re.search(r"\d", s):
        return f"EDN не должен содержать цифр (ожидается 6 латинских букв): «{s}»"
    if not _EDN_PATTERN.match(s):
        return f"EDN должен состоять из 6 латинских букв: «{s[:15]}{'…' if len(s) > 15 else ''}»"
    return None


def looks_like_edn(value: Optional[str]) -> bool:
    """True только для EDN из 6 латинских букв (не числовой article id)."""
    if not value or not str(value).strip():
        return False
    s = str(value).strip()
    return bool(_EDN_PATTERN.match(s))


def parse_pages_value(value: Optional[object]) -> Optional[Dict[str, object]]:
    """
    Разобрать страницы: диапазон start–end или eLocator.
    Возвращает dict: start, end, elocation, display, dash, dash_ok.
    """
    if value is None:
        return None
    if isinstance(value, dict) and (value.get("start") is not None or value.get("elocation")):
        start = value.get("start")
        end = value.get("end", start)
        eloc = value.get("elocation")
        if eloc and start is None:
            return {
                "start": None,
                "end": None,
                "elocation": str(eloc).strip(),
                "display": str(eloc).strip(),
                "dash": None,
                "dash_ok": True,
            }
        try:
            si, ei = int(start), int(end)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        return {
            "start": si,
            "end": ei,
            "elocation": None,
            "display": f"{si}{_CORRECT_PAGE_DASH}{ei}",
            "dash": _CORRECT_PAGE_DASH,
            "dash_ok": True,
        }

    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"(?i)^(?:стр|с|pages?|pp?)\.?\s*", "", s).strip()
    m = _PAGE_RANGE_RE.search(s)
    if m:
        start, end = int(m.group("start")), int(m.group("end"))
        dash = m.group("dash")
        return {
            "start": start,
            "end": end,
            "elocation": None,
            "display": f"{start}{_CORRECT_PAGE_DASH}{end}",
            "dash": dash,
            "dash_ok": dash == _CORRECT_PAGE_DASH,
            "raw": s,
        }
    # одиночная страница или eLocator
    if re.fullmatch(r"\d{1,5}", s):
        n = int(s)
        return {
            "start": n,
            "end": n,
            "elocation": None,
            "display": str(n),
            "dash": None,
            "dash_ok": True,
            "raw": s,
        }
    if re.fullmatch(r"[eE]\d{3,}", s):
        return {
            "start": None,
            "end": None,
            "elocation": s,
            "display": s,
            "dash": None,
            "dash_ok": True,
            "raw": s,
        }
    return None


def format_pages_display(parsed: Optional[Dict[str, object]]) -> Optional[str]:
    if not parsed:
        return None
    if parsed.get("elocation"):
        return str(parsed["elocation"])
    start, end = parsed.get("start"), parsed.get("end")
    if isinstance(start, int) and isinstance(end, int):
        if start == end:
            return str(start)
        return f"{start}{_CORRECT_PAGE_DASH}{end}"
    return str(parsed.get("display") or "") or None


def pages_key(parsed: Optional[Dict[str, object]]) -> Optional[str]:
    """Ключ для сравнения источников (без учёта типа тире)."""
    if not parsed:
        return None
    if parsed.get("elocation"):
        return f"e:{str(parsed['elocation']).lower()}"
    start, end = parsed.get("start"), parsed.get("end")
    if isinstance(start, int) and isinstance(end, int):
        return f"{start}-{end}"
    return None


def extract_pages_from_doi(doi: Optional[str]) -> Optional[Tuple[int, int]]:
    """Извлечь диапазон страниц из хвоста DOI вида …-94-106."""
    if not doi or not str(doi).strip():
        return None
    s = str(doi).strip().rstrip("/")
    m = _PAGES_FROM_DOI_RE.search(s)
    if not m:
        return None
    start, end = int(m.group(1)), int(m.group(2))
    if start <= 0 or end <= 0 or end < start:
        return None
    if end - start > 200:
        return None
    return start, end


def enrich_article_pages(article: Dict[str, object]) -> None:
    """
    Выбрать страницы статьи.

    Канонический источник и проверки — только JATS (fpage/lpage).
    HTML / OJS meta / библиоописание сохраняются в pages_sources для справки,
    но не участвуют в согласованности и не перебивают JATS.
    DOI — только неподтверждённая подсказка, если JATS нет.
    """
    sources_in = article.get("pages_sources")
    if not isinstance(sources_in, dict):
        sources_in = {}

    # Подтянуть DOI-кандидат, не записывая его как факт
    identifiers = article.get("identifiers") or {}
    doi = identifiers.get("doi") if isinstance(identifiers, dict) else None
    doi_pages = extract_pages_from_doi(str(doi) if doi else None)
    if doi_pages and "doi" not in sources_in:
        sources_in["doi"] = f"{doi_pages[0]}-{doi_pages[1]}"

    # Уже заданные page_start/end без источника — считаем ojs_meta (fallback без JATS)
    if (
        "html" not in sources_in
        and "jats" not in sources_in
        and "ojs_meta" not in sources_in
        and article.get("page_start") is not None
        and article.get("page_end") is not None
    ):
        sources_in.setdefault(
            "ojs_meta",
            f"{article.get('page_start')}-{article.get('page_end')}",
        )
    elif article.get("pages") and "html" not in sources_in and "jats" not in sources_in:
        sources_in.setdefault("ojs_meta", article.get("pages"))

    parsed_sources: Dict[str, Optional[Dict[str, object]]] = {}
    for key in ("jats", "html", "ojs_meta", "biblio", "doi"):
        raw = sources_in.get(key)
        parsed_sources[key] = parse_pages_value(raw) if raw not in (None, "") else None

    article["pages_sources"] = {
        k: (format_pages_display(v) if v else None) for k, v in parsed_sources.items()
    }
    article["pages_sources_raw"] = dict(sources_in)

    # Канон: только JATS; иначе fallback HTML → OJS → biblio → DOI
    canonical = None
    canonical_name = None
    if parsed_sources.get("jats"):
        canonical = parsed_sources["jats"]
        canonical_name = "jats"
    else:
        for name in ("html", "ojs_meta", "biblio"):
            if parsed_sources.get(name):
                canonical = parsed_sources[name]
                canonical_name = name
                break

    pages_from_doi_only = False
    if not canonical and parsed_sources.get("doi"):
        canonical = parsed_sources["doi"]
        canonical_name = "doi"
        pages_from_doi_only = True

    article["pages_source"] = canonical_name
    article["pages_from_doi_unconfirmed"] = pages_from_doi_only

    if canonical:
        article["page_start"] = canonical.get("start")
        article["page_end"] = canonical.get("end")
        article["pages"] = format_pages_display(canonical)
        article["pages_elocation"] = canonical.get("elocation")
        article["pages_dash_ok"] = bool(canonical.get("dash_ok", True))
    else:
        # Не затираем, если уже было что-то осмысленное
        if article.get("page_start") is None and article.get("page_end") is None:
            article["pages"] = article.get("pages") or None

    # Согласованность HTML/meta/biblio с JATS не проверяем — канон только JATS
    article["pages_sources_conflict"] = False
    article.pop("pages_sources_conflict_detail", None)


def _script_majority(text: str) -> Optional[str]:
    """Вернуть 'lat' / 'cyr' по доминирующему алфавиту или None."""
    lat = len(re.findall(r"[A-Za-z]", text))
    cyr = len(re.findall(r"[А-Яа-яЁё]", text))
    total = lat + cyr
    if total == 0:
        return None
    if lat >= cyr:
        return "lat"
    return "cyr"


def _join_keywords(values: object) -> str:
    if isinstance(values, list):
        return " ".join(str(v) for v in values if v)
    if isinstance(values, str):
        return values
    return ""


def is_mostly_uppercase_title(title: Optional[str]) -> bool:
    """Название почти целиком прописными (без автозамены из‑за формул)."""
    if not title or not title.strip():
        return False
    letters = [c for c in title if c.isalpha()]
    if len(letters) < 12:
        return False
    upper = sum(1 for c in letters if c.isupper())
    return (upper / len(letters)) >= 0.85


def _title_word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", text))


def looks_like_transliteration(title_en: Optional[str], title_ru: Optional[str] = None) -> bool:
    """
    Совокупный критерий транслитерации:
    - доля транслит-маркеров;
    - мало/нет английских служебных слов;
    - сходство числа слов с русским названием (если есть).
    """
    if not title_en or not str(title_en).strip():
        return False
    t = str(title_en).strip()
    if _script_majority(t) != "lat":
        return False

    words = re.findall(r"[A-Za-z']+", t)
    if len(words) < 3:
        return False

    eng_func = len(_EN_FUNCTION_WORDS_RE.findall(t))
    translit_hits = len(_TRANSLIT_MARKERS_RE.findall(t))
    odd_caps = bool(re.search(r"(?:[A-Z]{2,}[a-z][A-Z])|(?:[a-z]Ch)|(?:Ch[A-Z])|(?:Zh[A-Z])|(?:Ya[A-Z])|(?:Sh[A-Z])", t))
    translit_ratio = translit_hits / max(len(words), 1)

    similar_len = False
    if title_ru and str(title_ru).strip():
        n_en = _title_word_count(t)
        n_ru = _title_word_count(str(title_ru))
        if n_en > 0 and n_ru > 0:
            similar_len = abs(n_en - n_ru) <= max(2, int(0.35 * max(n_en, n_ru)))

    # Один маркер сам по себе недостаточен.
    score = 0
    if translit_hits >= 2:
        score += 2
    elif translit_hits == 1 and (odd_caps or is_mostly_uppercase_title(t)):
        score += 1
    if translit_ratio >= 0.25:
        score += 1
    if eng_func == 0:
        score += 2
    elif eng_func <= 1 and translit_hits >= 2:
        score += 1
    if odd_caps:
        score += 1
    if similar_len and translit_hits >= 1 and eng_func == 0:
        score += 2

    return score >= 4


def title_has_html_garbage(title: Optional[str]) -> bool:
    return bool(title and _HTML_TAG_RE.search(title))


def title_has_whitespace_issues(title: Optional[str]) -> bool:
    if not title:
        return False
    if "\n" in title or "\r" in title:
        return True
    if "  " in title:
        return True
    return bool(title != title.strip())


def validate_orcid(value: Optional[str]) -> Optional[str]:
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    if not _ORCID_RE.match(s):
        return (
            "ORCID не соответствует формату XXXX-XXXX-XXXX-XXXX "
            f"(последний символ — цифра или X): «{s[:40]}»"
        )
    return None


def validate_email(value: Optional[str]) -> Optional[str]:
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    if not _EMAIL_RE.match(s):
        return f"Email имеет некорректный формат: «{s[:40]}»"
    return None


def _first_suspicious_stuck_name(text: str) -> Optional[str]:
    """Склеенная фамилия у автора (CamelCase + инициалы); бренды/журналы не трогаем."""
    # ГОСТ: после // — журнал/издание (AlfaBuild, TekhnoPrint и т.п.)
    zone = text.split("//", 1)[0] if "//" in text else text
    for stuck in _REF_STUCK_NAME_RE.finditer(zone):
        token = stuck.group(0)
        if token in _REF_STUCK_NAME_ALLOW:
            continue
        if stuck.group(1).lower() in _REF_STUCK_PARTICLE_PREFIXES:
            continue
        after = text[stuck.end() : stuck.end() + 24]
        before = text[max(0, stuck.start() - 16) : stuck.start()]
        # Только если рядом инициалы — иначе это заголовок/бренд (ExoPass, …)
        if not (
            _REF_STUCK_AUTHOR_AFTER_RE.match(after)
            or _REF_STUCK_AUTHOR_BEFORE_RE.search(before)
        ):
            continue
        return token
    return None


def split_glued_bibliography_item(text: str) -> List[str]:
    """Разрезать запись, где несколько источников слиплись без перевода строки."""
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    starts = [0]
    for m in _REF_GLUE_SPLIT_AT_RE.finditer(cleaned):
        # m.end() — начало «N. Author» следующей записи
        pos = m.end()
        if pos > starts[-1]:
            starts.append(pos)
    if len(starts) < 2:
        return [cleaned]
    parts: List[str] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(cleaned)
        part = cleaned[start:end].strip()
        if part:
            parts.append(part)
    return parts if len(parts) >= 2 else [cleaned]


def bibliography_item_looks_glued(text: str) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    return bool(
        _REF_GLUE_RE.search(cleaned)
        or _REF_GLUE_NEXT_NUM_RE.search(cleaned)
        or _REF_GLUE_TWO_PUBS_RE.search(cleaned)
        or _REF_GLUE_GOST_RE.search(cleaned)
        or len(split_glued_bibliography_item(cleaned)) >= 2
    )


def _normalize_ref_for_dup(text: str) -> str:
    t = re.sub(r"^\s*(?:\[\d+\]|\d+[.)]\s+)", "", text)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def article_issue(
    issues: List[Dict[str, object]],
    text: str,
    severity: str = "warning",
    field: Optional[str] = None,
    *,
    rule_id: Optional[str] = None,
    category: Optional[str] = None,
) -> None:
    from ipsas.modules.issue_metadata.rules import resolve_category, resolve_rule_id

    rid = rule_id or resolve_rule_id(field=field, text=text)
    cat = category or resolve_category(field=field, text=text, rule_id=rid)
    item: Dict[str, object] = {"text": text, "severity": severity, "category": cat}
    if field:
        item["field"] = field
    if rid:
        item["rule_id"] = rid
    issues.append(item)


def classify_bibliography_item_lang(text: str) -> str:
    """Язык одной записи списка: ru / en / unk по доминирующему алфавиту."""
    if not text or not str(text).strip():
        return "unk"
    s = str(text)
    lat = len(re.findall(r"[A-Za-z]", s))
    cyr = len(re.findall(r"[А-Яа-яЁё]", s))
    if cyr == 0 and lat == 0:
        return "unk"
    # Научные ссылки часто латиница + кириллические инициалы/город — берём большинство
    if cyr > lat:
        return "ru"
    if lat > cyr:
        return "en"
    return "unk"


def recompute_bibliography_lang_stats(article: Dict[str, object]) -> None:
    """
    Пересчитать RU/EN/unk по фактическому списку references.

    Если язык уже взят из xml:lang в JATS (citation-alternatives) — не перезаписывать
    эвристикой по алфавиту (иначе EN-версия с «(In Russ.)» и т.п. съезжает).
    Не путать с двумя параллельными блоками Литература/References на HTML.
    """
    mode = str(article.get("references_mode") or "single_list")
    if mode == "parallel_blocks":
        return
    if article.get("references_lang_source") == "xml_lang":
        # Счётчики уже из JATS по атрибуту lang; total — число <ref>
        items_raw = article.get("references") or []
        if isinstance(items_raw, list) and items_raw:
            article["references_count"] = len([x for x in items_raw if x])
        return
    if article.get("references_lang_source") == "unspecified":
        # JATS без xml:lang — не классифицируем записи по алфавиту для UI
        items_raw = article.get("references") or []
        if isinstance(items_raw, list) and items_raw:
            article["references_count"] = len([x for x in items_raw if x])
        article["references_ru_count"] = 0
        article["references_en_count"] = 0
        article["references_unk_count"] = 0
        return
    if mode == "parallel_citations":
        # RU/EN — параллельные версии одних и тех же <ref>, сумма ≠ total
        return

    items_raw = article.get("references") or []
    items: List[str] = [str(x) for x in items_raw if x] if isinstance(items_raw, list) else []
    if not items:
        return

    ru_n = en_n = unk_n = 0
    for text in items:
        lang = classify_bibliography_item_lang(text)
        if lang == "ru":
            ru_n += 1
        elif lang == "en":
            en_n += 1
        else:
            unk_n += 1

    article["references_ru_count"] = ru_n
    article["references_en_count"] = en_n
    article["references_unk_count"] = unk_n
    total = len(items)
    # Каноническое число — длина анализируемого списка (обычно JATS)
    article["references_count"] = total


def _strip_ref_num_prefix(text: str) -> str:
    return _REF_NUM_PREFIX_RE.sub("", text.strip(), count=1).strip()


def _normalize_overlap_text(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").lower()).strip()
    t = t.replace("«", '"').replace("»", '"').replace("–", "-").replace("—", "-")
    return t


def looks_like_bibliographic_citation(text: str) -> bool:
    """Грубая эвристика: есть год + библио-маркеры / DOI / ГОСТ //."""
    body = _strip_ref_num_prefix(text)
    if not body or len(body) < 12:
        return False
    if _REF_DOI_RE.search(body) or re.search(r"(?i)\bdoi\b", body):
        return True
    if "//" in body:
        return True
    if _REF_CITATION_YEAR_RE.search(body) and _REF_CITATION_BIBLIO_MARK_RE.search(body):
        return True
    # Короткая запись с годом (часто «Author. Title. Journal. 2020.»)
    if _REF_CITATION_YEAR_RE.search(body) and len(body) <= 320:
        return True
    return False


def looks_like_article_prose_in_references(text: str) -> bool:
    """Текст выводов/аннотации вместо библиографической записи."""
    if looks_like_bibliographic_citation(text):
        return False
    body = _strip_ref_num_prefix(text)
    if _REF_PROSE_START_RE.match(text) or _REF_PROSE_START_RE.match(body):
        return True
    # Длинное предложение без библио-признаков
    if len(body) >= 160 and not _REF_CITATION_YEAR_RE.search(body):
        return True
    return False


def reference_overlaps_abstract(ref_text: str, abstract: str, *, min_chars: int = _MIN_REF_ABSTRACT_OVERLAP) -> bool:
    """Совпадение начала записи списка с аннотацией."""
    abs_n = _normalize_overlap_text(abstract)
    ref_n = _normalize_overlap_text(_strip_ref_num_prefix(ref_text))
    if not abs_n or not ref_n:
        return False
    n = min(min_chars, len(abs_n), len(ref_n))
    if n < 40:
        return False
    if abs_n[:n] == ref_n[:n]:
        return True
    # Аннотация целиком «вшита» в «источник» или наоборот
    sample = min(200, len(ref_n), len(abs_n))
    if sample >= 60 and (ref_n[:sample] in abs_n or abs_n[:sample] in ref_n):
        return True
    return False


def analyze_bibliography_items(items: List[str]) -> Dict[str, object]:
    """Проверки качества записей одного списка литературы."""
    result: Dict[str, object] = {
        "duplicate_count": 0,
        "numbering_ok": True,
        "numbering_issues": [],
        "suspicious": [],
        "glued": [],
        "broken": [],
        "bad_doi": [],
        "not_citation": [],
    }
    if not items:
        return result

    # Нумерация
    nums: List[int] = []
    for i, text in enumerate(items, start=1):
        m = _REF_NUM_PREFIX_RE.match(text)
        if m:
            n = int(m.group(1) or m.group(2))
            nums.append(n)
        else:
            nums.append(0)
    present = [n for n in nums if n > 0]
    if present and len(present) >= max(3, len(items) // 2):
        expected = list(range(1, len(items) + 1))
        if nums[: len(expected)] != expected and present != expected[: len(present)]:
            # Проверяем строго возрастающую последовательность без дыр
            if present != list(range(present[0], present[0] + len(present))):
                result["numbering_ok"] = False
                result["numbering_issues"] = [
                    f"Нумерация источников выглядит неверной (начало: {present[:8]})"
                ]

    # Дубли
    seen: Dict[str, int] = {}
    dups = 0
    for i, text in enumerate(items, start=1):
        key = _normalize_ref_for_dup(text)
        if len(key) < 20:
            continue
        if key in seen:
            dups += 1
            result["suspicious"].append(
                {"index": i, "reason": f"дубль записи (как у №{seen[key]})", "sample": text[:80]}
            )
        else:
            seen[key] = i
    result["duplicate_count"] = dups

    for i, text in enumerate(items, start=1):
        cleaned = text.strip()
        parts = split_glued_bibliography_item(cleaned)
        looks_glued = bibliography_item_looks_glued(cleaned)
        if looks_glued:
            result["glued"].append(
                {
                    "index": i,
                    "sample": cleaned[:100],
                    "estimated_parts": max(len(parts), 2),
                    "first_part": (parts[0] if parts else cleaned)[:220],
                    "last_part": (parts[-1] if parts else cleaned)[:220],
                }
            )
        # Разорванные / обрезанные (полный https://doi.org/... в конце — норма)
        if len(cleaned) < 25 or _REF_BROKEN_RE.search(cleaned):
            result["broken"].append({"index": i, "sample": cleaned[:80]})
        # Подозрительная нормализация
        if _REF_TILDE_RE.search(text):
            result["suspicious"].append(
                {"index": i, "reason": "символ «~» в имени/тексте (возможна опечатка нормализации)", "sample": text[:80]}
            )
        stuck_token = _first_suspicious_stuck_name(text)
        if stuck_token:
            result["suspicious"].append(
                {
                    "index": i,
                    "reason": f"склеенное имя без пробела «{stuck_token}»",
                    "sample": text[:80],
                }
            )
        if looks_like_article_prose_in_references(text):
            result["not_citation"].append(
                {
                    "index": i,
                    "reason": "похоже на текст статьи/выводов, а не на библиографическую запись",
                    "sample": text[:100],
                }
            )
        for pm in _REF_ODD_PAGES_RE.finditer(text):
            a, b = int(pm.group(1)), int(pm.group(2))
            if a > b or (b - a) > 400:
                result["suspicious"].append(
                    {
                        "index": i,
                        "reason": f"необычный диапазон страниц {a}–{b}",
                        "sample": text[:80],
                    }
                )
                break
        # DOI внутри источника
        for dm in _REF_DOI_RE.finditer(text):
            doi_raw = dm.group(1).rstrip(".,;)")
            err = validate_doi(doi_raw)
            if err:
                result["bad_doi"].append({"index": i, "doi": doi_raw[:60], "error": err})

    return result


def validate_bibliography(article: Dict[str, object], issues: List[Dict[str, object]]) -> None:
    """Проверки библиографии с учётом single_list vs parallel_blocks."""
    recompute_bibliography_lang_stats(article)

    references_count = int(article.get("references_count") or 0)
    references_ru_count = int(article.get("references_ru_count") or 0)
    references_en_count = int(article.get("references_en_count") or 0)
    references_unk_count = int(article.get("references_unk_count") or 0)
    mode = str(article.get("references_mode") or "single_list")
    items_raw = article.get("references") or []
    items: List[str] = [str(x) for x in items_raw if x] if isinstance(items_raw, list) else []

    if references_count == 0 and not items:
        article_issue(issues, "Отсутствует список литературы", "error", "references")
        return

    if references_count == 0 and items:
        references_count = len(items)
        article["references_count"] = references_count

    # Параллельные самостоятельные блоки — единственный случай сравнения числа
    if mode == "parallel_blocks":
        pr = article.get("references_parallel_ru_count")
        pe = article.get("references_parallel_en_count")
        try:
            pr_i = int(pr) if pr is not None else None
            pe_i = int(pe) if pe is not None else None
        except (TypeError, ValueError):
            pr_i = pe_i = None
        if pr_i is not None and pe_i is not None and pr_i > 0 and pe_i > 0 and pr_i != pe_i:
            article_issue(
                issues,
                (
                    f"Два самостоятельных блока библиографии различаются по числу записей: "
                    f"Литература — {pr_i}, References — {pe_i}"
                ),
                "warning",
                "references",
            )
    # single_list / parallel_citations — НЕ сравниваем ru/en как два списка

    if article.get("references_count_mismatch_html_jats"):
        html_n = article.get("references_html_count") or "—"
        jats_n = article.get("references_jats_count") or article.get("references_count")
        article_issue(
            issues,
            (
                f"Число источников на HTML ({html_n}) и в JATS ({jats_n}) различается; "
                f"для анализа используется список из JATS"
            ),
            "warning",
            "references",
        )

    analysis = analyze_bibliography_items(items)
    numbered_in_text = sum(1 for t in items if _REF_NUM_PREFIX_RE.match(t))
    article["references_analysis"] = {
        "duplicate_count": analysis.get("duplicate_count"),
        "numbering_ok": analysis.get("numbering_ok"),
        "numbered_in_text_count": numbered_in_text,
        "suspicious_count": len(analysis.get("suspicious") or []),
        "glued_count": len(analysis.get("glued") or []),
        "broken_count": len(analysis.get("broken") or []),
        "bad_doi_count": len(analysis.get("bad_doi") or []),
        "not_citation_count": len(analysis.get("not_citation") or []),
    }

    # Нумерация в тексте не нужна: платформа/система проставляет номера сама
    if numbered_in_text:
        article_issue(
            issues,
            (
                f"Список литературы содержит нумерацию в тексте источников "
                f"({numbered_in_text} из {len(items)}) — нумерации в списке литературы "
                f"быть не должно: система проставляет номера сама"
            ),
            "warning",
            "references",
            rule_id="REF_NUMBERING",
        )

    # Аннотация / выводы ошибочно вставлены в ref-list (как в 440589)
    abstracts: List[str] = []
    for key in ("abstract_ru", "abstract_en", "page_abstract_ru", "page_abstract_en"):
        val = article.get(key)
        if isinstance(val, str) and val.strip():
            abstracts.append(val.strip())
    overlap_hits: List[int] = []
    for i, text in enumerate(items, start=1):
        if any(reference_overlaps_abstract(text, abs_text) for abs_text in abstracts):
            overlap_hits.append(i)
    if overlap_hits:
        sample_idx = overlap_hits[0]
        article_issue(
            issues,
            (
                f"Запись списка литературы №{sample_idx} совпадает с аннотацией — "
                f"в ref-list попал текст статьи, а не библиографические источники"
                + (f" (ещё совпадений: {len(overlap_hits) - 1})" if len(overlap_hits) > 1 else "")
            ),
            "error",
            "references",
        )

    not_citation = analysis.get("not_citation") or []
    if not_citation:
        n_bad = len(not_citation)
        n_all = len(items)
        if n_all >= 3 and n_bad * 2 >= n_all:
            article_issue(
                issues,
                (
                    f"Список литературы похоже содержит текст статьи/выводов, "
                    f"а не библиографические записи ({n_bad} из {n_all})"
                ),
                "error",
                "references",
            )
        else:
            sample = not_citation[0]
            article_issue(
                issues,
                (
                    f"Запись библиографии №{sample.get('index')} не похожа на источник: "
                    f"{sample.get('reason')}"
                ),
                "warning",
                "references",
            )

    if analysis.get("numbering_ok") is False:
        for msg in analysis.get("numbering_issues") or []:
            article_issue(issues, str(msg), "warning", "references")

    dup_n = int(analysis.get("duplicate_count") or 0)
    if dup_n:
        article_issue(
            issues,
            f"В списке литературы обнаружены дубли записей: {dup_n}",
            "warning",
            "references",
        )

    glued = analysis.get("glued") or []
    if glued:
        sample = glued[0]
        parts_n = int(sample.get("estimated_parts") or 0)
        snippet = str(sample.get("sample") or "").rstrip()
        if len(snippet) >= 100:
            snippet = snippet[:97].rstrip() + "…"
        if parts_n >= 3 or len(str(sample.get("sample") or "")) >= 80:
            msg = (
                f"В записи библиографии №{sample.get('index')} склеены несколько источников"
                + (f" (около {parts_n})" if parts_n >= 2 else "")
                + f": «{snippet}»"
            )
            severity = "error"
        else:
            msg = (
                f"Возможно склеенные записи библиографии (напр. №{sample.get('index')}): «{snippet}»"
            )
            severity = "warning"
        article_issue(issues, msg, severity, "references")

    broken = analysis.get("broken") or []
    if broken:
        sample = broken[0]
        snippet = str(sample.get("sample") or "").rstrip()
        article_issue(
            issues,
            f"Возможно разорванная/обрезанная запись (№{sample.get('index')}): «{snippet}»",
            "warning",
            "references",
        )

    bad_doi = analysis.get("bad_doi") or []
    if bad_doi:
        sample = bad_doi[0]
        article_issue(
            issues,
            f"В источнике №{sample.get('index')} подозрительный DOI: {sample.get('error')}",
            "warning",
            "references",
        )

    # Подозрительные нормализации — предупреждения, не безусловные ошибки
    suspicious = analysis.get("suspicious") or []
    shown = 0
    for s in suspicious:
        if s.get("reason", "").startswith("дубль"):
            continue
        article_issue(
            issues,
            f"Подозрительная запись библиографии №{s.get('index')}: {s.get('reason')}",
            "warning",
            "references",
        )
        shown += 1
        if shown >= 5:
            rest = len([x for x in suspicious if not str(x.get("reason", "")).startswith("дубль")]) - shown
            if rest > 0:
                article_issue(
                    issues,
                    f"Ещё подозрительных записей библиографии: {rest}",
                    "warning",
                    "references",
                )
            break

    # Смешение скриптов в первой записи
    for ref_key in (
        "reference_ru_first",
        "reference_en_first",
        "reference_first",
        "reference_unk_first",
    ):
        ref = article.get(ref_key)
        if isinstance(ref, str) and ref.strip() and _MIXED_SCRIPT_WORD_RE.search(ref[:120]):
            article_issue(
                issues,
                f"В первом источнике смешаны кириллица и латиница в одном слове: «{ref[:60]}…»"
                if len(ref) > 60
                else f"В первом источнике смешаны кириллица и латиница в одном слове: «{ref}»",
                "warning",
                "references",
            )
            break

    # Сохранить сводку для отчёта (не ошибка)
    article["references_report"] = {
        "mode": mode,
        "total": references_count,
        "ru_presumed": references_ru_count,
        "en_presumed": references_en_count,
        "unk_presumed": references_unk_count,
    }


def build_article_issues(article: Dict[str, object]) -> List[Dict[str, object]]:
    """Структурированные замечания по статье (error / warning)."""
    issues: List[Dict[str, object]] = []
    enrich_article_pages(article)

    title_ru = article.get("title_ru")
    title_en = article.get("title_en")
    abstract_ru = article.get("abstract_ru")
    abstract_en = article.get("abstract_en")
    abstract_ru_stats = article.get("abstract_ru_stats") or {}
    abstract_en_stats = article.get("abstract_en_stats") or {}
    keywords_ru = article.get("keywords_ru") or []
    keywords_en = article.get("keywords_en") or []
    keywords_ru_count = article.get("keywords_ru_count", 0) or 0
    keywords_en_count = article.get("keywords_en_count", 0) or 0
    references_count = article.get("references_count", 0) or 0
    references_ru_count = int(article.get("references_ru_count") or 0)
    references_en_count = int(article.get("references_en_count") or 0)
    references_unk_count = int(article.get("references_unk_count") or 0)
    identifiers = article.get("identifiers") or {}
    affiliations = article.get("affiliations") or []
    pdf_files = article.get("pdf_files") or []

    if not (title_ru or "").strip() and not (title_en or "").strip():
        article_issue(issues, "Отсутствует название статьи", "error", "title")
    elif not (title_ru or "").strip():
        article_issue(issues, "Отсутствует название статьи (RU)", "error", "title_ru")
    elif not (title_en or "").strip():
        article_issue(issues, "Отсутствует название статьи (EN)", "error", "title_en")

    if article.get("title_ru_from_jats_only"):
        article_issue(
            issues,
            "Название RU есть в JATS, но не представлено на публичной странице",
            "error",
            "title_ru",
        )
    if article.get("title_en_from_jats_only"):
        article_issue(
            issues,
            "Название EN есть в JATS, но не представлено на публичной странице",
            "error",
            "title_en",
        )

    title_ru_s = (title_ru or "").strip()
    title_en_s = (title_en or "").strip()
    if title_ru_s and _script_majority(title_ru_s) == "lat":
        article_issue(
            issues,
            "Название (RU) целиком или преимущественно на латинице",
            "error",
            "title_ru",
        )
    if title_en_s and _script_majority(title_en_s) == "cyr":
        article_issue(
            issues,
            "Название (EN) целиком или преимущественно на кириллице",
            "error",
            "title_en",
        )
    for label, value, field in (
        ("RU", title_ru, "title_ru"),
        ("EN", title_en, "title_en"),
    ):
        if not value:
            continue
        if title_has_html_garbage(str(value)):
            article_issue(issues, f"Название ({label}) содержит HTML-разметку", "warning", field)
        if title_has_whitespace_issues(str(value)):
            article_issue(
                issues,
                f"Название ({label}): лишние переносы или двойные пробелы",
                "warning",
                field,
            )

    page_abstract_ru = article.get("page_abstract_ru")
    page_abstract_en = article.get("page_abstract_en")
    has_page_ru = (
        bool(str(page_abstract_ru).strip())
        if "page_abstract_ru" in article
        else bool((abstract_ru or "").strip())
    )
    has_page_en = (
        bool(str(page_abstract_en).strip())
        if "page_abstract_en" in article
        else bool((abstract_en or "").strip())
    )

    if not has_page_ru:
        if article.get("abstract_ru_from_jats_only") or (abstract_ru or "").strip():
            article_issue(
                issues,
                "Русская аннотация есть в исходных данных (JATS), но не представлена на публичной странице",
                "error",
                "abstract_ru",
            )
        else:
            article_issue(issues, "Отсутствует аннотация (RU)", "error", "abstract_ru")

    if not has_page_en:
        if article.get("abstract_en_from_jats_only") or (abstract_en or "").strip():
            article_issue(
                issues,
                "Английская аннотация есть в исходных данных (JATS), но не представлена на публичной странице",
                "error",
                "abstract_en",
            )
        else:
            article_issue(issues, "Отсутствует аннотация (EN)", "error", "abstract_en")

    abstract_ru_s = (str(page_abstract_ru).strip() if has_page_ru else (abstract_ru or "").strip())
    abstract_en_s = (str(page_abstract_en).strip() if has_page_en else (abstract_en or "").strip())
    if abstract_ru_s and _script_majority(abstract_ru_s) == "lat":
        article_issue(
            issues,
            "Аннотация (RU) целиком или преимущественно на латинице",
            "error",
            "abstract_ru",
        )
    if abstract_en_s and _script_majority(abstract_en_s) == "cyr":
        article_issue(
            issues,
            "Аннотация (EN) целиком или преимущественно на кириллице",
            "error",
            "abstract_en",
        )

    len_ru = abstract_ru_stats.get("length") if isinstance(abstract_ru_stats, dict) else None
    len_en = abstract_en_stats.get("length") if isinstance(abstract_en_stats, dict) else None
    if len_ru is None and abstract_ru_s:
        len_ru = _title_word_count(abstract_ru_s)
    if len_en is None and abstract_en_s:
        len_en = _title_word_count(abstract_en_s)

    for label, length, field in (
        ("RU", len_ru, "abstract_ru"),
        ("EN", len_en, "abstract_en"),
    ):
        if length is None:
            continue
        if length < _MIN_ABSTRACT_WORDS:
            article_issue(
                issues,
                f"Слишком короткая аннотация ({label}): {length} слов (рекомендуется не менее {_MIN_ABSTRACT_WORDS})",
                "warning",
                field,
            )
        elif length > _MAX_ABSTRACT_WORDS:
            article_issue(
                issues,
                f"Слишком длинная аннотация ({label}): {length} слов (рекомендуется не более {_MAX_ABSTRACT_WORDS})",
                "warning",
                field,
            )

    for label, text, field in (
        ("RU", abstract_ru_s, "abstract_ru"),
        ("EN", abstract_en_s, "abstract_en"),
    ):
        if not text:
            continue
        if _ABSTRACT_HEADING_RE.match(text):
            article_issue(
                issues,
                f"Аннотация ({label}) начинается с заголовка «Аннотация»/Abstract внутри текста",
                "warning",
                field,
            )
        if _ABSTRACT_LEAK_RE.search(text):
            article_issue(
                issues,
                f"Аннотация ({label}) похоже содержит авторов, ключевые слова или библиографию",
                "warning",
                field,
            )
        if re.search(r"\.\.\.\s*$", text) or text.rstrip().endswith("…"):
            article_issue(
                issues,
                f"Аннотация ({label}) обрывается (многоточие в конце)",
                "warning",
                field,
            )

    if abstract_ru_s and abstract_en_s:
        norm_ru = re.sub(r"\s+", " ", abstract_ru_s.lower())
        norm_en = re.sub(r"\s+", " ", abstract_en_s.lower())
        if norm_ru == norm_en:
            article_issue(
                issues,
                "Текст аннотаций RU и EN полностью совпадает",
                "error",
                "abstract",
            )

    # --- Ключевые слова (presence по публичной странице) ---
    page_kw_ru = article.get("page_keywords_ru")
    page_kw_en = article.get("page_keywords_en")
    page_kw_ru_n = (
        len([k for k in page_kw_ru if str(k).strip()])
        if isinstance(page_kw_ru, list)
        else keywords_ru_count
    )
    page_kw_en_n = (
        len([k for k in page_kw_en if str(k).strip()])
        if isinstance(page_kw_en, list)
        else keywords_en_count
    )

    if page_kw_ru_n == 0:
        if article.get("keywords_ru_from_jats_only"):
            article_issue(
                issues,
                "Ключевые слова RU есть в JATS, но не представлены на публичной странице",
                "error",
                "keywords_ru",
            )
        else:
            article_issue(issues, "Отсутствуют ключевые слова RU", "error", "keywords_ru")

    if page_kw_en_n == 0:
        # Только явный признак «взято из JATS», без эвристики по keywords_en_count:
        # иначе пустой page_keywords_en=[] при заполненном keywords_en даёт ложный вывод.
        if article.get("keywords_en_from_jats_only"):
            article_issue(
                issues,
                "Ключевые слова EN есть в JATS, но не представлены на публичной странице",
                "error",
                "keywords_en",
            )
        else:
            article_issue(issues, "Отсутствуют ключевые слова EN", "error", "keywords_en")
    elif page_kw_ru_n > 0 and page_kw_en_n > 0 and page_kw_ru_n != page_kw_en_n:
        article_issue(
            issues,
            f"Количество ключевых слов RU и EN различается: RU — {page_kw_ru_n}, EN — {page_kw_en_n}",
            "warning",
            "keywords",
        )

    # Для качественных проверок используем то, что реально на странице, иначе общий список
    kw_ru_list = page_kw_ru if isinstance(page_kw_ru, list) and page_kw_ru_n else keywords_ru
    kw_en_list = page_kw_en if isinstance(page_kw_en, list) and page_kw_en_n else keywords_en

    def _check_keyword_list(values: object, label: str, field: str) -> None:
        if not isinstance(values, list) or not values:
            return
        cleaned = [str(k).strip() for k in values]
        if any(not k for k in cleaned):
            article_issue(issues, f"Ключевые слова ({label}): есть пустые элементы", "warning", field)
        non_empty = [k for k in cleaned if k]
        if len(non_empty) == 1 and ("," in non_empty[0] or ";" in non_empty[0]):
            article_issue(
                issues,
                f"Ключевые слова ({label}): все термины помещены в один элемент массива",
                "warning",
                field,
            )
        norm = [re.sub(r"\s+", " ", k.lower()) for k in non_empty]
        if len(norm) != len(set(norm)):
            article_issue(issues, f"Ключевые слова ({label}): обнаружены дубли", "warning", field)
        if any(k.endswith(".") for k in non_empty):
            article_issue(
                issues,
                f"Ключевые слова ({label}): точка в конце отдельного ключевого слова",
                "warning",
                field,
            )

    _check_keyword_list(kw_ru_list, "RU", "keywords_ru")
    _check_keyword_list(kw_en_list, "EN", "keywords_en")

    kw_ru_text = _join_keywords(kw_ru_list)
    kw_en_text = _join_keywords(kw_en_list)
    if page_kw_ru_n > 0 and kw_ru_text and _script_majority(kw_ru_text) == "lat":
        article_issue(
            issues,
            "Язык ключевых слов не соответствует полю RU (текст преимущественно на латинице)",
            "error",
            "keywords_ru",
        )
    if page_kw_en_n > 0 and kw_en_text and _script_majority(kw_en_text) == "cyr":
        article_issue(
            issues,
            "Язык ключевых слов не соответствует полю EN (текст преимущественно на кириллице)",
            "error",
            "keywords_en",
        )

    if references_count == 0 and not (article.get("references") or []):
        # validate_bibliography тоже проверит; оставляем вызов единой функции
        pass
    validate_bibliography(article, issues)

    if isinstance(identifiers, dict):
        doi_check = article.get("doi_check")
        if not isinstance(doi_check, dict):
            doi_check = build_doi_check(article, resolve=False)
        # DOI необязателен: журнал может не присваивать DOI статьям.
        if doi_check.get("present"):
            if not doi_check.get("format_ok"):
                for msg in doi_check.get("format_errors") or ["DOI: формат некорректен"]:
                    article_issue(issues, str(msg), "error", "doi")
            # Разрешение через doi.org не поднимаем в замечания:
            # HTTP 404 часто = ещё не зарегистрированный DOI и не помогает правке метаданных.
        edn = identifiers.get("edn")
        internal_id = identifiers.get("internal_id")
        if edn:
            edn_s = str(edn).strip()
            if internal_id and edn_s == str(internal_id).strip():
                identifiers["edn"] = None
                article_issue(
                    issues,
                    f"В поле EDN указан внутренний ID платформы «{edn_s}» — EDN отсутствует",
                    "error",
                    "edn",
                )
            elif not looks_like_edn(edn_s):
                # Числовой ID статьи часто попадает в DC.Identifier — не считаем его EDN.
                if edn_s.isdigit() or (internal_id and edn_s == str(internal_id)):
                    if not identifiers.get("internal_id"):
                        identifiers["internal_id"] = edn_s
                    identifiers["edn"] = None
                    article_issue(
                        issues,
                        f"Вместо EDN обнаружен внутренний ID статьи «{edn_s}» — EDN отсутствует",
                        "error",
                        "edn",
                    )
                else:
                    identifiers["edn"] = None
                    err = validate_edn(edn_s)
                    article_issue(
                        issues,
                        err or f"Некорректный EDN «{edn_s}»",
                        "error",
                        "edn",
                    )
            else:
                err = validate_edn(edn_s)
                if err:
                    article_issue(issues, err, "error", "edn")
        elif identifiers.get("edn_missing_explicit"):
            pass  # «не указан» — без ошибки, если журнал не обязан
        # Если в HTML был «ID: NNN» без EDN — это норма; internal_id уже заполнен

    pub_date = article.get("publication_date")
    if pub_date:
        err = validate_date(pub_date if isinstance(pub_date, str) else str(pub_date))
        if err:
            article_issue(issues, err, "warning", "publication_date")

    authors_count = article.get("authors_count") or 0
    authors_ru = article.get("authors_ru") or []
    authors_en_list = article.get("authors_en") or []
    if not authors_ru and not article.get("authors_en") and not article.get("authors"):
        article_issue(issues, "Отсутствуют авторы", "error", "authors")
    elif authors_count == 0 and (authors_ru or authors_en_list):
        article_issue(issues, "Количество авторов не согласовано с списком", "warning", "authors")
    if (
        isinstance(authors_ru, list)
        and isinstance(authors_en_list, list)
        and authors_ru
        and authors_en_list
        and len(authors_ru) != len(authors_en_list)
    ):
        article_issue(
            issues,
            f"Число авторов RU и EN не совпадает: RU — {len(authors_ru)}, EN — {len(authors_en_list)}",
            "warning",
            "authors",
        )

    # Дубли авторов внутри языковой версии + пунктуация инициалов
    for lang_label, names in (("RU", authors_ru), ("EN", authors_en_list)):
        if not isinstance(names, list):
            continue
        normalized = [re.sub(r"\s+", " ", str(n).strip().lower()) for n in names if n]
        if len(normalized) != len(set(normalized)):
            article_issue(
                issues,
                f"Обнаружены дублирующиеся авторы ({lang_label})",
                "warning",
                "authors",
            )
        # Пунктуация инициалов — по каждому имени отдельно.
        # Нельзя склеивать ФИО через «; »: иначе «Олегович; Беликов» даёт ложное срабатывание.
        for name in names:
            name_s = str(name).strip()
            if not name_s:
                continue
            # «В. М;» / «В.М;» — нет точки у последнего инициала перед «;» внутри строки
            if re.search(r"[A-Za-zА-Яа-яЁё]\.\s*[A-Za-zА-Яа-яЁё]\s*;", name_s):
                article_issue(
                    issues,
                    "Пунктуация инициалов авторов: ожидается точка перед разделителем (например, «В. М.;»)",
                    "warning",
                    "authors",
                )
                break

    for name in list(authors_ru or [])[:8] + list(authors_en_list or [])[:8]:
        name_s = str(name)
        err = validate_author_name(name_s)
        if err:
            article_issue(issues, err, "warning", "authors")
            break
        # Фамилия не должна состоять только из инициалов
        if re.fullmatch(r"(?:[A-Za-zА-Яа-яЁё]\.\s*){1,4}", name_s.strip()):
            article_issue(
                issues,
                f"Имя автора состоит только из инициалов: «{name_s.strip()}»",
                "warning",
                "authors",
            )
            break
        if _AFFIL_MARK_IN_NAME_RE.search(name_s):
            article_issue(
                issues,
                f"В имени автора есть цифры/индексы аффилиации: «{name_s.strip()[:40]}»",
                "warning",
                "authors",
            )
            break
        # Инициалы без завершающей точки: «Зароченцев В. М»
        if re.search(r"[A-Za-zА-Яа-яЁё]\.\s*[A-Za-zА-Яа-яЁё]$", name_s.strip()):
            article_issue(
                issues,
                f"Инициалы автора без завершающей точки: «{name_s.strip()}» (ожидается «В. М.»)",
                "warning",
                "authors",
            )
            break

    # ORCID / email
    for orcid in article.get("orcids") or []:
        err = validate_orcid(str(orcid))
        if err:
            article_issue(issues, err, "warning", "orcid")
            break
    emails = [str(e).strip() for e in (article.get("emails") or []) if e and str(e).strip()]
    for email in emails:
        err = validate_email(email)
        if err:
            article_issue(issues, err, "warning", "email")
            break
    if len(emails) >= 2 and len(set(e.lower() for e in emails)) == 1:
        article_issue(
            issues,
            "Один и тот же email указан у всех авторов — вероятно ошибка",
            "warning",
            "email",
        )
    page_affs = article.get("page_affiliations") or []
    author_aff_refs = article.get("author_affiliation_refs") or []
    affiliations_ru = article.get("affiliations_ru") or []
    affiliations_en = article.get("affiliations_en") or []
    org_list = list(affiliations or []) or list(article.get("organizations") or [])

    # Пустая отрисовка: индекс есть, названия нет
    empty_page_idxs: List[int] = []
    if isinstance(page_affs, list):
        for it in page_affs:
            if not isinstance(it, dict):
                continue
            idx = it.get("index")
            name = str(it.get("name") or "").strip()
            if idx is not None and not name:
                try:
                    empty_page_idxs.append(int(idx))
                except (TypeError, ValueError):
                    pass
            elif name:
                err = validate_affiliation(name)
                if err:
                    article_issue(issues, err, "warning", "organizations")
                if _MULTI_ORG_RE.search(name) and len(name) > 60:
                    article_issue(
                        issues,
                        f"Возможно, несколько организаций объединены в одну строку: «{name[:80]}…»"
                        if len(name) > 80
                        else f"Возможно, несколько организаций объединены в одну строку: «{name}»",
                        "warning",
                        "organizations",
                    )
                if it.get("ror") and not _ROR_RE.search(str(it.get("ror"))):
                    article_issue(issues, f"Некорректный ROR организации: «{it.get('ror')}»", "warning", "ror")

    referenced_idxs: set[int] = set()
    if isinstance(author_aff_refs, list):
        for refs in author_aff_refs:
            if isinstance(refs, list):
                for n in refs:
                    try:
                        referenced_idxs.add(int(n))
                    except (TypeError, ValueError):
                        pass

    reported_empty: set[int] = set()
    for idx in sorted(set(empty_page_idxs) | referenced_idxs):
        # Автор ссылается на индекс без названия на странице
        page_item = None
        if isinstance(page_affs, list):
            for it in page_affs:
                if isinstance(it, dict) and it.get("index") == idx:
                    page_item = it
                    break
        name_ok = bool(page_item and str(page_item.get("name") or "").strip())
        if idx in empty_page_idxs or (idx in referenced_idxs and not name_ok and article.get("affiliations_section_present")):
            if idx not in reported_empty:
                reported_empty.add(idx)
                article_issue(
                    issues,
                    f"Для авторов указана ссылка на аффилиацию {idx}, но название организации отсутствует на странице",
                    "error",
                    "organizations",
                )

    if not org_list and not any(
        isinstance(it, dict) and str(it.get("name") or "").strip() for it in (page_affs or [])
    ):
        if not reported_empty:
            article_issue(issues, "Отсутствуют организации (аффилиации)", "error", "organizations")

    if affiliations_ru and affiliations_en and len(affiliations_ru) != len(affiliations_en):
        article_issue(
            issues,
            (
                "Несоответствие метаданных организаций RU/EN: "
                f"число названий различается (RU — {len(affiliations_ru)}, EN — {len(affiliations_en)})"
            ),
            "error",
            "organizations",
        )
    elif org_list and affiliations_ru and not affiliations_en:
        article_issue(
            issues,
            "Несоответствие метаданных организаций RU/EN: отсутствует английская форма названия",
            "error",
            "organizations",
        )
    elif org_list and affiliations_en and not affiliations_ru:
        article_issue(
            issues,
            "Несоответствие метаданных организаций RU/EN: отсутствует русская форма названия",
            "error",
            "organizations",
        )

    for aff in org_list[:5]:
        err = validate_affiliation(str(aff))
        if err:
            article_issue(issues, err, "warning", "organizations")
            break

    # Связи авторов с организациями в метаданных статьи
    jats_affs = article.get("jats_affiliations") or []
    broken_aff_refs = article.get("broken_affiliation_refs") or []
    if isinstance(broken_aff_refs, list):
        for br in broken_aff_refs:
            if not isinstance(br, dict):
                continue
            reason = br.get("reason")
            author = str(br.get("author") or "").strip()
            if reason == "missing_aff":
                if author:
                    msg = (
                        f"У автора «{author}» в метаданных указана организация, "
                        "которой нет в списке организаций этой статьи"
                    )
                else:
                    msg = (
                        "В метаданных автора указана организация, "
                        "которой нет в списке организаций этой статьи"
                    )
                article_issue(issues, msg, "error", "organizations")
            elif reason == "empty_aff":
                article_issue(
                    issues,
                    "В списке организаций статьи есть запись без названия организации",
                    "error",
                    "organizations",
                )
    page_org_names = [
        str(it.get("name") or "").strip()
        for it in (page_affs or [])
        if isinstance(it, dict) and str(it.get("name") or "").strip()
    ]
    jats_named = [
        a for a in jats_affs
        if isinstance(a, dict) and str(a.get("name") or "").strip()
    ]
    if jats_named and not page_org_names and not org_list:
        article_issue(
            issues,
            "Организации указаны в метаданных статьи, но не отображаются на публичной странице",
            "error",
            "organizations",
        )

    invalid_edn = identifiers.get("invalid_edn") if isinstance(identifiers, dict) else None
    if invalid_edn:
        article_issue(
            issues,
            f"Поле EDN содержит некорректное значение: {invalid_edn}",
            "error",
            "edn",
        )

    title_ru_s = (title_ru or "").strip()
    title_en_s = (title_en or "").strip()
    if title_ru_s and len(title_ru_s) < 5:
        article_issue(issues, "Название статьи (RU) слишком короткое", "warning", "title_ru")
    if title_en_s and len(title_en_s) < 5:
        article_issue(issues, "Название статьи (EN) слишком короткое", "warning", "title_en")
    if title_en_s and looks_like_transliteration(title_en_s, title_ru_s):
        article_issue(
            issues,
            "Поле «Название EN» содержит транслитерацию русского названия",
            "error",
            "title_en",
        )

    if not pdf_files:
        article_issue(issues, "Не найден PDF файл статьи", "warning", "pdf")

    # --- Страницы (канон и проверки — только JATS; HTML/meta не сравниваем) ---
    start = article.get("page_start")
    end = article.get("page_end")
    pages_disp = (article.get("pages") or "").strip()
    eloc = article.get("pages_elocation")

    if article.get("pages_from_doi_unconfirmed"):
        article_issue(
            issues,
            f"Страницы «{pages_disp}» взяты из DOI без подтверждения другими источниками",
            "warning",
            "pages",
        )
    if article.get("pages_dash_ok") is False:
        article_issue(
            issues,
            "В диапазоне страниц использован неверный тип тире (ожидается среднее тире «–»)",
            "warning",
            "pages",
        )

    if isinstance(start, int) and isinstance(end, int) and start > end:
        article_issue(
            issues,
            f"Первая страница больше последней: {start}–{end}",
            "error",
            "pages",
        )

    if start is None and end is None and not eloc and not pages_disp:
        article_issue(
            issues,
            "Не указаны страницы статьи",
            "warning",
            "pages",
        )

    return issues


def apply_article_validation(article: Dict[str, object]) -> List[Dict[str, object]]:
    """
    Применить валидацию к статье: заполнить pages, errors, problems, issues.
    Существующие errors (сбой парсинга) сохраняются.
    """
    issues = build_article_issues(article)

    # Предупреждения источников (recover JATS, skip XML и т.п.)
    src_warns = article.get("source_warnings")
    if isinstance(src_warns, list):
        for w in src_warns:
            if not isinstance(w, dict):
                continue
            text = str(w.get("text") or "").strip()
            if not text:
                continue
            if any(str(i.get("text") or "") == text for i in issues):
                continue
            article_issue(
                issues,
                text,
                str(w.get("severity") or "warning"),
                str(w.get("field") or "jats_xml"),
            )

    if article.get("jats_skip_reason"):
        text = str(article["jats_skip_reason"])
        if not any(str(i.get("text") or "") == text for i in issues):
            article_issue(issues, text, "warning", "jats_xml")

    parse_errors = [str(e) for e in (article.get("errors") or []) if e]
    for pe in parse_errors:
        if not any(str(i.get("text") or "") == pe for i in issues):
            article_issue(issues, pe, "error", "article")

    val_errors = [str(i["text"]) for i in issues if i.get("severity") == "error"]
    val_warns = [str(i["text"]) for i in issues if i.get("severity") == "warning"]
    article["issues"] = issues
    # errors = все ошибки (парсинг + валидация); problems = предупреждения (как в UI)
    article["errors"] = list(dict.fromkeys(parse_errors + val_errors))
    article["problems"] = val_warns

    from ipsas.modules.issue_metadata.report_display import enrich_article_report_display

    enrich_article_report_display(article)
    return issues


def build_article_problems(article: Dict[str, object]) -> List[str]:
    """Обратная совместимость: все тексты замечаний (error+warning)."""
    return [str(i["text"]) for i in build_article_issues(article)]


def issue_warn(
    warnings: List[Dict[str, object]],
    text: str,
    severity: str = "warning",
    field: Optional[str] = None,
    *,
    rule_id: Optional[str] = None,
    category: Optional[str] = None,
) -> None:
    from ipsas.modules.issue_metadata.rules import resolve_category, resolve_rule_id

    rid = rule_id or resolve_rule_id(field=field, text=text)
    cat = category or resolve_category(field=field, text=text, rule_id=rid)
    w: Dict[str, object] = {"text": text, "severity": severity, "category": cat}
    if field:
        w["field"] = field
    if rid:
        w["rule_id"] = rid
    warnings.append(w)


def _check_article_page_order(articles: List[Dict[str, object]], warnings: List[Dict[str, object]]) -> None:
    """
    Последовательность страниц:
    - start текущей должен быть > end предыдущей;
    - пересечения — ошибка/предупреждение;
    - пропуски (start > prev_end + 1) — предупреждение (обложка/содержание).
    """
    ranges: List[Tuple[int, int, int]] = []
    for idx, article in enumerate(articles, start=1):
        enrich_article_pages(article)
        start = article.get("page_start")
        end = article.get("page_end")
        if isinstance(start, int) and isinstance(end, int):
            ranges.append((idx, start, end))
            if end < start:
                issue_warn(
                    warnings,
                    f"Статья {idx}: некорректный диапазон страниц {start}–{end}",
                    "error",
                    "pages",
                )

    if len(ranges) < 2:
        return

    for i in range(1, len(ranges)):
        prev_idx, _prev_start, prev_end = ranges[i - 1]
        cur_idx, cur_start, cur_end = ranges[i]
        # Правило: start_page текущей > end_page предыдущей
        if cur_start <= prev_end:
            issue_warn(
                warnings,
                (
                    f"Пересечение или нарушение последовательности страниц: "
                    f"статья {prev_idx} заканчивается на {prev_end}, "
                    f"статья {cur_idx} начинается с {cur_start} ({cur_start}–{cur_end})"
                ),
                "error",
                "pages",
            )
        elif cur_start > prev_end + 1:
            gap_from = prev_end + 1
            gap_to = cur_start - 1
            issue_warn(
                warnings,
                (
                    f"Пропуск страниц между статьями {prev_idx} и {cur_idx}: "
                    f"{gap_from}–{gap_to} (допустимо для обложки, содержания или спецстраниц)"
                ),
                "warning",
                "pages",
            )


def _norm_title_key(title: Optional[object]) -> Optional[str]:
    if not title or not str(title).strip():
        return None
    return re.sub(r"\s+", " ", str(title).strip().lower())


def _article_pdf_urls(article: Dict[str, object]) -> List[str]:
    """Уникальные PDF URL статьи (порядок сохраняется)."""
    urls: List[str] = []
    seen: set[str] = set()

    def add(raw: object) -> None:
        url = str(raw or "").strip()
        if not url:
            return
        key = url.rstrip("/").lower()
        if key in seen:
            return
        seen.add(key)
        urls.append(url)

    identifiers = article.get("identifiers") or {}
    if isinstance(identifiers, dict):
        add(identifiers.get("pdf_url"))
    for item in article.get("pdf_files") or []:
        if isinstance(item, dict):
            add(item.get("url"))
        elif isinstance(item, str):
            add(item)
    return urls


def _check_cross_article_duplicates(
    articles: List[Dict[str, object]],
    warnings: List[Dict[str, object]],
) -> None:
    """Дубли article_id / DOI / названия / PDF URL между статьями выпуска."""
    seen_doi: Dict[str, int] = {}
    seen_id: Dict[str, int] = {}
    seen_title: Dict[str, int] = {}
    seen_pdf: Dict[str, int] = {}

    for idx, article in enumerate(articles, start=1):
        identifiers = article.get("identifiers") or {}
        if not isinstance(identifiers, dict):
            identifiers = {}

        doi = identifiers.get("doi")
        if doi and str(doi).strip():
            key = str(doi).strip().lower()
            if key in seen_doi and seen_doi[key] != idx:
                issue_warn(
                    warnings,
                    f"Дублирующийся DOI у статей {seen_doi[key]} и {idx}: «{str(doi)[:60]}»",
                    "error",
                    "doi",
                )
            else:
                seen_doi[key] = idx

        internal_id = identifiers.get("internal_id")
        if internal_id and str(internal_id).strip():
            key = str(internal_id).strip()
            if key in seen_id and seen_id[key] != idx:
                issue_warn(
                    warnings,
                    f"Дублирующийся article_id у статей {seen_id[key]} и {idx}: «{key}»",
                    "error",
                    "article_id",
                )
            else:
                seen_id[key] = idx

        for title in (article.get("title_ru"), article.get("title_en")):
            tkey = _norm_title_key(title)
            if not tkey or len(tkey) < 8:
                continue
            if tkey in seen_title and seen_title[tkey] != idx:
                issue_warn(
                    warnings,
                    f"Дублирующееся название у статей {seen_title[tkey]} и {idx}",
                    "error",
                    "title",
                )
            else:
                seen_title[tkey] = idx

        for pdf in _article_pdf_urls(article):
            pkey = pdf.rstrip("/").lower()
            if pkey in seen_pdf and seen_pdf[pkey] != idx:
                issue_warn(
                    warnings,
                    f"Дублирующийся PDF URL у статей {seen_pdf[pkey]} и {idx}",
                    "error",
                    "pdf",
                )
            else:
                seen_pdf[pkey] = idx


def _years_mentioned_in_doi(doi: str) -> List[str]:
    """Годы из DOI, без ложных срабатываний на ISSN (NNNN-NNNN)."""
    years: List[str] = []
    for m in re.finditer(r"(?:^|[/\-_.])((?:19|20)\d{2})(?=([/\-_.]|$))", doi):
        after = doi[m.end(1) :]
        # ISSN: 2072-0823 / 0130-3082 — четыре символа после дефиса, не номер выпуска
        if re.match(r"-(\d{3}[\dXx])(?:[/\-_.]|$)", after):
            continue
        years.append(m.group(1))
    return years


def _check_articles_belong_to_issue(
    issue_metadata: Dict[str, object],
    articles: List[Dict[str, object]],
    warnings: List[Dict[str, object]],
) -> None:
    """Эвристика: явный год публикации в DOI не совпадает с карточкой выпуска."""
    year = str(issue_metadata.get("year") or "").strip()
    issue_no = str(issue_metadata.get("issue") or "").strip()
    if not year and not issue_no:
        return

    for idx, article in enumerate(articles, start=1):
        identifiers = article.get("identifiers") or {}
        if not isinstance(identifiers, dict):
            continue
        doi = str(identifiers.get("doi") or "")
        if not doi:
            continue
        doi_years = _years_mentioned_in_doi(doi)
        # Если в DOI есть год выпуска — ок (даже при ISSN-подобных фрагментах рядом)
        if year and doi_years and year not in doi_years:
            shown = doi_years[0]
            issue_warn(
                warnings,
                (
                    f"Статья {idx}: год в DOI ({shown}) не совпадает "
                    f"с годом выпуска ({year}) — возможно, статья из другого выпуска"
                ),
                "warning",
                "issue_membership",
            )
            continue
        # …-YYYY-VOL-ISSUE-pages или …-YYYY-ISSUE-pages
        m_iss = re.search(
            rf"-{re.escape(year)}-(?:\d{{1,3}}-)?(\d{{1,3}})-\d{{1,5}}-\d{{1,5}}\b",
            doi,
        ) if year else None
        if issue_no and m_iss and m_iss.group(1).lstrip("0") != issue_no.lstrip("0"):
            # слабая эвристика: не шумим, если в DOI нет явного номера выпуска
            pass


def _infer_uses_volume_from_articles(articles: Optional[List[Dict[str, object]]]) -> Optional[bool]:
    """
    Эвристика по DOI статей: …-YYYY-VOL-ISSUE-pages.
    Возвращает True при устойчивых признаках тома, иначе None.
    """
    if not articles:
        return None
    hits = 0
    checked = 0
    for article in articles[:15]:
        if not isinstance(article, dict):
            continue
        identifiers = article.get("identifiers") or {}
        doi = identifiers.get("doi") if isinstance(identifiers, dict) else None
        if not doi:
            continue
        checked += 1
        # 10.xxxx/issn-2024-19-4-94-106 → том 19, номер 4
        if re.search(r"-(?:19|20)\d{2}-(\d{1,3})-(\d{1,3})-\d{1,5}-\d{1,5}\b", str(doi)):
            hits += 1
    if checked >= 2 and hits >= max(2, checked // 2):
        return True
    return None


def resolve_uses_volume(
    issue_metadata: Dict[str, object],
    articles: Optional[List[Dict[str, object]]] = None,
) -> Optional[bool]:
    """
    Определить, использует ли журнал тома.

    True  — том обязателен
    False — журнал публикуется только по номерам
    None  — недостаточно данных (том не требуем)
    """
    explicit = issue_metadata.get("uses_volume")
    if explicit is True or explicit is False:
        return explicit
    if issue_metadata.get("volume") not in (None, ""):
        return True
    title = str(issue_metadata.get("issue_title") or "")
    if re.search(r"(?:том|vol\.?|volume)\b", title, re.IGNORECASE):
        return True
    if re.search(r"(?:№|No\.?)\s*\d+", title, re.IGNORECASE) and not re.search(
        r"(?:том|vol\.?|volume)\b", title, re.IGNORECASE
    ):
        return False
    return _infer_uses_volume_from_articles(articles)


def build_issue_warnings(
    issue_metadata: Dict[str, object],
    articles: Optional[List[Dict[str, object]]] = None,
) -> List[Dict[str, object]]:
    warnings: List[Dict[str, object]] = []
    if not issue_metadata.get("journal_title") and not issue_metadata.get("journal_title_ru"):
        issue_warn(warnings, "Не найдено название журнала", "error", "journal_title")
    else:
        title = issue_metadata.get("journal_title_ru") or issue_metadata.get("journal_title")
        err = validate_journal_title(str(title) if title else None)
        if err:
            issue_warn(warnings, err, "warning", "journal_title")

    if not issue_metadata.get("issue_title"):
        issue_warn(warnings, "Не найден заголовок выпуска", "error", "issue_title")

    urls = issue_metadata.get("article_urls") or []
    article_count = issue_metadata.get("article_count")
    parsed_count = len(articles) if articles is not None else None
    if not urls:
        issue_warn(warnings, "Не найден список статей в выпуске", "error", "article_count")
    elif article_count is None:
        issue_warn(warnings, "Не определено количество статей в выпуске", "warning", "article_count")
    elif article_count == 0:
        issue_warn(warnings, "Количество статей указано как 0 при наличии ссылок на статьи", "warning", "article_count")
    elif article_count != len(urls):
        issue_warn(
            warnings,
            f"Количество статей в карточке выпуска ({article_count}) не совпадает "
            f"с числом ссылок/карточек ({len(urls)})",
            "error",
            "article_count",
        )
    if parsed_count is not None and urls and parsed_count != len(urls):
        issue_warn(
            warnings,
            f"Фактически разобрано статей: {parsed_count}, ссылок в выпуске: {len(urls)}",
            "warning",
            "article_count",
        )
    if (
        parsed_count is not None
        and article_count is not None
        and int(article_count) != parsed_count
        and (not urls or article_count == len(urls))
    ):
        issue_warn(
            warnings,
            f"В карточке выпуска указано {article_count} статей, фактически разобрано {parsed_count}",
            "error",
            "article_count",
        )

    # Год
    if not issue_metadata.get("year"):
        issue_warn(warnings, "Не определен год выпуска", "error", "year")
    else:
        err = validate_year(issue_metadata.get("year"))
        if err:
            issue_warn(warnings, err, "error", "year")

    # Номер выпуска
    if not issue_metadata.get("issue"):
        issue_warn(warnings, "Не определен номер выпуска", "error", "issue")
    else:
        err = validate_volume_issue(issue_metadata.get("issue"), "Номер выпуска")
        if err:
            issue_warn(warnings, err, "error", "issue")

    # Том — только если модель журнала предполагает тома
    uses_volume = resolve_uses_volume(issue_metadata, articles)
    issue_metadata["uses_volume"] = uses_volume
    volume = issue_metadata.get("volume")
    if uses_volume is True:
        if volume in (None, ""):
            issue_warn(
                warnings,
                "Том предусмотрен издательской моделью журнала, но не определён",
                "error",
                "volume",
            )
        else:
            err = validate_volume_issue(volume, "Том")
            if err:
                issue_warn(warnings, err, "error", "volume")
    elif volume not in (None, ""):
        err = validate_volume_issue(volume, "Том")
        if err:
            issue_warn(warnings, err, "warning", "volume")
    # uses_volume is False/None и тома нет — замечание не формируем

    # Сквозной / дополнительный номер в скобках — необязателен.
    # Если указан, проверяем только формат значения.
    serial = issue_metadata.get("issue_serial")
    if serial not in (None, ""):
        err = validate_volume_issue(serial, "Дополнительный номер выпуска")
        if err:
            issue_warn(warnings, err, "warning", "issue_serial")

    # Дата публикации выпуска не проверяется: на OJS часто отсутствует или не нужна для отчёта.

    # Язык выпуска
    if not (issue_metadata.get("issue_language") or "").strip():
        issue_warn(warnings, "Не определен язык выпуска", "warning", "issue_language")

    # Обложка
    if not issue_metadata.get("cover_url"):
        issue_warn(warnings, "Не найдена обложка выпуска", "warning", "cover")

    issn = (issue_metadata.get("issn") or "") if isinstance(issue_metadata.get("issn"), str) else issue_metadata.get("issn")
    eissn = (issue_metadata.get("eissn") or "") if isinstance(issue_metadata.get("eissn"), str) else issue_metadata.get("eissn")
    if isinstance(issn, str) and issn.strip():
        err = validate_issn(issn.strip())
        if err:
            issue_warn(warnings, err, "warning", "issn")
    if isinstance(eissn, str) and eissn.strip():
        err = validate_issn(eissn.strip())
        if err:
            issue_warn(warnings, err, "warning", "eissn")
    if (
        isinstance(issn, str)
        and isinstance(eissn, str)
        and issn.strip()
        and eissn.strip()
        and issn.strip().lower() == eissn.strip().lower()
    ):
        issue_warn(
            warnings,
            "Печатный и электронный ISSN совпадают. Необходимо проверить типы ISSN.",
            "warning",
            "issn",
        )

    galleys = issue_metadata.get("issue_galleys") or []
    if not galleys:
        issue_warn(
            warnings,
            "Отсутствует общий PDF выпуска (файл выпуска не найден на странице)",
            "warning",
            "issue_pdf",
        )

    if articles:
        _check_article_page_order(articles, warnings)
        _check_cross_article_duplicates(articles, warnings)
        _check_articles_belong_to_issue(issue_metadata, articles, warnings)

    return warnings
