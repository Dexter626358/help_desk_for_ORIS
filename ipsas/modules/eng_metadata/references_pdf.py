"""Rule-based разбор списка литературы, скопированного из PDF.

Одна библиографическая запись → одна строка. Без LLM и сети.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# --- regexes -----------------------------------------------------------------

_NUMBERED_START_RE = re.compile(
    r"^(?:\[\d+\]|\d+[.)]|№\s*\d+[.)]?)\s+\S",
)
# Снятие нумерации: [1] / 1. / 1) / № 1. в начале строки (с опциональным пробелом)
_LEADING_REFNUM_RE = re.compile(
    r"^(?:\[\d+\]|\d+[.)]|№\s*\d+[.)]?)\s+",
)
_AUTHOR_START_RE = re.compile(
    r"^[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё'’\-]+"
    r"(?:,\s*|\s+)"
    r"(?:[A-ZА-ЯЁ]\.\s*){1,4}"
    r"(?:[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё'’\-]+(?:,\s*|\s+)(?:[A-ZА-ЯЁ]\.\s*){1,4})*",
)
_AUTHOR_YEAR_RE = re.compile(
    r"^[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё'’\-,.\s]{2,80}\(\d{4}[a-z]?\)",
)
_ORG_START_RE = re.compile(
    r"^(?:World Health Organization|WHO|UNESCO|United Nations|UNICEF|"
    r"European Commission|OECD|ISO|IEEE|ASTM|IUPAC)\b",
    re.IGNORECASE,
)
_CONTINUATION_START_RE = re.compile(
    r"^(?:"
    r"https?://|www\.|doi\s*:|"
    r"//|"
    r"vol\.?|volume|v\.|"
    r"no\.?|n\.|№|"
    r"pp?\.|pages?|"
    r"т\.|с\.|"
    r"and|of|in|on|for|with|from|to|by|the|a|an|"
    r"и|в|на|для|с|из|от|по|к|у|о|об|"
    r"et\s+al"
    r")\b",
    re.IGNORECASE,
)
_URL_OR_DOI_RE = re.compile(
    r"(?:https?://|doi\s*:|doi\.org/)",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}[a-z]?\b")
_FINISHED_END_RE = re.compile(
    r"(?:[.!?]|[\dXx])\s*$|(?:https?://\S+|doi\.org/\S+)\s*$",
    re.IGNORECASE,
)
_BROKEN_URL_END_RE = re.compile(
    r"(?:https?://\S+|doi:\S*)$",
    re.IGNORECASE,
)
_URL_CONTINUE_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%\-]*$",
)

BOUNDARY_THRESHOLD = 4


@dataclass(frozen=True)
class ParsedReference:
    text: str
    raw_lines: list[str]
    start_line: int
    end_line: int
    confidence: float | None = None


@dataclass
class LineDecision:
    line: str
    score: int
    reasons: list[str] = field(default_factory=list)
    decision: str = "join"  # split | join | empty


@dataclass
class ParseResult:
    references: list[ParsedReference]
    text: str
    raw_text: str
    joined_breaks: int
    debug: list[LineDecision] = field(default_factory=list)

    def to_dict(self, *, include_debug: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "text": self.text,
            "raw_text": self.raw_text,
            "count": len(self.references),
            "joined_breaks": self.joined_breaks,
            "references": [
                {
                    "text": ref.text,
                    "raw_lines": list(ref.raw_lines),
                    "start_line": ref.start_line,
                    "end_line": ref.end_line,
                    "confidence": ref.confidence,
                }
                for ref in self.references
            ],
        }
        if include_debug:
            payload["debug"] = [
                {
                    "line": d.line,
                    "score": d.score,
                    "reasons": list(d.reasons),
                    "decision": d.decision,
                }
                for d in self.debug
            ]
        return payload


def normalize_reference_text(text: str) -> str:
    """Глобальная нормализация перевода строк и NBSP."""
    value = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    value = value.replace("\u00a0", " ").replace("\u202f", " ")
    return value


def normalize_line(line: str) -> str:
    value = str(line or "").replace("\u00a0", " ").replace("\u202f", " ")
    value = re.sub(r"[ \t]+", " ", value)
    return value.strip()


def is_numbered_reference_start(line: str) -> bool:
    return bool(_NUMBERED_START_RE.match(line))


def strip_leading_reference_number(line: str) -> str:
    """Убрать ведущую нумерацию источника: ``117.``, ``[3]``, ``1)``, ``№ 2.``."""
    text = str(line or "").strip()
    if not text:
        return ""
    return _LEADING_REFNUM_RE.sub("", text, count=1).strip()


def strip_reference_numbers(text: str) -> tuple[str, int]:
    """Убрать нумерацию у каждой непустой строки списка.

    Returns:
        (новый текст, сколько строк реально изменилось)
    """
    raw = str(text or "")
    out_lines: list[str] = []
    changed = 0
    for line in raw.splitlines():
        if not line.strip():
            out_lines.append("")
            continue
        cleaned = strip_leading_reference_number(line)
        if cleaned != line.strip():
            changed += 1
        out_lines.append(cleaned)
    # Сохраняем структуру строк; хвостовой \\n исходника не критичен для UI
    return "\n".join(out_lines), changed


def looks_like_author_start(line: str) -> bool:
    if is_numbered_reference_start(line):
        # «117. Smith J.» — автор после номера
        rest = re.sub(r"^(?:\[\d+\]|\d+[.)]|№\s*\d+[.)]?)\s+", "", line)
        return bool(_AUTHOR_START_RE.match(rest) or _ORG_START_RE.match(rest))
    return bool(_AUTHOR_START_RE.match(line) or _ORG_START_RE.match(line))


def looks_like_author_year_start(line: str) -> bool:
    body = re.sub(r"^(?:\[\d+\]|\d+[.)]|№\s*\d+[.)]?)\s+", "", line)
    return bool(_AUTHOR_YEAR_RE.match(body))


def looks_like_continuation(line: str) -> bool:
    if not line:
        return False
    if line[0].islower() or ("а" <= line[0] <= "я") or line[0] == "ё":
        return True
    if line[0] in ",;:.)]":
        return True
    if _CONTINUATION_START_RE.match(line):
        return True
    if _URL_OR_DOI_RE.match(line):
        return True
    return False


def previous_line_looks_finished(line: str) -> bool:
    if not line:
        return False
    if _URL_OR_DOI_RE.search(line) and _FINISHED_END_RE.search(line):
        return True
    # год + точка / страницы в конце
    if re.search(
        r"(?:pp?\.\s*\d|с\.\s*\d|\d{4}\s*\.|Vol\.\s*\d|No\.\s*\d).{0,12}$",
        line,
        re.IGNORECASE,
    ):
        return True
    if line.endswith(".") and _YEAR_RE.search(line) and len(line) > 40:
        return True
    return False


def previous_line_has_complete_doi_or_url(line: str) -> bool:
    if not line:
        return False
    if re.search(r"https?://\S+\.\S+\s*$", line, re.IGNORECASE):
        return True
    if re.search(r"doi\.org/10\.\S+\s*$", line, re.IGNORECASE):
        return True
    if re.search(r"\bdoi:\s*10\.\S+\s*$", line, re.IGNORECASE):
        return True
    return False


def _word_count(line: str) -> int:
    return len([w for w in re.split(r"\s+", line) if w])


def boundary_score(
    previous_line: str | None,
    current_line: str,
    next_line: str | None = None,
) -> tuple[int, list[str]]:
    """Оценка вероятности, что current — начало новой reference."""
    score = 0
    reasons: list[str] = []
    prev = previous_line or ""
    nxt = next_line or ""

    if is_numbered_reference_start(current_line):
        score += 10
        reasons.append("numbered-start +10")

    if looks_like_author_start(current_line):
        score += 4
        reasons.append("author-start +4")

    if looks_like_author_year_start(current_line):
        score += 3
        reasons.append("author-year +3")

    if prev and previous_line_looks_finished(prev):
        score += 2
        reasons.append("previous-finished +2")

    if prev and previous_line_has_complete_doi_or_url(prev):
        score += 2
        reasons.append("previous-doi-url +2")

    if looks_like_continuation(current_line):
        score -= 5
        reasons.append("continuation -5")

    if current_line and (
        current_line[0].islower()
        or ("а" <= current_line[0] <= "я")
        or current_line[0] == "ё"
    ):
        # уже учтено continuation, но усиливаем явный lowercase
        if "continuation -5" not in reasons:
            score -= 5
            reasons.append("lowercase-start -5")

    if re.match(r"^(?:https?://|www\.|doi\s*:)", current_line, re.IGNORECASE):
        if "continuation -5" not in reasons:
            score -= 5
            reasons.append("url-doi-start -5")

    if re.match(r"^[,;:.)\]]", current_line):
        score -= 5
        reasons.append("punct-start -5")

    if prev and re.search(r"[,;:/–—\-]$", prev):
        score -= 3
        reasons.append("prev-open-punct -3")

    words = _word_count(current_line)
    if words <= 3 and not is_numbered_reference_start(current_line):
        if not looks_like_author_start(current_line) and not _YEAR_RE.search(
            current_line
        ):
            score -= 3
            reasons.append("short-line -3")

    # next помогает: если current короткий «заголовочный» кусок, а next — continuation
    if (
        words <= 4
        and current_line
        and current_line[0].isupper()
        and not looks_like_author_start(current_line)
        and not is_numbered_reference_start(current_line)
        and nxt
        and looks_like_continuation(nxt)
    ):
        score -= 3
        reasons.append("title-fragment-before-continuation -3")

    return score, reasons


def should_start_new_reference(
    previous_line: str | None,
    current_line: str,
    next_line: str | None = None,
    *,
    threshold: int = BOUNDARY_THRESHOLD,
) -> tuple[bool, int, list[str]]:
    if previous_line is None:
        return True, 100, ["first-line"]
    score, reasons = boundary_score(previous_line, current_line, next_line)
    return score >= threshold, score, reasons


def _is_broken_url_join(previous: str, current: str) -> bool:
    if not previous or not current:
        return False
    if not _BROKEN_URL_END_RE.search(previous.rstrip()):
        # обрыв после «…doi.org/10.1016/»
        if not re.search(r"(?:https?://\S+|doi:\S*)/$", previous, re.IGNORECASE):
            return False
    if looks_like_author_start(current) or is_numbered_reference_start(current):
        return False
    if _URL_CONTINUE_RE.match(current) or current.startswith("10."):
        return True
    # типичный хвост DOI без схемы
    if re.match(r"^[a-z][a-z0-9._\-/]+$", current, re.IGNORECASE):
        return True
    return False


def join_lines(previous: str, current: str) -> str:
    """Склеить две физические строки одной reference."""
    prev = previous.rstrip()
    cur = current.lstrip()
    if not prev:
        return cur
    if not cur:
        return prev
    if _is_broken_url_join(prev, cur):
        return prev + cur
    # консервативно: дефис переноса + продолжение со строчной → без пробела
    if prev.endswith("-") and cur[0].islower():
        return prev + cur
    return f"{prev} {cur}"


def serialize_references(references: list[ParsedReference] | list[str]) -> str:
    lines: list[str] = []
    for item in references:
        if isinstance(item, ParsedReference):
            text = item.text.strip()
        else:
            text = str(item).strip()
        if text:
            lines.append(text)
    return "\n".join(lines)


def parse_references(
    text: str,
    *,
    threshold: int = BOUNDARY_THRESHOLD,
) -> ParseResult:
    """Главный pipeline: PDF-текст → список references (1 запись = 1 строка)."""
    raw_text = str(text or "")
    normalized = normalize_reference_text(raw_text)
    physical = [normalize_line(line) for line in normalized.split("\n")]

    # Пустые строки — сильный boundary, но не абсолютный: помечаем как separators
    groups: list[list[tuple[int, str]]] = []
    current_group: list[tuple[int, str]] = []
    debug: list[LineDecision] = []

    def flush_group() -> None:
        nonlocal current_group
        if current_group:
            groups.append(current_group)
            current_group = []

    for idx, line in enumerate(physical):
        if not line:
            debug.append(LineDecision(line="", score=0, reasons=["empty"], decision="empty"))
            flush_group()
            continue
        current_group.append((idx, line))
    flush_group()

    references: list[ParsedReference] = []
    joined_breaks = 0

    for group in groups:
        if not group:
            continue
        # внутри группы без пустых строк — scoring
        chunks: list[list[tuple[int, str]]] = []
        buf: list[tuple[int, str]] = []
        for i, (line_no, line) in enumerate(group):
            prev_line = buf[-1][1] if buf else None
            next_line = group[i + 1][1] if i + 1 < len(group) else None
            if not buf:
                buf = [(line_no, line)]
                debug.append(
                    LineDecision(
                        line=line,
                        score=100,
                        reasons=["group-start"],
                        decision="split",
                    )
                )
                continue
            is_new, score, reasons = should_start_new_reference(
                prev_line, line, next_line, threshold=threshold
            )
            debug.append(
                LineDecision(
                    line=line,
                    score=score,
                    reasons=reasons,
                    decision="split" if is_new else "join",
                )
            )
            if is_new:
                chunks.append(buf)
                buf = [(line_no, line)]
            else:
                buf.append((line_no, line))
                joined_breaks += 1
        if buf:
            chunks.append(buf)

        for chunk in chunks:
            lines = [ln for _, ln in chunk]
            text_joined = lines[0]
            for part in lines[1:]:
                text_joined = join_lines(text_joined, part)
            text_joined = re.sub(r"[ \t]{2,}", " ", text_joined).strip()
            if not text_joined:
                continue
            start = chunk[0][0]
            end = chunk[-1][0]
            # confidence: доля положительных split-решений на старте
            references.append(
                ParsedReference(
                    text=text_joined,
                    raw_lines=lines,
                    start_line=start,
                    end_line=end,
                    confidence=None,
                )
            )

    return ParseResult(
        references=references,
        text=serialize_references(references),
        raw_text=raw_text,
        joined_breaks=joined_breaks,
        debug=debug,
    )


def unwrap_pdf_references(text: str, *, include_debug: bool = False) -> dict[str, Any]:
    """Удобная обёртка для API / UI."""
    return parse_references(text).to_dict(include_debug=include_debug)
