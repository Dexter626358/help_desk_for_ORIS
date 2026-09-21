"""
Архивация рукописей из «Новые» (submissionsUnassigned), если отправитель совпадает.

Алгоритм:
  1) все страницы /editor/submissions/submissionsUnassigned (пагинация OJS: submissionsPage)
  2) открыть статью по ссылке в названии
  3) проверить поле «Отправитель»
  4) если совпадает — «Отклонить и заархивировать рукопись»
  5) на форме письма нажать «Пропустить» (без отправки email)

Страница «Новые»:
  https://journals.rcsi.science/0002-337X/editor/submissions/submissionsUnassigned

Примеры:
  # проверка без изменений (одна статья из списка)
  python archive_submissions_by_sender.py --dry-run --limit 1

  # одна конкретная рукопись
  python archive_submissions_by_sender.py --article-id 129710

  # все «Новые» журнала с подходящим отправителем
  python archive_submissions_by_sender.py --journal 0002-3337

  # все журналы РАН из RAS_journals_FULL_updated.json
  python archive_submissions_by_sender.py --all-ras

  # все журналы РАН, сначала dry-run на 2 журналах
  python archive_submissions_by_sender.py --all-ras --dry-run --journal-limit 2

  # продолжить после обрыва (пропуск уже обработанных ISSN из отчёта)
  python archive_submissions_by_sender.py --all-ras --resume

  # другой журнал и отправитель
  python archive_submissions_by_sender.py --journal 0002-337X --sender "Алла Викторовна Тыкманова"

  # несколько отправителей (можно указать --sender несколько раз)
  python archive_submissions_by_sender.py --journal 0002-337X --sender "Алла Викторовна Тыкманова" --sender "Максим Дмитриевич Боярский" --sender "Анастасия Юрьевна Беляева" --sender "Сергей Николаевич Гусев" --sender "Ольга Викторовна Кузьмичева" --sender "Елена Викторовна Жилякова" --sender "Марианна Михайловна Кандохова"

  # ограничить число обрабатываемых рукописей (на журнал)
  python archive_submissions_by_sender.py --journal 0002-337X --limit 5

  # подробный лог
  python archive_submissions_by_sender.py --journal 0002-337X --limit 1 -v
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font

from extract_journal_sections import DEFAULT_JOURNALS, ensure_auth
from rcsi_auth import RcsiAuthClient, RcsiAuthError

logger = logging.getLogger(__name__)

BASE_URL = "https://journals.rcsi.science"
DEFAULT_JOURNAL = "0002-337X"
DEFAULT_SENDERS = (
    "Алла Викторовна Тыкманова",
    "Максим Дмитриевич Боярский",
    "Анастасия Юрьевна Беляева",
    "Сергей Николаевич Гусев",
    "Ольга Викторовна Кузьмичева",
    "Елена Викторовна Жилякова",
    "Марианна Михайловна Кандохова",
    "Янина Юрьевна Мироненко",
    "Дерья Тарасова (Гребенникова)",
    "Анастасия Анатольевна Тарасова",
    "Анастасия Игоревна Заренкова",
    "Всеволод Игоревич Трофеев",
    "Светлана Викторовна Савицкая",
    "Дарья Полякова",
    "Марина Морозова",
    "Оксана Шестерова",
    "Татьяна Гуторова",
)

SUBMISSION_ROW_RE = re.compile(
    r"<td>(\d+)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>"
    r'\s*<td><a href="([^"]+)"[^>]*>([^<]+)</a></td>',
    re.I | re.S,
)
SENDER_RE = re.compile(
    r'<td class="label">Отправитель</td>\s*<td[^>]*>\s*(.*?)(?:<a|</td>)',
    re.I | re.S,
)
UNSUITABLE_URL_RE = re.compile(
    r'href="([^"]+/editor/unsuitableSubmission\?articleId=(\d+))"',
    re.I,
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


@dataclass
class SubmissionInfo:
    article_id: int
    submit_date: str
    section: str
    authors: str
    title: str
    url: str


@dataclass
class ArchiveResult:
    journal: str
    article_id: int
    title: str
    sender: str
    status: str
    message: str


def load_journals(path: Path) -> list[dict[str, str]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    journals: list[dict[str, str]] = []
    for item in raw:
        rcsi = str(item.get("rcsi_url") or "").strip().rstrip("/")
        issn = str(item.get("issn_print") or "").strip()
        title = str(item.get("title_ru") or "").strip()
        if not rcsi:
            continue
        slug = rcsi.rsplit("/", 1)[-1]
        journals.append(
            {
                "issn": issn or slug,
                "title_ru": title,
                "slug": slug,
                "rcsi_url": rcsi,
            }
        )
    return journals


def load_done_issns(csv_path: Path) -> set[str]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return set()
    done: set[str] = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            issn = (row.get("journal") or "").strip()
            if issn:
                done.add(issn.casefold())
    return done


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


def unassigned_url(journal: str) -> str:
    return f"{BASE_URL}/{journal}/editor/submissions/submissionsUnassigned"


def unassigned_page_url(
    journal: str,
    page: int,
    params: dict[str, str] | None = None,
) -> str:
    """URL страницы списка «Новые» с параметрами поиска/сортировки."""
    query = dict(params or {})
    if page > 1:
        query["submissionsPage"] = str(page)
    elif "submissionsPage" in query:
        query.pop("submissionsPage")
    base = unassigned_url(journal)
    if not query:
        return base
    return f"{base}?{urllib.parse.urlencode(query)}"


def parse_result_range(html: str) -> tuple[int, int, int] | None:
    """Возвращает (start, end, total) из блока «1 - 25 из 100 результатов»."""
    match = RESULT_RANGE_RE.search(html)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def extract_search_params(html: str) -> dict[str, str]:
    """Скрытые поля searchForm — sort, sortDirection и т.п."""
    form_match = SEARCH_FORM_RE.search(html)
    if not form_match:
        return {}
    params: dict[str, str] = {}
    for name, value in HIDDEN_INPUT_RE.findall(form_match.group(1)):
        params[name] = unescape(value)
    return params


def extract_page_numbers(html: str) -> list[int]:
    return [int(page) for page in re.findall(r"submissionsPage=(\d+)", html, re.I)]


def fetch_all_unassigned_submissions(
    auth: RcsiAuthClient,
    journal: str,
    *,
    delay: float = 0.0,
) -> list[SubmissionInfo]:
    """Загружает все страницы списка «Новые», не только первую."""
    all_items: list[SubmissionInfo] = []
    seen: set[int] = set()
    base_params: dict[str, str] = {}
    page = 1
    max_pages = 1

    while page <= max_pages:
        url = unassigned_page_url(journal, page, base_params)
        logger.info("Список «Новые», страница %s: %s", page, url)
        html = auth.get_text(url)
        if is_login_page(html):
            raise RcsiAuthError(f"Нет доступа к {url}")

        if page == 1:
            base_params = extract_search_params(html)

        batch = parse_submissions(html)
        new_count = 0
        for item in batch:
            if item.article_id not in seen:
                seen.add(item.article_id)
                all_items.append(item)
                new_count += 1

        range_info = parse_result_range(html)
        link_pages = extract_page_numbers(html)
        if link_pages:
            max_pages = max(max_pages, max(link_pages))

        if range_info:
            start, end, total = range_info
            page_size = max(end - start + 1, 1)
            max_pages = max(max_pages, (total + page_size - 1) // page_size)
            logger.info(
                "  диапазон %s-%s из %s, на странице %s, новых %s",
                start,
                end,
                total,
                len(batch),
                new_count,
            )
            if end >= total:
                break
        else:
            logger.info("  на странице %s, новых %s", len(batch), new_count)
            if not batch:
                break

        if new_count == 0 and page > 1:
            logger.warning("Страница %s не добавила новых рукописей — остановка", page)
            break

        page += 1
        if delay > 0:
            time.sleep(delay)

    return all_items


def submission_url(journal: str, article_id: int) -> str:
    return f"{BASE_URL}/{journal}/editor/submission/{article_id}"


def unsuitable_url(journal: str, article_id: int) -> str:
    return f"{BASE_URL}/{journal}/editor/unsuitableSubmission?articleId={article_id}"


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


def archive_submission(
    auth: RcsiAuthClient,
    journal: str,
    submission: SubmissionInfo,
    *,
    sender_names: list[str],
    dry_run: bool = False,
    verify: bool = True,
) -> ArchiveResult:
    detail_html = auth.get_text(submission.url)
    if is_login_page(detail_html):
        raise RcsiAuthError(f"Нет доступа к {submission.url}")

    sender = extract_sender(detail_html)
    result = ArchiveResult(
        journal=journal,
        article_id=submission.article_id,
        title=submission.title,
        sender=sender,
        status="pending",
        message="",
    )

    if not sender_matches(sender, sender_names):
        result.status = "skipped_sender"
        result.message = (
            f"отправитель не совпадает: {sender!r} "
            f"(ожидается один из: {', '.join(sender_names)})"
        )
        return result

    reject_url = unsuitable_url(journal, submission.article_id)
    reject_html = auth.get_text(reject_url)
    if is_login_page(reject_html):
        raise RcsiAuthError(f"Нет доступа к {reject_url}")

    form = parse_email_form(reject_html)
    payload = build_skip_payload(form)

    if dry_run:
        result.status = "dry_run"
        result.message = (
            f"будет отклонена без письма через {form.form_action} "
            f"({form.skip_field}={form.skip_value!r})"
        )
        return result

    status, final_url, content = auth.request(
        form.form_action,
        data=payload,
        headers={
            "Referer": reject_url,
            "Origin": BASE_URL,
        },
    )
    body = content.decode("utf-8", "replace")
    if status >= 400:
        result.status = "error"
        result.message = f"HTTP {status}"
        return result
    if is_login_page(body):
        result.status = "error"
        result.message = "после POST снова страница логина"
        return result

    if verify:
        list_html = auth.get_text(unassigned_url(journal))
        if submission_in_unassigned(list_html, submission.article_id):
            result.status = "verify_failed"
            result.message = "POST выполнен, но статья всё ещё в «Новые»"
            return result

    result.status = "archived"
    result.message = f"отклонена и заархивирована без письма ({final_url})"
    return result


def write_report(path: Path, results: list[ArchiveResult]) -> None:
    headers = ["journal", "article_id", "title", "sender", "status", "message"]
    wb = Workbook()
    ws = wb.active
    ws.title = "results"
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for r in results:
        ws.append([r.journal, r.article_id, r.title, r.sender, r.status, r.message])
    wb.save(path)

    csv_path = path.with_suffix(".csv")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for r in results:
            writer.writerow(
                [r.journal, r.article_id, r.title, r.sender, r.status, r.message]
            )


def process_journal(
    auth: RcsiAuthClient,
    journal: str,
    *,
    sender_names: list[str],
    article_id: int | None = None,
    limit: int | None = None,
    delay: float = 0.35,
    dry_run: bool = False,
    verify: bool = True,
) -> list[ArchiveResult]:
    if article_id is not None:
        submissions = [
            SubmissionInfo(
                article_id=article_id,
                submit_date="",
                section="",
                authors="",
                title=f"article {article_id}",
                url=submission_url(journal, article_id),
            )
        ]
    else:
        logger.info("Загрузка всех страниц «Новые» для %s", journal)
        submissions = fetch_all_unassigned_submissions(
            auth,
            journal,
            delay=delay,
        )
        logger.info(
            "Найдено рукописей в «Новые» (все страницы): %s",
            len(submissions),
        )

    if limit is not None:
        submissions = submissions[:limit]
    if not submissions:
        logger.info("Нет рукописей для обработки в %s", journal)
        return []

    results: list[ArchiveResult] = []
    for idx, submission in enumerate(submissions, start=1):
        logger.info(
            "[%s/%s] %s id=%s | %s",
            idx,
            len(submissions),
            journal,
            submission.article_id,
            submission.title[:80],
        )
        try:
            result = archive_submission(
                auth,
                journal,
                submission,
                sender_names=sender_names,
                dry_run=dry_run,
                verify=verify,
            )
        except Exception as exc:  # noqa: BLE001
            result = ArchiveResult(
                journal=journal,
                article_id=submission.article_id,
                title=submission.title,
                sender="",
                status="error",
                message=str(exc),
            )
            logger.error("Ошибка: %s", exc)
        results.append(result)
        logger.info(
            "  -> %s | sender=%r | %s",
            result.status,
            result.sender,
            result.message,
        )
        if delay > 0:
            time.sleep(delay)
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Архивация рукописей из «Новые», если отправитель совпадает "
            "(без отправки email, кнопка «Пропустить»)."
        )
    )
    parser.add_argument(
        "--journal",
        default=None,
        help="ISSN/путь журнала, например 0002-337X (по умолчанию, если нет --all-ras)",
    )
    parser.add_argument(
        "--all-ras",
        action="store_true",
        help="Обработать все журналы РАН из JSON (--journals)",
    )
    parser.add_argument(
        "--journals",
        type=Path,
        default=DEFAULT_JOURNALS,
        help="JSON со списком журналов РАН (для --all-ras)",
    )
    parser.add_argument(
        "--journal-limit",
        type=int,
        default=None,
        help="Ограничить число журналов при --all-ras",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="При --all-ras пропустить ISSN, уже есть в CSV-отчёте",
    )
    parser.add_argument(
        "--sender",
        action="append",
        default=None,
        help=(
            "ФИО отправителя для фильтрации (можно указать несколько раз). "
            f"По умолчанию: {', '.join(DEFAULT_SENDERS)}"
        ),
    )
    parser.add_argument(
        "--article-id",
        type=int,
        default=None,
        help="Обработать только указанную рукопись (нужен --journal)",
    )
    parser.add_argument(
        "--cookie-file",
        type=Path,
        default=Path(__file__).resolve().parent / "rcsi_cookies.txt",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Куда сохранить отчёт (xlsx)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Ограничить число рукописей на каждый журнал",
    )
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только проверить отправителя и форму, без архивации",
    )
    parser.add_argument("--no-verify", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    sender_names = args.sender if args.sender else list(DEFAULT_SENDERS)
    report_path = (
        args.report.resolve()
        if args.report
        else Path(__file__).resolve().parent
        / f"archive_sender_report{'_dry' if args.dry_run else ''}.xlsx"
    )
    report_csv = report_path.with_suffix(".csv")

    if args.article_id is not None and args.all_ras:
        logger.error("Нельзя одновременно использовать --article-id и --all-ras")
        return 1
    if args.article_id is not None and not args.journal:
        logger.error("Для --article-id укажите --journal")
        return 1

    if args.all_ras:
        journal_list = load_journals(args.journals.resolve())
        if args.journal_limit is not None:
            journal_list = journal_list[: args.journal_limit]
        done = load_done_issns(report_csv) if args.resume else set()
        journals = [
            j["slug"]
            for j in journal_list
            if j["issn"].casefold() not in done and j["slug"].casefold() not in done
        ]
        skipped = len(journal_list) - len(journals)
        logger.info(
            "Журналов РАН: %s (к обработке: %s, resume-пропуск: %s)",
            len(journal_list),
            len(journals),
            skipped,
        )
    else:
        journal = (args.journal or DEFAULT_JOURNAL).strip("/").split("/")[0]
        journals = [journal]

    if not journals:
        logger.error("Нет журналов для обработки")
        return 1

    try:
        auth = ensure_auth(None, args.cookie_file)
        auth.save_cookies(args.cookie_file)
    except Exception as exc:  # noqa: BLE001
        logger.error("Авторизация не удалась: %s", exc)
        return 1

    results: list[ArchiveResult] = []
    if args.resume and report_csv.exists() and args.all_ras:
        with report_csv.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                results.append(
                    ArchiveResult(
                        journal=row.get("journal") or "",
                        article_id=int(row["article_id"])
                        if (row.get("article_id") or "").strip().isdigit()
                        else 0,
                        title=row.get("title") or "",
                        sender=row.get("sender") or "",
                        status=row.get("status") or "",
                        message=row.get("message") or "",
                    )
                )

    for j_idx, journal in enumerate(journals, start=1):
        logger.info("===== Журнал [%s/%s]: %s =====", j_idx, len(journals), journal)
        try:
            journal_results = process_journal(
                auth,
                journal,
                sender_names=sender_names,
                article_id=args.article_id,
                limit=args.limit,
                delay=args.delay,
                dry_run=args.dry_run,
                verify=not args.no_verify,
            )
        except RcsiAuthError as exc:
            logger.error("%s: %s", journal, exc)
            journal_results = [
                ArchiveResult(
                    journal=journal,
                    article_id=0,
                    title="",
                    sender="",
                    status="no_access",
                    message=str(exc),
                )
            ]
        except Exception as exc:  # noqa: BLE001
            logger.error("%s: %s", journal, exc)
            journal_results = [
                ArchiveResult(
                    journal=journal,
                    article_id=0,
                    title="",
                    sender="",
                    status="error",
                    message=str(exc),
                )
            ]

        results.extend(journal_results)
        write_report(report_path, results)

    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    if args.all_ras:
        print(f"Журналов обработано: {len(journals)}")
    else:
        print(f"Журнал: {journals[0]}")
    print(f"Отправители-фильтр: {', '.join(sender_names)}")
    print(f"Всего записей: {len(results)}")
    for status, n in sorted(counts.items()):
        print(f"  {status}: {n}")
    print(f"Отчёт: {report_path}")
    print(f"CSV:   {report_csv}")

    bad = sum(
        1
        for r in results
        if r.status in {"error", "verify_failed", "no_access"}
    )
    return 0 if bad == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
