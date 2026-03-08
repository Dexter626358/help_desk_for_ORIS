"""Build CSV rows for linking article PDFs to a published issue."""

from __future__ import annotations

import csv
import re
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ipsas.modules.issue_metadata_parser import IssueMetadataParser
from ipsas.modules.pdf_matcher import PDFMatcher
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class IssueArticle:
    doi: str
    surnames: List[str]
    url: str


@dataclass
class PdfCandidate:
    filename: str
    doi_candidates: Set[str]


class IssuePdfCsvBuilder:
    """Generate CSV (doi;pdf_filename;lang) from issue URL + ZIP with PDFs."""

    DOI_PATTERN = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+", re.IGNORECASE)
    CYR_PATTERN = re.compile(r"[А-Яа-яЁё]")
    LAT_PATTERN = re.compile(r"[A-Za-z]")

    def __init__(self, max_download_size: int = 0):
        self.parser = IssueMetadataParser(max_download_size=max_download_size)
        self.pdf_matcher = PDFMatcher(adaptive_thresholds=False, verbose=False)

    def build_csv(self, issue_url: str, zip_path: Path, output_csv_path: Path, extract_dir: Path) -> Dict[str, object]:
        """Build CSV and return details for UI."""
        if not zip_path.exists() or not zip_path.is_file():
            raise ValueError("ZIP архив не найден")
        if not zipfile.is_zipfile(zip_path):
            raise ValueError("Загруженный файл не является ZIP архивом")

        parsed = self.parser.parse_issue_url(issue_url)
        articles = self._collect_issue_articles(parsed)
        if not articles:
            raise ValueError("В выпуске не найдены статьи с DOI")

        pdf_candidates = self._extract_pdf_candidates(zip_path, extract_dir)
        if not pdf_candidates:
            raise ValueError("В архиве не найдены PDF файлы")

        matches, unmatched_articles, unused_pdfs = self._match_articles_to_pdfs(articles, pdf_candidates)
        rows = []
        for idx, article in enumerate(articles):
            matched_filename = matches.get(idx, "")
            rows.append([article.doi, ", ".join(article.surnames), matched_filename, "en_US"])

        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f, delimiter=";", lineterminator="\n")
            for row in rows:
                # authors are kept only for UI verification, not for export
                writer.writerow([row[0], row[2], row[3]])

        return {
            "success": True,
            "total_articles_with_doi": len(articles),
            "matched_articles": len(articles) - len(unmatched_articles),
            "unmatched_articles": len(unmatched_articles),
            "total_pdfs": len(pdf_candidates),
            "all_pdf_filenames": sorted({p.filename for p in pdf_candidates}),
            "unused_pdfs": len(unused_pdfs),
            "rows": rows,
            "unmatched_dois": unmatched_articles,
            "unused_pdf_filenames": sorted(unused_pdfs),
            "output_csv": output_csv_path,
        }

    def _collect_issue_articles(self, parsed: Dict[str, object]) -> List[IssueArticle]:
        rows: List[IssueArticle] = []
        articles = parsed.get("articles") or []
        for article in articles:
            identifiers = article.get("identifiers") or {}
            raw_doi = identifiers.get("doi")
            doi = self._normalize_doi(raw_doi)
            if not doi:
                continue

            authors = article.get("authors") or article.get("authors_ru") or article.get("authors_en") or []
            surnames = self._extract_surnames(authors)
            if not surnames:
                surnames = ["unknown"]

            rows.append(
                IssueArticle(
                    doi=doi,
                    surnames=surnames,
                    url=article.get("url") or "",
                )
            )
        return rows

    def _extract_pdf_candidates(self, zip_path: Path, extract_dir: Path) -> List[PdfCandidate]:
        if extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)
        extract_dir.mkdir(parents=True, exist_ok=True)

        candidates: List[PdfCandidate] = []
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                name = member.filename or ""
                if not name.lower().endswith(".pdf"):
                    continue

                safe_name = Path(name).name
                if not safe_name:
                    continue
                out_path = extract_dir / safe_name
                out_path.write_bytes(zf.read(member))

                doi_set: Set[str] = set()
                filename_doi = self._extract_doi_from_text(safe_name)
                if filename_doi:
                    doi_set.add(filename_doi)

                try:
                    meta = self.pdf_matcher.extract_pdf_metadata(out_path)
                    if meta.doi:
                        norm = self._normalize_doi(meta.doi)
                        if norm:
                            doi_set.add(norm)
                    for cand in meta.doi_candidates or []:
                        norm = self._normalize_doi(cand)
                        if norm:
                            doi_set.add(norm)
                except Exception as exc:
                    logger.warning("Не удалось извлечь DOI из PDF %s: %s", safe_name, exc)

                candidates.append(PdfCandidate(filename=safe_name, doi_candidates=doi_set))
        return candidates

    def _match_articles_to_pdfs(
        self,
        articles: List[IssueArticle],
        pdf_candidates: List[PdfCandidate],
    ) -> Tuple[Dict[int, str], List[str], Set[str]]:
        by_doi: Dict[str, List[PdfCandidate]] = {}
        for pdf in pdf_candidates:
            for doi in pdf.doi_candidates:
                by_doi.setdefault(doi, []).append(pdf)

        used_files: Set[str] = set()
        matches: Dict[int, str] = {}
        unmatched_articles: List[str] = []

        for idx, article in enumerate(articles):
            matched = None
            candidates = by_doi.get(article.doi, [])
            candidates = [c for c in candidates if c.filename not in used_files]

            if candidates:
                matched = self._select_best_by_surnames(candidates, article.surnames)
            else:
                remaining = [p for p in pdf_candidates if p.filename not in used_files]
                surname_candidates = self._find_by_surnames(remaining, article.surnames)
                if surname_candidates:
                    matched = surname_candidates[0]

            if matched:
                matches[idx] = matched.filename
                used_files.add(matched.filename)
            else:
                unmatched_articles.append(article.doi)

        unused = {p.filename for p in pdf_candidates if p.filename not in used_files}
        return matches, unmatched_articles, unused

    def _select_best_by_surnames(self, candidates: List[PdfCandidate], surnames: List[str]) -> Optional[PdfCandidate]:
        if len(candidates) == 1:
            return candidates[0]

        best: Optional[PdfCandidate] = None
        best_score = -1
        for cand in candidates:
            score = self._surname_score(cand.filename, surnames)
            if score > best_score:
                best = cand
                best_score = score
        return best

    def _find_by_surnames(self, candidates: List[PdfCandidate], surnames: List[str]) -> List[PdfCandidate]:
        scored: List[Tuple[int, PdfCandidate]] = []
        for cand in candidates:
            score = self._surname_score(cand.filename, surnames)
            if score > 0:
                scored.append((score, cand))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [item[1] for item in scored]

    def _surname_score(self, filename: str, surnames: List[str]) -> int:
        base = self._normalize_for_match(Path(filename).stem)
        score = 0
        for surname in surnames:
            token = self._normalize_for_match(surname)
            if token and token in base:
                score += 1
        return score

    def _extract_surnames(self, authors: List[str]) -> List[str]:
        result: List[str] = []
        seen: Set[str] = set()
        for raw_name in authors:
            if not raw_name:
                continue
            name = re.sub(r"\s+", " ", raw_name).strip()
            if not name:
                continue

            parts = [p for p in re.split(r"[,\s]+", name) if p]
            if not parts:
                continue

            if self.CYR_PATTERN.search(name):
                candidate = parts[0]
            elif self.LAT_PATTERN.search(name):
                candidate = parts[-1] if len(parts) > 1 else parts[0]
            else:
                candidate = parts[0]

            candidate = re.sub(r"[^A-Za-zА-Яа-яЁё-]", "", candidate).strip("-")
            if not candidate:
                continue
            lower = candidate.lower()
            if lower in seen:
                continue
            seen.add(lower)
            result.append(candidate)
        return result

    def _normalize_doi(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        text = value.strip()
        if not text:
            return None
        if text.lower().startswith("https://doi.org/"):
            text = text[16:]
        elif text.lower().startswith("http://doi.org/"):
            text = text[15:]
        text = text.strip().strip(".;,")
        m = self.DOI_PATTERN.search(text)
        if m:
            text = m.group(0)
        return text.lower() if text.lower().startswith("10.") else None

    def _extract_doi_from_text(self, text: str) -> Optional[str]:
        m = self.DOI_PATTERN.search(text or "")
        return self._normalize_doi(m.group(0)) if m else None

    def _normalize_for_match(self, value: str) -> str:
        value = value.lower()
        value = value.replace("ё", "е")
        value = re.sub(r"[^a-zа-я0-9]+", "", value)
        return value
