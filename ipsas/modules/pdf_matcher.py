"""
Улучшенный модуль для сопоставления PDF файлов со статьями в XML.

Ключевые улучшения:
1. Более надёжное извлечение DOI (учёт переносов строк, обрезанных DOI)
2. Улучшенное извлечение названий и авторов из PDF
3. Многоуровневая стратегия матчинга с приоритетами
4. Расширенная диагностика и логирование
5. Автоматическая подстройка порогов на основе данных
6. Поддержка частичных совпадений DOI
"""

import zipfile
import re
import math
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from collections import Counter
from enum import Enum

from lxml import etree
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

try:
    from PyPDF2 import PdfReader
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logger.warning("PyPDF2 не установлен. Извлечение метаданных из PDF будет недоступно.")


class MatchMethod(Enum):
    """Методы сопоставления PDF и статей"""
    EDN_EXACT = "edn_exact"              # Точное совпадение EDN
    DOI_EXACT = "doi_exact"              # Точное совпадение DOI
    DOI_PARTIAL = "doi_partial"          # Частичное совпадение DOI (обрезанный)
    TITLE_HIGH = "title_high"            # Высокое совпадение по названию (>0.85)
    TITLE_AUTHORS = "title_authors"      # Совпадение по названию + авторам
    PAGES_TITLE = "pages_title"          # Совпадение по страницам + названию
    FALLBACK = "fallback"                # Общий fallback
    UNMATCHED = "unmatched"              # Не сопоставлено


@dataclass(frozen=True)
class PDFEntry:
    """PDF файл на диске + исходный путь (arcname) внутри ZIP."""
    path: Path
    arcname: str


@dataclass
class ArticleInfo:
    """Информация о статье из XML"""
    index: int
    element: etree.Element
    article_id: Optional[str]
    num: Optional[str]
    pages: Optional[Tuple[int, int]]
    title_rus: Optional[str]
    title_eng: Optional[str]
    authors_rus: List[str]
    authors_eng: List[str]
    doi: Optional[str]
    edn: Optional[str]  # eLIBRARY Document Number (6 латинских символов)


@dataclass
class PDFMetadata:
    """Метаданные извлечённые из PDF"""
    title: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    doi: Optional[str] = None
    doi_candidates: List[str] = field(default_factory=list)  # Все найденные DOI
    edn: Optional[str] = None  # eLIBRARY Document Number
    text_length: int = 0
    extraction_quality: str = "unknown"  # low, medium, high


@dataclass
class MatchResult:
    """Результат сопоставления"""
    article_index: int
    article_id: Optional[str]
    article_title: str
    pdf_filename: Optional[str]
    score: float
    method: MatchMethod
    doi: Optional[str]
    confidence: str = "low"  # low, medium, high
    details: Dict[str, Any] = field(default_factory=dict)
    pdf_metadata: Optional[Dict[str, Any]] = None  # Для совместимости с шаблоном


class PDFMatcher:
    """Улучшенный класс для сопоставления PDF файлов со статьями в XML."""

    # Динамические пороги (могут подстраиваться)
    MIN_SCORE_HIGH_CONFIDENCE = 0.75    # Высокая уверенность
    MIN_SCORE_MEDIUM_CONFIDENCE = 0.45  # Средняя уверенность
    MIN_SCORE_LOW_CONFIDENCE = 0.25     # Низкая уверенность (требует ручной проверки)
    MARGIN_SCORE_GAP = 0.15             # Зазор между топ-1 и топ-2
    READ_PAGES_FOR_TEXT = 5             # Страниц для извлечения текста
    
    # Веса для комбинированного score
    WEIGHTS = {
        "doi": 1.0,
        "title": 0.60,
        "authors": 0.30,
        "pages": 0.05,
        "filename": 0.05,
    }

    def __init__(self, adaptive_thresholds: bool = True, verbose: bool = True):
        """
        Args:
            adaptive_thresholds: Автоматически подстраивать пороги на основе данных
            verbose: Подробное логирование
        """
        self.adaptive_thresholds = adaptive_thresholds
        self.verbose = verbose
        self.stats = {
            "doi_extractions": 0,
            "doi_extraction_failures": 0,
            "edn_extractions": 0,
            "edn_extraction_failures": 0,
            "title_extractions": 0,
            "title_extraction_failures": 0,
            "author_extractions": 0,
            "author_extraction_failures": 0,
        }

    def extract_zip(self, zip_path: Path, extract_to: Path) -> Dict[str, Any]:
        """Извлечь файлы из ZIP архива, сохранив исходные пути (arcnames)."""
        extract_to.mkdir(parents=True, exist_ok=True)

        xml_path: Optional[Path] = None
        xml_arcname: Optional[str] = None
        pdfs: List[PDFEntry] = []

        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue

                zf.extract(member, extract_to)
                extracted_path = extract_to / member.filename
                if not extracted_path.exists():
                    continue

                suffix = extracted_path.suffix.lower()
                if suffix == ".xml":
                    if xml_path is None:
                        xml_path = extracted_path
                        xml_arcname = member.filename
                    else:
                        logger.warning(f"Найдено несколько XML, используется первый: {xml_path.name}")
                elif suffix == ".pdf":
                    pdfs.append(PDFEntry(path=extracted_path, arcname=member.filename))

        if xml_path is None or xml_arcname is None:
            raise ValueError("В архиве не найден XML файл")

        return {"xml": xml_path, "xml_arcname": xml_arcname, "pdfs": pdfs}

    # ===========================
    # Нормализация и парсинг
    # ===========================

    def parse_article_pages(self, pages_str: str) -> Optional[Tuple[int, int]]:
        """Парсинг диапазона страниц из строки"""
        if not pages_str:
            return None

        s = pages_str.strip().lower()
        s = re.sub(r'^(стр|с|page|p|pages)\.?\s*', '', s, flags=re.IGNORECASE)
        if not s:
            return None

        # Различные форматы диапазонов
        patterns = [
            r'(\d+)\s*[-–—]\s*(\d+)',  # 7-24, 7–24, 7—24
            r'(\d+)\s*\.\.\s*(\d+)',    # 7..24
            r'(\d+)\s*,\s*(\d+)',       # 7,24 (менее вероятно, но возможно)
        ]
        
        for pattern in patterns:
            m = re.search(pattern, s)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                return (a, b) if a <= b else (b, a)

        # Одиночное число
        nums = re.findall(r'\d+', s)
        if nums:
            if len(nums) == 1:
                p = int(nums[0])
                return (p, p)
            # Берём первое и последнее
            a, b = int(nums[0]), int(nums[-1])
            return (a, b) if a <= b else (b, a)

        return None

    def normalize_text(self, text: str) -> str:
        """Нормализация общего текста (для сравнения, НЕ для DOI)"""
        if not text:
            return ""
        t = text.lower()
        t = re.sub(r'[^\w\s]', ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    def normalize_doi(self, doi: str) -> str:
        """
        Улучшенная нормализация DOI.
        Сохраняет структуру DOI, убирает только явно лишнее.
        """
        if not doi:
            return ""
        
        d = doi.strip()
        
        # Убираем префиксы
        d = re.sub(r'^\s*(doi|DOI)[:\s]+', '', d)
        d = re.sub(r'^(https?://)?((dx\.)?doi\.org/|doi\.org/)', '', d)
        
        # Убираем trailing мусор
        d = re.sub(r'[)\]},;\.]+$', '', d)
        
        # Нижний регистр для сравнения
        d = d.lower().strip()
        
        return d

    def normalize_edn(self, edn: str) -> str:
        """
        Нормализация EDN (eLIBRARY Document Number).
        EDN должен содержать 6 латинских символов (буквы и/или цифры).
        """
        if not edn:
            return ""
        
        e = edn.strip().upper()  # EDN обычно в верхнем регистре
        
        # Убираем префиксы
        e = re.sub(r'^\s*(edn|EDN)[:\s]+', '', e, flags=re.IGNORECASE)
        
        # Извлекаем только латинские буквы и цифры (максимум 6 символов)
        e = re.sub(r'[^A-Z0-9]', '', e)
        
        # Проверяем длину (должно быть 6 символов)
        if len(e) == 6:
            return e
        elif len(e) > 6:
            # Если больше 6, берем первые 6
            return e[:6]
        else:
            # Если меньше 6, возвращаем как есть (может быть обрезан)
            return e

    def extract_doi_from_text(self, text: str) -> Tuple[Optional[str], List[str]]:
        """
        Улучшенное извлечение DOI из текста.
        
        Returns:
            (best_doi, all_candidates) - лучший DOI и все найденные кандидаты
        """
        if not text:
            return None, []

        # Убираем переносы строк внутри потенциальных DOI
        text_compact = text.replace("\n", " ").replace("\r", " ")
        text_compact = re.sub(r'\s+', ' ', text_compact)

        # Паттерны для поиска DOI (от специфичных к общим)
        patterns = [
            # С явным указанием "DOI:"
            r'(?:doi|DOI)\s*[:=]\s*(10\.\d{3,9}/[^\s\)\]\}<>",;]+)',
            # С URL
            r'(?:https?://)?(?:dx\.)?doi\.org/(10\.\d{3,9}/[^\s\)\]\}<>",;]+)',
            # Просто DOI
            r'\b(10\.\d{3,9}/[^\s\)\]\}<>",;]+)',
        ]

        all_candidates = []
        seen = set()

        for pattern in patterns:
            matches = re.finditer(pattern, text_compact, flags=re.IGNORECASE)
            for m in matches:
                doi_raw = m.group(1)
                
                # Расширяем DOI, если он обрезан
                # DOI может содержать: буквы, цифры, дефисы, точки, подчеркивания, скобки
                end_pos = m.end()
                if end_pos < len(text_compact):
                    continuation = text_compact[end_pos:end_pos+200]
                    cont_match = re.match(r'[a-zA-Z0-9\-_\.\(\)]+', continuation)
                    if cont_match:
                        doi_full = doi_raw + cont_match.group(0)
                    else:
                        doi_full = doi_raw
                else:
                    doi_full = doi_raw

                # Нормализуем
                doi_normalized = self.normalize_doi(doi_full)
                
                # Проверяем валидность
                if self._is_valid_doi(doi_normalized):
                    if doi_normalized not in seen:
                        all_candidates.append(doi_normalized)
                        seen.add(doi_normalized)

        # Выбираем лучший DOI (самый длинный и полный)
        if all_candidates:
            # Сортируем по длине (более длинный обычно более полный)
            all_candidates.sort(key=len, reverse=True)
            best_doi = all_candidates[0]
            
            # Проверяем, нет ли более "качественного" DOI среди коротких
            # (иногда длинный DOI может включать мусор)
            for doi in all_candidates[1:]:
                # Если короткий DOI является префиксом длинного - используем длинный
                if best_doi.startswith(doi):
                    break
                # Если нашли более структурированный DOI - используем его
                if self._doi_quality_score(doi) > self._doi_quality_score(best_doi):
                    best_doi = doi
                    break
            
            return best_doi, all_candidates
        
        return None, []

    def extract_edn_from_text(self, text: str) -> Optional[str]:
        """
        Извлечение EDN из текста.
        EDN - это 6 латинских символов (буквы и/или цифры).
        
        Returns:
            Нормализованный EDN или None
        """
        if not text:
            return None

        # Убираем переносы строк
        text_compact = text.replace("\n", " ").replace("\r", " ")
        text_compact = re.sub(r'\s+', ' ', text_compact)

        # Паттерны для поиска EDN (от специфичных к общим)
        patterns = [
            # С явным указанием "EDN:" или "EDN="
            r'(?:edn|EDN)\s*[:=]\s*([A-Z0-9]{6})',
            # 6 латинских символов после слова "EDN"
            r'\b(?:edn|EDN)\s+([A-Z0-9]{6})\b',
            # Просто 6 латинских символов (может быть ложное срабатывание)
            # Используем только если есть контекст (например, рядом есть "elibrary" или "document")
            r'\b([A-Z0-9]{6})\b(?=.*(?:elibrary|document|номер|number))',
        ]

        candidates = []

        for pattern in patterns:
            matches = re.finditer(pattern, text_compact, flags=re.IGNORECASE)
            for m in matches:
                edn_raw = m.group(1).upper()
                # Проверяем, что это действительно EDN (6 символов, латинские буквы/цифры)
                if len(edn_raw) == 6 and re.match(r'^[A-Z0-9]{6}$', edn_raw):
                    edn_normalized = self.normalize_edn(edn_raw)
                    if edn_normalized and len(edn_normalized) == 6:
                        candidates.append(edn_normalized)

        # Убираем дубликаты
        unique_candidates = list(dict.fromkeys(candidates))

        if unique_candidates:
            # Если несколько кандидатов, выбираем первый (обычно EDN уникален)
            return unique_candidates[0]

        return None

    def _is_valid_doi(self, doi: str) -> bool:
        """Проверка валидности DOI"""
        if not doi:
            return False
        
        # Минимальная длина DOI
        if len(doi) < 10:
            return False
        
        # Должен начинаться с "10."
        if not doi.startswith("10."):
            return False
        
        # Должен содержать "/"
        if "/" not in doi:
            return False
        
        # Проверяем структуру: 10.XXXX/suffix
        parts = doi.split("/", 1)
        if len(parts) != 2:
            return False
        
        prefix, suffix = parts
        
        # Префикс: 10.XXXX где XXXX - цифры (обычно 3-9 цифр)
        if not re.match(r'10\.\d{3,9}$', prefix):
            return False
        
        # Суффикс не должен быть пустым
        if not suffix or len(suffix) < 2:
            return False
        
        return True

    def _doi_quality_score(self, doi: str) -> float:
        """
        Оценка качества DOI (больше = лучше).
        Учитывает структуру и отсутствие подозрительных символов.
        """
        if not doi:
            return 0.0
        
        score = 0.0
        
        # Базовая валидация
        if self._is_valid_doi(doi):
            score += 1.0
        else:
            return 0.0
        
        # Длина (оптимальная 15-50 символов)
        length = len(doi)
        if 15 <= length <= 50:
            score += 1.0
        elif length < 15:
            score += 0.5  # Может быть обрезан
        
        # Отсутствие подозрительных последовательностей
        suspicious_patterns = [
            r'\.\.+',      # Двойные точки
            r'--+',        # Двойные дефисы
            r'//',         # Двойные слеши
            r'[\(\)\[\]]', # Скобки (редко в DOI)
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, doi):
                score -= 0.2
        
        # Наличие типичных окончаний (увеличивает уверенность)
        if re.search(r'[a-zA-Z0-9]{5,}$', doi):
            score += 0.5
        
        return max(0.0, score)

    def _extract_title_from_text(self, text: str, max_attempts: int = 3) -> Optional[str]:
        """
        Улучшенное извлечение названия статьи из текста PDF.
        Использует несколько стратегий.
        """
        if not text:
            return None

        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if not lines:
            return None

        # Стратегия 1: Пропуск служебной информации
        title1 = self._extract_title_strategy1(lines)
        
        # Стратегия 2: Поиск по структуре (между заголовком журнала и abstract)
        title2 = self._extract_title_strategy2(text)
        
        # Стратегия 3: Поиск самой длинной строки в начале (после служебной информации)
        title3 = self._extract_title_strategy3(lines)
        
        # Выбираем лучший результат
        candidates = [t for t in [title1, title2, title3] if t]
        
        if not candidates:
            return None
        
        # Оцениваем качество каждого кандидата
        scored_candidates = []
        for title in candidates:
            score = self._title_quality_score(title)
            scored_candidates.append((score, title))
        
        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        
        best_title = scored_candidates[0][1]
        
        if self.verbose:
            logger.debug(f"    Извлечено название: '{best_title[:80]}...' (score={scored_candidates[0][0]:.2f})")
            if len(scored_candidates) > 1:
                logger.debug(f"    Альтернативы: {[(t[:40], f'{s:.2f}') for s, t in scored_candidates[1:3]]}")
        
        return best_title

    def _extract_title_strategy1(self, lines: List[str]) -> Optional[str]:
        """Стратегия 1: Пропуск служебной информации"""
        skip_keywords = [
            'труды', 'proceedings', 'учебных', 'заведений', 'связи',
            'telecommunication', 'известия', 'вестник', 'журнал',
            'journal', 'bulletin', 'университет', 'university',
            'институт', 'institute', 'издательство', 'publisher',
            'issn', 'eissn', 'том', 'volume', 'выпуск', 'issue',
        ]
        
        stop_keywords = [
            'abstract', 'аннотация', 'keywords', 'ключевые слова',
            'doi', 'introduction', 'введение', 'резюме', 'summary'
        ]
        
        title_lines = []
        skip_count = 0
        
        for i, line in enumerate(lines[:60]):
            line_lower = line.lower()
            
            # Стоп-слова
            if any(kw in line_lower for kw in stop_keywords):
                break
            
            # Пропускаем служебные строки
            if i < 15 and any(kw in line_lower for kw in skip_keywords):
                skip_count += 1
                continue
            
            # Пропускаем короткие, email, URL
            if len(line) < 10 or '@' in line or 'http' in line_lower:
                continue
            
            # Пропускаем ЗАГЛАВНЫЕ строки (название журнала)
            if line.isupper() and len(line) > 30 and skip_count < 10:
                skip_count += 1
                continue
            
            # Если пропустили достаточно - начинаем собирать название
            if skip_count >= 3 or i >= 12:
                title_lines.append(line)
                if len(' '.join(title_lines)) > 60:
                    break
        
        if title_lines:
            title = ' '.join(title_lines).strip()
            title = re.sub(r'\s+', ' ', title)
            title = re.sub(r'[.,;:]+$', '', title).strip()
            
            if 15 <= len(title) <= 500:
                return title
        
        return None

    def _extract_title_strategy2(self, text: str) -> Optional[str]:
        """Стратегия 2: Поиск между маркерами"""
        # Ищем текст между концом header-секции и началом abstract
        # Header обычно содержит название журнала, том, выпуск
        # После идёт название статьи
        # Затем авторы
        # Затем abstract/keywords
        
        # Разбиваем на блоки по двойным переносам
        blocks = re.split(r'\n\s*\n', text[:3000])  # Первые 3000 символов
        
        for i, block in enumerate(blocks):
            block_lower = block.lower()
            
            # Пропускаем блоки с служебными словами
            if any(kw in block_lower for kw in ['issn', 'volume', 'journal', 'proceedings']):
                continue
            
            # Пропускаем блоки с abstract/keywords
            if any(kw in block_lower for kw in ['abstract', 'keywords', 'аннотация']):
                break
            
            # Пропускаем блоки с авторами (содержат инициалы)
            if re.search(r'\b[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]\.', block):
                continue
            
            # Если блок достаточно длинный и похож на название
            if 30 <= len(block) <= 500:
                # Очищаем
                title = ' '.join(block.split())
                title = re.sub(r'[.,;:]+$', '', title).strip()
                
                # Проверяем качество
                if self._title_quality_score(title) > 0.5:
                    return title
        
        return None

    def _extract_title_strategy3(self, lines: List[str]) -> Optional[str]:
        """Стратегия 3: Самая длинная строка в начале"""
        # После пропуска первых 5-15 строк, ищем самую длинную строку
        candidates = []
        
        for i, line in enumerate(lines[5:40], start=5):
            line_lower = line.lower()
            
            # Пропускаем служебные
            if any(kw in line_lower for kw in ['issn', 'journal', 'abstract', '@', 'http']):
                continue
            
            # Пропускаем ЗАГЛАВНЫЕ (журнал)
            if line.isupper() and len(line) > 30:
                continue
            
            if 30 <= len(line) <= 400:
                candidates.append(line)
        
        if candidates:
            # Сортируем по длине
            candidates.sort(key=len, reverse=True)
            title = candidates[0].strip()
            title = re.sub(r'[.,;:]+$', '', title).strip()
            return title
        
        return None

    def _title_quality_score(self, title: str) -> float:
        """Оценка качества извлечённого названия"""
        if not title:
            return 0.0
        
        score = 0.0
        
        # Длина (оптимальная 30-250 символов)
        length = len(title)
        if 30 <= length <= 250:
            score += 1.0
        elif 20 <= length < 30 or 250 < length <= 400:
            score += 0.5
        else:
            score += 0.2
        
        # Количество слов (оптимально 5-30)
        word_count = len([w for w in title.split() if len(w) > 2])
        if 5 <= word_count <= 30:
            score += 1.0
        elif 3 <= word_count < 5 or 30 < word_count <= 50:
            score += 0.5
        
        # Наличие строчных букв (не только ЗАГЛАВНЫЕ)
        if re.search(r'[a-zа-я]', title):
            score += 0.5
        
        # Отсутствие служебных слов
        bad_keywords = ['issn', 'journal', 'proceedings', 'volume', 'issue', '@']
        if not any(kw in title.lower() for kw in bad_keywords):
            score += 0.5
        
        # Начинается с заглавной буквы
        if title[0].isupper():
            score += 0.3
        
        # Не содержит email/url
        if '@' not in title and 'http' not in title.lower():
            score += 0.3
        
        return score

    def _extract_authors_from_text(self, text: str) -> List[str]:
        """
        Улучшенное извлечение авторов из текста PDF.
        Использует несколько паттернов и стратегий.
        """
        if not text:
            return []

        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if not lines:
            return []

        authors_found = []
        
        # Паттерны для распознавания авторов
        # Формат: Фамилия И.О. или Фамилия И. О.
        pattern1 = r'([А-ЯЁA-Z][а-яёa-z]+)\s+([А-ЯЁA-Z]\.)\s*([А-ЯЁA-Z]\.)?'
        # Формат: Фамилия, И.О.
        pattern2 = r'([А-ЯЁA-Z][а-яёa-z]+),\s*([А-ЯЁA-Z]\.)\s*([А-ЯЁA-Z]\.)?'
        # Формат: Фамилия Имя Отчество (полное)
        pattern3 = r'([А-ЯЁA-Z][а-яёa-z]+)\s+([А-ЯЁA-Z][а-яёa-z]+)\s+([А-ЯЁA-Z][а-яёa-z]+)'
        
        skip_keywords = ['труды', 'proceedings', 'journal', 'issn', 'university', 'bmv', 'bmw']
        stop_keywords = ['abstract', 'аннотация', 'keywords', 'ключевые слова', 'doi']
        
        in_author_section = False
        title_passed = False
        
        for i, line in enumerate(lines[:50]):
            line_lower = line.lower()
            
            # Стоп-слова
            if any(kw in line_lower for kw in stop_keywords):
                break
            
            # Пропускаем служебные строки
            if any(kw in line_lower for kw in skip_keywords):
                continue
            
            # Детектируем, что прошли название (длинная строка без инициалов)
            if len(line) > 50 and not re.search(r'\b[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]\.', line):
                title_passed = True
                continue
            
            # После названия ищем авторов
            if title_passed or i > 10:
                in_author_section = True
            
            if in_author_section:
                # Пробуем все паттерны
                for pattern in [pattern1, pattern2, pattern3]:
                    matches = re.finditer(pattern, line)
                    for match in matches:
                        author = match.group(0).strip()
                        # Фильтруем мусор
                        if len(author) >= 5 and author.lower() not in ['bmv', 'bmw']:
                            # Проверяем, что это не аббревиатура
                            if not (author.isupper() and len(author) <= 8):
                                authors_found.append(author)
                
                # Если нашли авторов в этой строке, проверяем следующую
                if authors_found and i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    # Если следующая строка тоже содержит авторов - добавляем
                    if len(next_line) > 5 and re.search(pattern1, next_line):
                        continue
                    else:
                        # Иначе завершаем поиск
                        break
        
        # Убираем дубликаты, сохраняя порядок
        unique_authors = []
        seen = set()
        for author in authors_found[:15]:  # Максимум 15 авторов
            author_normalized = self.normalize_text(author)
            if author_normalized not in seen:
                seen.add(author_normalized)
                unique_authors.append(author)
        
        return unique_authors

    def _trigrams(self, s: str) -> Set[str]:
        """Создать набор триграмм из строки"""
        s = re.sub(r'\s+', ' ', s.strip())
        if len(s) < 3:
            return {s} if s else set()
        return {s[i:i+3] for i in range(len(s) - 2)}

    def _cosine_similarity(self, vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
        """Косинусное сходство между двумя векторами"""
        common = set(vec1.keys()) & set(vec2.keys())
        if not common:
            return 0.0
        
        dot_product = sum(vec1[w] * vec2[w] for w in common)
        
        norm1 = math.sqrt(sum(v * v for v in vec1.values()))
        norm2 = math.sqrt(sum(v * v for v in vec2.values()))
        
        if norm1 == 0.0 or norm2 == 0.0:
            return 0.0
        
        return dot_product / (norm1 * norm2)

    def _text_to_vector(self, text: str) -> Dict[str, float]:
        """Преобразовать текст в TF вектор"""
        if not text:
            return {}
        
        text_norm = self.normalize_text(text)
        if not text_norm:
            return {}
        
        words = [w for w in text_norm.split() if len(w) > 2]
        if not words:
            return {}
        
        word_counts = Counter(words)
        total = len(words)
        
        return {word: count / total for word, count in word_counts.items()}

    def calculate_title_similarity(self, title1: str, title2: str) -> float:
        """
        Улучшенное вычисление схожести названий.
        Комбинирует несколько метрик.
        """
        if not title1 or not title2:
            return 0.0
        
        t1_norm = self.normalize_text(title1)
        t2_norm = self.normalize_text(title2)
        
        if not t1_norm or not t2_norm:
            return 0.0
        
        # Точное совпадение
        if t1_norm == t2_norm:
            return 1.0
        
        # 1. Косинусное сходство (TF vectors)
        vec1 = self._text_to_vector(t1_norm)
        vec2 = self._text_to_vector(t2_norm)
        cosine_sim = self._cosine_similarity(vec1, vec2) if (vec1 and vec2) else 0.0
        
        # 2. Jaccard по токенам (слова длиннее 3 символов)
        tokens1 = {w for w in t1_norm.split() if len(w) > 3}
        tokens2 = {w for w in t2_norm.split() if len(w) > 3}
        token_jaccard = 0.0
        if tokens1 and tokens2:
            token_jaccard = len(tokens1 & tokens2) / len(tokens1 | tokens2)
        
        # 3. Jaccard по триграммам
        tri1 = self._trigrams(t1_norm)
        tri2 = self._trigrams(t2_norm)
        tri_jaccard = 0.0
        if tri1 and tri2:
            tri_jaccard = len(tri1 & tri2) / len(tri1 | tri2)
        
        # 4. Longest Common Subsequence (нормализованная)
        lcs_sim = self._lcs_similarity(t1_norm, t2_norm)
        
        # Комбинированный score с весами
        # Косинусное сходство - основной показатель
        # Token Jaccard - важен для ключевых слов
        # Trigram Jaccard - учитывает порядок символов
        # LCS - учитывает порядок слов
        combined = (
            0.40 * cosine_sim +
            0.30 * token_jaccard +
            0.15 * tri_jaccard +
            0.15 * lcs_sim
        )
        
        return max(0.0, min(1.0, combined))

    def _lcs_similarity(self, s1: str, s2: str) -> float:
        """Longest Common Subsequence similarity"""
        # Для строк разбиваем на слова (быстрее, чем по символам)
        words1 = s1.split()
        words2 = s2.split()
        
        if not words1 or not words2:
            return 0.0
        
        # Динамическое программирование для LCS
        m, n = len(words1), len(words2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if words1[i-1] == words2[j-1]:
                    dp[i][j] = dp[i-1][j-1] + 1
                else:
                    dp[i][j] = max(dp[i-1][j], dp[i][j-1])
        
        lcs_length = dp[m][n]
        
        # Нормализуем по длине более короткой последовательности
        max_possible = min(m, n)
        
        return lcs_length / max_possible if max_possible > 0 else 0.0

    def _norm_surname(self, s: str) -> str:
        """Нормализация фамилии"""
        if not s:
            return ""
        s = s.strip().lower()
        s = s.replace("ё", "е")
        s = re.sub(r"[^a-zа-я\-']", "", s)  # Оставляем апостроф для иностранных фамилий
        return s

    def compare_authors(self, pdf_authors: List[str], xml_surnames: List[str]) -> float:
        """
        Улучшенное сравнение авторов.
        Учитывает различные форматы записи.
        """
        if not pdf_authors or not xml_surnames:
            return 0.0

        # Извлекаем фамилии из PDF
        pdf_surnames = []
        for author in pdf_authors:
            if not author:
                continue
            # Фамилия - первое слово
            parts = re.split(r"[,\s]+", author.strip())
            if parts and len(parts[0]) > 2:
                pdf_surnames.append(self._norm_surname(parts[0]))

        xml_surn_norm = [self._norm_surname(s) for s in xml_surnames if s]
        pdf_surn_norm = [s for s in pdf_surnames if s]

        if not xml_surn_norm or not pdf_surn_norm:
            return 0.0

        xml_set = set(xml_surn_norm)
        pdf_set = set(pdf_surn_norm)

        # 1. Точные совпадения
        exact_matches = len(xml_set & pdf_set)
        total_unique = len(xml_set | pdf_set)
        
        if total_unique == 0:
            return 0.0
        
        exact_score = exact_matches / max(len(xml_set), len(pdf_set))

        # 2. Частичные совпадения (префиксы для учёта транслитерации)
        partial_matches = 0
        for pdf_s in pdf_set:
            if pdf_s in xml_set:
                continue  # Уже учтено в exact
            if len(pdf_s) >= 5:
                for xml_s in xml_set:
                    if xml_s in pdf_set:
                        continue
                    if len(xml_s) >= 5:
                        # Совпадение первых 5 символов
                        if pdf_s[:5] == xml_s[:5]:
                            partial_matches += 0.5
                            break

        partial_score = partial_matches / max(len(xml_set), len(pdf_set))

        # 3. Косинусное сходство на основе наборов фамилий
        all_surnames = list(xml_set | pdf_set)
        if all_surnames:
            vec_xml = {s: 1.0 if s in xml_set else 0.0 for s in all_surnames}
            vec_pdf = {s: 1.0 if s in pdf_set else 0.0 for s in all_surnames}
            cosine_sim = self._cosine_similarity(vec_xml, vec_pdf)
        else:
            cosine_sim = 0.0

        # Комбинированный score
        combined = 0.5 * exact_score + 0.3 * cosine_sim + 0.2 * partial_score

        return min(1.0, combined)

    # ===========================
    # Извлечение данных из XML
    # ===========================

    def get_article_info(self, article_elem: etree.Element, index: int) -> ArticleInfo:
        """Извлечь информацию о статье из XML элемента"""
        # Страницы
        pages = None
        pages_elem = article_elem.find("./pages")
        if pages_elem is not None and pages_elem.text:
            pages = self.parse_article_pages(pages_elem.text)

        # Названия
        title_rus = None
        title_eng = None
        for t in article_elem.findall(".//artTitles/artTitle"):
            lang = (t.get("lang") or "").upper()
            text = "".join(t.itertext()).strip() if t is not None else ""
            if not text:
                continue
            if lang == "RUS" and title_rus is None:
                title_rus = text
            elif lang == "ENG" and title_eng is None:
                title_eng = text

        # Авторы (фамилии)
        authors_rus: List[str] = []
        authors_eng: List[str] = []

        for author in article_elem.findall(".//authors/author"):
            for ind in author.findall("./individInfo"):
                lang = (ind.get("lang") or "").upper()
                s_el = ind.find("./surname")
                if s_el is None or not (s_el.text or "").strip():
                    continue
                surname = s_el.text.strip()
                if lang == "RUS":
                    authors_rus.append(surname)
                elif lang == "ENG":
                    authors_eng.append(surname)

        # DOI строго из codes/doi
        doi = None
        doi_el = article_elem.find(".//codes/doi")
        if doi_el is not None and doi_el.text and doi_el.text.strip():
            doi = self.normalize_doi(doi_el.text)

        # EDN строго из codes/edn
        edn = None
        edn_el = article_elem.find(".//codes/edn")
        if edn_el is not None and edn_el.text and edn_el.text.strip():
            edn = self.normalize_edn(edn_el.text)

        return ArticleInfo(
            index=index,
            element=article_elem,
            article_id=article_elem.get("id"),
            num=article_elem.get("num"),
            pages=pages,
            title_rus=title_rus,
            title_eng=title_eng,
            authors_rus=authors_rus,
            authors_eng=authors_eng,
            doi=doi,
            edn=edn,
        )

    # ===========================
    # Извлечение метаданных PDF
    # ===========================

    def extract_pdf_metadata(self, pdf_path: Path) -> PDFMetadata:
        """
        Извлечь метаданные из PDF с улучшенной обработкой.
        """
        meta = PDFMetadata()

        if not PDF_SUPPORT:
            meta.extraction_quality = "no_support"
            return meta

        try:
            with open(pdf_path, "rb") as f:
                reader = PdfReader(f)

                # Метаданные документа
                doc_meta = reader.metadata
                if doc_meta:
                    # Title
                    title_meta = doc_meta.get("/Title") or doc_meta.get("Title")
                    if title_meta and str(title_meta).strip():
                        title_str = str(title_meta).strip()
                        # Проверяем качество
                        if self._title_quality_score(title_str) > 0.5:
                            meta.title = title_str
                            self.stats["title_extractions"] += 1

                    # Authors
                    author_meta = doc_meta.get("/Author") or doc_meta.get("Author")
                    if author_meta and str(author_meta).strip():
                        author_str = str(author_meta)
                        parts = re.split(r"[,;]", author_str)
                        authors_list = []
                        for p in parts:
                            p = p.strip()
                            # Фильтруем мусор
                            if p and len(p) > 3 and p.lower() not in ['bmv', 'bmw']:
                                if not (p.isupper() and len(p) <= 5):
                                    authors_list.append(p)
                        if authors_list:
                            meta.authors = authors_list
                            self.stats["author_extractions"] += 1

                # Извлекаем текст
                text_pages = []
                max_pages = min(self.READ_PAGES_FOR_TEXT, len(reader.pages))
                
                for i in range(max_pages):
                    try:
                        page_text = reader.pages[i].extract_text()
                        if page_text:
                            text_pages.append(page_text)
                    except Exception as e:
                        logger.debug(f"Ошибка извлечения текста со страницы {i}: {e}")
                        continue

                full_text = "\n".join(text_pages)
                meta.text_length = len(full_text)

                if full_text:
                    # DOI
                    doi, doi_candidates = self.extract_doi_from_text(full_text)
                    if doi:
                        meta.doi = doi
                        meta.doi_candidates = doi_candidates
                        self.stats["doi_extractions"] += 1
                    else:
                        self.stats["doi_extraction_failures"] += 1

                    # EDN
                    edn = self.extract_edn_from_text(full_text)
                    if edn:
                        meta.edn = edn
                        self.stats["edn_extractions"] += 1
                    else:
                        self.stats["edn_extraction_failures"] += 1

                    # Title (если не было в метаданных или низкого качества)
                    if not meta.title:
                        title = self._extract_title_from_text(full_text)
                        if title:
                            meta.title = title
                            self.stats["title_extractions"] += 1
                        else:
                            self.stats["title_extraction_failures"] += 1

                    # Authors (если не было в метаданных)
                    if not meta.authors:
                        authors = self._extract_authors_from_text(full_text)
                        if authors:
                            meta.authors = authors
                            self.stats["author_extractions"] += 1
                        else:
                            self.stats["author_extraction_failures"] += 1

                # Оценка качества извлечения
                quality_score = 0
                if meta.doi:
                    quality_score += 3
                if meta.edn:
                    quality_score += 3  # EDN также высоко ценится
                if meta.title:
                    quality_score += 2
                if meta.authors:
                    quality_score += 1

                if quality_score >= 5:
                    meta.extraction_quality = "high"
                elif quality_score >= 3:
                    meta.extraction_quality = "medium"
                else:
                    meta.extraction_quality = "low"

        except Exception as e:
            logger.error(f"Ошибка чтения PDF {pdf_path.name}: {e}", exc_info=True)
            meta.extraction_quality = "error"

        return meta

    # ===========================
    # Очистка и запись XML
    # ===========================

    def cleanup_pdf_files_in_articles(self, root: etree.Element) -> int:
        """Удалить все <file desc="PDF"> внутри <article>/<files>."""
        removed = 0
        for article in root.findall(".//article"):
            files = article.find("./files")
            if files is None:
                continue

            to_remove = []
            for fe in files.findall("./file"):
                if (fe.get("desc") or "").strip().lower() == "pdf":
                    to_remove.append(fe)

            for fe in to_remove:
                files.remove(fe)
                removed += 1

            if len(files) == 0:
                article.remove(files)

        return removed

    def set_pdf_file_in_article(self, article_elem: etree.Element, pdf_filename: str) -> None:
        """Установить/заменить <files>/<file desc="PDF">."""
        files = article_elem.find("./files")
        if files is None:
            files = etree.SubElement(article_elem, "files")

        # Replace if exists
        for fe in files.findall("./file"):
            if (fe.get("desc") or "").strip().lower() == "pdf":
                fe.text = pdf_filename
                return

        # Create new
        fe = etree.SubElement(files, "file")
        fe.set("desc", "PDF")
        fe.text = pdf_filename

    # ===========================
    # Матчинг - Многоуровневая стратегия
    # ===========================

    def _calculate_combined_score(
        self,
        pdf_meta: PDFMetadata,
        article: ArticleInfo,
        pdf_name: str
    ) -> Tuple[float, Dict[str, float]]:
        """
        Вычислить комбинированный score для PDF и статьи.
        
        Returns:
            (total_score, component_scores)
        """
        components = {
            "title": 0.0,
            "authors": 0.0,
            "pages": 0.0,
            "filename": 0.0,
        }

        # 1. Title similarity
        if pdf_meta.title:
            title_scores = []
            if article.title_rus:
                title_scores.append(self.calculate_title_similarity(pdf_meta.title, article.title_rus))
            if article.title_eng:
                title_scores.append(self.calculate_title_similarity(pdf_meta.title, article.title_eng))
            
            if title_scores:
                components["title"] = max(title_scores)

        # 2. Authors similarity
        if pdf_meta.authors:
            author_scores = []
            if article.authors_rus:
                author_scores.append(self.compare_authors(pdf_meta.authors, article.authors_rus))
            if article.authors_eng:
                author_scores.append(self.compare_authors(pdf_meta.authors, article.authors_eng))
            
            if author_scores:
                components["authors"] = max(author_scores)

        # 3. Pages match (from filename)
        if article.pages:
            start, end = article.pages
            pages_pattern = f"{start}[-–—]{end}"
            if pages_pattern in pdf_name.lower():
                components["pages"] = 1.0
            else:
                # Частичное совпадение (только start или end)
                if f"{start}" in pdf_name or f"{end}" in pdf_name:
                    components["pages"] = 0.5

        # 4. Filename similarity (по ключевым словам из title)
        pdf_name_base = Path(pdf_name).stem.lower()
        article_title = article.title_rus or article.title_eng or ""
        
        if article_title:
            title_words = set(re.findall(r'\b[а-яёa-z]{4,}\b', article_title.lower()))
            filename_words = set(re.findall(r'\b[а-яёa-z]{4,}\b', pdf_name_base))
            
            if title_words and filename_words:
                common = len(title_words & filename_words)
                total = len(title_words | filename_words)
                if total > 0:
                    components["filename"] = common / total

        # Вычисляем взвешенный total score
        total_score = (
            self.WEIGHTS["title"] * components["title"] +
            self.WEIGHTS["authors"] * components["authors"] +
            self.WEIGHTS["pages"] * components["pages"] +
            self.WEIGHTS["filename"] * components["filename"]
        )

        return total_score, components

    def _match_by_edn(
        self,
        pdf_entries: List[PDFEntry],
        articles_info: List[ArticleInfo],
        pdf_metadata: Dict[Path, PDFMetadata]
    ) -> Tuple[List[MatchResult], Set[int], Set[Path]]:
        """
        Phase 0: Сопоставление по EDN (eLIBRARY Document Number).
        EDN имеет приоритет над DOI, так как это более специфичный идентификатор.
        
        Returns:
            (matches, matched_articles, matched_pdfs)
        """
        matches = []
        matched_articles = set()
        matched_pdfs = set()

        # Создаём индекс EDN -> статьи
        edn_index: Dict[str, List[ArticleInfo]] = {}
        for art in articles_info:
            if art.edn:
                edn_index.setdefault(art.edn, []).append(art)

        logger.info("=" * 80)
        logger.info("Phase 0: Сопоставление по EDN")
        logger.info("=" * 80)

        for pe in pdf_entries:
            meta = pdf_metadata.get(pe.path)
            if not meta or not meta.edn:
                continue

            pdf_edn = meta.edn
            
            # Точное совпадение EDN
            if pdf_edn in edn_index:
                articles = edn_index[pdf_edn]
                
                if len(articles) == 1:
                    art = articles[0]
                    
                    if art.index not in matched_articles and pe.path not in matched_pdfs:
                        self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                        matched_articles.add(art.index)
                        matched_pdfs.add(pe.path)
                        
                        match = MatchResult(
                            article_index=art.index,
                            article_id=art.article_id,
                            article_title=art.title_rus or art.title_eng or "Без названия",
                            pdf_filename=Path(pe.arcname).name,
                            score=1.0,
                            method=MatchMethod.EDN_EXACT,
                            doi=art.doi,
                            confidence="high",
                            details={"edn_match": "exact", "edn": pdf_edn},
                            pdf_metadata=self._pdf_metadata_to_dict(meta)
                        )
                        matches.append(match)
                        
                        logger.info(f"✓ EDN exact match: article#{art.index+1} <-> {pe.arcname} (EDN: {pdf_edn})")
                else:
                    logger.warning(f"⚠ EDN {pdf_edn} найден в {len(articles)} статьях, пропускаем")

        logger.info(f"Phase 0 завершена: {len(matched_articles)} сопоставлений по EDN")
        
        return matches, matched_articles, matched_pdfs

    def _match_by_doi(
        self,
        pdf_entries: List[PDFEntry],
        articles_info: List[ArticleInfo],
        pdf_metadata: Dict[Path, PDFMetadata],
        matched_articles: Set[int],
        matched_pdfs: Set[Path]
    ) -> Tuple[List[MatchResult], Set[int], Set[Path]]:
        """
        Phase 1: Сопоставление по DOI (точное и частичное).
        
        Returns:
            (matches, matched_articles, matched_pdfs)
        """
        matches = []
        matched_articles = set()
        matched_pdfs = set()

        # Создаём индекс DOI -> статьи
        doi_index: Dict[str, List[ArticleInfo]] = {}
        for art in articles_info:
            if art.doi:
                doi_index.setdefault(art.doi, []).append(art)

        logger.info("=" * 80)
        logger.info("Phase 1: Сопоставление по DOI")
        logger.info("=" * 80)

        for pe in pdf_entries:
            # Пропускаем уже сопоставленные PDF
            if pe.path in matched_pdfs:
                continue
            
            meta = pdf_metadata.get(pe.path)
            if not meta or not meta.doi:
                continue

            pdf_doi = meta.doi
            
            # 1. Точное совпадение
            if pdf_doi in doi_index:
                articles = doi_index[pdf_doi]
                
                if len(articles) == 1:
                    art = articles[0]
                    
                    if art.index not in matched_articles and pe.path not in matched_pdfs:
                        self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                        matched_articles.add(art.index)
                        matched_pdfs.add(pe.path)
                        
                        match = MatchResult(
                            article_index=art.index,
                            article_id=art.article_id,
                            article_title=art.title_rus or art.title_eng or "Без названия",
                            pdf_filename=Path(pe.arcname).name,
                            score=1.0,
                            method=MatchMethod.DOI_EXACT,
                            doi=pdf_doi,
                            confidence="high",
                            details={"doi_match": "exact"},
                            pdf_metadata=self._pdf_metadata_to_dict(meta)
                        )
                        matches.append(match)
                        
                        logger.info(f"✓ DOI exact match: article#{art.index+1} <-> {pe.arcname}")
                else:
                    logger.warning(f"⚠ DOI {pdf_doi} найден в {len(articles)} статьях, пропускаем")
            
            # 2. Частичное совпадение (PDF DOI - префикс XML DOI)
            else:
                partial_match_found = False
                
                for xml_doi, articles in doi_index.items():
                    # Проверяем, является ли PDF DOI префиксом XML DOI
                    if xml_doi.startswith(pdf_doi) and len(xml_doi) > len(pdf_doi):
                        # Разница должна быть разумной (не более 50% длины)
                        if len(xml_doi) - len(pdf_doi) <= len(pdf_doi) * 0.5:
                            if len(articles) == 1:
                                art = articles[0]
                                
                                if art.index not in matched_articles and pe.path not in matched_pdfs:
                                    self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                                    matched_articles.add(art.index)
                                    matched_pdfs.add(pe.path)
                                    
                                    match = MatchResult(
                                        article_index=art.index,
                                        article_id=art.article_id,
                                        article_title=art.title_rus or art.title_eng or "Без названия",
                                        pdf_filename=Path(pe.arcname).name,
                                        score=0.95,
                                        method=MatchMethod.DOI_PARTIAL,
                                        doi=xml_doi,
                                        confidence="high",
                                        details={
                                            "doi_match": "partial",
                                            "pdf_doi": pdf_doi,
                                            "xml_doi": xml_doi
                                        },
                                        pdf_metadata=self._pdf_metadata_to_dict(meta)
                                    )
                                    matches.append(match)
                                    
                                    logger.info(f"~ DOI partial match: article#{art.index+1} <-> {pe.arcname}")
                                    logger.info(f"  PDF DOI: {pdf_doi}")
                                    logger.info(f"  XML DOI: {xml_doi}")
                                    
                                    partial_match_found = True
                                    break

                if not partial_match_found:
                    # Проверяем обратное: XML DOI - префикс PDF DOI (PDF более полный)
                    for xml_doi, articles in doi_index.items():
                        if pdf_doi.startswith(xml_doi) and len(pdf_doi) > len(xml_doi):
                            if len(pdf_doi) - len(xml_doi) <= len(xml_doi) * 0.5:
                                if len(articles) == 1:
                                    art = articles[0]
                                    
                                    if art.index not in matched_articles and pe.path not in matched_pdfs:
                                        self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                                        matched_articles.add(art.index)
                                        matched_pdfs.add(pe.path)
                                        
                                        match = MatchResult(
                                            article_index=art.index,
                                            article_id=art.article_id,
                                            article_title=art.title_rus or art.title_eng or "Без названия",
                                            pdf_filename=Path(pe.arcname).name,
                                            score=0.95,
                                            method=MatchMethod.DOI_PARTIAL,
                                            doi=pdf_doi,
                                            confidence="high",
                                            details={
                                                "doi_match": "partial_reverse",
                                                "pdf_doi": pdf_doi,
                                                "xml_doi": xml_doi
                                            },
                                            pdf_metadata=self._pdf_metadata_to_dict(meta)
                                        )
                                        matches.append(match)
                                        
                                        logger.info(f"~ DOI partial match (reverse): article#{art.index+1} <-> {pe.arcname}")
                                        logger.info(f"  PDF DOI: {pdf_doi}")
                                        logger.info(f"  XML DOI: {xml_doi}")
                                        
                                        partial_match_found = True
                                        break

        logger.info(f"Phase 1 завершена: {len(matched_articles)} сопоставлений по DOI")
        
        return matches, matched_articles, matched_pdfs

    def _match_fallback(
        self,
        pdf_entries: List[PDFEntry],
        articles_info: List[ArticleInfo],
        pdf_metadata: Dict[Path, PDFMetadata],
        matched_articles: Set[int],
        matched_pdfs: Set[Path]
    ) -> List[MatchResult]:
        """
        Phase 2: Fallback сопоставление (title + authors + pages).
        Использует многоуровневую стратегию с адаптивными порогами.
        """
        matches = []
        
        # Несопоставленные кандидаты
        remaining_articles = [a for a in articles_info if a.index not in matched_articles]
        remaining_pdfs = [pe for pe in pdf_entries if pe.path not in matched_pdfs]
        
        if not remaining_articles or not remaining_pdfs:
            logger.info("Phase 2: нет кандидатов для fallback")
            return matches

        logger.info("=" * 80)
        logger.info(f"Phase 2: Fallback сопоставление")
        logger.info(f"  Статей: {len(remaining_articles)}, PDF: {len(remaining_pdfs)}")
        logger.info("=" * 80)

        # Вычисляем scores для всех пар
        scored_pairs: List[Tuple[float, ArticleInfo, PDFEntry, Dict[str, float]]] = []
        
        for art in remaining_articles:
            for pe in remaining_pdfs:
                meta = pdf_metadata.get(pe.path, PDFMetadata())
                
                total_score, components = self._calculate_combined_score(
                    meta, art, Path(pe.arcname).name
                )
                
                if total_score > 0:
                    scored_pairs.append((total_score, art, pe, components))
        
        if not scored_pairs:
            logger.warning("Phase 2: не найдено совпадений (все scores = 0)")
            return matches

        # Сортируем по убыванию score
        scored_pairs.sort(key=lambda x: x[0], reverse=True)

        # Статистика для адаптивной подстройки порогов
        all_scores = [s[0] for s in scored_pairs]
        if self.adaptive_thresholds and all_scores:
            self._adjust_thresholds(all_scores)

        # Группируем по статьям для проверки неоднозначности
        by_article: Dict[int, List[Tuple[float, PDFEntry, Dict[str, float]]]] = {}
        for score, art, pe, comps in scored_pairs:
            by_article.setdefault(art.index, []).append((score, pe, comps))

        # Определяем неоднозначные статьи (margin rule)
        ambiguous_articles = set()
        for art_idx, candidates in by_article.items():
            if len(candidates) >= 2:
                candidates_sorted = sorted(candidates, key=lambda x: x[0], reverse=True)
                top1_score = candidates_sorted[0][0]
                top2_score = candidates_sorted[1][0]
                
                if (top1_score - top2_score) < self.MARGIN_SCORE_GAP:
                    ambiguous_articles.add(art_idx)
                    
                    if self.verbose:
                        logger.info(f"  Статья #{art_idx+1}: неоднозначность")
                        logger.info(f"    Top-1: {candidates_sorted[0][1].arcname} (score={top1_score:.3f})")
                        logger.info(f"    Top-2: {candidates_sorted[1][1].arcname} (score={top2_score:.3f})")
                        logger.info(f"    Gap: {top1_score - top2_score:.3f} < {self.MARGIN_SCORE_GAP}")

        # Greedy assignment с учётом уровней уверенности
        local_matched_articles = set()
        local_matched_pdfs = set()

        # Уровень 1: Высокая уверенность (не неоднозначные)
        for score, art, pe, components in scored_pairs:
            if art.index in local_matched_articles or pe.path in local_matched_pdfs:
                continue
            
            if art.index in ambiguous_articles:
                continue
            
            if score >= self.MIN_SCORE_HIGH_CONFIDENCE:
                meta = pdf_metadata.get(pe.path, PDFMetadata())
                self._assign_match(
                    art, pe, score, components,
                    matched_articles, matched_pdfs,
                    local_matched_articles, local_matched_pdfs,
                    matches, confidence="high",
                    pdf_meta=meta
                )

        # Уровень 2: Средняя уверенность
        for score, art, pe, components in scored_pairs:
            if art.index in local_matched_articles or pe.path in local_matched_pdfs:
                continue
            
            if art.index in ambiguous_articles:
                continue
            
            if self.MIN_SCORE_MEDIUM_CONFIDENCE <= score < self.MIN_SCORE_HIGH_CONFIDENCE:
                meta = pdf_metadata.get(pe.path, PDFMetadata())
                self._assign_match(
                    art, pe, score, components,
                    matched_articles, matched_pdfs,
                    local_matched_articles, local_matched_pdfs,
                    matches, confidence="medium",
                    pdf_meta=meta
                )

        # Уровень 3: Низкая уверенность (только если единственный кандидат)
        for art_idx in ambiguous_articles:
            if art_idx in local_matched_articles:
                continue
            
            candidates = by_article[art_idx]
            # Фильтруем доступные PDF
            available = [c for c in candidates if c[1].path not in local_matched_pdfs]
            
            if len(available) == 1:
                score, pe, components = available[0]
                art = next(a for a in articles_info if a.index == art_idx)
                meta = pdf_metadata.get(pe.path, PDFMetadata())
                
                if score >= self.MIN_SCORE_LOW_CONFIDENCE:
                    self._assign_match(
                        art, pe, score, components,
                        matched_articles, matched_pdfs,
                        local_matched_articles, local_matched_pdfs,
                        matches, confidence="low",
                        pdf_meta=meta
                    )

        logger.info(f"Phase 2 завершена: {len(matches)} новых сопоставлений")
        
        return matches

    def _adjust_thresholds(self, scores: List[float]) -> None:
        """Адаптивная подстройка порогов на основе распределения scores"""
        if not scores:
            return
        
        scores_sorted = sorted(scores, reverse=True)
        n = len(scores_sorted)
        
        # Используем перцентили для определения порогов
        p75 = scores_sorted[int(n * 0.25)] if n > 3 else scores_sorted[0]
        p50 = scores_sorted[int(n * 0.50)] if n > 1 else scores_sorted[0]
        p25 = scores_sorted[int(n * 0.75)] if n > 3 else scores_sorted[-1]
        
        # Подстраиваем пороги (с ограничениями)
        new_high = max(0.65, min(0.85, p75))
        new_medium = max(0.35, min(0.65, p50))
        new_low = max(0.15, min(0.45, p25))
        
        # Обновляем только если изменения значительны
        if abs(new_high - self.MIN_SCORE_HIGH_CONFIDENCE) > 0.05:
            logger.info(f"Адаптивная подстройка порога high: {self.MIN_SCORE_HIGH_CONFIDENCE:.2f} -> {new_high:.2f}")
            self.MIN_SCORE_HIGH_CONFIDENCE = new_high
        
        if abs(new_medium - self.MIN_SCORE_MEDIUM_CONFIDENCE) > 0.05:
            logger.info(f"Адаптивная подстройка порога medium: {self.MIN_SCORE_MEDIUM_CONFIDENCE:.2f} -> {new_medium:.2f}")
            self.MIN_SCORE_MEDIUM_CONFIDENCE = new_medium

    def _assign_match(
        self,
        art: ArticleInfo,
        pe: PDFEntry,
        score: float,
        components: Dict[str, float],
        matched_articles: Set[int],
        matched_pdfs: Set[Path],
        local_matched_articles: Set[int],
        local_matched_pdfs: Set[Path],
        matches: List[MatchResult],
        confidence: str,
        pdf_meta: PDFMetadata
    ) -> None:
        """Вспомогательный метод для регистрации сопоставления"""
        self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
        
        matched_articles.add(art.index)
        matched_pdfs.add(pe.path)
        local_matched_articles.add(art.index)
        local_matched_pdfs.add(pe.path)
        
        # Определяем метод на основе компонентов
        method = self._determine_match_method(components, score)
        
        match = MatchResult(
            article_index=art.index,
            article_id=art.article_id,
            article_title=art.title_rus or art.title_eng or "Без названия",
            pdf_filename=Path(pe.arcname).name,
            score=score,
            method=method,
            doi=art.doi,
            confidence=confidence,
            details=components,
            pdf_metadata=self._pdf_metadata_to_dict(pdf_meta)
        )
        matches.append(match)
        
        if self.verbose:
            logger.info(f"✓ Match ({confidence}): article#{art.index+1} <-> {pe.arcname}")
            logger.info(f"  Score: {score:.3f}, Components: {components}")

    def _determine_match_method(self, components: Dict[str, float], total_score: float) -> MatchMethod:
        """Определить метод сопоставления на основе компонентов"""
        title_score = components.get("title", 0.0)
        authors_score = components.get("authors", 0.0)
        pages_score = components.get("pages", 0.0)
        
        # Приоритеты
        if title_score > 0.85:
            return MatchMethod.TITLE_HIGH
        elif title_score > 0.5 and authors_score > 0.5:
            return MatchMethod.TITLE_AUTHORS
        elif pages_score > 0.8 and title_score > 0.4:
            return MatchMethod.PAGES_TITLE
        else:
            return MatchMethod.FALLBACK

    def _pdf_metadata_to_dict(self, meta: PDFMetadata) -> Dict[str, Any]:
        """Преобразовать PDFMetadata в словарь для шаблона"""
        return {
            "title": meta.title,
            "authors": meta.authors,
            "doi": meta.doi,
            "doi_candidates": meta.doi_candidates,
            "edn": meta.edn,
            "extraction_quality": meta.extraction_quality,
        }

    # ===========================
    # Главный процесс
    # ===========================

    def process_zip(self, zip_path: Path, extract_to: Path, cleanup_old: bool = True) -> Dict[str, Any]:
        """
        Главный метод обработки ZIP архива.
        
        Args:
            zip_path: Путь к ZIP архиву
            extract_to: Директория для извлечения
            cleanup_old: Удалять старые <file desc="PDF"> перед обработкой
        
        Returns:
            Словарь с результатами обработки
        """
        # Извлечение
        extracted = self.extract_zip(zip_path, extract_to)
        xml_path: Path = extracted["xml"]
        xml_arcname: str = extracted["xml_arcname"]
        pdf_entries: List[PDFEntry] = extracted["pdfs"]

        if not pdf_entries:
            raise ValueError("В архиве не найдены PDF файлы")

        # Парсинг XML
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()

        articles = root.findall(".//article")
        if not articles:
            raise ValueError("В XML не найдены статьи")

        # Cleanup
        removed = 0
        if cleanup_old:
            removed = self.cleanup_pdf_files_in_articles(root)
            if removed:
                logger.info(f"Очистка: удалено {removed} старых <file desc='PDF'>")

        # Сбор информации о статьях
        articles_info = [self.get_article_info(a, idx) for idx, a in enumerate(articles)]

        # Извлечение метаданных из PDF
        logger.info(f"Найдено статей: {len(articles_info)}, PDF: {len(pdf_entries)}")
        logger.info("=" * 80)
        logger.info("Извлечение метаданных из PDF")
        logger.info("=" * 80)
        
        pdf_metadata: Dict[Path, PDFMetadata] = {}
        for pe in sorted(pdf_entries, key=lambda x: x.arcname.lower()):
            logger.info(f"PDF: {pe.arcname}")
            meta = self.extract_pdf_metadata(pe.path)
            pdf_metadata[pe.path] = meta
            
            logger.info(f"  DOI: {meta.doi or 'не найдено'}")
            if meta.doi_candidates and len(meta.doi_candidates) > 1:
                logger.info(f"  DOI кандидаты: {meta.doi_candidates}")
            logger.info(f"  EDN: {meta.edn or 'не найдено'}")
            logger.info(f"  Title: {meta.title[:80] if meta.title else 'не найдено'}...")
            logger.info(f"  Authors: {meta.authors or 'не найдены'}")
            logger.info(f"  Quality: {meta.extraction_quality}")

        # Сопоставление - Phase 0: EDN (приоритет над DOI)
        matches_edn, matched_articles, matched_pdfs = self._match_by_edn(
            pdf_entries, articles_info, pdf_metadata
        )

        # Сопоставление - Phase 1: DOI
        matches_doi, matched_articles, matched_pdfs = self._match_by_doi(
            pdf_entries, articles_info, pdf_metadata,
            matched_articles, matched_pdfs
        )

        # Сопоставление - Phase 2: Fallback
        matches_fallback = self._match_fallback(
            pdf_entries, articles_info, pdf_metadata,
            matched_articles, matched_pdfs
        )

        # Объединяем результаты (EDN имеет приоритет)
        all_matches = matches_edn + matches_doi + matches_fallback

        # Добавляем несопоставленные статьи
        for art in articles_info:
            if art.index not in matched_articles:
                match = MatchResult(
                    article_index=art.index,
                    article_id=art.article_id,
                    article_title=art.title_rus or art.title_eng or "Без названия",
                    pdf_filename=None,
                    score=0.0,
                    method=MatchMethod.UNMATCHED,
                    doi=art.doi,
                    confidence="none",
                    details={"reason": "no_suitable_pdf_found"},
                    pdf_metadata=None
                )
                all_matches.append(match)
                
                logger.warning(f"⚠ Статья #{art.index+1} не сопоставлена")

        # Сортируем по индексу статьи
        all_matches.sort(key=lambda x: x.article_index)

        # Сохраняем XML
        tree.write(str(xml_path), encoding="UTF-8", xml_declaration=True, pretty_print=True)

        # Создаём копию для скачивания
        output_xml = extract_to / f"{zip_path.stem}_processed.xml"
        shutil.copy2(xml_path, output_xml)

        # Формируем результат
        result = {
            "success": True,
            "xml_path": xml_path,
            "output_xml": output_xml,
            "xml_arcname": xml_arcname,
            "matches": [self._match_result_to_dict(m) for m in all_matches],
            "total_articles": len(articles_info),
            "matched_articles": len(matched_articles),
            "unmatched_articles": len(articles_info) - len(matched_articles),
            "cleanup_removed_pdf_tags": removed,
            "statistics": {
                "doi_extractions": self.stats["doi_extractions"],
                "doi_extraction_failures": self.stats["doi_extraction_failures"],
                "edn_extractions": self.stats["edn_extractions"],
                "edn_extraction_failures": self.stats["edn_extraction_failures"],
                "title_extractions": self.stats["title_extractions"],
                "title_extraction_failures": self.stats["title_extraction_failures"],
                "author_extractions": self.stats["author_extractions"],
                "author_extraction_failures": self.stats["author_extraction_failures"],
            },
            "settings": {
                "min_score_high": self.MIN_SCORE_HIGH_CONFIDENCE,
                "min_score_medium": self.MIN_SCORE_MEDIUM_CONFIDENCE,
                "min_score_low": self.MIN_SCORE_LOW_CONFIDENCE,
                "margin_score_gap": self.MARGIN_SCORE_GAP,
                "read_pages_for_text": self.READ_PAGES_FOR_TEXT,
                "adaptive_thresholds": self.adaptive_thresholds,
            }
        }

        # Итоговая статистика
        logger.info("=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 80)
        logger.info(f"Всего статей: {result['total_articles']}")
        logger.info(f"Сопоставлено: {result['matched_articles']}")
        logger.info(f"Не сопоставлено: {result['unmatched_articles']}")
        logger.info(f"Процент покрытия: {result['matched_articles']/result['total_articles']*100:.1f}%")
        logger.info("")
        logger.info("Распределение по методам:")
        method_counts = Counter(m["method"] for m in result["matches"])
        for method, count in method_counts.most_common():
            logger.info(f"  {method}: {count}")
        logger.info("")
        logger.info("Распределение по уверенности:")
        conf_counts = Counter(m["confidence"] for m in result["matches"] if m["pdf_filename"])
        for conf, count in conf_counts.most_common():
            logger.info(f"  {conf}: {count}")

        return result

    def _match_result_to_dict(self, match: MatchResult) -> Dict[str, Any]:
        """Преобразовать MatchResult в словарь"""
        return {
            "article_index": match.article_index,
            "article_id": match.article_id,
            "article_title": match.article_title,
            "pdf_filename": match.pdf_filename,
            "score": match.score,
            "method": match.method.value,
            "doi": match.doi,
            "confidence": match.confidence,
            "details": match.details,
            "pdf_metadata": match.pdf_metadata,
        }


# ===========================
# Утилиты для использования
# ===========================

def process_archive(
    zip_path: str,
    extract_dir: str = "./extracted",
    adaptive: bool = True,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Удобная функция для обработки одного архива.
    
    Args:
        zip_path: Путь к ZIP архиву
        extract_dir: Директория для извлечения
        adaptive: Использовать адаптивные пороги
        verbose: Подробное логирование
    
    Returns:
        Словарь с результатами
    """
    matcher = PDFMatcher(adaptive_thresholds=adaptive, verbose=verbose)
    
    zip_p = Path(zip_path)
    extract_p = Path(extract_dir) / zip_p.stem
    
    return matcher.process_zip(zip_p, extract_p)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python pdf_matcher.py <path_to_zip>")
        sys.exit(1)
    
    result = process_archive(sys.argv[1])
    
    print("\n" + "=" * 80)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    print(f"Успешно: {result['success']}")
    print(f"Обработано статей: {result['total_articles']}")
    print(f"Сопоставлено: {result['matched_articles']}")
    print(f"Не сопоставлено: {result['unmatched_articles']}")
    print(f"\nОбработанный XML: {result['output_xml']}")
