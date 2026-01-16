"""
Модуль для сопоставления PDF файлов со статьями в XML и корректной записи имени PDF в <article>/<files>.

Ключевые улучшения по сравнению с исходной версией:
- DOI статьи берётся строго из .//codes/doi (а не из .//doi, чтобы не цеплять литературу/прочие блоки).
- DOI сравнивается через отдельную normalize_doi (без удаления "/" и ".").
- Перед новым сопоставлением удаляются ранее добавленные/ошибочные <file desc="PDF"> (опционально).
- Запись PDF в XML: всегда replace-or-create для <file desc="PDF">, без дублей.
- Двухфазный матчинг:
  1) точное совпадение DOI (гарантия),
  2) fallback по названию + авторам (с margin-отсечением неоднозначных).
- Title similarity улучшено: token-Jaccard + trigram-Jaccard.
- Авторы из XML берутся по <individInfo lang="ENG/RUS"><surname>.
- ZIP перепаковывается с сохранением исходных путей (arcname), если они были в архиве.
"""

import zipfile
import re
import math
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from collections import Counter

from lxml import etree
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

try:
    from PyPDF2 import PdfReader
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logger.warning("PyPDF2 не установлен. Извлечение метаданных из PDF будет недоступно.")


@dataclass(frozen=True)
class PDFEntry:
    """PDF файл на диске + исходный путь (arcname) внутри ZIP."""
    path: Path
    arcname: str


@dataclass
class ArticleInfo:
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


class PDFMatcher:
    """Класс для сопоставления PDF файлов со статьями в XML."""

    # Пороги/настройки по умолчанию
    MIN_SCORE_FALLBACK = 0.20          # минимальный балл для fallback (title+authors) - снижен для лучшего покрытия
    MARGIN_SCORE_GAP = 0.10            # если топ-1 и топ-2 ближе, чем gap — не матчим автоматически
    READ_PAGES_FOR_TEXT = 3            # сколько первых страниц читать для DOI/текста (увеличено для лучшего извлечения)

    def extract_zip(self, zip_path: Path, extract_to: Path) -> Dict[str, Any]:
        """
        Извлечь файлы из ZIP архива, сохранив исходные пути (arcnames).

        Returns:
            {'xml': Path, 'xml_arcname': str, 'pdfs': List[PDFEntry]}
        """
        extract_to.mkdir(parents=True, exist_ok=True)

        xml_path: Optional[Path] = None
        xml_arcname: Optional[str] = None
        pdfs: List[PDFEntry] = []

        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue

                # Извлекаем с сохранением поддиректорий
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

    # ----------------------------
    # Нормализация/парсинг
    # ----------------------------

    def parse_article_pages(self, pages_str: str) -> Optional[Tuple[int, int]]:
        if not pages_str:
            return None

        s = pages_str.strip().lower()
        s = re.sub(r'^(стр|с|page|p|pages)\.?\s*', '', s, flags=re.IGNORECASE)
        if not s:
            return None

        # 7-24, 7–24, 7—24, 7..24
        m = re.search(r'(\d+)\s*[-–—]\s*(\d+)', s)
        if not m:
            m = re.search(r'(\d+)\s*\.\.\s*(\d+)', s)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            return (a, b) if a <= b else (b, a)

        nums = re.findall(r'\d+', s)
        if not nums:
            return None
        if len(nums) == 1:
            p = int(nums[0])
            return (p, p)

        a, b = int(nums[0]), int(nums[-1])
        return (a, b) if a <= b else (b, a)

    def normalize_text(self, text: str) -> str:
        """
        Нормализация общего текста (НЕ для DOI):
        - lower
        - удаление пунктуации
        - схлопывание пробелов
        """
        if not text:
            return ""
        t = text.lower()
        t = re.sub(r'[^\w\s]', ' ', t)         # пунктуацию в пробелы
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    def normalize_doi(self, doi: str) -> str:
        """Нормализация DOI без ломания '/' и '.'."""
        if not doi:
            return ""
        d = doi.strip()
        # Убираем префикс "doi:" или "DOI:" если есть
        d = re.sub(r'^\s*doi[:\s]*', '', d, flags=re.IGNORECASE)
        d = d.strip()
        # Убираем лишние символы в конце (скобки, точки, запятые, точки с запятой)
        d = re.sub(r'[).,;]+$', '', d)
        # Приводим к нижнему регистру для сравнения
        d = d.lower()
        return d

    def extract_doi_from_text(self, text: str) -> Optional[str]:
        if not text:
            return None

        # DOI core pattern: 10.<digits>/<non-space>
        # Убираем переносы внутри DOI: иногда бывает "10.1234/\nabc"
        compact = text.replace("\n", " ")
        compact = re.sub(r'\s+', ' ', compact)

        patterns = [
            r'\bdoi[:\s]*\s*(10\.\d{3,9}/[^\s\)\]\}<>",;]+)',
            r'\b(10\.\d{3,9}/[^\s\)\]\}<>",;]+)',
        ]
        
        best_doi = None
        best_length = 0
        
        for pat in patterns:
            matches = re.finditer(pat, compact, flags=re.IGNORECASE)
            for m in matches:
                doi_raw = m.group(1)
                # Пробуем взять более длинный DOI (может быть обрезан на пробеле или спецсимволе)
                # Ищем продолжение DOI после найденного фрагмента
                end_pos = m.end()
                if end_pos < len(compact):
                    # Берем следующие символы, которые могут быть частью DOI
                    # DOI может содержать: буквы, цифры, дефисы, точки, подчеркивания
                    continuation = compact[end_pos:end_pos+100]  # увеличиваем до 100 символов
                    # Ищем продолжение DOI (до пробела, скобки, запятой и т.д.)
                    continuation_match = re.match(r'[a-zA-Z0-9\-_\.]+', continuation)
                    if continuation_match:
                        doi_full = doi_raw + continuation_match.group(0)
                    else:
                        doi_full = doi_raw
                else:
                    doi_full = doi_raw
                
                doi = self.normalize_doi(doi_full)
                if "/" in doi and doi.startswith("10."):
                    # Проверяем, что DOI достаточно длинный и выбираем самый длинный
                    if len(doi) > best_length:
                        best_doi = doi
                        best_length = len(doi)
        
        return best_doi

    def _extract_title_from_text(self, text: str) -> Optional[str]:
        """
        Извлечь название статьи из текста PDF.
        Ищет название после пропуска служебной информации (название журнала, издательство и т.д.).
        """
        if not text:
            return None
        
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if not lines:
            return None
        
        # Ключевые слова для пропуска (название журнала, издательство)
        skip_keywords = [
            'труды', 'proceedings', 'учебных', 'заведений', 'связи', 'telecommunication',
            'известия', 'вестник', 'журнал', 'journal', 'bulletin',
            'университет', 'university', 'институт', 'institute',
            'издательство', 'publisher', 'issn', 'eissn',
            'том', 'volume', 'выпуск', 'issue', 'номер', 'number',
            'год', 'year', 'страница', 'page', 'стр', 'pp',
            'российская федерация', 'russian federation', 'bmv', 'bmw'
        ]
        
        # Паттерны названий журналов
        journal_patterns = [
            r'труды\s+учебных\s+заведений',
            r'proceedings\s+of\s+telecommunication',
            r'известия\s+вузов',
            r'прикладная\s+химия',
        ]
        
        # Ключевые слова окончания
        stop_keywords = ['abstract', 'аннотация', 'keywords', 'ключевые слова', 'doi', 'introduction', 'введение', 'резюме', 'summary']
        
        skip_count = 0
        title_lines = []
        found_title_start = False
        
        for i, line in enumerate(lines[:50]):  # Проверяем первые 50 строк
            line_lower = line.lower()
            
            # Пропускаем строки с ключевыми словами окончания
            if any(keyword in line_lower for keyword in stop_keywords):
                break
            
            # Пропускаем строки с названием журнала
            if any(re.search(pattern, line_lower) for pattern in journal_patterns):
                skip_count += 1
                continue
            
            # Пропускаем строки с служебной информацией (первые 10 строк)
            if i < 10 and any(keyword in line_lower for keyword in skip_keywords):
                skip_count += 1
                continue
            
            # Пропускаем очень короткие строки (меньше 8 символов)
            if len(line) < 8:
                continue
            
            # Пропускаем строки с email, адресами, DOI
            if '@' in line or 'http' in line_lower or (i < 8 and 'doi' in line_lower):
                continue
            
            # Пропускаем строки только с заглавными буквами (название журнала) - но только если они длинные
            if line.isupper() and len(line) > 30 and skip_count < 8:
                skip_count += 1
                continue
            
            # Пропускаем строки с номерами томов/выпусков
            if re.search(r'\b(том|volume|выпуск|issue|№|no\.?|год|year)\s*\d+', line_lower):
                continue
            
            # Пропускаем строки с адресами
            if re.search(r'(российская\s+федерация|russian\s+federation|г\.|ул\.|пр\.|street|avenue)', line_lower):
                continue
            
            # Пропускаем строки с именами авторов в формате "Имя Ф.О., email@"
            if re.search(r'[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.\s*,?\s*[a-z]+@', line):
                continue
            
            # Пропускаем строки, которые выглядят как авторы (Фамилия И.О.)
            if re.match(r'^[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]?\.?\s*$', line):
                continue
            
            # Если уже пропустили достаточно строк (минимум 3), начинаем собирать название
            if skip_count >= 3 or i >= 10:
                if not found_title_start:
                    found_title_start = True
                title_lines.append(line)
                # Останавливаемся, если собрали достаточно длинное название
                if len(' '.join(title_lines)) > 50:
                    # Проверяем следующую строку - если она тоже похожа на название, добавляем
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        if len(next_line) > 10 and not any(keyword in next_line.lower() for keyword in stop_keywords):
                            if not re.match(r'^[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]?\.?\s*$', next_line):
                                title_lines.append(next_line)
                    break
        
        if title_lines:
            title = ' '.join(title_lines).strip()
            title = re.sub(r'\s+', ' ', title)
            # Убираем точки/запятые в конце, но оставляем если это часть текста
            title = re.sub(r'[.,;]+$', '', title).strip()
            
            # Проверяем, что это не название журнала
            title_lower = title.lower()
            is_journal = any(re.search(pattern, title_lower) for pattern in journal_patterns)
            
            # Проверяем, что название содержит достаточно слов (минимум 3)
            word_count = len([w for w in title.split() if len(w) > 2])
            
            if not is_journal and 15 <= len(title) <= 400 and word_count >= 3:
                return title
        
        return None

    def _extract_authors_from_text(self, text: str) -> List[str]:
        """
        Извлечь авторов из текста PDF.
        Ищет авторов после названия статьи, до Abstract/Keywords.
        """
        if not text:
            return []
        
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if not lines:
            return []
        
        # Ключевые слова для пропуска
        skip_keywords = [
            'труды', 'proceedings', 'учебных', 'заведений', 'связи', 'telecommunication',
            'известия', 'вестник', 'журнал', 'journal', 'issn', 'university', 'institute',
            'bmv', 'bmw', 'российская федерация', 'russian federation', 'publisher', 'издательство'
        ]
        
        stop_keywords = ['abstract', 'аннотация', 'keywords', 'ключевые слова', 'doi', 'introduction', 'введение', 'summary', 'резюме']
        
        authors_found = []
        title_found = False
        
        # Ищем авторов в первых 40 строках
        for i, line in enumerate(lines[:40]):
            line_lower = line.lower()
            
            # Останавливаемся на ключевых словах
            if any(keyword in line_lower for keyword in stop_keywords):
                break
            
            # Пропускаем служебные строки
            if any(keyword in line_lower for keyword in skip_keywords):
                continue
            
            # Пропускаем очень короткие или очень длинные строки
            if len(line) < 5 or len(line) > 250:
                continue
            
            # Пропускаем строки только с заглавными буквами (кроме коротких, которые могут быть инициалами)
            if line.isupper() and len(line) > 30:
                continue
            
            # Пропускаем строки с email, адресами
            if '@' in line or 'http' in line_lower:
                continue
            
            # Пропускаем строки с номерами томов/выпусков
            if re.search(r'\b(том|volume|выпуск|issue|№|no\.?|год|year)\s*\d+', line_lower):
                continue
            
            # Пропускаем строки с адресами
            if re.search(r'(г\.|ул\.|пр\.|street|avenue|российская\s+федерация)', line_lower):
                continue
            
            # Если строка достаточно длинная и не похожа на название (больше 100 символов), это может быть название
            if len(line) > 100 and not title_found:
                title_found = True
                continue
            
            # Проверяем, похоже ли на авторов
            # Паттерн 1: Фамилия И.О. или Фамилия И. О. (русский/английский)
            author_pattern1 = r'[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]?\.?'
            # Паттерн 2: Фамилия Имя Отчество (полное имя)
            author_pattern2 = r'[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z][а-яёa-z]+'
            # Паттерн 3: Фамилия, И.О. (с запятой)
            author_pattern3 = r'[А-ЯЁA-Z][а-яёa-z]+,\s*[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]?\.?'
            
            is_author_line = (
                re.search(author_pattern1, line) or 
                re.search(author_pattern2, line) or 
                re.search(author_pattern3, line)
            )
            
            if is_author_line:
                # Дополнительная проверка: должна содержать строчные буквы
                if re.search(r'[а-яёa-z]', line):
                    # Разделяем по запятым, точкам с запятой, "and", "и"
                    parts = re.split(r'[,;]\s*|(?:\s+и\s+|\s+and\s+)', line, flags=re.IGNORECASE)
                    for part in parts:
                        part = part.strip()
                        # Пропускаем слишком короткие или длинные
                        if 5 <= len(part) <= 120:
                            # Пропускаем аббревиатуры (только заглавные, короткие)
                            if part.isupper() and len(part) <= 8:
                                continue
                            # Пропускаем если это не похоже на имя (нет инициалов или полного имени)
                            if not (re.search(r'[А-ЯЁA-Z]\.', part) or len(part.split()) >= 2):
                                continue
                            # Очищаем от лишних символов, но оставляем точки, дефисы, апострофы
                            part_clean = re.sub(r'[^\w\s\.\-\']', '', part).strip()
                            if part_clean and re.search(r'[а-яёa-z]', part_clean):
                                # Проверяем, что это не служебное значение
                                part_lower = part_clean.lower()
                                if part_lower not in ['bmv', 'bmw'] and len(part_clean) > 5:
                                    authors_found.append(part_clean)
            
            # Если нашли авторов, проверяем следующую строку (может быть продолжение списка)
            if authors_found and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if len(next_line) > 5 and len(next_line) < 200:
                    if re.search(author_pattern1, next_line) or re.search(author_pattern3, next_line):
                        # Добавляем авторов из следующей строки
                        parts = re.split(r'[,;]\s*|(?:\s+и\s+|\s+and\s+)', next_line, flags=re.IGNORECASE)
                        for part in parts:
                            part = part.strip()
                            if 5 <= len(part) <= 120:
                                part_clean = re.sub(r'[^\w\s\.\-\']', '', part).strip()
                                if part_clean and re.search(r'[а-яёa-z]', part_clean):
                                    part_lower = part_clean.lower()
                                    if part_lower not in ['bmv', 'bmw'] and len(part_clean) > 5:
                                        authors_found.append(part_clean)
                break
        
        # Ограничиваем количество авторов и убираем дубликаты
        unique_authors = []
        seen = set()
        for author in authors_found[:10]:
            author_lower = author.lower()
            if author_lower not in seen:
                seen.add(author_lower)
                unique_authors.append(author)
        
        return unique_authors

    def _trigrams(self, s: str) -> set:
        s = re.sub(r'\s+', ' ', s.strip())
        if len(s) < 3:
            return {s} if s else set()
        return {s[i:i+3] for i in range(len(s) - 2)}

    def _cosine_similarity(self, vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
        """
        Вычислить косинусное сходство между двумя векторами.
        
        Args:
            vec1: Первый вектор (словарь слово -> вес)
            vec2: Второй вектор (словарь слово -> вес)
            
        Returns:
            Косинусное сходство (0.0 - 1.0)
        """
        # Находим общие слова
        common_words = set(vec1.keys()) & set(vec2.keys())
        if not common_words:
            return 0.0
        
        # Вычисляем скалярное произведение
        dot_product = sum(vec1[word] * vec2[word] for word in common_words)
        
        # Вычисляем нормы векторов
        norm1 = math.sqrt(sum(v * v for v in vec1.values()))
        norm2 = math.sqrt(sum(v * v for v in vec2.values()))
        
        if norm1 == 0.0 or norm2 == 0.0:
            return 0.0
        
        # Косинусное сходство
        return dot_product / (norm1 * norm2)

    def _text_to_vector(self, text: str, use_tf_idf: bool = False) -> Dict[str, float]:
        """
        Преобразовать текст в вектор слов.
        
        Args:
            text: Исходный текст
            use_tf_idf: Использовать TF-IDF веса (пока просто TF)
            
        Returns:
            Словарь слово -> вес
        """
        if not text:
            return {}
        
        # Нормализуем текст
        text_norm = self.normalize_text(text)
        if not text_norm:
            return {}
        
        # Разбиваем на слова (длиннее 2 символов)
        words = [w for w in text_norm.split() if len(w) > 2]
        if not words:
            return {}
        
        # Подсчитываем частоту (TF)
        word_counts = Counter(words)
        total_words = len(words)
        
        # Создаем вектор (TF веса)
        vector = {}
        for word, count in word_counts.items():
            vector[word] = count / total_words  # TF нормализация
        
        return vector

    def calculate_title_similarity(self, a: str, b: str) -> float:
        """
        Улучшенная похожесть названий с использованием косинусного сходства:
        0.5 * cosine_similarity + 0.3 * token_jaccard + 0.2 * trigram_jaccard
        """
        if not a or not b:
            return 0.0
        a_norm = self.normalize_text(a)
        b_norm = self.normalize_text(b)
        if not a_norm or not b_norm:
            return 0.0
        if a_norm == b_norm:
            return 1.0

        # Косинусное сходство
        vec_a = self._text_to_vector(a_norm)
        vec_b = self._text_to_vector(b_norm)
        cosine_sim = 0.0
        if vec_a and vec_b:
            cosine_sim = self._cosine_similarity(vec_a, vec_b)

        # Токены (Jaccard)
        a_tokens = {w for w in a_norm.split() if len(w) > 3}
        b_tokens = {w for w in b_norm.split() if len(w) > 3}
        token_j = 0.0
        if a_tokens and b_tokens:
            token_j = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)

        # Триграммы (Jaccard)
        a_tri = self._trigrams(a_norm)
        b_tri = self._trigrams(b_norm)
        tri_j = 0.0
        if a_tri and b_tri:
            tri_j = len(a_tri & b_tri) / len(a_tri | b_tri)

        # Комбинированный score
        score = 0.5 * cosine_sim + 0.3 * token_j + 0.2 * tri_j
        return max(0.0, min(score, 1.0))

    def _norm_surname(self, s: str) -> str:
        if not s:
            return ""
        s = s.strip().lower()
        s = s.replace("ё", "е")
        # оставляем буквы/дефис
        s = re.sub(r"[^a-zа-я\-]", "", s)
        return s

    def compare_authors(self, pdf_authors: List[str], xml_surnames: List[str]) -> float:
        """
        Сравнение авторов с использованием косинусного сходства:
        - pdf_authors: список строк (могут быть "Ivanov I.I.")
        - xml_surnames: уже фамилии (ENG или RUS)
        """
        if not pdf_authors or not xml_surnames:
            return 0.0

        # Извлекаем фамилии из PDF
        pdf_surn = []
        for a in pdf_authors:
            if not a:
                continue
            # фамилия = первое слово до пробела/запятой
            parts = re.split(r"[,\s]+", a.strip())
            if parts:
                pdf_surn.append(self._norm_surname(parts[0]))

        xml_surn = [self._norm_surname(x) for x in xml_surnames if x]
        pdf_surn = [x for x in pdf_surn if x]

        if not xml_surn or not pdf_surn:
            return 0.0

        # Метод 1: Точное совпадение множеств
        xml_set = set(xml_surn)
        pdf_set = set(pdf_surn)
        exact = len(xml_set & pdf_set)
        exact_match_score = exact / max(len(xml_set), len(pdf_set))

        # Метод 2: Косинусное сходство на основе векторов фамилий
        # Создаем векторы: каждая фамилия = отдельное измерение
        all_surnames = list(xml_set | pdf_set)
        if not all_surnames:
            return 0.0
        
        # Вектор для XML: 1.0 если фамилия есть, 0.0 если нет
        vec_xml = {surname: 1.0 if surname in xml_set else 0.0 for surname in all_surnames}
        # Вектор для PDF: 1.0 если фамилия есть, 0.0 если нет
        vec_pdf = {surname: 1.0 if surname in pdf_set else 0.0 for surname in all_surnames}
        
        cosine_sim = self._cosine_similarity(vec_xml, vec_pdf)

        # Метод 3: Prefix бонус для частичных совпадений (латиница/опечатки)
        prefix_bonus = 0.0
        if exact_match_score < 1.0:
            matched_prefixes = set()
            for p in pdf_set:
                if len(p) < 5:
                    continue
                for x in xml_set:
                    if len(x) < 5:
                        continue
                    # Проверяем префикс (первые 5 символов)
                    if p[:5] == x[:5] and p != x:
                        prefix_key = (p[:5], x[:5])
                        if prefix_key not in matched_prefixes:
                            prefix_bonus += 0.15
                            matched_prefixes.add(prefix_key)
                        break
            prefix_bonus = min(0.3, prefix_bonus)  # Ограничиваем бонус

        # Комбинированный score: 60% косинусное, 30% точное совпадение, 10% prefix бонус
        combined_score = 0.6 * cosine_sim + 0.3 * exact_match_score + 0.1 * prefix_bonus
        
        return min(1.0, combined_score)

    # ----------------------------
    # Извлечение данных из XML
    # ----------------------------

    def get_article_info(self, article_elem: etree.Element, index: int) -> ArticleInfo:
        # pages
        pages = None
        pages_elem = article_elem.find("./pages")
        if pages_elem is not None and pages_elem.text:
            pages = self.parse_article_pages(pages_elem.text)

        # titles
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

        # authors (surnames)
        authors_rus: List[str] = []
        authors_eng: List[str] = []

        for author in article_elem.findall(".//authors/author"):
            for ind in author.findall("./individInfo"):
                lang = (ind.get("lang") or "").upper()
                s_el = ind.find("./surname")
                if s_el is None or not (s_el.text or "").strip():
                    continue
                s = s_el.text.strip()
                if lang == "RUS":
                    authors_rus.append(s)
                elif lang == "ENG":
                    authors_eng.append(s)

        # DOI строго из codes/doi
        doi = None
        doi_el = article_elem.find(".//codes/doi")
        if doi_el is not None and doi_el.text and doi_el.text.strip():
            doi = self.normalize_doi(doi_el.text)

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
        )

    # ----------------------------
    # Извлечение метаданных PDF
    # ----------------------------

    def extract_pdf_metadata(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Извлечь метаданные из PDF:
        - title (если есть в metadata, иначе из текста)
        - authors (из /Author либо из текста)
        - doi (по тексту первых страниц)
        """
        meta: Dict[str, Any] = {"title": None, "authors": [], "doi": None}

        if not PDF_SUPPORT:
            return meta

        try:
            with open(pdf_path, "rb") as f:
                reader = PdfReader(f)

                # doc metadata
                md = reader.metadata
                title_from_meta = None
                authors_from_meta = []
                
                if md:
                    t = md.get("/Title") or md.get("Title")
                    if t and str(t).strip():
                        title_from_meta = str(t).strip()
                        # Проверяем, что это не служебная информация
                        title_lower = title_from_meta.lower()
                        skip_patterns = [
                            r'труды\s+учебных\s+заведений',
                            r'proceedings\s+of\s+telecommunication',
                            r'известия\s+вузов',
                            r'журнал',
                            r'journal',
                            r'issn',
                            r'volume',
                            r'том'
                        ]
                        is_journal = any(re.search(pattern, title_lower) for pattern in skip_patterns)
                        if not is_journal and len(title_from_meta) > 10:
                            meta["title"] = title_from_meta
                            logger.debug(f"  PDF {pdf_path.name}: Title из метаданных: '{title_from_meta[:60]}...'")

                    a = md.get("/Author") or md.get("Author")
                    if a and str(a).strip():
                        parts = re.split(r"[,;]", str(a))
                        authors_from_meta = [p.strip() for p in parts if p.strip()]
                        # Фильтруем служебные значения (BMV, BMW и т.д.)
                        valid_authors = []
                        for author in authors_from_meta:
                            author_lower = author.lower().strip()
                            # Пропускаем служебные значения
                            if author_lower in ['bmv', 'bmw'] or len(author) < 3:
                                continue
                            # Пропускаем если это не похоже на имя (только заглавные, короткое)
                            if author.isupper() and len(author) <= 5:
                                continue
                            valid_authors.append(author)
                        
                        if valid_authors:
                            meta["authors"] = valid_authors
                            logger.debug(f"  PDF {pdf_path.name}: Authors из метаданных: {valid_authors}")
                        else:
                            logger.debug(f"  PDF {pdf_path.name}: Authors из метаданных отфильтрованы (служебные значения): {authors_from_meta}")

                # text from first pages
                text_pages: List[str] = []
                max_pages = min(self.READ_PAGES_FOR_TEXT, len(reader.pages))
                for i in range(max_pages):
                    try:
                        txt = reader.pages[i].extract_text()
                        if txt:
                            text_pages.append(txt)
                    except Exception:
                        continue
                full_text = "\n".join(text_pages)

                if full_text:
                    logger.debug(f"  PDF {pdf_path.name}: Извлечено {len(full_text)} символов текста из {max_pages} страниц")
                    
                    # Извлекаем DOI
                    doi = self.extract_doi_from_text(full_text)
                    if doi:
                        meta["doi"] = doi
                        logger.info(f"  PDF {pdf_path.name}: DOI извлечен из текста: '{doi}'")
                    else:
                        logger.debug(f"  PDF {pdf_path.name}: DOI не найден в тексте")
                        # Попробуем найти частичные совпадения для диагностики
                        doi_candidates = re.findall(r'10\.\d+/[^\s\)]+', full_text[:2000], re.IGNORECASE)
                        if doi_candidates:
                            logger.debug(f"    Найдены потенциальные DOI фрагменты: {doi_candidates[:3]}")
                    
                    # Извлекаем title из текста, если не найден в метаданных или был служебным
                    if not meta["title"]:
                        title = self._extract_title_from_text(full_text)
                        if title:
                            meta["title"] = title
                            logger.info(f"  PDF {pdf_path.name}: Title извлечен из текста: '{title[:80]}...'")
                        else:
                            logger.debug(f"  PDF {pdf_path.name}: Title не найден в тексте")
                    
                    # Извлекаем authors из текста, если не найдены в метаданных или были служебными
                    if not meta["authors"]:
                        authors = self._extract_authors_from_text(full_text)
                        if authors:
                            meta["authors"] = authors
                            logger.info(f"  PDF {pdf_path.name}: Authors извлечены из текста: {authors}")
                        else:
                            logger.debug(f"  PDF {pdf_path.name}: Authors не найдены в тексте")
                    else:
                        # Даже если авторы есть в метаданных, попробуем улучшить из текста
                        authors_from_text = self._extract_authors_from_text(full_text)
                        if authors_from_text and len(authors_from_text) > len(meta["authors"]):
                            logger.info(f"  PDF {pdf_path.name}: Authors дополнены из текста: {authors_from_text}")
                            meta["authors"] = authors_from_text

        except Exception as e:
            logger.error(f"Ошибка чтения PDF {pdf_path.name}: {e}", exc_info=True)

        return meta

    # ----------------------------
    # Очистка и запись XML
    # ----------------------------

    def cleanup_pdf_files_in_articles(self, root: etree.Element) -> int:
        """
        Удалить все <file desc="PDF"> внутри <article>/<files>.
        Использовать, если предыдущие прогоны внесли неверные привязки.
        """
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

            # если в <files> не осталось детей — удалить <files>
            if len(files) == 0:
                article.remove(files)

        return removed

    def set_pdf_file_in_article(self, article_elem: etree.Element, pdf_filename: str) -> None:
        """
        Установить/заменить <files>/<file desc="PDF">.
        Не создаёт дублей.
        """
        files = article_elem.find("./files")
        if files is None:
            files = etree.SubElement(article_elem, "files")

        # replace if exists
        for fe in files.findall("./file"):
            if (fe.get("desc") or "").strip().lower() == "pdf":
                fe.text = pdf_filename
                return

        fe = etree.SubElement(files, "file")
        fe.set("desc", "PDF")
        fe.text = pdf_filename

    # ----------------------------
    # Матчинг
    # ----------------------------

    def _score_fallback(self, pdf_name: str, pdf_meta: Dict[str, Any], art: ArticleInfo) -> float:
        """
        Fallback score без DOI: по названию + авторам + страницам + имени файла.
        Используется только когда DOI в PDF не найден.
        """
        score = 0.0
        # Динамические веса: если один из компонентов отсутствует, перераспределяем вес
        has_title = bool(pdf_meta.get("title"))
        has_authors = bool(pdf_meta.get("authors"))
        
        if has_title and has_authors:
            weights = {"title": 0.65, "authors": 0.30, "filename": 0.05}
        elif has_title:
            weights = {"title": 0.85, "authors": 0.0, "filename": 0.15}
        elif has_authors:
            weights = {"title": 0.0, "authors": 0.85, "filename": 0.15}
        else:
            # Если нет ни title, ни authors, используем только имя файла
            weights = {"title": 0.0, "authors": 0.0, "filename": 1.0}

        # 2.1 Заголовок: сравниваем с RUS и ENG, берём максимум
        title_sim = 0.0
        title_details = []
        if pdf_meta.get("title"):
            candidates = []
            if art.title_rus:
                candidates.append(("RUS", art.title_rus))
            if art.title_eng:
                candidates.append(("ENG", art.title_eng))
            
            for lang, xml_title in candidates:
                sim = self.calculate_title_similarity(pdf_meta["title"], xml_title)
                title_details.append(f"{lang}:{sim:.3f}")
                if sim > title_sim:
                    title_sim = sim
            logger.info(f"      Title similarity: {title_sim:.3f} ({', '.join(title_details) if title_details else 'нет данных'})")
        else:
            logger.info(f"      Title: не найдено в PDF метаданных")
        
        score += weights["title"] * title_sim

        # 2.2 Авторы: сравниваем с ENG и RUS фамилиями, берём максимум
        author_sim = 0.0
        author_details = []
        if pdf_meta.get("authors"):
            if art.authors_eng:
                a1 = self.compare_authors(pdf_meta["authors"], art.authors_eng)
                author_details.append(f"ENG:{a1:.3f}")
                if a1 > author_sim:
                    author_sim = a1
            if art.authors_rus:
                a2 = self.compare_authors(pdf_meta["authors"], art.authors_rus)
                author_details.append(f"RUS:{a2:.3f}")
                if a2 > author_sim:
                    author_sim = a2
            logger.info(f"      Author similarity: {author_sim:.3f} ({', '.join(author_details) if author_details else 'нет данных'})")
        else:
            logger.info(f"      Authors: не найдены в PDF метаданных")
        
        score += weights["authors"] * author_sim

        # 2.3 Сравнение с именем файла (дополнительный сигнал)
        filename_sim = 0.0
        pdf_name_base = Path(pdf_name).stem.lower()  # без расширения
        
        # Извлекаем ключевые слова из названия статьи
        if art.title_rus or art.title_eng:
            title_for_match = (art.title_rus or art.title_eng).lower()
            # Нормализуем: убираем знаки препинания, оставляем только слова
            title_words = set(re.findall(r'\b[а-яёa-z]{4,}\b', title_for_match))
            filename_words = set(re.findall(r'\b[а-яёa-z]{4,}\b', pdf_name_base))
            
            if title_words and filename_words:
                # Jaccard similarity по словам
                common = len(title_words & filename_words)
                total = len(title_words | filename_words)
                if total > 0:
                    filename_sim = common / total
                    logger.info(f"      Filename similarity: {filename_sim:.3f} (общих слов: {common}/{total})")
        
        score += weights["filename"] * filename_sim

        # 2.4 Контроль страниц (слабый сигнал - бонус)
        pages_hit = 0.0
        if art.pages:
            s, e = art.pages
            pages_str = f"{s}-{e}"
            name_l = pdf_name.lower()
            # Проверяем точный диапазон в имени файла
            if pages_str in name_l or f"{s}–{e}" in name_l or f"{s}—{e}" in name_l:
                pages_hit = 1.0
                logger.info(f"      Pages match: найдено '{pages_str}' в имени файла (+0.05)")
        
        # pages_hit добавляется как небольшой бонус (не входит в основную формулу)
        if pages_hit > 0:
            score = min(1.0, score + 0.05)  # небольшой бонус

        logger.info(f"      ИТОГОВЫЙ SCORE: {score:.3f} = title({title_sim:.3f}*{weights['title']}) + authors({author_sim:.3f}*{weights['authors']}) + filename({filename_sim:.3f}*{weights['filename']})")
        return max(0.0, min(score, 1.0))

    def process_zip(self, zip_path: Path, extract_to: Path) -> Dict[str, Any]:
        """
        Обработать ZIP архив:
        - извлечь,
        - очистить старые неверные <file desc="PDF"> (по умолчанию да),
        - сопоставить PDF со статьями,
        - записать в XML,
        - собрать новый ZIP с сохранением структуры.
        """
        extracted = self.extract_zip(zip_path, extract_to)
        xml_path: Path = extracted["xml"]
        xml_arcname: str = extracted["xml_arcname"]
        pdf_entries: List[PDFEntry] = extracted["pdfs"]

        if not pdf_entries:
            raise ValueError("В архиве не найдены PDF файлы")

        # parse XML
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()

        articles = root.findall(".//article")
        if not articles:
            raise ValueError("В XML не найдены статьи")

        # CLEANUP: убрать старые неверные привязки PDF
        removed = self.cleanup_pdf_files_in_articles(root)
        if removed:
            logger.info(f"Очистка: удалено старых <file desc='PDF'>: {removed}")

        # collect articles info
        articles_info: List[ArticleInfo] = []
        for idx, a in enumerate(articles):
            articles_info.append(self.get_article_info(a, idx))

        # extract pdf metadata
        logger.info(f"Найдено статей: {len(articles_info)}, PDF файлов: {len(pdf_entries)}")
        pdf_meta: Dict[Path, Dict[str, Any]] = {}
        logger.info("=" * 80)
        logger.info("Извлечение метаданных из PDF:")
        logger.info("=" * 80)
        for pe in sorted(pdf_entries, key=lambda x: x.arcname.lower()):
            logger.info(f"PDF: {pe.arcname}")
            meta = self.extract_pdf_metadata(pe.path)
            pdf_meta[pe.path] = meta
            logger.info(f"  Извлечено: DOI='{meta.get('doi')}', Title='{meta.get('title')}', Authors={meta.get('authors')}")
        
        # Логируем информацию о статьях
        logger.info("=" * 80)
        logger.info("Информация о статьях из XML:")
        logger.info("=" * 80)
        for art in articles_info:
            title_display = art.title_rus or art.title_eng or "Без названия"
            if len(title_display) > 60:
                title_display = title_display[:60] + "..."
            logger.info(f"  Статья #{art.index+1}: DOI='{art.doi}', Title='{title_display}', Authors_RUS={art.authors_rus}, Authors_ENG={art.authors_eng}")

        # -----------------------
        # Phase 1: Жёсткая проверка по DOI
        # -----------------------
        matches: List[Dict[str, Any]] = []
        matched_articles = set()
        matched_pdfs = set()
        rejected_pdfs = set()  # PDF с DOI, которые не совпали

        # build map doi->article indices (DOI в выпуске должен быть уникален)
        doi_to_article = {}
        articles_with_doi = 0
        for art in articles_info:
            if art.doi:
                doi_to_article.setdefault(art.doi, []).append(art)
                articles_with_doi += 1
        
        logger.info(f"Статей с DOI в XML: {articles_with_doi} из {len(articles_info)}")
        pdfs_with_doi = sum(1 for m in pdf_meta.values() if m.get('doi'))
        logger.info(f"PDF файлов с DOI: {pdfs_with_doi} из {len(pdf_entries)}")

        # Шаг 1: Жёсткая проверка по DOI
        logger.info("=" * 80)
        logger.info("Phase 1: Проверка по DOI")
        logger.info("=" * 80)
        
        for pe in pdf_entries:
            m = pdf_meta.get(pe.path, {})
            pdf_doi_raw = m.get("doi")
            if not pdf_doi_raw:
                logger.debug(f"  PDF {pe.arcname}: DOI не найден в PDF - будет использован fallback")
                continue  # DOI не найден в PDF - переходим к fallback
            
            pdf_doi = self.normalize_doi(pdf_doi_raw)
            logger.debug(f"  PDF {pe.arcname}: DOI из PDF (нормализован) = '{pdf_doi}'")
            
            # Ищем совпадение по DOI
            if pdf_doi in doi_to_article:
                if len(doi_to_article[pdf_doi]) == 1:
                    art = doi_to_article[pdf_doi][0]
                    if art.index in matched_articles or pe.path in matched_pdfs:
                        continue
                    # MATCH (подтверждено)
                    self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                    matched_articles.add(art.index)
                    matched_pdfs.add(pe.path)
                    matches.append({
                        "article_index": art.index,
                        "article_id": art.article_id,
                        "article_title": art.title_rus or art.title_eng or "Без названия",
                        "pdf_filename": Path(pe.arcname).name,
                        "score": 1.0,
                        "method": "doi_exact",
                        "doi": pdf_doi,
                    })
                    logger.info(f"✓ DOI match (подтверждено): article#{art.index+1} <-> {pe.arcname} (DOI: {pdf_doi})")
                else:
                    logger.warning(f"⚠ DOI {pdf_doi} найден в нескольких статьях, пропускаем")
            else:
                # NOT MATCH: DOI в PDF найден, но не совпадает с XML
                # Проверяем, может быть это частичное совпадение (DOI в PDF обрезан)
                partial_match = False
                matched_xml_doi = None
                for xml_doi in doi_to_article.keys():
                    # Если DOI из PDF является началом DOI из XML - это частичное совпадение
                    if xml_doi.startswith(pdf_doi) and len(xml_doi) > len(pdf_doi):
                        partial_match = True
                        matched_xml_doi = xml_doi
                        logger.info(f"  ~ Частичное совпадение DOI: PDF {pe.arcname} имеет '{pdf_doi}', XML имеет '{xml_doi}' (начинается с PDF DOI)")
                        # Используем fallback вместо полного отвержения
                        break
                
                if not partial_match:
                    # Полное несовпадение - отвергаем только если DOI в PDF достаточно длинный
                    # Если DOI очень короткий (например, "10.31854/181"), возможно он обрезан
                    if len(pdf_doi) < 15:  # очень короткий DOI - вероятно обрезан
                        logger.info(f"  → PDF {pe.arcname} имеет короткий DOI '{pdf_doi}' (возможно обрезан) - будет использован fallback")
                        # Не отвергаем, используем fallback
                    else:
                        # Длинный DOI, но не совпадает - отвергаем
                        rejected_pdfs.add(pe.path)
                        logger.warning(f"✗ DOI не совпадает: PDF {pe.arcname} имеет DOI '{pdf_doi}', но такой DOI не найден в XML - отвергнуто")
                        # Логируем доступные DOI из XML для отладки
                        if doi_to_article:
                            available_dois = list(doi_to_article.keys())[:5]  # первые 5 для примера
                            logger.debug(f"    Доступные DOI в XML (примеры): {available_dois}")
                else:
                    logger.info(f"  → PDF {pe.arcname} будет использован в fallback (частичное совпадение DOI)")
        
        logger.info(f"Phase 1 завершена: сопоставлено {len(matched_articles)} статей по DOI, отвергнуто {len(rejected_pdfs)} PDF")

        # -----------------------------------------
        # Phase 2: Fallback (title+authors), margin
        # -----------------------------------------
        # Шаг 2: Фоллбек-проверка (только когда DOI в PDF не найден)
        # candidates: только несопоставленные статьи и PDF без DOI (или с DOI, но не отвергнутые)
        remaining_articles = [a for a in articles_info if a.index not in matched_articles]
        # Исключаем PDF, которые были отвергнуты по DOI
        remaining_pdfs = [pe for pe in pdf_entries if pe.path not in matched_pdfs and pe.path not in rejected_pdfs]
        
        logger.info(f"Fallback матчинг: {len(remaining_articles)} статей, {len(remaining_pdfs)} PDF (исключены {len(rejected_pdfs)} PDF с несовпадающим DOI)")

        # score matrix as list of pairs
        scored_pairs = []
        all_scores = []  # для диагностики
        # Сохраняем все scores для каждой статьи (даже ниже порога) для диагностики
        article_scores_map: Dict[int, List[Tuple[float, str]]] = {}  # article_index -> [(score, pdf_name), ...]
        
        logger.info("=" * 80)
        logger.info(f"Phase 2: Fallback матчинг ({len(remaining_articles)} статей, {len(remaining_pdfs)} PDF)")
        logger.info("=" * 80)
        
        if not remaining_articles or not remaining_pdfs:
            logger.warning(f"⚠ Нет кандидатов для fallback: статей={len(remaining_articles)}, PDF={len(remaining_pdfs)}")
        
        for art in remaining_articles:
            article_scores_map[art.index] = []
            art_title = art.title_rus or art.title_eng or "Без названия"
            if len(art_title) > 50:
                art_title_display = art_title[:50] + "..."
            else:
                art_title_display = art_title
            logger.info(f"Обработка статьи #{art.index+1}: '{art_title_display}'")
            logger.info(f"  XML данные: Title_RUS='{art.title_rus[:60] if art.title_rus and len(art.title_rus) > 60 else art.title_rus}...', Title_ENG='{art.title_eng[:60] if art.title_eng and len(art.title_eng) > 60 else art.title_eng}...', Authors_RUS={art.authors_rus}, Authors_ENG={art.authors_eng}")
            
            for pe in remaining_pdfs:
                pdf_meta_for_file = pdf_meta.get(pe.path, {})
                pdf_title = pdf_meta_for_file.get('title', 'Не найдено')
                pdf_authors = pdf_meta_for_file.get('authors', [])
                logger.info(f"  Сравнение с PDF {pe.arcname}:")
                logger.info(f"    PDF Title='{pdf_title[:60] if pdf_title and len(pdf_title) > 60 else pdf_title}', Authors={pdf_authors}")
                
                sc = self._score_fallback(Path(pe.arcname).name, pdf_meta_for_file, art)
                all_scores.append(sc)
                # Сохраняем все scores для диагностики
                article_scores_map[art.index].append((sc, pe.arcname))
                
                if sc >= self.MIN_SCORE_FALLBACK:
                    scored_pairs.append((sc, art, pe))
                    logger.info(f"    ✓✓✓ КАНДИДАТ: статья#{art.index+1} <-> {pe.arcname} (score={sc:.3f}) ✓✓✓")
                elif sc > 0:
                    logger.info(f"    - Низкий score: статья#{art.index+1} <-> {pe.arcname} (score={sc:.3f}, порог={self.MIN_SCORE_FALLBACK})")
                else:
                    logger.info(f"    - Score=0: статья#{art.index+1} <-> {pe.arcname} (нет совпадений)")
        
        if all_scores:
            logger.info(f"Статистика fallback scores: min={min(all_scores):.3f}, max={max(all_scores):.3f}, avg={sum(all_scores)/len(all_scores):.3f}")
            # Показываем топ-5 лучших scores
            top_scores = sorted(all_scores, reverse=True)[:5]
            logger.info(f"Топ-5 scores: {[f'{s:.3f}' for s in top_scores]}")
        logger.info(f"Найдено кандидатов для fallback (порог {self.MIN_SCORE_FALLBACK}): {len(scored_pairs)} из {len(all_scores)}")

        scored_pairs.sort(key=lambda x: x[0], reverse=True)

        # margin rule per article: compute top1/top2 among its candidates
        top_by_article: Dict[int, List[Tuple[float, PDFEntry]]] = {}
        for sc, art, pe in scored_pairs:
            top_by_article.setdefault(art.index, []).append((sc, pe))
        
        ambiguous = set()
        for idx, candidates in top_by_article.items():
            candidates_sorted = sorted(candidates, key=lambda x: x[0], reverse=True)
            if len(candidates_sorted) >= 2:
                top1_score = candidates_sorted[0][0]
                top2_score = candidates_sorted[1][0]
                if (top1_score - top2_score) < self.MARGIN_SCORE_GAP:
                    ambiguous.add(idx)
                    logger.info(f"  Статья #{idx+1}: неоднозначность (top1={top1_score:.3f}, top2={top2_score:.3f}, gap={top1_score-top2_score:.3f} < {self.MARGIN_SCORE_GAP})")

        if ambiguous:
            logger.info(f"Неоднозначные статьи (margin<{self.MARGIN_SCORE_GAP}): {sorted(i+1 for i in ambiguous)}")

        # greedy assignment (после отсечения ambiguous)
        # Первый проход: сопоставляем однозначные
        for sc, art, pe in scored_pairs:
            if art.index in matched_articles or pe.path in matched_pdfs:
                continue
            if art.index in ambiguous:
                continue

            self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
            matched_articles.add(art.index)
            matched_pdfs.add(pe.path)
            matches.append({
                "article_index": art.index,
                "article_id": art.article_id,
                "article_title": art.title_rus or art.title_eng or "Без названия",
                "pdf_filename": Path(pe.arcname).name,
                "score": float(sc),
                "method": "fallback",
                "doi": art.doi,
            })
            logger.info(f"~ fallback match: article#{art.index+1} <-> {pe.arcname} (score={sc:.2f})")
        
        # Второй проход: для ambiguous статей, если остался только один свободный PDF - сопоставляем его
        for idx in list(ambiguous):
            candidates = top_by_article[idx]
            candidates_sorted = sorted(candidates, key=lambda x: x[0], reverse=True)
            # Если это единственный доступный PDF для этой статьи - сопоставляем
            available_for_article = [c for c in candidates_sorted if c[1].path not in matched_pdfs]
            if len(available_for_article) == 1:
                sc, pe = available_for_article[0]
                art = next(a for a in articles_info if a.index == idx)
                self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                matched_articles.add(idx)
                matched_pdfs.add(pe.path)
                matches.append({
                    "article_index": idx,
                    "article_id": art.article_id,
                    "article_title": art.title_rus or art.title_eng or "Без названия",
                    "pdf_filename": Path(pe.arcname).name,
                    "score": float(sc),
                    "method": "fallback_ambiguous",
                    "doi": art.doi,
                })
                logger.info(f"~ fallback match (ambiguous, единственный кандидат): article#{idx+1} <-> {pe.arcname} (score={sc:.2f})")
                ambiguous.remove(idx)
        
        # Третий проход: для оставшихся ambiguous статей - берем лучший доступный вариант
        for idx in list(ambiguous):
            candidates = top_by_article[idx]
            candidates_sorted = sorted(candidates, key=lambda x: x[0], reverse=True)
            for sc, pe in candidates_sorted:
                if idx in matched_articles or pe.path in matched_pdfs:
                    continue
                # Берем первый доступный (лучший по score)
                art = next(a for a in articles_info if a.index == idx)
                self.set_pdf_file_in_article(art.element, Path(pe.arcname).name)
                matched_articles.add(idx)
                matched_pdfs.add(pe.path)
                matches.append({
                    "article_index": idx,
                    "article_id": art.article_id,
                    "article_title": art.title_rus or art.title_eng or "Без названия",
                    "pdf_filename": Path(pe.arcname).name,
                    "score": float(sc),
                    "method": "fallback_ambiguous_best",
                    "doi": art.doi,
                })
                logger.info(f"~ fallback match (ambiguous, лучший доступный): article#{idx+1} <-> {pe.arcname} (score={sc:.2f})")
                ambiguous.remove(idx)
                break

        # add unmatched articles to report with diagnostic info
        for art in articles_info:
            if art.index not in matched_articles:
                # Найдем лучший score для этой статьи из scored_pairs (выше порога)
                best_score = 0.0
                best_pdf = None
                for sc, a, pe in scored_pairs:
                    if a.index == art.index:
                        if sc > best_score:
                            best_score = sc
                            best_pdf = pe.arcname
                
                # Также проверим все scores для этой статьи (даже те, что ниже порога)
                all_scores_for_article = article_scores_map.get(art.index, [])
                if all_scores_for_article:
                    all_scores_for_article.sort(key=lambda x: x[0], reverse=True)
                    best_score_all = all_scores_for_article[0][0]
                    best_pdf_all = all_scores_for_article[0][1]
                    if best_score_all > best_score:
                        best_score = best_score_all
                        best_pdf = best_pdf_all
                    
                    # Покажем топ-3 лучших scores для диагностики
                    top3 = all_scores_for_article[:3]
                    logger.info(f"  📊 Топ-3 scores для статьи #{art.index+1}: {[(pdf, f'{sc:.3f}') for sc, pdf in top3]}")
                
                reason = "unknown"
                if art.index in ambiguous:
                    reason = f"ambiguous (score={best_score:.2f}, margin<{self.MARGIN_SCORE_GAP})"
                elif best_score > 0 and best_score < self.MIN_SCORE_FALLBACK:
                    reason = f"low_score ({best_score:.3f} < {self.MIN_SCORE_FALLBACK})"
                elif not remaining_pdfs:
                    reason = "no_pdfs_available"
                elif best_score == 0.0:
                    reason = "no_matches_found (score=0 для всех PDF)"
                else:
                    reason = "no_matches_found"
                
                logger.warning(f"⚠ Статья #{art.index+1} не сопоставлена: {reason}" + 
                             (f" (лучший кандидат: {best_pdf}, score={best_score:.3f})" if best_pdf else 
                              (f" (лучший score: {best_score:.3f}, но PDF уже использован)" if best_score > 0 else "")))
                
                # Если есть лучший score, но он ниже порога - все равно покажем его
                if best_score > 0 and best_score < self.MIN_SCORE_FALLBACK:
                    logger.info(f"  💡 Рекомендация: для статьи #{art.index+1} лучший score={best_score:.3f} (порог={self.MIN_SCORE_FALLBACK}). "
                              f"Можно попробовать снизить порог или улучшить извлечение метаданных.")
                
                matches.append({
                    "article_index": art.index,
                    "article_id": art.article_id,
                    "article_title": art.title_rus or art.title_eng or "Без названия",
                    "pdf_filename": None,
                    "score": best_score,
                    "method": "unmatched",
                    "doi": art.doi,
                    "reason": reason,
                })

        matches.sort(key=lambda x: x["article_index"])

        # save XML
        tree.write(str(xml_path), encoding="UTF-8", xml_declaration=True, pretty_print=True)

        # Создаем уникальное имя для обработанного XML файла
        output_xml = extract_to / f"{zip_path.stem}_processed.xml"
        # Копируем обработанный XML в файл с уникальным именем для скачивания
        shutil.copy2(xml_path, output_xml)

        return {
            "success": True,
            "xml_path": xml_path,
            "output_xml": output_xml,
            "xml_arcname": xml_arcname,
            "matches": matches,
            "total_articles": len(articles_info),
            "matched_articles": sum(1 for m in matches if m["pdf_filename"]),
            "unmatched_articles": sum(1 for m in matches if not m["pdf_filename"]),
            "cleanup_removed_pdf_tags": removed,
            "settings": {
                "min_score_fallback": self.MIN_SCORE_FALLBACK,
                "margin_score_gap": self.MARGIN_SCORE_GAP,
                "read_pages_for_text": self.READ_PAGES_FOR_TEXT,
            }
        }
