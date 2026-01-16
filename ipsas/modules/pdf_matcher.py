"""Модуль для сопоставления PDF файлов со статьями в XML."""

import zipfile
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from lxml import etree
from ipsas.utils.logger import get_logger

logger = get_logger(__name__)

try:
    from PyPDF2 import PdfReader
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logger.warning("PyPDF2 не установлен. Извлечение метаданных из PDF будет недоступно.")


class PDFMatcher:
    """Класс для сопоставления PDF файлов со статьями в XML."""

    def __init__(self):
        """Инициализация матчера."""
        self.namespaces = {
            'xsd': 'http://www.w3.org/2001/XMLSchema'
        }

    def extract_zip(self, zip_path: Path, extract_to: Path) -> Dict[str, Path]:
        """
        Извлечь файлы из ZIP архива.
        
        Args:
            zip_path: Путь к ZIP архиву
            extract_to: Директория для извлечения
            
        Returns:
            Словарь с путями: {'xml': Path, 'pdfs': List[Path]}
        """
        extract_to.mkdir(parents=True, exist_ok=True)
        
        xml_file = None
        pdf_files = []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.namelist():
                # Пропускаем директории
                if file_info.endswith('/'):
                    continue
                
                # Извлекаем файл
                zip_ref.extract(file_info, extract_to)
                file_path = extract_to / file_info
                
                # Проверяем, что файл существует (может быть вложенная структура)
                if not file_path.exists():
                    continue
                
                # Определяем тип файла
                if file_path.suffix.lower() == '.xml':
                    if xml_file is None:
                        xml_file = file_path
                    else:
                        logger.warning(f"Найдено несколько XML файлов, используется первый: {xml_file}")
                elif file_path.suffix.lower() == '.pdf':
                    pdf_files.append(file_path)
        
        if xml_file is None:
            raise ValueError("В архиве не найден XML файл")
        
        return {
            'xml': xml_file,
            'pdfs': pdf_files
        }

    def parse_article_pages(self, pages_str: str) -> Optional[Tuple[int, int]]:
        """
        Парсить строку страниц в диапазон.
        
        Args:
            pages_str: Строка вида "1-25", "1–25", "1-25, 30-35", "1", "стр. 5-10"
            
        Returns:
            Кортеж (начало, конец) или None
        """
        if not pages_str:
            return None
        
        # Убираем пробелы и приводим к нижнему регистру
        pages_str = pages_str.strip().lower()
        
        # Убираем префиксы типа "стр.", "с.", "page", "p."
        pages_str = re.sub(r'^(стр|с|page|p|pages)\.?\s*', '', pages_str, flags=re.IGNORECASE)
        
        # Извлекаем все числа из строки
        numbers = re.findall(r'\d+', pages_str)
        
        if not numbers:
            return None
        
        # Если есть несколько диапазонов (например, "1-25, 30-35"), берем первый
        # Ищем паттерны диапазонов
        range_patterns = [
            r'(\d+)\s*[-–—]\s*(\d+)',  # "1-25" или "1 – 25"
            r'(\d+)\s*\.\.\s*(\d+)',   # "1..25"
            r'(\d+)\s*—\s*(\d+)',      # "1—25" (длинное тире)
        ]
        
        for pattern in range_patterns:
            match = re.search(pattern, pages_str)
            if match:
                try:
                    start = int(match.group(1))
                    end = int(match.group(2))
                    if start <= end:
                        return (start, end)
                except ValueError:
                    continue
        
        # Если только одно число
        if len(numbers) == 1:
            try:
                page = int(numbers[0])
                return (page, page)
            except ValueError:
                return None
        
        # Если несколько чисел, берем первое и последнее
        if len(numbers) >= 2:
            try:
                start = int(numbers[0])
                end = int(numbers[-1])
                if start <= end:
                    return (start, end)
            except ValueError:
                pass
        
        return None

    def get_article_info(self, article_elem: etree.Element) -> Dict:
        """
        Извлечь информацию о статье из XML элемента.
        
        Args:
            article_elem: Элемент article
            
        Returns:
            Словарь с информацией о статье
        """
        info = {
            'id': article_elem.get('id'),
            'num': article_elem.get('num'),
            'pages': None,
            'title': None,
            'first_author': None,
            'all_authors': [],
            'doi': None
        }
        
        # Получаем страницы (может быть в разных местах)
        pages_elem = article_elem.find('.//pages')
        if pages_elem is not None:
            pages_text = pages_elem.text
            if pages_text:
                info['pages'] = self.parse_article_pages(pages_text)
        
        # Получаем заголовок (может быть несколько artTitle с разными языками)
        title_elems = article_elem.findall('.//artTitle')
        if title_elems:
            # Берем первый непустой заголовок
            for title_elem in title_elems:
                if title_elem.text and title_elem.text.strip():
                    # Объединяем весь текст заголовка (может быть смешанный контент)
                    title_text = ''.join(title_elem.itertext()).strip()
                    if title_text:
                        info['title'] = title_text[:150]  # Увеличиваем лимит
                        break
        
        # Получаем авторов
        authors = article_elem.findall('.//author')
        if authors:
            for author in authors:
                surname_elem = author.find('.//surname')
                if surname_elem is not None and surname_elem.text:
                    surname = ''.join(surname_elem.itertext()).strip()
                    if surname:
                        info['all_authors'].append(surname)
                        if info['first_author'] is None:
                            info['first_author'] = surname
        
        # Получаем DOI из codes/doi
        doi_elem = article_elem.find('.//doi')
        if doi_elem is not None and doi_elem.text:
            info['doi'] = doi_elem.text.strip()
        else:
            # Пробуем найти DOI в других местах
            codes_elem = article_elem.find('.//codes')
            if codes_elem is not None:
                doi_elem = codes_elem.find('.//doi')
                if doi_elem is not None and doi_elem.text:
                    info['doi'] = doi_elem.text.strip()
        
        return info

    def extract_numbers_from_filename(self, filename: str) -> List[int]:
        """
        Извлечь все числа из имени файла.
        
        Args:
            filename: Имя файла
            
        Returns:
            Список чисел
        """
        numbers = []
        for match in re.finditer(r'\d+', filename):
            try:
                numbers.append(int(match.group()))
            except ValueError:
                continue
        return numbers

    def normalize_text(self, text: str) -> str:
        """
        Нормализовать текст для сравнения (убрать лишние символы, привести к нижнему регистру).
        
        Args:
            text: Исходный текст
            
        Returns:
            Нормализованный текст
        """
        if not text:
            return ""
        # Убираем расширения, спецсимволы, приводим к нижнему регистру
        text = re.sub(r'[^\w\s]', '', text.lower())
        # Убираем множественные пробелы
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def extract_doi_from_text(self, text: str) -> Optional[str]:
        """
        Извлечь DOI из текста.
        
        Args:
            text: Текст для поиска DOI
            
        Returns:
            DOI или None
        """
        if not text:
            return None
        
        # Паттерны для DOI
        doi_patterns = [
            r'doi[:\s]*10\.\d+/[^\s\)]+',  # "doi: 10.1234/abc" или "doi 10.1234/abc"
            r'10\.\d+/[^\s\)]+',  # Просто "10.1234/abc"
        ]
        
        for pattern in doi_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                doi = match.group(0)
                # Убираем префикс "doi:" или "DOI:" если есть
                doi = re.sub(r'^doi[:\s]*', '', doi, flags=re.IGNORECASE)
                # Очищаем DOI от лишних символов в конце (скобки, точки, запятые)
                doi = re.sub(r'[^\d\./]+$', '', doi)
                return doi.strip()
        
        return None

    def extract_pdf_metadata(self, pdf_path: Path) -> Dict:
        """
        Извлечь метаданные из PDF файла (название, авторы, DOI).
        
        Args:
            pdf_path: Путь к PDF файлу
            
        Returns:
            Словарь с метаданными: {'title': str, 'authors': List[str], 'doi': str}
        """
        metadata = {
            'title': None,
            'authors': [],
            'doi': None
        }
        
        if not PDF_SUPPORT:
            logger.warning(f"PyPDF2 не установлен, метаданные из PDF {pdf_path.name} не извлекаются")
            logger.warning("Установите PyPDF2: pip install PyPDF2")
            return metadata
        
        try:
            logger.debug(f"Начало извлечения метаданных из {pdf_path.name}")
            with open(pdf_path, 'rb') as pdf_file:
                reader = PdfReader(pdf_file)
                logger.debug(f"PDF файл открыт, страниц: {len(reader.pages)}")
                
                # Извлекаем метаданные из документа
                if reader.metadata:
                    logger.debug(f"Метаданные документа найдены: {list(reader.metadata.keys())}")
                    # Название из метаданных
                    if reader.metadata.get('/Title'):
                        metadata['title'] = reader.metadata['/Title']
                        logger.debug(f"Название из метаданных: {metadata['title'][:50]}...")
                    elif reader.metadata.get('Title'):
                        metadata['title'] = reader.metadata['Title']
                        logger.debug(f"Название из метаданных (alt): {metadata['title'][:50]}...")
                    
                    # Авторы из метаданных
                    if reader.metadata.get('/Author'):
                        authors_str = reader.metadata['/Author']
                        # Разделяем авторов по запятым или точкам с запятой
                        authors = re.split(r'[,;]', authors_str)
                        metadata['authors'] = [a.strip() for a in authors if a.strip()]
                        logger.debug(f"Авторы из метаданных: {len(metadata['authors'])} авторов")
                    elif reader.metadata.get('Author'):
                        authors_str = reader.metadata['Author']
                        authors = re.split(r'[,;]', authors_str)
                        metadata['authors'] = [a.strip() for a in authors if a.strip()]
                        logger.debug(f"Авторы из метаданных (alt): {len(metadata['authors'])} авторов")
                else:
                    logger.debug("Метаданные документа отсутствуют, извлекаем из текста")
                
                # Извлекаем текст с первых страниц для поиска названия, авторов и DOI
                text_pages = []
                max_pages_to_read = min(3, len(reader.pages))  # Читаем первые 3 страницы
                
                for page_num in range(max_pages_to_read):
                    try:
                        page = reader.pages[page_num]
                        text = page.extract_text()
                        if text:
                            text_pages.append(text)
                    except Exception as e:
                        logger.debug(f"Ошибка при чтении страницы {page_num}: {e}")
                        continue
                
                full_text = '\n'.join(text_pages)
                
                # Ищем DOI в тексте (приоритет - ищем во всем тексте)
                if full_text:
                    logger.debug(f"Извлечен текст из {len(text_pages)} страниц, длина: {len(full_text)} символов")
                    doi = self.extract_doi_from_text(full_text)
                    if doi:
                        metadata['doi'] = doi
                        logger.debug(f"DOI найден в тексте: {doi}")
                    else:
                        logger.debug("DOI не найден в тексте")
                    
                    # Улучшенное извлечение названия из первой страницы
                    if not metadata['title'] and text_pages:
                        first_page_text = text_pages[0]
                        first_page_lines = [line.strip() for line in first_page_text.split('\n') if line.strip()]
                        
                        # Ключевые слова, которые указывают на служебную информацию (название журнала, издательство и т.д.)
                        skip_keywords = [
                            'известия', 'вестник', 'журнал', 'journal', 'bulletin',
                            'университет', 'university', 'институт', 'institute',
                            'издательство', 'publisher', 'issn', 'eissn',
                            'том', 'volume', 'выпуск', 'issue', 'номер', 'number',
                            'год', 'year', 'страница', 'page', 'стр', 'pp',
                            'прикладная химия', 'applied chemistry'
                        ]
                        
                        # Ключевые слова, которые указывают на конец названия статьи
                        stop_keywords = ['abstract', 'аннотация', 'keywords', 'ключевые слова', 'doi', 'introduction', 'введение', 'резюме']
                        
                        title_lines = []
                        skip_count = 0  # Счетчик пропущенных строк (название журнала обычно в начале)
                        
                        for i, line in enumerate(first_page_lines[:20]):  # Проверяем первые 20 строк
                            line_lower = line.lower()
                            
                            # Пропускаем строки с ключевыми словами окончания
                            if any(keyword in line_lower for keyword in stop_keywords):
                                break
                            
                            # Пропускаем строки с названием журнала/издательства (первые несколько строк)
                            if i < 5 and any(keyword in line_lower for keyword in skip_keywords):
                                skip_count += 1
                                continue
                            
                            # Пропускаем очень короткие строки (менее 10 символов)
                            if len(line) < 10:
                                continue
                            
                            # Пропускаем строки, которые выглядят как адреса, email или DOI
                            if '@' in line or 'http' in line_lower or 'doi' in line_lower:
                                continue
                            
                            # Пропускаем строки с только заглавными буквами (обычно это название журнала)
                            if line.isupper() and len(line) > 30:
                                if skip_count < 3:  # Пропускаем только первые несколько таких строк
                                    skip_count += 1
                                    continue
                            
                            # Пропускаем строки с номерами томов/выпусков
                            if re.search(r'\b(том|volume|выпуск|issue|№|no\.?)\s*\d+', line_lower):
                                continue
                            
                            title_lines.append(line)
                            
                            # Если нашли достаточно длинное название (более 20 символов), останавливаемся
                            if len(' '.join(title_lines)) > 30:
                                break
                        
                        if title_lines:
                            potential_title = ' '.join(title_lines).strip()
                            # Очищаем от лишних пробелов и символов
                            potential_title = re.sub(r'\s+', ' ', potential_title)
                            # Убираем лишние точки и запятые в конце
                            potential_title = re.sub(r'[.,;]+$', '', potential_title).strip()
                            if len(potential_title) > 20 and len(potential_title) < 300:
                                metadata['title'] = potential_title
                                logger.debug(f"Название извлечено из текста: {potential_title[:80]}...")
                    
                    # Улучшенное извлечение авторов из текста
                    if not metadata['authors'] and text_pages:
                        # Ищем паттерны авторов в первой странице
                        first_page_text = text_pages[0]
                        first_page_lines = [line.strip() for line in first_page_text.split('\n') if line.strip()]
                        
                        # Находим позицию названия статьи (если оно было извлечено)
                        title_found = False
                        title_end_pos = 0
                        if metadata['title']:
                            title_words = metadata['title'].split()[:3]  # Первые 3 слова названия
                            for i, line in enumerate(first_page_lines):
                                if all(word.lower() in line.lower() for word in title_words if len(word) > 3):
                                    title_found = True
                                    title_end_pos = i
                                    break
                        
                        # Ищем авторов после названия статьи
                        authors_found = False
                        author_lines = []
                        
                        # Паттерны для поиска авторов
                        author_keywords = ['author', 'authors', 'автор', 'авторы']
                        stop_keywords = ['abstract', 'аннотация', 'keywords', 'ключевые слова', 'doi', 'introduction', 'введение']
                        
                        # Ищем строки с авторами после названия
                        start_search = title_end_pos + 1 if title_found else 0
                        for i in range(start_search, min(start_search + 10, len(first_page_lines))):
                            line = first_page_lines[i]
                            line_lower = line.lower()
                            
                            # Останавливаемся на ключевых словах окончания
                            if any(keyword in line_lower for keyword in stop_keywords):
                                break
                            
                            # Пропускаем пустые строки
                            if not line or len(line) < 3:
                                continue
                            
                            # Пропускаем строки с названием журнала/издательства
                            skip_keywords = ['известия', 'вестник', 'журнал', 'journal', 'issn', 'university', 'institute']
                            if any(keyword in line_lower for keyword in skip_keywords):
                                continue
                            
                            # Пропускаем строки только с заглавными буквами (название журнала)
                            if line.isupper() and len(line) > 30:
                                continue
                            
                            # Пропускаем строки с email или адресами
                            if '@' in line or 'http' in line_lower:
                                continue
                            
                            # Проверяем, похоже ли на авторов (содержит инициалы, фамилии)
                            # Паттерн: Фамилия И.О. или Фамилия И. О.
                            if re.search(r'[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z]\.\s*[А-ЯЁA-Z]?\.?', line):
                                author_lines.append(line)
                                authors_found = True
                            # Или если строка содержит запятые и выглядит как список авторов
                            elif ',' in line and len(line.split(',')) >= 2:
                                author_lines.append(line)
                                authors_found = True
                        
                        if author_lines:
                            # Объединяем строки авторов
                            authors_text = ' '.join(author_lines)
                            # Очищаем текст авторов
                            authors_text = re.sub(r'\s+', ' ', authors_text)
                            # Разделяем по запятым, точкам с запятой или переносам строк
                            authors = re.split(r'[,;\n]', authors_text)
                            authors_clean = []
                            for author in authors:
                                author = author.strip()
                                # Пропускаем слишком короткие или слишком длинные строки
                                if 3 <= len(author) <= 100:
                                    # Убираем лишние символы, но оставляем точки и дефисы
                                    author = re.sub(r'[^\w\s\.\-\']', '', author)
                                    if author:
                                        authors_clean.append(author)
                            
                            if authors_clean:
                                metadata['authors'] = authors_clean[:8]  # Ограничиваем 8 авторами
                                logger.debug(f"Авторы извлечены из текста: {len(authors_clean)} авторов")
                
        except Exception as e:
            logger.warning(f"Ошибка при извлечении метаданных из PDF {pdf_path.name}: {e}")
        
        return metadata

    def calculate_title_similarity(self, pdf_title: str, article_title: str) -> float:
        """
        Вычислить схожесть названия статьи из PDF и XML.
        
        Args:
            pdf_title: Название из PDF
            article_title: Название статьи из XML
            
        Returns:
            Оценка схожести (0.0 - 1.0)
        """
        if not article_title or not pdf_title:
            return 0.0
        
        pdf_norm = self.normalize_text(pdf_title)
        title_norm = self.normalize_text(article_title)
        
        if not title_norm or not pdf_norm:
            return 0.0
        
        # Точное совпадение после нормализации
        if pdf_norm == title_norm:
            return 1.0
        
        # Извлекаем ключевые слова из обоих названий (слова длиннее 3 символов)
        pdf_words = set(w for w in pdf_norm.split() if len(w) > 3)
        title_words = set(w for w in title_norm.split() if len(w) > 3)
        
        if not title_words or not pdf_words:
            return 0.0
        
        # Подсчитываем совпадения ключевых слов
        common_words = pdf_words & title_words
        total_unique_words = len(pdf_words | title_words)
        
        if total_unique_words == 0:
            return 0.0
        
        # Jaccard similarity (пересечение / объединение)
        jaccard = len(common_words) / total_unique_words
        
        # Также учитываем процент совпадающих слов от названия статьи
        word_match_ratio = len(common_words) / len(title_words)
        
        # Комбинируем метрики
        similarity = (jaccard * 0.6 + word_match_ratio * 0.4)
        
        return min(similarity, 1.0)

    def extract_surname(self, author_name: str) -> str:
        """
        Извлечь фамилию из полного имени автора.
        
        Args:
            author_name: Полное имя автора (может быть "Иванов И.И." или "Ivanov I.I.")
            
        Returns:
            Фамилия автора
        """
        if not author_name:
            return ""
        
        # Нормализуем
        author_name = author_name.strip()
        
        # Разделяем по пробелам
        parts = author_name.split()
        if not parts:
            return ""
        
        # Берем первую часть (обычно это фамилия)
        surname = parts[0]
        
        # Убираем лишние символы
        surname = re.sub(r'[^\w]', '', surname)
        
        return surname.lower()

    def compare_authors(self, pdf_authors: List[str], xml_authors: List[str]) -> float:
        """
        Сравнить списки авторов из PDF и XML.
        
        Args:
            pdf_authors: Список авторов из PDF
            xml_authors: Список авторов из XML
            
        Returns:
            Оценка совпадения (0.0 - 1.0)
        """
        if not pdf_authors or not xml_authors:
            return 0.0
        
        # Извлекаем фамилии из обоих списков
        pdf_surnames = [self.extract_surname(a) for a in pdf_authors if a]
        xml_surnames = [self.extract_surname(a) for a in xml_authors if a]
        
        if not pdf_surnames or not xml_surnames:
            return 0.0
        
        # Подсчитываем точные совпадения фамилий
        exact_matches = 0
        partial_matches = 0
        
        pdf_surnames_set = set(pdf_surnames)
        xml_surnames_set = set(xml_surnames)
        
        # Точные совпадения
        exact_matches = len(pdf_surnames_set & xml_surnames_set)
        
        # Частичные совпадения (первые 4+ символа)
        for pdf_surname in pdf_surnames:
            if len(pdf_surname) >= 4:
                for xml_surname in xml_surnames:
                    if len(xml_surname) >= 4:
                        # Проверяем совпадение начала фамилий
                        min_len = min(len(pdf_surname), len(xml_surname))
                        if pdf_surname[:min_len] == xml_surname[:min_len] and min_len >= 4:
                            if pdf_surname not in xml_surnames_set:  # Не считаем дважды
                                partial_matches += 0.3
                                break
        
        # Вычисляем оценку
        max_authors = max(len(pdf_authors), len(xml_authors))
        if max_authors == 0:
            return 0.0
        
        # Комбинируем точные и частичные совпадения
        total_score = exact_matches + partial_matches
        similarity = min(total_score / max_authors, 1.0)
        
        # Если совпало больше половины авторов, это очень хорошо
        if exact_matches >= len(xml_authors) * 0.5:
            similarity = min(similarity * 1.2, 1.0)
        
        return similarity

    def match_pdf_to_article(
        self, 
        pdf_filename: str, 
        pdf_metadata: Dict,
        article_info: Dict,
        article_index: int,
        pdf_index: int = None
    ) -> float:
        """
        Вычислить вероятность соответствия PDF файла статье.
        
        Args:
            pdf_filename: Имя PDF файла
            pdf_metadata: Метаданные из PDF (title, authors, doi)
            article_info: Информация о статье из XML
            article_index: Индекс статьи (начиная с 0)
            pdf_index: Индекс PDF файла в списке (начиная с 0)
            
        Returns:
            Оценка соответствия (0.0 - 1.0)
        """
        score = 0.0
        pdf_lower = pdf_filename.lower()
        pdf_name_no_ext = Path(pdf_filename).stem.lower()
        
        # Извлекаем все числа из имени файла
        pdf_numbers = self.extract_numbers_from_filename(pdf_filename)
        
        # ПРИОРИТЕТ 1: Сопоставление по DOI (самый надежный критерий - почти гарантия)
        if pdf_metadata.get('doi') and article_info.get('doi'):
            pdf_doi = self.normalize_text(pdf_metadata['doi'])
            xml_doi = self.normalize_text(article_info['doi'])
            if pdf_doi == xml_doi:
                score += 1.0  # Максимальный приоритет - точное совпадение DOI
                logger.debug(f"✓ Точное совпадение по DOI: {pdf_doi}")
                return min(score, 1.0)  # Если DOI совпадает, это гарантия
        
        # ПРИОРИТЕТ 2: Сопоставление по названию статьи из PDF и XML
        if pdf_metadata.get('title') and article_info.get('title'):
            title_similarity = self.calculate_title_similarity(
                pdf_metadata['title'],
                article_info['title']
            )
            if title_similarity > 0.6:  # Высокий порог для названия
                score += title_similarity * 0.7  # Очень высокий приоритет
                logger.debug(f"✓ Совпадение по названию: {title_similarity:.2f}")
            elif title_similarity > 0.4:  # Средний порог
                score += title_similarity * 0.4
                logger.debug(f"~ Частичное совпадение по названию: {title_similarity:.2f}")
        
        # ПРИОРИТЕТ 3: Сопоставление по авторам из PDF и XML
        if pdf_metadata.get('authors') and article_info.get('all_authors'):
            author_similarity = self.compare_authors(
                pdf_metadata['authors'],
                article_info['all_authors']
            )
            if author_similarity > 0.6:  # Высокий порог для авторов
                score += author_similarity * 0.6  # Высокий приоритет
                logger.debug(f"✓ Совпадение по авторам: {author_similarity:.2f}")
            elif author_similarity > 0.4:  # Средний порог
                score += author_similarity * 0.3
                logger.debug(f"~ Частичное совпадение по авторам: {author_similarity:.2f}")
        
        # Комбинированный бонус: если есть совпадения и по названию, и по авторам
        if (pdf_metadata.get('title') and article_info.get('title') and
            pdf_metadata.get('authors') and article_info.get('all_authors')):
            title_sim = self.calculate_title_similarity(
                pdf_metadata['title'],
                article_info['title']
            )
            author_sim = self.compare_authors(
                pdf_metadata['authors'],
                article_info['all_authors']
            )
            if title_sim > 0.4 and author_sim > 0.4:
                score += 0.2  # Бонус за комбинацию
                logger.debug(f"✓ Бонус за комбинацию названия и авторов")
        
        # ПРИОРИТЕТ 4: Сопоставление по порядковому номеру файла
        if pdf_index is not None and pdf_index == article_index:
            score += 0.4  # Высокий приоритет для точного совпадения индексов
        
        # 2. Сопоставление по номеру статьи из атрибута num
        if article_info.get('num'):
            try:
                article_num = int(article_info['num'])
                if article_num in pdf_numbers:
                    score += 0.4
                # Также проверяем номер как строку в имени файла
                if str(article_num) in pdf_name_no_ext:
                    score += 0.2
            except ValueError:
                pass
        
        # 3. Сопоставление по порядковому номеру в имени файла (1, 2, 3...)
        for num in pdf_numbers:
            if num == article_index + 1:  # Индекс начинается с 0, номера с 1
                score += 0.35
                break
        
        # 4. Сопоставление по страницам статьи
        if article_info.get('pages'):
            start, end = article_info['pages']
            for num in pdf_numbers:
                # Проверяем, попадает ли число в диапазон страниц
                if start <= num <= end:
                    score += 0.25
                    break
                # Также проверяем начало диапазона
                if num == start:
                    score += 0.15
                    break
        
        # 5. Сопоставление по названию статьи
        if article_info.get('title'):
            title_similarity = self.calculate_title_similarity(
                pdf_filename, 
                article_info['title']
            )
            score += title_similarity * 0.3
        
        # 6. Сопоставление по авторам (проверяем всех авторов)
        authors = article_info.get('all_authors', [])
        if not authors and article_info.get('first_author'):
            authors = [article_info['first_author']]
        
        for author in authors:
            if not author:
                continue
            author_lower = author.lower().strip()
            # Убираем спецсимволы из фамилии
            author_clean = re.sub(r'[^\w]', '', author_lower)
            if len(author_clean) >= 3:
                # Проверяем точное совпадение или начало фамилии
                if author_clean in pdf_lower:
                    score += 0.2
                    break  # Достаточно одного совпадения
                elif len(author_clean) >= 5 and author_clean[:5] in pdf_lower:
                    score += 0.1
                    break
        
        # 7. Проверка по ID статьи (если ID есть в имени файла)
        if article_info.get('id'):
            article_id = article_info['id'].lower()
            if article_id in pdf_lower:
                score += 0.3
        
        return min(score, 1.0)

    def add_pdf_to_article(
        self, 
        article_elem: etree.Element, 
        pdf_filename: str
    ) -> None:
        """
        Добавить имя PDF файла в элемент article.
        
        Args:
            article_elem: Элемент article
            pdf_filename: Имя PDF файла
        """
        # Ищем или создаем элемент files
        files_elem = article_elem.find('files')
        if files_elem is None:
            files_elem = etree.Element('files')
            # Вставляем files после references или в конец
            references_elem = article_elem.find('references')
            if references_elem is not None:
                # Вставляем после references
                parent = article_elem
                index = list(parent).index(references_elem) + 1
                parent.insert(index, files_elem)
            else:
                # Добавляем в конец
                article_elem.append(files_elem)
        
        # Проверяем, не добавлен ли уже этот файл
        existing_files = files_elem.findall('file')
        for existing_file in existing_files:
            if existing_file.text == pdf_filename:
                # Файл уже добавлен
                return
        
        # Создаем элемент file
        file_elem = etree.Element('file')
        file_elem.text = pdf_filename
        file_elem.set('desc', 'PDF')
        
        files_elem.append(file_elem)

    def process_zip(
        self, 
        zip_path: Path, 
        extract_to: Path
    ) -> Dict:
        """
        Обработать ZIP архив: извлечь файлы, сопоставить PDF со статьями, обновить XML.
        
        Args:
            zip_path: Путь к ZIP архиву
            extract_to: Директория для извлечения
            
        Returns:
            Словарь с результатами обработки
        """
        # Извлекаем файлы
        extracted = self.extract_zip(zip_path, extract_to)
        xml_path = extracted['xml']
        pdf_files = extracted['pdfs']
        
        if not pdf_files:
            raise ValueError("В архиве не найдены PDF файлы")
        
        # Парсим XML
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()
        
        # Находим все статьи
        articles = root.findall('.//article')
        if not articles:
            raise ValueError("В XML не найдены статьи")
        
        # Собираем информацию о статьях
        articles_info = []
        for idx, article in enumerate(articles):
            info = self.get_article_info(article)
            info['index'] = idx
            info['element'] = article
            articles_info.append(info)
        
        # Сортируем PDF файлы по имени для предсказуемости
        pdf_files_sorted = sorted(pdf_files, key=lambda x: x.name)
        
        logger.info(f"Найдено статей: {len(articles_info)}, PDF файлов: {len(pdf_files_sorted)}")
        
        # Извлекаем метаданные из всех PDF файлов
        logger.info("Извлечение метаданных из PDF файлов...")
        pdf_metadata_dict = {}
        extracted_count = 0
        for pdf_path in pdf_files_sorted:
            logger.info(f"Обработка PDF: {pdf_path.name}")
            metadata = self.extract_pdf_metadata(pdf_path)
            pdf_metadata_dict[pdf_path] = metadata
            
            # Логируем результаты извлечения
            has_title = bool(metadata.get('title'))
            has_authors = bool(metadata.get('authors'))
            has_doi = bool(metadata.get('doi'))
            
            if has_title or has_authors or has_doi:
                extracted_count += 1
                logger.info(
                    f"  ✓ Метаданные извлечены из {pdf_path.name}: "
                    f"title={'✓' if has_title else '✗'}, "
                    f"authors={'✓' if has_authors else '✗'} ({len(metadata.get('authors', []))}), "
                    f"doi={'✓' if has_doi else '✗'}"
                )
                if has_title:
                    logger.info(f"    Название: {metadata['title'][:80]}...")
                if has_authors:
                    authors_str = ', '.join(metadata['authors'][:3])
                    if len(metadata['authors']) > 3:
                        authors_str += f" и еще {len(metadata['authors']) - 3}"
                    logger.info(f"    Авторы: {authors_str}")
                if has_doi:
                    logger.info(f"    DOI: {metadata['doi']}")
            else:
                logger.warning(f"  ✗ Не удалось извлечь метаданные из {pdf_path.name}")
        
        logger.info(f"Метаданные успешно извлечены из {extracted_count} из {len(pdf_files_sorted)} PDF файлов")
        
        # Сопоставляем PDF файлы со статьями используя улучшенный алгоритм
        matches = []
        used_pdfs = set()
        
        # Минимальный порог соответствия для автоматического сопоставления
        # Увеличиваем порог для более строгого сопоставления
        MIN_SCORE_THRESHOLD = 0.4
        
        logger.info(f"Минимальный порог соответствия: {MIN_SCORE_THRESHOLD}")
        
        # Сначала собираем все возможные пары (статья, PDF) с оценками
        all_pairs = []
        for article_info in articles_info:
            for pdf_idx, pdf_path in enumerate(pdf_files_sorted):
                if pdf_path in used_pdfs:
                    continue
                
                pdf_metadata = pdf_metadata_dict.get(pdf_path, {})
                score = self.match_pdf_to_article(
                    pdf_path.name,
                    pdf_metadata,
                    article_info,
                    article_info['index'],
                    pdf_idx
                )
                
                if score >= MIN_SCORE_THRESHOLD:
                    all_pairs.append({
                        'article': article_info,
                        'pdf': pdf_path,
                        'score': score,
                        'pdf_index': pdf_idx
                    })
        
        # Сортируем пары по оценке (лучшие сначала)
        all_pairs.sort(key=lambda x: x['score'], reverse=True)
        
        # Жадный алгоритм: выбираем лучшие пары, избегая конфликтов
        matched_articles = set()
        matched_pdfs = set()
        
        for pair in all_pairs:
            article_info = pair['article']
            pdf_path = pair['pdf']
            
            # Пропускаем, если статья или PDF уже сопоставлены
            if article_info['index'] in matched_articles or pdf_path in matched_pdfs:
                continue
            
            # Сопоставляем эту пару
            self.add_pdf_to_article(article_info['element'], pdf_path.name)
            matched_articles.add(article_info['index'])
            matched_pdfs.add(pdf_path)
            
            pdf_meta = pdf_metadata_dict.get(pdf_path, {})
            match_info = (
                f"Сопоставлено: статья #{article_info['index'] + 1} "
                f"({article_info.get('title', 'Без названия')[:50]}...) "
                f"<-> {pdf_path.name} (оценка: {pair['score']:.2f})"
            )
            if pdf_meta.get('doi') or pdf_meta.get('title') or pdf_meta.get('authors'):
                match_info += f" [DOI: {pdf_meta.get('doi', 'N/A')}, "
                match_info += f"Title: {'✓' if pdf_meta.get('title') else '✗'}, "
                match_info += f"Authors: {'✓' if pdf_meta.get('authors') else '✗'}]"
            logger.info(match_info)
            
            matches.append({
                'article_index': article_info['index'],
                'article_id': article_info.get('id'),
                'article_title': article_info.get('title', 'Без названия'),
                'pdf_filename': pdf_path.name,
                'score': pair['score'],
                'pdf_metadata': pdf_meta
            })
        
        # Добавляем статьи, которые не были сопоставлены
        for article_info in articles_info:
            if article_info['index'] not in matched_articles:
                matches.append({
                    'article_index': article_info['index'],
                    'article_id': article_info.get('id'),
                    'article_title': article_info.get('title', 'Без названия'),
                    'pdf_filename': None,
                    'score': 0.0
                })
        
        # Сортируем результаты по индексу статьи
        matches.sort(key=lambda x: x['article_index'])
        
        # Сохраняем обновленный XML
        tree.write(
            str(xml_path),
            encoding='UTF-8',
            xml_declaration=True,
            pretty_print=True
        )
        
        # Создаем новый ZIP архив с обновленным XML
        output_zip = extract_to / f"{zip_path.stem}_processed.zip"
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_out:
            # Добавляем обновленный XML
            zip_out.write(xml_path, xml_path.name)
            # Добавляем все PDF файлы
            for pdf_path in pdf_files:
                zip_out.write(pdf_path, pdf_path.name)
        
        return {
            'success': True,
            'xml_path': xml_path,
            'output_zip': output_zip,
            'matches': matches,
            'total_articles': len(articles_info),
            'matched_articles': len([m for m in matches if m['pdf_filename']]),
            'unmatched_articles': len([m for m in matches if not m['pdf_filename']])
        }
