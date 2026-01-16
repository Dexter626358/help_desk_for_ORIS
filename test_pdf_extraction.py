"""Тестовый скрипт для анализа извлечения метаданных из PDF."""

import sys
from pathlib import Path
from ipsas.modules.pdf_matcher import PDFMatcher
from ipsas.utils.logger import setup_logger

def main():
    """Тестирование извлечения метаданных из PDF."""
    # Настройка логирования
    logger = setup_logger(log_level="DEBUG")
    
    # Путь к архиву
    if len(sys.argv) > 1:
        zip_path = Path(sys.argv[1])
    else:
        zip_path = Path("1813-324X_2025_11_6.zip")
    
    if not zip_path.exists():
        logger.error(f"Архив не найден: {zip_path}")
        return
    
    logger.info(f"Анализ архива: {zip_path}")
    
    # Создаем экземпляр PDFMatcher
    matcher = PDFMatcher()
    
    # Извлекаем файлы из архива
    extracted_dir = Path("temp_extracted")
    extracted_dir.mkdir(exist_ok=True)
    
    try:
        xml_file, pdf_files = matcher.extract_zip(zip_path, extracted_dir)
        
        logger.info(f"Найдено PDF файлов: {len(pdf_files)}")
        logger.info("=" * 80)
        
        # Анализируем каждый PDF
        for pdf_file in pdf_files:
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"Анализ PDF: {pdf_file.name}")
            logger.info("=" * 80)
            
            metadata = matcher.extract_pdf_metadata(pdf_file)
            
            logger.info("")
            logger.info("РЕЗУЛЬТАТЫ ИЗВЛЕЧЕНИЯ:")
            logger.info(f"  Название: {metadata['title']}")
            logger.info(f"  Авторы: {metadata['authors']}")
            logger.info(f"  DOI: {metadata['doi']}")
            logger.info("")
    
    finally:
        # Очистка
        import shutil
        if extracted_dir.exists():
            shutil.rmtree(extracted_dir)
            logger.info(f"Временная директория удалена: {extracted_dir}")

if __name__ == "__main__":
    main()
