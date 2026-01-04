"""Точка входа для запуска IPSAS веб-сервера."""

from ipsas.web.app import create_app
from ipsas.config.settings import get_settings
from ipsas.utils.logger import setup_logger


def main():
    """Точка входа в приложение."""
    # Инициализация настроек
    settings = get_settings()

    # Настройка логирования
    logger = setup_logger(
        log_file=settings.log_file,
        log_level=settings.log_level
    )

    logger.info("=" * 50)
    logger.info("Internal Publishing Support System (IPSAS)")
    logger.info(f"Версия: {__import__('ipsas').__version__}")
    logger.info("=" * 50)

    # Создание Flask приложения
    app = create_app()

    logger.info("Веб-приложение инициализировано")
    logger.info(f"Директория данных: {settings.data_dir}")
    logger.info(f"Директория логов: {settings.logs_dir}")
    logger.info(f"База данных: {settings.database_uri}")

    # Запуск веб-сервера
    host = "0.0.0.0"
    port = 5000
    debug = settings.log_level == "DEBUG"

    logger.info(f"\nСервер запущен на http://{host}:{port}")
    logger.info("Для остановки нажмите Ctrl+C")

    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()

