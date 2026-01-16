"""Точка входа для запуска IPSAS веб-сервера."""

import os
import webbrowser
import threading
import time
import socket
import sys
from ipsas.web.app import create_app
from ipsas.config.settings import get_settings
from ipsas.utils.logger import setup_logger

# Создание Flask приложения для WSGI серверов (gunicorn, uwsgi и т.д.)
app = create_app()


def main():
    """Точка входа в приложение (для локальной разработки)."""
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

    logger.info("Веб-приложение инициализировано")
    logger.info(f"Директория данных: {settings.data_dir}")
    logger.info(f"Директория логов: {settings.logs_dir}")
    logger.info(f"База данных: {settings.database_uri}")

    # Запуск веб-сервера
    host = "0.0.0.0"
    port = int(os.getenv("PORT", 5000))
    debug = settings.log_level == "DEBUG"
    url = f"http://127.0.0.1:{port}"

    def check_server_ready(max_attempts=50):
        """Проверить, готов ли сервер принимать соединения."""
        for attempt in range(max_attempts):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                result = sock.connect_ex(('127.0.0.1', port))
                sock.close()
                if result == 0:
                    logger.debug(f"Сервер готов (попытка {attempt + 1})")
                    return True
            except Exception:
                pass
            time.sleep(0.1)
        return False

    def open_browser():
        """Открыть браузер после того, как сервер будет готов."""
        # Ждем, пока сервер станет доступен
        logger.info("Ожидание запуска сервера...")
        if check_server_ready():
            try:
                webbrowser.open(url)
                logger.info(f"✓ Браузер открыт: {url}")
            except Exception as e:
                logger.warning(f"Не удалось открыть браузер автоматически: {e}")
                logger.info(f"Пожалуйста, откройте браузер вручную: {url}")
        else:
            logger.warning("Сервер не отвечает, браузер не открыт автоматически")
            logger.info(f"Пожалуйста, откройте браузер вручную: {url}")

    # Запускаем открытие браузера в отдельном потоке (только для локальной разработки)
    if not os.getenv("RAILWAY_ENVIRONMENT"):
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()

    logger.info(f"\nСервер запускается на {url}")
    logger.info("Для остановки нажмите Ctrl+C")
    if not os.getenv("RAILWAY_ENVIRONMENT"):
        logger.info("Браузер откроется автоматически после запуска сервера...")

    try:
        app.run(host=host, port=port, debug=debug)
    except KeyboardInterrupt:
        logger.info("\nОстановка сервера...")
        sys.exit(0)


if __name__ == "__main__":
    main()

