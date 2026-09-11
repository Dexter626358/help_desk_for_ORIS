"""Локальный dev-сервер: ``ipsas-web`` / ``python -m ipsas.web.devserver``."""

from __future__ import annotations

import os
import socket
import sys
import threading
import time
import webbrowser

from ipsas.config.settings import get_settings
from ipsas.utils.logger import setup_logger
from ipsas.web.app import create_app

app = create_app()


def main() -> None:
    """Точка входа для локальной разработки."""
    settings = get_settings()
    logger = setup_logger(
        log_file=settings.log_file,
        log_level=settings.log_level,
    )

    logger.info("=" * 50)
    logger.info("Internal Publishing Support System (IPSAS)")
    logger.info("Версия: %s", __import__("ipsas").__version__)
    logger.info("=" * 50)
    logger.info("Веб-приложение инициализировано")
    logger.info("Директория данных: %s", settings.data_dir)
    logger.info("Директория логов: %s", settings.logs_dir)

    host = "0.0.0.0"
    port = int(os.getenv("PORT", "5000"))
    debug = settings.flask_debug
    url = f"http://127.0.0.1:{port}"

    def check_server_ready(max_attempts: int = 50) -> bool:
        for attempt in range(max_attempts):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                result = sock.connect_ex(("127.0.0.1", port))
                sock.close()
                if result == 0:
                    logger.debug("Сервер готов (попытка %s)", attempt + 1)
                    return True
            except OSError:
                pass
            time.sleep(0.1)
        return False

    def open_browser() -> None:
        logger.info("Ожидание запуска сервера...")
        if check_server_ready():
            try:
                webbrowser.open(url)
                logger.info("✓ Браузер открыт: %s", url)
            except Exception as exc:
                logger.warning("Не удалось открыть браузер автоматически: %s", exc)
                logger.info("Пожалуйста, откройте браузер вручную: %s", url)
        else:
            logger.warning("Сервер не отвечает, браузер не открыт автоматически")
            logger.info("Пожалуйста, откройте браузер вручную: %s", url)

    if not os.getenv("RAILWAY_ENVIRONMENT"):
        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()

    logger.info("\nСервер запускается на %s", url)
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
