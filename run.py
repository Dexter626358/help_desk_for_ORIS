"""Точка входа для запуска IPSAS веб-сервера (совместимость).

Предпочтительно: ``ipsas-web`` или ``python -m ipsas.web.devserver``.
"""

from ipsas.web.devserver import app, main

__all__ = ["app", "main"]


if __name__ == "__main__":
    main()
