"""WSGI entrypoint for Gunicorn / production.

Example: ``gunicorn ipsas.web.wsgi:app`` (или ``gunicorn wsgi:app`` из корня репо).
"""

from ipsas.web.wsgi import app

__all__ = ["app"]
