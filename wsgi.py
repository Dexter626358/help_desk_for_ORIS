"""WSGI entrypoint for Gunicorn / production.

Example: ``gunicorn wsgi:app``
"""

from ipsas.web.app import create_app

app = create_app()
