"""WSGI entrypoint внутри пакета: ``gunicorn ipsas.web.wsgi:app``."""

from ipsas.web.app import create_app

app = create_app()
