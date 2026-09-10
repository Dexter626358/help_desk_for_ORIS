# syntax=docker/dockerfile:1
FROM python:3.11.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    IPSAS_ENV=production \
    FLASK_ENV=production \
    TEMP_DIR=/var/lib/ipsas/temp \
    DATA_DIR=/var/lib/ipsas/data \
    LOGS_DIR=/var/log/ipsas \
    LOG_TO_FILE=false \
    GUNICORN_WORKERS=1 \
    GUNICORN_THREADS=4 \
    GUNICORN_TIMEOUT=120 \
    PORT=8000

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 ipsas \
    && useradd --system --uid 10001 --gid ipsas --home-dir /app --shell /usr/sbin/nologin ipsas \
    && mkdir -p /var/lib/ipsas/temp /var/lib/ipsas/data /var/log/ipsas \
    && chown -R ipsas:ipsas /var/lib/ipsas /var/log/ipsas

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY --chown=ipsas:ipsas . .

USER ipsas

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"

# Один worker: in-memory RequestGuard (rate limit / concurrency)
CMD gunicorn wsgi:app \
    --bind 0.0.0.0:${PORT} \
    --workers ${GUNICORN_WORKERS} \
    --threads ${GUNICORN_THREADS} \
    --timeout ${GUNICORN_TIMEOUT} \
    --graceful-timeout 30 \
    --access-logfile - \
    --error-logfile -
