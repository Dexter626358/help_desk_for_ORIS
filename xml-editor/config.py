"""Конфигурация редактора XML."""

from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
SAMPLES_DIR = BASE_DIR / "samples"

MAX_CONTENT_LENGTH = int(os.getenv("XML_EDITOR_MAX_UPLOAD_MB", "20")) * 1024 * 1024
SESSION_TTL_HOURS = int(os.getenv("XML_EDITOR_SESSION_TTL_HOURS", "24"))
SECRET_KEY = os.getenv("XML_EDITOR_SECRET_KEY") or os.getenv("SECRET_KEY") or "dev-xml-editor-change-me"

# session_id: только url-safe токен
SESSION_ID_RE = r"^[A-Za-z0-9_-]{16,64}$"
ARTICLE_ID_RE = r"^\d{1,5}$"

ORIGINAL_NAME = "original.xml"
EDITED_NAME = "edited.xml"
META_NAME = "meta.json"
