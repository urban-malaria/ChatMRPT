"""Twilio media downloader.

Twilio media URLs require HTTP Basic Auth with the account SID and auth token.
"""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)


def download_twilio_media(media_url: str) -> tuple[bytes, str]:
    """Download a Twilio media object and return ``(bytes, content_type)``."""
    if not media_url:
        raise ValueError("Missing Twilio media URL")

    max_bytes = int(os.getenv("WHATSAPP_MAX_UPLOAD_BYTES", str(32 * 1024 * 1024)))
    resp = requests.get(
        media_url,
        auth=(os.getenv("TWILIO_ACCOUNT_SID", ""), os.getenv("TWILIO_AUTH_TOKEN", "")),
        timeout=60,
        stream=True,
    )
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "").split(";", 1)[0].strip()
    content_length = int(resp.headers.get("Content-Length") or 0)
    if content_length and content_length > max_bytes:
        raise ValueError("File is too large for WhatsApp upload processing")

    chunks = []
    total = 0
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError("File is too large for WhatsApp upload processing")
        chunks.append(chunk)

    file_bytes = b"".join(chunks)
    logger.info("Downloaded Twilio media: %s bytes, type=%s", len(file_bytes), content_type)
    return file_bytes, content_type
