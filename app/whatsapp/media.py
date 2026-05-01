"""
Twilio media downloader.

Twilio media URLs require HTTP Basic Auth with (ACCOUNT_SID, AUTH_TOKEN).
A plain GET returns 401.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)


def download_twilio_media(media_url: str) -> tuple[bytes, str]:
    """
    Download a file from a Twilio media URL.

    Returns (file_bytes, content_type).
    Raises requests.HTTPError on non-2xx response.
    """
    account_sid = os.getenv('TWILIO_ACCOUNT_SID', '')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN', '')

    resp = requests.get(
        media_url,
        auth=(account_sid, auth_token),
        timeout=60,
    )
    resp.raise_for_status()

    content_type = resp.headers.get('Content-Type', '').split(';')[0].strip()
    logger.info(f'Downloaded Twilio media: {len(resp.content)} bytes, type={content_type}')
    return resp.content, content_type
