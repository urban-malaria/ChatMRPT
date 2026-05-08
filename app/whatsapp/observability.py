"""Structured WhatsApp logging helpers for CloudWatch metric filters."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("app.whatsapp")


def log_event(event: str, level: int = logging.INFO, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logger.log(level, "whatsapp_event=%s payload=%s", event, json.dumps(payload, default=str, sort_keys=True))
