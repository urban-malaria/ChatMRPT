"""
Redis-backed WhatsApp session manager.

Maps a WhatsApp phone number to a ChatMRPT session_id and maintains
per-user conversation history. Uses the same Redis instance as the
main app so state is shared across both EC2 instances.

Keys:
  wa_session:{phone}  → session_id (string, TTL 24h)
  wa_history:{phone}  → JSON list of {role, content} dicts (TTL 24h)
"""

import json
import logging
import uuid
from typing import Optional

logger = logging.getLogger(__name__)

_TTL = 86400  # 24 hours — matches WhatsApp service conversation window
_MAX_HISTORY = 20  # keep last 10 exchanges (20 messages)


class WhatsAppSessionManager:
    def __init__(self, redis_client):
        self.redis = redis_client

    # ------------------------------------------------------------------ #
    #  Session ID
    # ------------------------------------------------------------------ #

    def get_or_create_session(self, phone: str) -> str:
        """Return existing session_id for this phone number, or create one."""
        key = f'wa_session:{phone}'
        session_id = self.redis.get(key)
        if session_id:
            self.redis.expire(key, _TTL)
            return session_id.decode() if isinstance(session_id, bytes) else session_id

        session_id = str(uuid.uuid4())
        self.redis.setex(key, _TTL, session_id)
        logger.info(f'New WhatsApp session created: {phone} → {session_id}')
        return session_id

    def get_session_id(self, phone: str) -> Optional[str]:
        key = f'wa_session:{phone}'
        val = self.redis.get(key)
        if val:
            return val.decode() if isinstance(val, bytes) else val
        return None

    # ------------------------------------------------------------------ #
    #  Conversation history
    # ------------------------------------------------------------------ #

    def get_history(self, phone: str) -> list[dict]:
        key = f'wa_history:{phone}'
        raw = self.redis.get(key)
        if not raw:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def append_history(self, phone: str, role: str, content: str) -> None:
        key = f'wa_history:{phone}'
        history = self.get_history(phone)
        history.append({'role': role, 'content': content})
        if len(history) > _MAX_HISTORY:
            history = history[-_MAX_HISTORY:]
        self.redis.setex(key, _TTL, json.dumps(history))

    def clear_session(self, phone: str) -> None:
        self.redis.delete(f'wa_session:{phone}')
        self.redis.delete(f'wa_history:{phone}')
        logger.info(f'WhatsApp session cleared for {phone}')
