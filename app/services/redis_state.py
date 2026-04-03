"""
Redis State Manager

Lightweight helper to persist cross-worker state for sessions.
Used for:
- Analysis completion flags
- ITN planning completion flags
- Conversation state (slots, last options, etc.)

Best-effort: if Redis is unavailable, methods degrade gracefully.
"""

import os
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class _RedisStateManager:
    def __init__(self):
        self._client = None
        self._connect()

    def _connect(self):
        try:
            import redis  # type: ignore
            host = os.environ.get('REDIS_HOST', 'localhost')
            port = int(os.environ.get('REDIS_PORT', '6379'))
            password = os.environ.get('REDIS_PASSWORD')
            db = int(os.environ.get('REDIS_DB', '0'))
            self._client = redis.StrictRedis(
                host=host,
                port=port,
                password=password,
                db=db,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True,
            )
            # quick ping
            self._client.ping()
            logger.info(f"RedisStateManager connected to {host}:{port}")
        except Exception as e:
            logger.warning(f"RedisStateManager unavailable: {e}")
            self._client = None

    # --------- Keys ---------
    def _key(self, session_id: str, name: str) -> str:
        return f"chatmrpt:{name}:{session_id}"

    # --------- Analysis Flags ---------
    def mark_analysis_complete(self, session_id: str) -> bool:
        if not self._client:
            return False
        try:
            self._client.set(self._key(session_id, 'analysis_complete'), '1', ex=86400)
            return True
        except Exception as e:
            logger.debug(f"mark_analysis_complete failed: {e}")
            return False

    def is_analysis_complete(self, session_id: str) -> bool:
        if not self._client:
            return False
        try:
            return self._client.get(self._key(session_id, 'analysis_complete')) == '1'
        except Exception:
            return False

    # --------- ITN Flags ---------
    def mark_itn_planning_complete(self, session_id: str) -> bool:
        if not self._client:
            return False
        try:
            self._client.set(self._key(session_id, 'itn_complete'), '1', ex=86400)
            return True
        except Exception as e:
            logger.debug(f"mark_itn_planning_complete failed: {e}")
            return False

    def is_itn_planning_complete(self, session_id: str) -> bool:
        if not self._client:
            return False
        try:
            return self._client.get(self._key(session_id, 'itn_complete')) == '1'
        except Exception:
            return False

    # --------- Conversation State ---------
    def set_conversation_state(self, session_id: str, convo_state: Dict[str, Any]) -> bool:
        if not self._client:
            return False
        try:
            self._client.set(self._key(session_id, 'conversation_state'), json.dumps(convo_state), ex=86400)
            return True
        except Exception as e:
            logger.debug(f"set_conversation_state failed: {e}")
            return False

    def get_conversation_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        if not self._client:
            return None
        try:
            raw = self._client.get(self._key(session_id, 'conversation_state'))
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None

    # --------- Custom Data ---------
    def set_custom_data(self, session_id: str, key: str, value: Any) -> bool:
        if not self._client:
            return False
        try:
            self._client.set(self._key(session_id, f"custom:{key}"), json.dumps(value), ex=86400)
            return True
        except Exception:
            return False

    def get_custom_data(self, session_id: str, key: str) -> Optional[Any]:
        if not self._client:
            return None
        try:
            raw = self._client.get(self._key(session_id, f"custom:{key}"))
            return json.loads(raw) if raw else None
        except Exception:
            return None


_singleton: Optional[_RedisStateManager] = None


def get_redis_state_manager() -> _RedisStateManager:
    global _singleton
    if _singleton is None:
        _singleton = _RedisStateManager()
    return _singleton

