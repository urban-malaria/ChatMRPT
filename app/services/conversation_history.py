"""
Conversation History Service for ChatMRPT.

Stores conversation metadata (title, last activity, preview) to enable
users to list and resume past conversations. Uses Redis when available
(production) with filesystem fallback (local dev).

Redis key layout:
    user:{user_id}:conversations           -> Sorted set (score=timestamp, member=session_id)
    conversation:{session_id}:meta         -> Hash {title, created_at, last_activity, has_files, preview, user_id}
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ConversationHistoryService:
    """Manages conversation metadata for the chat history sidebar."""

    def __init__(self, redis_client=None, storage_path: str = "instance/conversation_history"):
        self._redis = redis_client
        self._storage_path = storage_path

        if not self._redis:
            os.makedirs(storage_path, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_conversation(self, user_id: str, session_id: str, title: Optional[str] = None) -> None:
        """Register a new conversation for a user."""
        now = time.time()
        meta = {
            "title": title or "New conversation",
            "created_at": str(now),
            "last_activity": str(now),
            "has_files": "0",
            "preview": "",
            "user_id": user_id,
        }

        if self._redis:
            pipe = self._redis.pipeline()
            pipe.zadd(self._user_key(user_id), {session_id: now})
            # Redis client has decode_responses=False — encode all values to bytes
            encoded = {k.encode(): str(v).encode() for k, v in meta.items()}
            pipe.hset(self._meta_key(session_id), mapping=encoded)
            pipe.execute()
        else:
            self._fs_update(user_id, session_id, meta, register=True)

        logger.debug("Registered conversation %s for user %s", session_id, user_id)

    def update_activity(self, user_id: str, session_id: str, preview: Optional[str] = None, has_files: bool = False) -> None:
        """Update last-activity timestamp and optional preview text."""
        now = time.time()

        if self._redis:
            pipe = self._redis.pipeline()
            pipe.zadd(self._user_key(user_id), {session_id: now})
            updates: Dict[bytes | str, bytes | str] = {b"last_activity": str(now).encode()}
            if preview is not None:
                updates[b"preview"] = preview[:120].encode()
            if has_files:
                updates[b"has_files"] = b"1"
            pipe.hset(self._meta_key(session_id), mapping=updates)
            pipe.execute()
        else:
            changes: Dict[str, str] = {"last_activity": str(now)}
            if preview is not None:
                changes["preview"] = preview[:120]
            if has_files:
                changes["has_files"] = "1"
            self._fs_update(user_id, session_id, changes)

    def set_title(self, user_id: str, session_id: str, title: str) -> None:
        """Set the conversation title (usually derived from the first user message)."""
        if self._redis:
            self._redis.hset(self._meta_key(session_id), b"title", title.encode())
        else:
            self._fs_update(user_id, session_id, {"title": title})

    def list_conversations(self, user_id: str, limit: int = 30, offset: int = 0) -> List[Dict[str, Any]]:
        """Return recent conversations for a user, newest first."""
        if self._redis:
            return self._redis_list(user_id, limit, offset)
        return self._fs_list(user_id, limit, offset)

    def get_conversation_meta(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return metadata hash for a single conversation."""
        if self._redis:
            raw = self._redis.hgetall(self._meta_key(session_id))
            if not raw:
                return None
            return {self._decode(k): self._decode(v) for k, v in raw.items()}
        return self._fs_get_meta(session_id)

    def conversation_belongs_to_user(self, user_id: str, session_id: str) -> bool:
        """Check whether a session_id belongs to a given user."""
        if self._redis:
            return self._redis.zscore(self._user_key(user_id), session_id) is not None
        return self._fs_belongs_to_user(user_id, session_id)

    # ------------------------------------------------------------------
    # Redis helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _user_key(user_id: str) -> str:
        return f"user:{user_id}:conversations"

    @staticmethod
    def _meta_key(session_id: str) -> str:
        return f"conversation:{session_id}:meta"

    @staticmethod
    def _decode(value) -> str:
        """Decode bytes returned by Redis (decode_responses=False)."""
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    def _redis_list(self, user_id: str, limit: int, offset: int) -> List[Dict[str, Any]]:
        session_ids = self._redis.zrevrange(self._user_key(user_id), offset, offset + limit - 1)
        if not session_ids:
            return []

        pipe = self._redis.pipeline()
        for sid in session_ids:
            pipe.hgetall(self._meta_key(self._decode(sid)))
        results = pipe.execute()

        conversations = []
        for sid, raw_meta in zip(session_ids, results):
            if not raw_meta:
                continue
            meta = {self._decode(k): self._decode(v) for k, v in raw_meta.items()}
            meta["session_id"] = self._decode(sid)
            conversations.append(meta)

        return conversations

    # ------------------------------------------------------------------
    # Filesystem fallback (local dev)
    # ------------------------------------------------------------------

    def _user_file(self, user_id: str) -> str:
        safe_id = user_id.replace("/", "_").replace(":", "_")
        return os.path.join(self._storage_path, f"{safe_id}.json")

    def _fs_load(self, user_id: str) -> Dict[str, Dict[str, Any]]:
        """Load {session_id: meta} mapping from disk."""
        path = self._user_file(user_id)
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _fs_save(self, user_id: str, data: Dict[str, Dict[str, Any]]) -> None:
        with open(self._user_file(user_id), "w") as f:
            json.dump(data, f, indent=2)

    def _fs_update(self, user_id: str, session_id: str, updates: Dict[str, str], register: bool = False) -> None:
        data = self._fs_load(user_id)
        if register or session_id not in data:
            data.setdefault(session_id, {})
        if session_id in data:
            data[session_id].update(updates)
        self._fs_save(user_id, data)

    def _fs_list(self, user_id: str, limit: int, offset: int) -> List[Dict[str, Any]]:
        data = self._fs_load(user_id)
        items = [
            {**meta, "session_id": sid}
            for sid, meta in data.items()
        ]
        items.sort(key=lambda x: float(x.get("last_activity", 0)), reverse=True)
        return items[offset: offset + limit]

    def _fs_get_meta(self, session_id: str) -> Optional[Dict[str, Any]]:
        # Scan all user files to find the session (only used in local dev)
        for fname in os.listdir(self._storage_path):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(self._storage_path, fname)
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                if session_id in data:
                    return data[session_id]
            except Exception:
                continue
        return None

    def _fs_belongs_to_user(self, user_id: str, session_id: str) -> bool:
        data = self._fs_load(user_id)
        return session_id in data


def generate_title(message: str) -> str:
    """Generate a conversation title from the first user message.

    Strips common greetings, truncates to 60 chars at a word boundary.
    """
    text = message.strip()

    # Strip leading greetings
    greetings = ["hello", "hi", "hey", "good morning", "good afternoon", "good evening"]
    lower = text.lower()
    for g in greetings:
        if lower.startswith(g):
            text = text[len(g):].lstrip(" ,!.")
            break

    text = text.strip()
    if not text:
        return "New conversation"

    if len(text) <= 60:
        return text

    # Truncate at word boundary
    truncated = text[:60]
    last_space = truncated.rfind(" ")
    if last_space > 20:
        truncated = truncated[:last_space]
    return truncated + "..."


def get_user_id() -> str:
    """Best-effort extraction of a user identifier from the current request context."""
    from flask import session, request

    # 1. Flask-Login current user
    try:
        from flask_login import current_user
        if hasattr(current_user, "is_authenticated") and current_user.is_authenticated:
            return str(current_user.id)
    except Exception:
        pass

    # 2. User ID stored in Flask session (set by auth flow)
    uid = session.get("user_id")
    if uid:
        return str(uid)

    # 3. Bearer token
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            from app.auth.user_model import User
            user = User.verify_session_token(auth_header[7:])
            if user:
                return str(user.id)
    except Exception:
        pass

    # 4. Fallback for local dev (DISABLE_AUTH) — stable ID so all
    #    conversations appear under one user in development
    return "local-dev-user"
