"""Helpers for deriving per-conversation session scope."""

import uuid
from typing import Optional

from flask import g, session


def ensure_base_session_id() -> str:
    """Return the stable browser session id used as the base for conversations."""
    base_session_id = session.get("base_session_id")
    session_id = session.get("session_id")

    if not base_session_id and session_id:
        base_session_id = session_id.split("__", 1)[0] if "__" in session_id else session_id

    if not base_session_id:
        base_session_id = str(uuid.uuid4())

    session["base_session_id"] = base_session_id
    if not session_id:
        session["session_id"] = base_session_id

    return base_session_id


def get_effective_session_id(conversation_id: Optional[str] = None) -> str:
    """Return the request-scoped session id for the active conversation."""
    active_conversation = conversation_id or getattr(g, "conversation_id", None)
    base_session_id = ensure_base_session_id()

    if active_conversation:
        return f"{base_session_id}__{active_conversation}"

    return session.get("session_id") or base_session_id
