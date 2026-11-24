"""Arena integration helpers for chat flow.

Provides functions to trigger arena mode from the chat streaming endpoints
when general knowledge questions are detected.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, Any, Optional

from app.config.arena import is_arena_available

logger = logging.getLogger(__name__)


class ArenaSetupError(Exception):
    """Raised when arena setup fails."""
    pass


# Questions about ChatMRPT itself, help with using it, or the user's own data always get one answer.
APP_QUESTION = re.compile(
    r"\b(chat\s?mrpt|mrpt|this (tool|app|site|platform|system|assistant)|how (do|does|can) (this|it|you|i)\b"
    r"|what (can|do|does) (you|this|it)\b|who are you|what are you|help me|get(ting)? started|where do i"
    r"|how do i|can i\b|upload|my (data|file|results?|wards?|state)|sample data|sample dataset|no data"
    r"|(don'?t|do not) have (any )?data|what data|which data|data do i need|download|export|map|chart"
    r"|workflow|next step|what next|tpr workflow)\b",
    re.IGNORECASE,
)
APP_TOPICS = {'about-chatmrpt', 'workflow', 'data-requirements', 'outputs', 'arena'}


def is_arena_eligible_message(message: str, is_first_message: bool = False) -> bool:
    """Whether a question goes to Arena (several models answer and the user votes).

    Arena is only for general malaria and public-health knowledge. A conversation's first message,
    greetings and confirmations, and questions about ChatMRPT, how to use it or the user's own data
    get a single answer.
    """
    text = message.strip()
    if is_first_message:
        return False
    if len(text.split()) < 3:
        return False
    if text.lower() in ['yes', 'no', 'ok', 'okay', 'sure', 'thanks', 'bye']:
        return False
    if APP_QUESTION.search(text):
        return False
    try:
        from app.knowledge import find
        if any(entry.id in APP_TOPICS for entry in find(text)):
            return False
    except Exception:
        pass
    return True


def start_arena_battle(user_message: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Start an arena battle for the given user message.

    Args:
        user_message: The user's question
        session_id: Optional session ID

    Returns:
        Arena battle result dict with battle_id, model_a/b, response_a/b, etc.

    Raises:
        ArenaSetupError: If arena is unavailable or fails to start
    """
    if not is_arena_available():
        raise ArenaSetupError("Arena is disabled or GROQ_API_KEY not set")

    try:
        # Use the shared arena_manager from arena_routes (not a new instance!)
        from app.api.arena_routes import arena_manager

        if arena_manager is None:
            # Initialize if not yet done
            from flask import current_app
            from app.api.arena_routes import init_arena_system
            init_arena_system(current_app)
            from app.api.arena_routes import arena_manager

        logger.info(f"Starting arena battle for session {session_id}: '{user_message[:50]}...'")

        result = arena_manager.start_battle(user_message, session_id)

        if 'error' in result:
            raise ArenaSetupError(result['error'])

        return result

    except ArenaSetupError:
        raise
    except Exception as e:
        logger.error(f"Arena battle failed: {e}")
        raise ArenaSetupError(f"Failed to start arena battle: {e}")


def format_arena_response(battle_result: Dict[str, Any], user_message: str) -> Dict[str, Any]:
    """
    Format arena battle result for streaming response.

    Args:
        battle_result: Result from start_arena_battle()
        user_message: Original user message

    Returns:
        Dict formatted for frontend ArenaMessage component
    """
    return {
        'arena_mode': True,
        'battle_id': battle_result.get('battle_id'),
        'status': 'ready',
        'model_a': battle_result.get('model_a'),
        'model_b': battle_result.get('model_b'),
        'response_a': battle_result.get('response_a', ''),
        'response_b': battle_result.get('response_b', ''),
        'current_round': battle_result.get('current_round', 1),
        'total_models': battle_result.get('total_models', 4),
        'user_message': user_message,
        'done': True,
    }
