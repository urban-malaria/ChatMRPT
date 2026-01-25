"""Arena integration helpers for chat flow.

Provides functions to trigger arena mode from the chat streaming endpoints
when general knowledge questions are detected.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional

from app.config.arena import is_arena_available

logger = logging.getLogger(__name__)


class ArenaSetupError(Exception):
    """Raised when arena setup fails."""
    pass


def is_arena_eligible_message(message: str) -> bool:
    """
    Check if a message is eligible for arena mode.

    Arena should trigger for substantive general knowledge questions,
    but NOT for:
    - Greetings (hi, hello)
    - Pleasantries (thanks, bye, ok)
    - Very short messages (< 3 words)
    - Follow-up acknowledgments

    Args:
        message: The user's message

    Returns:
        True if message should trigger arena mode
    """
    message_lower = message.lower().strip()

    # Skip greetings
    greetings = [
        'hi', 'hello', 'hey', 'greetings', 'good morning',
        'good afternoon', 'good evening', 'howdy', 'hiya',
    ]
    if message_lower in greetings or any(message_lower.startswith(g + ' ') for g in greetings):
        return False

    # Skip pleasantries and acknowledgments
    pleasantries = [
        'thanks', 'thank you', 'bye', 'goodbye', 'ok', 'okay',
        'sure', 'yes', 'no', 'got it', 'i see', 'understood',
        'alright', 'great', 'cool', 'nice', 'perfect', 'awesome',
    ]
    if message_lower in pleasantries:
        return False

    # Skip very short messages (likely not substantive questions)
    word_count = len(message.split())
    if word_count < 3:
        return False

    # Skip messages that are just follow-up confirmations
    confirmation_patterns = [
        'yes please', 'no thanks', 'that works', 'sounds good',
        'go ahead', 'do it', 'proceed', 'continue',
    ]
    if message_lower in confirmation_patterns:
        return False

    # Message is eligible for arena
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
        from app.core.arena_manager import ArenaManager
        arena_manager = ArenaManager()

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
