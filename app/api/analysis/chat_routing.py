"""Routing logic for analysis chat requests."""

from __future__ import annotations

import os
import logging

from . import logger

__all__ = ["route_with_mistral"]


async def route_with_mistral(message: str, session_context: dict) -> str:
    """Route a message to the appropriate handler.

    Uses semantic routing when enabled, with fallback to pattern-based routing.

    Args:
        message: User's message text
        session_context: Session state dictionary

    Returns:
        One of: 'needs_tools', 'can_answer', 'needs_clarification'
    """
    # Check if semantic router is enabled
    if _is_semantic_router_enabled():
        try:
            return await _route_with_semantic_router(message, session_context)
        except Exception as e:
            logger.error("Semantic router failed, falling back to patterns: %s", e)
            # Fall through to pattern-based routing

    # Pattern-based routing (legacy fallback)
    return _route_with_patterns(message, session_context)


def _is_semantic_router_enabled() -> bool:
    """Check if semantic router is enabled."""
    return os.getenv("SEMANTIC_ROUTER_ENABLED", "true").lower() == "true"


async def _route_with_semantic_router(message: str, session_context: dict) -> str:
    """Route using the semantic router."""
    from app.agent.semantic_router import SemanticChatRouter, get_semantic_router

    router = get_semantic_router()
    result = router.route(message, session_context)

    logger.info(
        "Semantic routing: '%s...' -> %s (route=%s, conf=%.3f)",
        message[:30],
        result.maps_to,
        result.route_name,
        result.confidence,
    )

    return result.maps_to


def _route_with_patterns(message: str, session_context: dict) -> str:
    """Simplified fallback routing (used when semantic router is disabled).

    The LLM handles intent classification — this fallback only needs to make
    two decisions: (1) is this a trivial greeting? (2) is data loaded?
    """
    message_lower = message.lower().strip()

    # Greetings and acknowledgments → can_answer
    if message_lower in ['hi', 'hello', 'hey', 'thanks', 'thank you', 'bye', 'ok', 'okay']:
        return "can_answer"

    # If data is loaded (any mode), route to tools — LLM decides what to do
    if (session_context.get('use_data_analysis_v3', False) or
        session_context.get('data_analysis_active', False) or
        session_context.get('has_uploaded_files', False)):
        return "needs_tools"

    # Default: can_answer (LLM handles it)

    return "can_answer"
