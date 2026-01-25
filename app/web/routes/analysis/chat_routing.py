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
    from app.routing import SemanticChatRouter, get_semantic_router

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
    """Legacy pattern-based routing (fallback).

    This preserves the original routing logic for when semantic routing
    is disabled or fails.
    """
    message_lower = message.lower().strip()

    # Quick greetings check
    common_greetings = [
        'hi', 'hello', 'hey', 'greetings',
        'good morning', 'good afternoon', 'good evening', 'howdy',
    ]
    if message_lower in common_greetings or any(
        message_lower.startswith(g) for g in common_greetings
    ):
        return "can_answer"

    # Quick acknowledgments check
    if message_lower in [
        'thanks', 'thank you', 'bye', 'goodbye', 'ok', 'okay', 'sure', 'yes', 'no'
    ]:
        return "can_answer"

    # Data analysis mode takes priority
    if session_context.get('use_data_analysis_v3', False) or session_context.get(
        'data_analysis_active', False
    ):
        logger.info("Data Analysis mode detected - routing to tools")
        return "needs_tools"

    # Check for explicit data references (should route to tools)
    data_references = ['my data', 'the data', 'my file', 'uploaded', 'my csv', 'in my']
    references_user_data = any(ref in message_lower for ref in data_references)

    # Check for result queries when analysis is complete
    if session_context.get('analysis_complete', False):
        result_queries = [
            'top', 'highest', 'lowest', 'rank', 'ranked',
            'best', 'worst', 'most at risk', 'least at risk',
            'results', 'findings', 'show me', 'what are the',
        ]
        if any(q in message_lower for q in result_queries):
            logger.info("Result query detected with analysis complete - routing to tools")
            return "needs_tools"

    # Has uploaded files - check for tool triggers
    if session_context.get('has_uploaded_files', False) or references_user_data:
        # Analysis triggers
        analysis_triggers = [
            'run', 'analyze', 'analysis', 'calculate', 'compute',
            'process', 'start', 'perform', 'check',
        ]
        if any(trigger in message_lower for trigger in analysis_triggers):
            logger.info("Analysis trigger detected - routing to tools")
            return "needs_tools"

        # Visualization triggers
        viz_triggers = [
            'plot', 'map', 'chart', 'visualize', 'graph',
            'show me', 'display', 'histogram', 'heatmap',
        ]
        if any(trigger in message_lower for trigger in viz_triggers):
            logger.info("Visualization trigger detected - routing to tools")
            return "needs_tools"

        # Ranking triggers
        if any(word in message_lower for word in ['top', 'highest', 'lowest', 'rank', 'worst', 'best']):
            if 'ward' in message_lower or 'lga' in message_lower or 'area' in message_lower:
                logger.info("Ranking query detected - routing to tools")
                return "needs_tools"

        # ITN triggers
        itn_triggers = [
            'bed net', 'bednet', 'itn', 'llin', 'net distribution',
            'intervention', 'mosquito net', 'allocate', 'distribute',
        ]
        if any(trigger in message_lower for trigger in itn_triggers):
            logger.info("ITN planning trigger detected - routing to tools")
            return "needs_tools"

    # Knowledge patterns (only if not referencing user data)
    if not references_user_data and not session_context.get('analysis_complete', False):
        knowledge_patterns = [
            'what is', 'what are', 'what does', 'what causes',
            'how does', 'how do', 'how is', 'how are', 'how can',
            'why is', 'why does', 'why do', 'why are',
            'explain', 'describe', 'tell me about', 'tell me more',
        ]
        is_knowledge_question = any(
            message_lower.startswith(pattern) or f' {pattern}' in f' {message_lower}'
            for pattern in knowledge_patterns
        )

        concept_keywords = [
            'malaria transmission', 'malaria prevention', 'malaria symptoms',
            'pca analysis', 'principal component', 'vulnerability index',
        ]
        is_concept_question = any(concept in message_lower for concept in concept_keywords)

        if is_knowledge_question or is_concept_question:
            logger.info("Knowledge question detected - routing to arena")
            return "can_answer"

    # Default: if files uploaded, route to tools; otherwise to arena
    if session_context.get('has_uploaded_files', False):
        return "needs_tools"

    return "can_answer"
