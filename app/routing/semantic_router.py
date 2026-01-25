"""Semantic router for ChatMRPT chat requests.

Uses embedding similarity to route user messages to the appropriate handler,
with context-aware biasing and LLM fallback for edge cases.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

from .context_bias import ContextBiaser
from .encoder import BaseEncoder, get_encoder
from .fallback_router import FallbackRouter, get_fallback_router
from .route_definitions import ROUTES, Route

logger = logging.getLogger(__name__)


@dataclass
class RouteResult:
    """Result of routing a message."""

    route_name: str
    confidence: float
    maps_to: str  # needs_tools, can_answer, needs_clarification
    all_scores: Dict[str, float] = field(default_factory=dict)
    used_fallback: bool = False
    latency_ms: float = 0.0


class SemanticChatRouter:
    """Semantic router using embedding similarity for intent classification."""

    def __init__(
        self,
        encoder: Optional[BaseEncoder] = None,
        biaser: Optional[ContextBiaser] = None,
        fallback_router: Optional[FallbackRouter] = None,
        confidence_threshold: float = 0.3,
    ):
        """Initialize the semantic router.

        Args:
            encoder: Text encoder for embeddings (default: auto-detect)
            biaser: Context biaser instance (default: new instance)
            fallback_router: LLM fallback for low confidence (default: auto-detect)
            confidence_threshold: Below this, use LLM fallback
        """
        self.encoder = encoder
        self.biaser = biaser or ContextBiaser()
        self.fallback_router = fallback_router
        self.confidence_threshold = confidence_threshold

        # Route embeddings (computed lazily)
        self._route_embeddings: Dict[str, np.ndarray] = {}
        self._routes_by_name: Dict[str, Route] = {}
        self._initialized = False

    def _ensure_initialized(self):
        """Lazy initialization of encoder and route embeddings."""
        if self._initialized:
            return

        # Initialize encoder if not provided
        if self.encoder is None:
            self.encoder = get_encoder()

        # Initialize fallback router if not provided
        if self.fallback_router is None:
            self.fallback_router = get_fallback_router()

        # Build route lookup
        for route in ROUTES:
            self._routes_by_name[route.name] = route

        # Compute embeddings for all route utterances
        logger.info("Computing route embeddings...")
        start = time.time()

        for route in ROUTES:
            if route.utterances:
                embeddings = self.encoder.encode(route.utterances)
                # Store mean embedding for each route
                self._route_embeddings[route.name] = embeddings.mean(axis=0)

        elapsed = (time.time() - start) * 1000
        logger.info(
            "Route embeddings computed in %.1fms for %d routes",
            elapsed,
            len(self._route_embeddings),
        )

        self._initialized = True

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Compute cosine similarity between two vectors."""
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    def _score_routes(self, message_embedding: np.ndarray) -> Dict[str, float]:
        """Score all routes against the message embedding."""
        scores = {}
        for route_name, route_embedding in self._route_embeddings.items():
            scores[route_name] = self._cosine_similarity(
                message_embedding, route_embedding
            )
        return scores

    def route(self, message: str, session_context: Dict) -> RouteResult:
        """Route a message to the appropriate handler.

        Args:
            message: User's message text
            session_context: Session state dictionary

        Returns:
            RouteResult with route name, confidence, and mapping
        """
        start_time = time.time()
        self._ensure_initialized()

        # Encode the message
        message_embedding = self.encoder.encode([message])[0]

        # Score against all routes
        raw_scores = self._score_routes(message_embedding)

        # Apply context bias
        biased_scores = self.biaser.apply_bias(raw_scores, session_context)

        # Find best route
        best_route = max(biased_scores.items(), key=lambda x: x[1])
        route_name, confidence = best_route

        # Check if confidence is too low
        used_fallback = False
        if confidence < self.confidence_threshold and self.fallback_router:
            logger.info(
                "Low confidence (%.3f < %.3f), using LLM fallback",
                confidence,
                self.confidence_threshold,
            )
            fallback_route = self.fallback_router.route(
                message, session_context, biased_scores
            )
            if fallback_route:
                route_name = fallback_route
                used_fallback = True
                # Recalculate confidence as the score for the fallback route
                confidence = biased_scores.get(route_name, confidence)

        # Get the route details
        route = self._routes_by_name.get(route_name)
        maps_to = route.maps_to if route else "needs_clarification"

        latency_ms = (time.time() - start_time) * 1000

        logger.info(
            "Routed '%s...' -> %s (conf=%.3f, maps_to=%s, fallback=%s, %.1fms)",
            message[:40],
            route_name,
            confidence,
            maps_to,
            used_fallback,
            latency_ms,
        )

        return RouteResult(
            route_name=route_name,
            confidence=confidence,
            maps_to=maps_to,
            all_scores=biased_scores,
            used_fallback=used_fallback,
            latency_ms=latency_ms,
        )


# Singleton instance
_router_instance: Optional[SemanticChatRouter] = None


def get_semantic_router() -> SemanticChatRouter:
    """Get or create the singleton semantic router instance."""
    global _router_instance
    if _router_instance is None:
        _router_instance = SemanticChatRouter()
    return _router_instance


def is_semantic_router_enabled() -> bool:
    """Check if semantic router is enabled via environment variable."""
    return os.getenv("SEMANTIC_ROUTER_ENABLED", "true").lower() == "true"
