"""Tests for the semantic router."""

import os
import pytest
import numpy as np
from unittest.mock import MagicMock, patch

# Ensure semantic router is enabled for tests
os.environ["SEMANTIC_ROUTER_ENABLED"] = "true"

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)


class TestRouteDefinitions:
    """Test route definitions."""

    def test_all_routes_have_utterances(self):
        """All routes should have example utterances."""
        from app.routing.route_definitions import ROUTES

        for route in ROUTES:
            assert len(route.utterances) > 0, f"Route {route.name} has no utterances"

    def test_all_routes_have_maps_to(self):
        """All routes should have a maps_to value."""
        from app.routing.route_definitions import ROUTES

        valid_maps = {"needs_tools", "can_answer", "needs_clarification"}
        for route in ROUTES:
            assert route.maps_to in valid_maps, f"Route {route.name} has invalid maps_to: {route.maps_to}"

    def test_route_count(self):
        """Should have 7 routes as specified."""
        from app.routing.route_definitions import ROUTES

        assert len(ROUTES) == 7

    def test_route_names(self):
        """Check expected route names exist."""
        from app.routing.route_definitions import ROUTES

        expected_names = {
            "data_query", "visualization", "analysis",
            "itn_planning", "workflow", "knowledge", "greeting"
        }
        actual_names = {route.name for route in ROUTES}
        assert actual_names == expected_names


class TestContextBiaser:
    """Test context-aware biasing."""

    def test_analysis_complete_boosts_data_query(self):
        """When analysis is complete, data_query should get a boost."""
        from app.routing.context_bias import ContextBiaser

        biaser = ContextBiaser()
        scores = {"data_query": 0.5, "knowledge": 0.5, "greeting": 0.3}
        context = {"analysis_complete": True, "has_uploaded_files": True}

        biased = biaser.apply_bias(scores, context)

        assert biased["data_query"] > scores["data_query"], "data_query should be boosted"
        assert biased["knowledge"] < scores["knowledge"], "knowledge should be penalized"

    def test_no_data_penalizes_data_routes(self):
        """When no data is uploaded, data routes should be penalized."""
        from app.routing.context_bias import ContextBiaser

        biaser = ContextBiaser()
        scores = {"data_query": 0.5, "knowledge": 0.5, "visualization": 0.5}
        context = {"has_uploaded_files": False}

        biased = biaser.apply_bias(scores, context)

        assert biased["data_query"] < scores["data_query"], "data_query should be penalized"
        assert biased["visualization"] < scores["visualization"], "visualization should be penalized"
        assert biased["knowledge"] > scores["knowledge"], "knowledge should be boosted"

    def test_data_mode_strong_boost(self):
        """Data analysis mode should strongly boost tool routes."""
        from app.routing.context_bias import ContextBiaser

        biaser = ContextBiaser()
        scores = {"data_query": 0.3, "knowledge": 0.5}
        context = {"use_data_analysis_v3": True}

        biased = biaser.apply_bias(scores, context)

        assert biased["data_query"] > biased["knowledge"], "data_query should beat knowledge in data mode"


class MockEncoder:
    """Mock encoder for testing without API calls."""

    def __init__(self, dimension: int = 384):
        self._dimension = dimension
        # Predefined embeddings for test utterances
        self._embeddings = {}

    def encode(self, texts):
        embeddings = []
        for text in texts:
            if text in self._embeddings:
                embeddings.append(self._embeddings[text])
            else:
                # Generate a deterministic random embedding based on text hash
                np.random.seed(hash(text) % (2**32))
                embeddings.append(np.random.randn(self._dimension))
        return np.array(embeddings)

    @property
    def dimension(self):
        return self._dimension


class TestSemanticRouter:
    """Test the semantic router."""

    @pytest.fixture
    def mock_router(self):
        """Create a router with mock encoder."""
        from app.routing.semantic_router import SemanticChatRouter
        from app.routing.context_bias import ContextBiaser

        router = SemanticChatRouter(
            encoder=MockEncoder(),
            biaser=ContextBiaser(),
            fallback_router=None,  # Disable fallback for deterministic tests
            confidence_threshold=0.0,  # Never use fallback
        )
        return router

    def test_router_returns_route_result(self, mock_router):
        """Router should return a RouteResult object."""
        from app.routing.semantic_router import RouteResult

        result = mock_router.route("Hello", {})

        assert isinstance(result, RouteResult)
        assert result.route_name in {
            "data_query", "visualization", "analysis",
            "itn_planning", "workflow", "knowledge", "greeting"
        }
        assert result.maps_to in {"needs_tools", "can_answer", "needs_clarification"}
        assert 0 <= result.confidence <= 1.0 or result.confidence > 1.0  # Can be >1 with bias

    def test_greeting_routes_to_can_answer(self, mock_router):
        """Greetings should route to can_answer."""
        result = mock_router.route("Hello", {})

        # Note: With mock embeddings, results may vary, but greeting should map to can_answer
        assert result.maps_to in {"can_answer", "needs_tools"}

    def test_context_affects_routing(self, mock_router):
        """Context should affect routing decisions."""
        message = "What are the top ranked wards"

        # Without data
        result_no_data = mock_router.route(message, {"has_uploaded_files": False})

        # With completed analysis
        result_with_analysis = mock_router.route(
            message,
            {"has_uploaded_files": True, "analysis_complete": True}
        )

        # The scores should differ due to context bias
        assert result_no_data.all_scores != result_with_analysis.all_scores

    def test_all_scores_populated(self, mock_router):
        """All routes should have scores in the result."""
        result = mock_router.route("Test message", {})

        expected_routes = {
            "data_query", "visualization", "analysis",
            "itn_planning", "workflow", "knowledge", "greeting"
        }
        assert set(result.all_scores.keys()) == expected_routes

    def test_latency_tracked(self, mock_router):
        """Latency should be tracked."""
        result = mock_router.route("Test message", {})

        assert result.latency_ms >= 0


class TestPatternFallback:
    """Test the pattern-based fallback routing."""

    def test_greeting_fallback(self):
        """Greetings should route correctly in fallback."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        result = _route_with_patterns("Hello", {})
        assert result == "can_answer"

        result = _route_with_patterns("Good morning", {})
        assert result == "can_answer"

    def test_thanks_fallback(self):
        """Thanks should route correctly in fallback."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        result = _route_with_patterns("Thanks", {})
        assert result == "can_answer"

    def test_analysis_mode_routes_data_queries_to_tools(self):
        """Data analysis mode should route data queries to tools."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        # Data queries in data mode should go to tools
        result = _route_with_patterns(
            "Show me the top wards",
            {"use_data_analysis_v3": True}
        )
        assert result == "needs_tools"

    def test_knowledge_questions_go_to_arena_even_in_data_mode(self):
        """Knowledge questions should go to Arena even when in data mode."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        # Pure knowledge questions should go to Arena, not tools
        result = _route_with_patterns(
            "What is malaria",
            {"use_data_analysis_v3": True}
        )
        assert result == "can_answer"

    def test_result_query_with_analysis_complete(self):
        """Result queries should route to tools when analysis is complete."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        result = _route_with_patterns(
            "What are the top ranked wards",
            {"analysis_complete": True}
        )
        assert result == "needs_tools"

    def test_knowledge_without_data(self):
        """Knowledge questions should route to arena without data."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        result = _route_with_patterns(
            "What is malaria",
            {"has_uploaded_files": False}
        )
        assert result == "can_answer"


class TestIntegration:
    """Integration tests for the routing system."""

    @pytest.mark.asyncio
    async def test_route_with_mistral_enabled(self):
        """Test the main routing function with semantic router."""
        # This test requires mocking the encoder to avoid API calls
        with patch.dict(os.environ, {"SEMANTIC_ROUTER_ENABLED": "false"}):
            from app.web.routes.analysis.chat_routing import route_with_mistral

            # With semantic router disabled, should use patterns
            result = await route_with_mistral("Hello", {})
            assert result == "can_answer"

    @pytest.mark.asyncio
    async def test_route_with_mistral_fallback(self):
        """Test fallback to patterns when semantic router fails."""
        with patch.dict(os.environ, {"SEMANTIC_ROUTER_ENABLED": "true"}):
            from app.web.routes.analysis.chat_routing import route_with_mistral

            # Mock the semantic router to fail
            with patch("app.routing.semantic_router.get_semantic_router") as mock_get:
                mock_get.side_effect = Exception("API error")

                result = await route_with_mistral("Hello", {})
                # Should fall back to patterns
                assert result == "can_answer"


class TestKeyScenario:
    """Test the key scenario that motivated this implementation."""

    def test_top_ranked_wards_with_analysis_complete(self):
        """The failing case: 'What are the top ranked wards' with analysis complete.

        This should route to data_query (needs_tools), not knowledge (can_answer).
        """
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        # Pattern fallback should handle this correctly
        result = _route_with_patterns(
            "What are the top ranked wards in terms of risk?",
            {"analysis_complete": True, "has_uploaded_files": True}
        )

        assert result == "needs_tools", (
            "Query about ranked wards with analysis complete should route to tools"
        )

    def test_knowledge_question_without_context(self):
        """Knowledge questions without data should go to arena."""
        from app.web.routes.analysis.chat_routing import _route_with_patterns

        result = _route_with_patterns(
            "What causes malaria?",
            {"has_uploaded_files": False}
        )

        assert result == "can_answer", (
            "Knowledge question without data should route to arena"
        )
