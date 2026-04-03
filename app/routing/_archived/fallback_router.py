"""LLM fallback router for low-confidence cases."""

from __future__ import annotations

import logging
import os
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Route descriptions for the LLM
ROUTE_DESCRIPTIONS = """
Available routes:
1. data_query - User wants to query their analysis results, see rankings, understand data values
2. visualization - User wants a map, chart, graph, or visual output
3. analysis - User wants to run analysis, calculate scores, process their data
4. itn_planning - User wants to plan bed net (ITN/LLIN) distribution
5. workflow - User wants to run TPR workflow, export results, manage their session
6. knowledge - User is asking a general question about malaria, methodology, or concepts
7. greeting - User is saying hello, thanks, goodbye, or making small talk
"""


class FallbackRouter:
    """Uses LLM to route messages when semantic routing has low confidence."""

    def __init__(self, model: str = "gpt-4o-mini"):
        self.model = model
        self._client = None

    @property
    def client(self):
        """Lazy-load the OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI

                self._client = OpenAI()
            except ImportError:
                raise ImportError("openai package is required for FallbackRouter")
        return self._client

    def route(
        self,
        message: str,
        session_context: Dict,
        semantic_scores: Optional[Dict[str, float]] = None,
    ) -> Optional[str]:
        """Use LLM to determine the best route.

        Args:
            message: The user's message
            session_context: Current session state
            semantic_scores: Optional scores from semantic routing (for context)

        Returns:
            Route name or None if unable to determine
        """
        # Build context description
        context_parts = []
        if session_context.get("has_uploaded_files"):
            context_parts.append("User has uploaded data files")
        if session_context.get("analysis_complete"):
            context_parts.append("Analysis has been completed")
        if session_context.get("use_data_analysis_v3") or session_context.get("data_analysis_active"):
            context_parts.append("User is in data analysis mode")
        if session_context.get("csv_loaded"):
            context_parts.append("CSV data is loaded")

        context_str = ". ".join(context_parts) if context_parts else "No data loaded"

        # Build prompt
        prompt = f"""You are a routing classifier for ChatMRPT, a malaria risk analysis assistant.

{ROUTE_DESCRIPTIONS}

Current context: {context_str}

User message: "{message}"

Based on the message and context, which route should handle this request?
Reply with ONLY the route name (one of: data_query, visualization, analysis, itn_planning, workflow, knowledge, greeting)."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=20,
                temperature=0.0,
            )

            route_name = response.choices[0].message.content.strip().lower()

            # Validate route name
            valid_routes = {
                "data_query",
                "visualization",
                "analysis",
                "itn_planning",
                "workflow",
                "knowledge",
                "greeting",
            }

            if route_name in valid_routes:
                logger.info("LLM fallback routed to: %s", route_name)
                return route_name
            else:
                logger.warning("LLM returned invalid route: %s", route_name)
                return None

        except Exception as e:
            logger.error("LLM fallback routing failed: %s", e)
            return None


def get_fallback_router() -> Optional[FallbackRouter]:
    """Get fallback router if enabled."""
    if os.getenv("SEMANTIC_ROUTER_LLM_FALLBACK", "true").lower() == "true":
        return FallbackRouter()
    return None
