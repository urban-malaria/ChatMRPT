"""Context-aware biasing for semantic routing.

Adjusts route scores based on session state to make routing decisions
that are appropriate for the current context.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

logger = logging.getLogger(__name__)


@dataclass
class BiasConfig:
    """Configuration for context biasing."""

    # When analysis is complete, boost data-related routes
    analysis_complete_boosts: Dict[str, float] = None

    # When files are uploaded but not analyzed, moderate boost
    files_uploaded_boosts: Dict[str, float] = None

    # When no data is present, suppress data routes
    no_data_penalties: Dict[str, float] = None

    # When in data analysis mode, strong boost to tools
    data_mode_boosts: Dict[str, float] = None

    def __post_init__(self):
        # Default bias configurations
        if self.analysis_complete_boosts is None:
            self.analysis_complete_boosts = {
                "data_query": 0.25,
                "visualization": 0.20,
                "itn_planning": 0.15,
                "analysis": 0.10,
                "knowledge": -0.15,
                "greeting": 0.0,
                "workflow": 0.05,
            }

        if self.files_uploaded_boosts is None:
            self.files_uploaded_boosts = {
                "data_query": 0.15,
                "visualization": 0.15,
                "analysis": 0.20,
                "itn_planning": 0.10,
                "knowledge": -0.10,
                "greeting": 0.0,
                "workflow": 0.10,
            }

        if self.no_data_penalties is None:
            self.no_data_penalties = {
                "data_query": -0.40,
                "visualization": -0.35,
                "analysis": -0.25,
                "itn_planning": -0.30,
                "knowledge": 0.20,
                "greeting": 0.05,
                "workflow": 0.0,
            }

        if self.data_mode_boosts is None:
            self.data_mode_boosts = {
                "data_query": 0.30,
                "visualization": 0.25,
                "analysis": 0.25,
                "itn_planning": 0.20,
                "knowledge": -0.20,
                "greeting": -0.05,
                "workflow": 0.15,
            }


class ContextBiaser:
    """Applies context-aware biases to route scores."""

    def __init__(self, config: BiasConfig | None = None):
        self.config = config or BiasConfig()

    def apply_bias(
        self, scores: Dict[str, float], session_context: Dict
    ) -> Dict[str, float]:
        """Apply context-based bias to route scores.

        Args:
            scores: Dictionary of {route_name: score}
            session_context: Session state including:
                - has_uploaded_files: bool
                - analysis_complete: bool
                - use_data_analysis_v3: bool
                - data_analysis_active: bool
                - csv_loaded: bool
                - shapefile_loaded: bool

        Returns:
            Adjusted scores dictionary
        """
        biased_scores = scores.copy()
        applied_biases = []

        # Check session state flags
        has_files = session_context.get("has_uploaded_files", False)
        analysis_complete = session_context.get("analysis_complete", False)
        data_mode = session_context.get("use_data_analysis_v3", False) or session_context.get("data_analysis_active", False)

        # Apply biases based on context (cumulative)
        if data_mode:
            # Strongest bias: actively in data analysis mode
            for route, boost in self.config.data_mode_boosts.items():
                if route in biased_scores:
                    biased_scores[route] += boost
            applied_biases.append("data_mode")

        elif analysis_complete:
            # User has completed analysis - boost result queries
            for route, boost in self.config.analysis_complete_boosts.items():
                if route in biased_scores:
                    biased_scores[route] += boost
            applied_biases.append("analysis_complete")

        elif has_files:
            # User has uploaded files but not analyzed yet
            for route, boost in self.config.files_uploaded_boosts.items():
                if route in biased_scores:
                    biased_scores[route] += boost
            applied_biases.append("files_uploaded")

        else:
            # No data context - suppress data routes
            for route, penalty in self.config.no_data_penalties.items():
                if route in biased_scores:
                    biased_scores[route] += penalty
            applied_biases.append("no_data")

        # Check for explicit data references in the message
        # This is done at a higher level, but we can add route-specific adjustments here

        if applied_biases:
            logger.debug(
                "Applied context biases: %s | Scores before: %s | After: %s",
                applied_biases,
                {k: f"{v:.3f}" for k, v in scores.items()},
                {k: f"{v:.3f}" for k, v in biased_scores.items()},
            )

        return biased_scores

    def get_context_summary(self, session_context: Dict) -> str:
        """Get a human-readable summary of the context state."""
        parts = []
        if session_context.get("use_data_analysis_v3") or session_context.get("data_analysis_active"):
            parts.append("data_mode=ON")
        if session_context.get("analysis_complete"):
            parts.append("analysis_complete")
        if session_context.get("has_uploaded_files"):
            parts.append("has_files")
        if session_context.get("csv_loaded"):
            parts.append("csv_loaded")
        if session_context.get("shapefile_loaded"):
            parts.append("shapefile_loaded")
        return ", ".join(parts) if parts else "no_context"
