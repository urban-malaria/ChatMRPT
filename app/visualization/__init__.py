"""Visualization package — maps, charts, and visual explanations."""

# Re-export rendering functions for backward compatibility
from .composite import (
    create_agent_box_plot_ranking,
    create_agent_vulnerability_map,
    create_agent_composite_score_maps,
)
from .pca import create_agent_pca_vulnerability_map
try:
    from .composite import create_agent_urban_extent_map
except ImportError:
    pass
try:
    from .composite import create_agent_decision_tree
except ImportError:
    pass
