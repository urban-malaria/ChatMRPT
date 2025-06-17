"""
Agent-Specific Visualization Functions

Clean, purpose-built visualization functions designed specifically for the VisualizationAgent.
These functions work natively with the unified dataset parquet structure and provide
agent-friendly interfaces.

Core Features:
- Direct unified dataset integration
- Clean, maintainable code structure  
- Agent-focused return formats
- Modern error handling and logging
"""

from .composite_visualizations import (
    create_agent_composite_score_maps,
    create_agent_vulnerability_map,
    create_agent_box_plot_ranking,
    create_agent_urban_extent_map,
    create_agent_decision_tree,
    get_agent_pagination_info
)

from .pca_visualizations import (
    create_agent_pca_vulnerability_map
)

# Dynamic generator removed - functionality moved to tools

from .core_utils import (
    prepare_unified_dataset,
    extract_plotly_json,
    save_agent_visualization
)

__all__ = [
    # Composite method visualizations
    'create_agent_composite_score_maps',
    'create_agent_vulnerability_map', 
    'create_agent_box_plot_ranking',
    'create_agent_urban_extent_map',
    'create_agent_decision_tree',
    
    # PCA method visualizations
    'create_agent_pca_vulnerability_map',
    
    # Utility functions
    'prepare_unified_dataset',
    'extract_plotly_json',
    'save_agent_visualization',
    'get_agent_pagination_info'
] 