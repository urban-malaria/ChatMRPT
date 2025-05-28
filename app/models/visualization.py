 # app/models/visualization.py
"""
REFACTORED: Visualization functionality has been moved to app.visualization package

This file serves as a compatibility layer. All visualization functionality
is now available through the modular app.visualization package.

For new code, import directly from:
- app.visualization.core
- app.visualization.maps  
- app.visualization.charts
- app.visualization.export
- app.visualization.themes
- app.visualization.utils

Legacy Support: This file re-exports all functions for backward compatibility.
"""

# Import all functions from the new modular visualization package
from app.visualization import *

# Legacy compatibility - ensure all original function names are available
from app.visualization import (
    # Core functions
    get_full_variable_name,
    is_id_column,
    get_variable_by_name,
    VisualizationCache,
    create_visualization,
    
    # Map functions
    create_variable_map,
    create_normalized_map,
    create_composite_map,
    create_vulnerability_map,
    create_urban_extent_map,
    
    # Chart functions
    box_plot_function,
    create_vulnerability_plot,
    create_decision_tree_plot,
    
    # Export functions
    ensure_wgs84_crs,
    prepare_geodataframe_for_json,
    create_plotly_html
)

# Log the compatibility layer usage
import logging
logger = logging.getLogger(__name__)
logger.info("COMPATIBILITY: Using refactored visualization package through compatibility layer")

__all__ = [
    # Re-export everything from visualization package
    'get_full_variable_name',
    'is_id_column', 
    'get_variable_by_name',
    'VisualizationCache',
    'create_visualization',
    'create_variable_map',
    'create_normalized_map',
    'create_composite_map',
    'create_vulnerability_map',
    'create_urban_extent_map',
    'box_plot_function',
    'create_vulnerability_plot',
    'create_decision_tree_plot',
    'ensure_wgs84_crs',
    'prepare_geodataframe_for_json',
    'create_plotly_html'
]