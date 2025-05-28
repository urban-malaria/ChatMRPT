"""
Visualization Package

A modular visualization system for malaria risk analysis with map-based and 
chart-based visualizations, theming, caching, and export capabilities.

Modules:
    core: Core utilities, caching, and coordination functions
    export: HTML export and file management
    maps: Map-based visualizations (choropleth, vulnerability, urban extent)
    charts: Chart/plot visualizations (box plots, decision trees)
    themes: Color schemes and styling utilities
    utils: General helper functions
"""

import logging

# Set up logging
logger = logging.getLogger(__name__)

# Import core functions
from .core import (
    get_full_variable_name,
    is_id_column, 
    get_variable_by_name,
    VisualizationCache,
    create_visualization
)

# Import export functions
from .export import (
    ensure_wgs84_crs,
    prepare_geodataframe_for_json,
    create_plotly_html,
    create_secure_filename,
    get_visualization_file_path,
    get_web_accessible_path,
    cleanup_old_visualizations,
    validate_plotly_figure,
    get_export_summary
)

# Import map functions
from .maps import (
    create_variable_map,
    create_normalized_map,
    create_composite_map,
    create_vulnerability_map,
    create_urban_extent_map,
    get_available_map_types,
    validate_map_inputs,
    get_map_summary
)

# Import chart functions
from .charts import (
    box_plot_function,
    create_vulnerability_plot,
    create_decision_tree_plot,
    get_available_chart_types,
    validate_chart_inputs,
    get_chart_summary
)

# Import theme functions
from .themes import (
    get_color_scheme,
    get_risk_labels,
    get_vulnerability_colors,
    get_map_styling,
    get_chart_styling,
    create_custom_colorscale,
    apply_theme_to_figure,
    get_theme_summary
)

# Import utility functions
from .utils import (
    calculate_zoom_level,
    get_map_center,
    safe_numeric_conversion,
    format_hover_text,
    create_tick_values,
    validate_data_columns,
    get_data_statistics,
    create_responsive_layout,
    log_visualization_event,
    get_utils_summary
)

# Define public API
__all__ = [
    # Core functions
    'get_full_variable_name',
    'is_id_column', 
    'get_variable_by_name',
    'VisualizationCache',
    'create_visualization',
    
    # Export functions
    'ensure_wgs84_crs',
    'prepare_geodataframe_for_json',
    'create_plotly_html',
    'create_secure_filename',
    'get_visualization_file_path',
    'get_web_accessible_path',
    'cleanup_old_visualizations',
    'validate_plotly_figure',
    'get_export_summary',
    
    # Map functions
    'create_variable_map',
    'create_normalized_map',
    'create_composite_map',
    'create_vulnerability_map',
    'create_urban_extent_map',
    'get_available_map_types',
    'validate_map_inputs',
    'get_map_summary',
    
    # Chart functions
    'box_plot_function',
    'create_vulnerability_plot',
    'create_decision_tree_plot',
    'get_available_chart_types',
    'validate_chart_inputs',
    'get_chart_summary',
    
    # Theme functions
    'get_color_scheme',
    'get_risk_labels',
    'get_vulnerability_colors',
    'get_map_styling',
    'get_chart_styling',
    'create_custom_colorscale',
    'apply_theme_to_figure',
    'get_theme_summary',
    
    # Utility functions
    'calculate_zoom_level',
    'get_map_center',
    'safe_numeric_conversion',
    'format_hover_text',
    'create_tick_values',
    'validate_data_columns',
    'get_data_statistics',
    'create_responsive_layout',
    'log_visualization_event',
    'get_utils_summary'
]

# Package metadata
__version__ = '1.0.0'
__author__ = 'ChatMRPT Refactoring Team'
__description__ = 'Modular visualization system for malaria risk analysis'


def get_package_summary():
    """
    Get comprehensive package information
    
    Returns:
        dict: Package summary with module information
    """
    return {
        'package': 'app.visualization',
        'version': __version__,
        'description': __description__,
        'modules': {
            'core': 'Core utilities, caching, and coordination',
            'export': 'HTML export and file management', 
            'maps': 'Map-based visualizations',
            'charts': 'Chart/plot visualizations',
            'themes': 'Color schemes and styling',
            'utils': 'General helper functions'
        },
        'total_functions': len(__all__),
        'map_types': 5,  # variable, normalized, composite, vulnerability, urban_extent
        'chart_types': 2,  # box_plot, decision_tree, vulnerability_plot
        'status': 'complete'
    }


def validate_package():
    """
    Validate that all modules and functions are importable
    
    Returns:
        dict: Validation results
    """
    validation_results = {
        'status': 'success',
        'modules_validated': [],
        'functions_validated': 0,
        'errors': []
    }
    
    modules_to_test = [
        ('core', ['get_full_variable_name', 'create_visualization']),
        ('export', ['create_plotly_html', 'ensure_wgs84_crs']),
        ('maps', ['create_variable_map', 'get_available_map_types']),
        ('charts', ['box_plot_function', 'get_available_chart_types']),
        ('themes', ['get_color_scheme', 'get_theme_summary']),
        ('utils', ['calculate_zoom_level', 'get_utils_summary'])
    ]
    
    for module_name, test_functions in modules_to_test:
        try:
            module = __import__('app.visualization.{}'.format(module_name), fromlist=[module_name])
            
            # Test that key functions exist
            for func_name in test_functions:
                if hasattr(module, func_name):
                    validation_results['functions_validated'] += 1
                else:
                    validation_results['errors'].append(
                        'Function {} not found in module {}'.format(func_name, module_name)
                    )
            
            validation_results['modules_validated'].append(module_name)
            
        except ImportError as e:
            validation_results['errors'].append(
                'Failed to import module {}: {}'.format(module_name, str(e))
            )
            validation_results['status'] = 'error'
    
    if validation_results['errors']:
        validation_results['status'] = 'error'
        
    return validation_results


# Log package initialization
logger.info("Visualization package initialized with {} modules and {} functions".format(
    len(['core', 'export', 'maps', 'charts', 'themes', 'utils']),
    len(__all__)
)) 