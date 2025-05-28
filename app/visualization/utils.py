"""
Visualization utilities and helper functions

This module provides general utility functions that support
visualization operations across all modules.
"""

import os
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Union

# Set up logging
logger = logging.getLogger(__name__)


def calculate_zoom_level(bounds, default_zoom=6):
    """
    Calculate appropriate zoom level based on geographic bounds
    
    Args:
        bounds: Geographic bounds [minx, miny, maxx, maxy]
        default_zoom: Default zoom level if calculation fails
        
    Returns:
        float: Appropriate zoom level
    """
    try:
        span_x = max(0.01, bounds[2] - bounds[0])
        span_y = max(0.01, bounds[3] - bounds[1])
        zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
        return float(zoom_level)
    except Exception as e:
        logger.warning("Error calculating zoom level: {}".format(str(e)))
        return float(default_zoom)


def get_map_center(gdf):
    """
    Calculate map center from geodataframe
    
    Args:
        gdf: GeoDataFrame with geometry column
        
    Returns:
        tuple: (center_lat, center_lon)
    """
    try:
        center_lat = float(gdf.geometry.centroid.y.mean())
        center_lon = float(gdf.geometry.centroid.x.mean())
        
        # Handle NaN values
        if pd.isna(center_lat) or pd.isna(center_lon):
            center_lat, center_lon = 0.0, 0.0
            
        return center_lat, center_lon
    except Exception as e:
        logger.warning("Error calculating map center: {}".format(str(e)))
        return 0.0, 0.0


def safe_numeric_conversion(series, fill_value=0):
    """
    Safely convert series to numeric, handling errors
    
    Args:
        series: Pandas series to convert
        fill_value: Value to use for NaN/errors
        
    Returns:
        pd.Series: Converted numeric series
    """
    try:
        return pd.to_numeric(series, errors='coerce').fillna(fill_value)
    except Exception as e:
        logger.warning("Error in numeric conversion: {}".format(str(e)))
        return pd.Series([fill_value] * len(series), index=series.index)


def format_hover_text(row, fields, formats=None):
    """
    Format hover text for visualizations
    
    Args:
        row: Data row (pandas Series)
        fields: List of field names to include
        formats: Optional dict of format strings for fields
        
    Returns:
        str: Formatted hover text
    """
    if formats is None:
        formats = {}
    
    lines = []
    for field in fields:
        if field in row and pd.notna(row[field]):
            value = row[field]
            
            # Apply formatting if specified
            if field in formats:
                try:
                    if isinstance(value, (int, float)):
                        value = formats[field].format(value)
                    else:
                        value = str(value)
                except:
                    value = str(value)
            else:
                value = str(value)
                
            lines.append("{}: {}".format(field, value))
    
    return "<br>".join(lines)


def create_tick_values(data_values, num_ticks=5):
    """
    Create appropriate tick values for colorbar
    
    Args:
        data_values: Array of data values
        num_ticks: Number of ticks to create
        
    Returns:
        tuple: (tick_values, tick_labels)
    """
    try:
        if len(data_values) == 0:
            return [0, 1], ['Min', 'Max']
        
        min_val = float(np.min(data_values))
        max_val = float(np.max(data_values))
        
        if min_val == max_val:
            return [min_val], [str(min_val)]
        
        tick_values = np.linspace(min_val, max_val, num_ticks).tolist()
        tick_labels = ["{:.1f}".format(val) for val in tick_values]
        
        return tick_values, tick_labels
        
    except Exception as e:
        logger.warning("Error creating tick values: {}".format(str(e)))
        return [0, 1], ['Min', 'Max']


def validate_data_columns(df, required_columns):
    """
    Validate that dataframe has required columns
    
    Args:
        df: Pandas DataFrame
        required_columns: List of required column names
        
    Returns:
        dict: Validation result
    """
    if df is None:
        return {
            'valid': False,
            'message': 'DataFrame is None',
            'missing_columns': required_columns
        }
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            'valid': False,
            'message': 'Missing required columns: {}'.format(', '.join(missing_columns)),
            'missing_columns': missing_columns
        }
    
    return {
        'valid': True,
        'message': 'All required columns present',
        'missing_columns': []
    }


def get_data_statistics(series, percentiles=None):
    """
    Get comprehensive statistics for a data series
    
    Args:
        series: Pandas Series
        percentiles: List of percentiles to calculate
        
    Returns:
        dict: Statistics dictionary
    """
    if percentiles is None:
        percentiles = [25, 50, 75]
    
    try:
        clean_series = series.dropna()
        
        if len(clean_series) == 0:
            return {
                'count': 0,
                'mean': None,
                'std': None,
                'min': None,
                'max': None,
                'missing_count': len(series),
                'missing_percentage': 100.0
            }
        
        stats = {
            'count': int(len(clean_series)),
            'mean': float(clean_series.mean()),
            'std': float(clean_series.std()),
            'min': float(clean_series.min()),
            'max': float(clean_series.max()),
            'missing_count': int(series.isna().sum()),
            'missing_percentage': float(series.isna().sum() / len(series) * 100)
        }
        
        # Add percentiles
        for p in percentiles:
            stats['p{}'.format(p)] = float(clean_series.quantile(p / 100))
        
        return stats
        
    except Exception as e:
        logger.error("Error calculating statistics: {}".format(str(e)))
        return {
            'count': 0,
            'error': str(e)
        }


def create_responsive_layout(base_height=600, aspect_ratio=None):
    """
    Create responsive layout configuration
    
    Args:
        base_height: Base height in pixels
        aspect_ratio: Optional aspect ratio (width/height)
        
    Returns:
        dict: Layout configuration
    """
    layout = {
        'autosize': True,
        'margin': dict(l=20, r=20, t=80, b=20)
    }
    
    if aspect_ratio:
        # Calculate responsive height based on aspect ratio
        layout['height'] = base_height
        # Note: Plotly will handle width automatically with autosize=True
    else:
        layout['height'] = base_height
    
    return layout


def log_visualization_event(event_type, details=None, level='info'):
    """
    Log visualization events with consistent formatting
    
    Args:
        event_type: Type of event (e.g., 'map_created', 'error')
        details: Additional details dictionary
        level: Log level ('info', 'warning', 'error')
    """
    message = "Visualization event: {}".format(event_type)
    
    if details:
        detail_str = ", ".join(["{}={}".format(k, v) for k, v in details.items()])
        message += " - {}".format(detail_str)
    
    log_func = getattr(logger, level, logger.info)
    log_func(message)


def get_utils_summary():
    """
    Get summary of available utility functions
    
    Returns:
        dict: Summary of utilities
    """
    return {
        'status': 'success',
        'available_functions': [
            'calculate_zoom_level',
            'get_map_center', 
            'safe_numeric_conversion',
            'format_hover_text',
            'create_tick_values',
            'validate_data_columns',
            'get_data_statistics',
            'create_responsive_layout',
            'log_visualization_event'
        ],
        'module_purpose': 'General utility functions for visualization operations'
    } 