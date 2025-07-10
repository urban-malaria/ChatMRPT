"""
Core Utility Functions for Agent Visualizations

Provides common functionality for agent-specific visualization functions including
data preparation, file handling, and output formatting.
"""

import json
import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Union
import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)

def ensure_wgs84_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure GeoDataFrame is in WGS84 CRS (EPSG:4326)
    
    Args:
        gdf: Input GeoDataFrame
        
    Returns:
        GeoDataFrame in WGS84 CRS
    """
    try:
        if gdf.crs is None:
            logger.warning("GeoDataFrame has no CRS, assuming WGS84")
            gdf = gdf.set_crs('EPSG:4326')
        elif gdf.crs.to_epsg() != 4326:
            logger.info(f"Converting from {gdf.crs} to WGS84")
            gdf = gdf.to_crs('EPSG:4326')
        return gdf
    except Exception as e:
        logger.error(f"Error ensuring WGS84 CRS: {e}")
        return gdf

def prepare_geodataframe_for_json(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Prepare GeoDataFrame for JSON serialization
    
    Args:
        gdf: Input GeoDataFrame
        
    Returns:
        Prepared GeoDataFrame
    """
    try:
        # Ensure WGS84 CRS
        prepared_gdf = ensure_wgs84_crs(gdf.copy())
        
        # Convert problematic columns to JSON-safe types
        for col in prepared_gdf.columns:
            if col == 'geometry':
                continue  # Skip geometry column
                
            # Handle datetime/timestamp columns - ENHANCED DETECTION
            if prepared_gdf[col].dtype == 'datetime64[ns]':
                prepared_gdf[col] = prepared_gdf[col].astype(str)
            elif 'datetime' in str(prepared_gdf[col].dtype).lower():
                prepared_gdf[col] = prepared_gdf[col].astype(str)
            elif prepared_gdf[col].dtype == 'object':
                # Enhanced timestamp detection for object columns
                try:
                    sample_vals = prepared_gdf[col].dropna()
                    if len(sample_vals) > 0:
                        sample_val = sample_vals.iloc[0]
                        
                        # Multiple timestamp detection strategies
                        is_timestamp = (
                            hasattr(sample_val, 'timestamp') or  # Pandas Timestamp
                            str(type(sample_val).__name__).find('Timestamp') != -1 or  # Any Timestamp class
                            'timestamp' in str(type(sample_val)).lower() or
                            hasattr(sample_val, 'strftime') or  # datetime-like objects
                            str(sample_val.__class__.__module__ + '.' + sample_val.__class__.__name__) in [
                                'pandas._libs.tslibs.timestamps.Timestamp',
                                'datetime.datetime',
                                'numpy.datetime64'
                            ]
                        )
                        
                        if is_timestamp:
                            logger.info(f"Converting timestamp column '{col}' to string")
                            prepared_gdf[col] = prepared_gdf[col].astype(str)
                        else:
                            # Convert other object types to string for safety
                            prepared_gdf[col] = prepared_gdf[col].astype(str)
                    else:
                        # Empty column, convert to string
                        prepared_gdf[col] = prepared_gdf[col].astype(str)
                        
                except Exception as e:
                    logger.warning(f"Error processing column '{col}': {e}. Converting to string.")
                    prepared_gdf[col] = prepared_gdf[col].astype(str)
            
            # Handle any NaN/inf values in numeric columns
            if prepared_gdf[col].dtype in ['float64', 'float32']:
                prepared_gdf[col] = prepared_gdf[col].fillna(0).replace([np.inf, -np.inf], 0)
        
        return prepared_gdf
    except Exception as e:
        logger.error(f"Error preparing GeoDataFrame for JSON: {e}")
        return gdf

def prepare_unified_dataset(unified_dataset: gpd.GeoDataFrame, 
                          required_columns: List[str] = None) -> Dict[str, Any]:
    """
    Prepare and validate unified dataset for visualization
    
    Args:
        unified_dataset: The enhanced unified dataset GeoDataFrame
        required_columns: List of columns that must be present
        
    Returns:
        Dict with prepared data and validation results
    """
    try:
        if unified_dataset is None:
            return {
                'status': 'error',
                'message': 'No unified dataset provided',
                'data': None
            }
        
        logger.info(f"📊 Preparing unified dataset: {unified_dataset.shape}")
        
        # Ensure proper CRS
        prepared_data = ensure_wgs84_crs(unified_dataset.copy())
        
        # Validate required columns
        missing_columns = []
        if required_columns:
            for col in required_columns:
                exists, _ = variable_resolver.check_column_exists(col, list(prepared_data.columns))
                if not exists:
                    missing_columns.append(col)
        
        if missing_columns:
            return {
                'status': 'error',
                'message': f'Missing required columns: {missing_columns}',
                'data': None,
                'available_columns': list(prepared_data.columns)
            }
        
        # Extract enhanced dataset categories
        enhanced_categories = {
            'composite_analysis': [],
            'pca_analysis': [],
            'individual_models': [],
            'spatial_metrics': [],
            'consensus_metrics': [],
            'original_variables': []
        }
        
        for col in prepared_data.columns:
            if col in ['composite_score', 'composite_rank', 'vulnerability_category']:
                enhanced_categories['composite_analysis'].append(col)
            elif col in ['pca_score', 'pca_rank', 'pca_category']:
                enhanced_categories['pca_analysis'].append(col)
            elif col.startswith('model_') and col not in ['model_mean_score', 'model_std_score', 'model_agreement', 'model_consensus']:
                enhanced_categories['individual_models'].append(col)
            elif col in ['model_mean_score', 'model_std_score', 'model_agreement', 'model_consensus']:
                enhanced_categories['consensus_metrics'].append(col)
            elif col in ['area_km2', 'centroid_lat', 'centroid_lon']:
                enhanced_categories['spatial_metrics'].append(col)
            elif col not in ['geometry', 'WardName']:
                enhanced_categories['original_variables'].append(col)
        
        # Calculate map bounds and center
        bounds = prepared_data.geometry.total_bounds  # minx, miny, maxx, maxy
        center_lat = prepared_data.geometry.centroid.y.mean()
        center_lon = prepared_data.geometry.centroid.x.mean()
        
        # Calculate zoom level
        span_x = max(0.01, bounds[2] - bounds[0])
        span_y = max(0.01, bounds[3] - bounds[1])
        zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
        
        return {
            'status': 'success',
            'message': f'Unified dataset prepared: {prepared_data.shape}',
            'data': prepared_data,
            'enhanced_categories': enhanced_categories,
            'map_center': {'lat': center_lat, 'lon': center_lon},
            'zoom_level': zoom_level,
            'bounds': bounds
        }
        
    except Exception as e:
        logger.error(f"Error preparing unified dataset: {e}")
        return {
            'status': 'error',
            'message': f'Dataset preparation failed: {str(e)}',
            'data': None
        }

def extract_plotly_json(fig: go.Figure) -> Dict[str, Any]:
    """
    Extract plotly JSON data for frontend rendering
    
    Args:
        fig: Plotly figure object
        
    Returns:
        Dictionary with plotly JSON data
    """
    try:
        plotly_data = fig.to_dict()
        
        # Clean and prepare for JSON serialization
        def clean_for_json(obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, (np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: clean_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_for_json(item) for item in obj]
            else:
                return obj
        
        cleaned_data = clean_for_json(plotly_data)
        
        return {
            'plotly_json': cleaned_data,
            'chart_type': 'plotly',
            'interactive': True
        }
        
    except Exception as e:
        logger.error(f"Error extracting plotly JSON: {e}")
        return {
            'plotly_json': None,
            'chart_type': 'static',
            'interactive': False,
            'error': str(e)
        }

def save_agent_visualization(fig_or_html: Union[go.Figure, str], 
                           filename: str,
                           session_id: str = 'default',
                           visualization_type: str = 'map',
                           is_html_content: bool = False) -> Dict[str, Any]:
    """
    Save visualization with agent-friendly paths and metadata
    
    Args:
        fig_or_html: Plotly figure to save OR raw HTML content string
        filename: Base filename (without extension)
        session_id: Session identifier for file organization
        visualization_type: Type of visualization for categorization
        is_html_content: True if fig_or_html is raw HTML string, False if it's a plotly figure
        
    Returns:
        Dictionary with file paths and metadata
    """
    try:
        # Create unique filename with timestamp - ensures multiple visualizations coexist
        # Files persist until session closure (browser closed or session expired)
        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]
        safe_filename = f"{filename}_{timestamp}_{unique_id}.html"
        
        # Create session directory
        session_dir = Path(f"instance/uploads/{session_id}")
        session_dir.mkdir(parents=True, exist_ok=True)
        
        # Save HTML file
        html_path = session_dir / safe_filename
        
        if is_html_content:
            # Save raw HTML content
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(fig_or_html)
            plotly_data = {'plotly_json': None, 'interactive': False}
        else:
            # Save plotly figure
            fig_or_html.write_html(str(html_path))
            # Extract plotly JSON for inline rendering
            plotly_data = extract_plotly_json(fig_or_html)
        
        # Create web-accessible path
        web_path = f"/serve_viz_file/{session_id}/{safe_filename}"
        
        # File size and metadata
        file_size = html_path.stat().st_size if html_path.exists() else 0
        
        return {
            'status': 'success',
            'file_path': str(html_path),
            'web_path': web_path,
            'filename': safe_filename,
            'file_size': file_size,
            'visualization_type': visualization_type,
            'session_id': session_id,
            'plotly_json': plotly_data.get('plotly_json'),
            'interactive': plotly_data.get('interactive', True),
            'created_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error saving agent visualization: {e}")
        return {
            'status': 'error',
            'message': f'Failed to save visualization: {str(e)}',
            'file_path': None,
            'web_path': None
        }

def get_vulnerability_colors() -> Dict[str, str]:
    """Get standard vulnerability category colors"""
    return {
        'Low': '#c7e9c0',       # Light green
        'Medium': '#a8d8b9',    # Medium green  
        'High': '#69b3a2',      # Darker green
        'Very High': '#238b45'  # Dark green
    }

def create_geojson_from_gdf(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """
    Convert GeoDataFrame to GeoJSON for plotly
    
    Args:
        gdf: GeoDataFrame with geometry column
        
    Returns:
        GeoJSON dictionary
    """
    try:
        # Prepare GeoDataFrame for JSON conversion
        gdf_prepared = prepare_geodataframe_for_json(gdf)
        geojson = json.loads(gdf_prepared.to_json())
        return geojson
        
    except Exception as e:
        logger.error(f"Error creating GeoJSON: {e}")
        raise

def calculate_data_statistics(data: pd.Series) -> Dict[str, float]:
    """
    Calculate comprehensive statistics for data series
    
    Args:
        data: Pandas Series with numerical data
        
    Returns:
        Dictionary with statistical measures
    """
    try:
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {'count': 0, 'has_data': False}
        
        stats = {
            'count': len(clean_data),
            'mean': clean_data.mean(),
            'median': clean_data.median(),
            'std': clean_data.std(),
            'min': clean_data.min(),
            'max': clean_data.max(),
            'q25': clean_data.quantile(0.25),
            'q75': clean_data.quantile(0.75),
            'has_data': True
        }
        
        # Add percentile ranks
        stats['p10'] = clean_data.quantile(0.10)
        stats['p90'] = clean_data.quantile(0.90)
        
        return stats
        
    except Exception as e:
        logger.error(f"Error calculating statistics: {e}")
        return {'count': 0, 'has_data': False, 'error': str(e)} 