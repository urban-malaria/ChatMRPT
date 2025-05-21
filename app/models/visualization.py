# app/models/visualization.py
import json
import logging
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import io
import base64
import re
from flask import current_app, session
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pyproj
from shapely.ops import transform
from functools import partial
from werkzeug.utils import secure_filename
import concurrent.futures
from joblib import Parallel, delayed
import time
import hashlib
import pickle
import tempfile

# Import analysis module for any remaining data processing needs
from app.models.analysis import normalize_variable, determine_variable_relationships, analyze_vulnerability
from app.ai_utils import get_llm_manager
from app.utilities import convert_to_json_serializable

# Set up logging
logger = logging.getLogger(__name__)

# Dictionary mapping variable codes to full names
VARIABLE_FULL_NAMES = {
    'tpr': 'Test Positivity Rate',
    'tpr_u5': 'Test Positivity Rate (Under 5)',
    'settlement_type': 'Settlement Type',
    'distance_to_water': 'Distance to Water Bodies',
    'mean_rainfall': 'Mean Rainfall',
    'mean_soil_wetness': 'Mean Soil Wetness',
    'mean_evi': 'Mean Enhanced Vegetation Index',
    'mean_ndvi': 'Mean Normalized Difference Vegetation Index',
    'mean_ndwi': 'Mean Normalized Difference Water Index',
    'pfpr': 'Plasmodium Falciparum Parasite Rate',
    'elevation': 'Elevation',
    'population': 'Population',
    'housing_quality': 'Housing Quality',
    'temp_mean': 'Mean Temperature',
    'rh_mean': 'Mean Relative Humidity',
    'flood': 'Flood Risk',
    'urbanpercent': 'Urban Percentage',
    'urbanarea': 'Urban Area',
    'avgrad': 'Average Radiation',
    'precipitation': 'Precipitation',
    'rainfall': 'Rainfall',
    'temp': 'Temperature',
    'temperature': 'Temperature',
    'soil_wetness': 'Soil Wetness',
    'evi': 'Enhanced Vegetation Index',
    'ndvi': 'Normalized Difference Vegetation Index',
    'ndwi': 'Normalized Difference Water Index',
}

def get_full_variable_name(var_code):
    """
    Get the full descriptive name for a variable code
    
    Args:
        var_code: Variable code/short name
        
    Returns:
        str: Full descriptive name
    """
    if not var_code:
        return "Unknown Variable"
        
    # Standardize the variable code (lowercase)
    var_code_lower = var_code.lower()
    
    # Check the dictionary first with exact match
    if var_code_lower in VARIABLE_FULL_NAMES:
        return VARIABLE_FULL_NAMES[var_code_lower]
    
    # Try matching with standardized keys (without underscores, etc.)
    normalized_inputs = {re.sub(r'[_\s]', '', k.lower()): v for k, v in VARIABLE_FULL_NAMES.items()}
    normalized_var = re.sub(r'[_\s]', '', var_code_lower)
    
    if normalized_var in normalized_inputs:
        return normalized_inputs[normalized_var]
    
    # If still not found, check for partial matches
    for key, value in VARIABLE_FULL_NAMES.items():
        if key in var_code_lower or var_code_lower in key:
            return value
    
    # If not found, try some heuristics to make the name more readable
    if '_' in var_code:
        # Split by underscore and capitalize each word
        parts = var_code.split('_')
        return ' '.join(word.capitalize() for word in parts)
    
    # Just capitalize the first letter as fallback
    return var_code.capitalize()

def ensure_wgs84_crs(gdf):
    """
    Ensure the GeoDataFrame is using WGS84 (EPSG:4326) CRS
    
    Args:
        gdf: GeoDataFrame to check/transform
        
    Returns:
        GeoDataFrame in WGS84 CRS
    """
    # Create a copy to avoid modifying the original
    gdf_copy = gdf.copy()
    
    # Check if the GeoDataFrame has a CRS
    if gdf_copy.crs is None:
        logger.warning("GeoDataFrame has no CRS. Assuming WGS84.")
        gdf_copy.set_crs(epsg=4326, inplace=True)
        return gdf_copy
    
    # Check if the CRS is already WGS84
    if gdf_copy.crs == "EPSG:4326" or gdf_copy.crs == 4326:
        return gdf_copy
    
    try:
        # Log the transformation
        logger.info(f"Transforming GeoDataFrame from {gdf_copy.crs} to WGS84 (EPSG:4326)")
        
        # Reproject to WGS84
        gdf_copy = gdf_copy.to_crs(epsg=4326)
        return gdf_copy
    except Exception as e:
        logger.error(f"Error transforming CRS: {str(e)}")
        # Return original if transformation fails
        return gdf

def prepare_geodataframe_for_json(gdf):
    """
    Prepare a GeoDataFrame for JSON serialization by converting non-serializable types
    
    Args:
        gdf: GeoDataFrame to prepare
        
    Returns:
        GeoDataFrame with serializable types
    """
    # Create a copy to avoid modifying the original
    gdf_copy = gdf.copy()
    
    # Convert any datetime/timestamp columns to strings
    for col in gdf_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf_copy[col]):
            gdf_copy[col] = gdf_copy[col].astype(str)
    
    return gdf_copy

def create_plotly_html(fig, filename, include_plotlyjs='cdn', config=None):
    """
    Convert plotly figure to HTML file, saving to the INSTANCE path's upload folder.

    Args:
        fig: Plotly figure object
        filename: Desired output filename (will be secured)
        include_plotlyjs: How to include plotly.js ('cdn' or True for full)
        config: Optional configuration dictionary for Plotly

    Returns:
        str: Web-accessible path using the /serve_viz_file/ route, or None on error.
    """
    if not filename:
        # Generate a random filename if none provided
        safe_filename = f"plotly_{np.random.randint(1000000)}.html"
    else:
        # Ensure the provided filename is web-safe and has .html extension
        safe_filename = secure_filename(filename)
        if not safe_filename.endswith('.html'):
             safe_filename += '.html'

    session_id = session.get('session_id', 'default')

    # --- THIS IS THE CRUCIAL PART ---
    # Get the UPLOAD_FOLDER path (which should point to instance/uploads) from the app config
    upload_dir = current_app.config.get('UPLOAD_FOLDER')
    if not upload_dir:
        logger.error("UPLOAD_FOLDER not configured in Flask app config.")
        return None # Cannot save without upload folder config

    # Define the specific session folder path ON DISK within the configured UPLOAD_FOLDER
    session_folder_disk = os.path.join(upload_dir, session_id)
    # ================================

    # Ensure the target directory exists
    try:
        os.makedirs(session_folder_disk, exist_ok=True)
    except OSError as e:
        logger.error(f"Could not create session upload directory {session_folder_disk}: {e}")
        return None

    # Define the full path to the file on disk
    file_path_disk = os.path.join(session_folder_disk, safe_filename)

    # Use provided config or default to responsive options
    if config is None:
        config = {
            'responsive': True,
            'displayModeBar': True,
            'scrollZoom': True,
            'fillFrame': True  # Add fillFrame to maximize plot area
        }

    # Write HTML file to the correct disk path
    try:
        fig.write_html(file_path_disk, include_plotlyjs=include_plotlyjs, full_html=True, config=config)
        logger.info(f"Successfully saved visualization to disk: {file_path_disk}")
    except Exception as e:
        logger.error(f"Failed to write Plotly HTML to {file_path_disk}: {e}")
        return None # Indicate failure

    # Return the web-accessible path using the dedicated route /serve_viz_file/
    # This URL tells the browser where to REQUEST the file from the server.
    web_path = f"/serve_viz_file/{session_id}/{safe_filename}"
    logger.info(f"Returning web path for visualization: {web_path}")
    return web_path

def is_id_column(column_name):
    """
    Check if a column name appears to be an ID or placeholder column
    
    Args:
        column_name: Name of the column to check
        
    Returns:
        bool: True if it appears to be an ID column
    """
    id_patterns = ['id', 'x.1', 'x', 'index', 'lga_code', 'wardid', 'ward_id']
    column_lower = column_name.lower()
    
    # Check if it matches common ID patterns
    for pattern in id_patterns:
        if pattern == column_lower or f"{pattern}_" in column_lower:
            return True
    
    return False

def get_variable_by_name(data_handler, variable_name):
    """
    Get the actual variable name that best matches the requested name
    
    Args:
        data_handler: DataHandler instance
        variable_name: Requested variable name (may be inexact)
        
    Returns:
        str: Best matching variable name, or None if not found
    """
    # IMPORTANT: Always allow access to all variables in the CSV data, not just cleaned or analysis variables
    if data_handler.csv_data is None:
        return None
    
    if not variable_name:
        logger.warning("No variable name provided")
        return None
    
    # Convert to lowercase for case-insensitive matching
    variable_lower = variable_name.lower()
    
    # Clean up the variable name (remove articles, common words)
    clean_variable = re.sub(r'\b(the|of|for|in|a|an)\b', '', variable_lower).strip()
    clean_variable = re.sub(r'\s+', ' ', clean_variable)
    
    # Get all column names from the original CSV data
    columns = list(data_handler.csv_data.columns)
    
    # Try exact match first
    for col in columns:
        if col.lower() == variable_lower or col.lower() == clean_variable:
            # Check if it's not an ID column
            if not is_id_column(col) and col != 'WardName':
                logger.info(f"Found exact match: {col} for {variable_name}")
                return col
    
    # Known variable name mappings and common variations
    variable_mappings = {
        'rainfall': ['rain', 'precipitation', 'precip', 'mean_rainfall', 'rainfall'],
        'temperature': ['temp', 'temperature', 'climate', 'mean_temperature', 'temp_mean'],
        'elevation': ['elev', 'altitude', 'height', 'dem'],
        'population': ['pop', 'people', 'inhabitants', 'population_density'],
        'distance_to_water': ['distance', 'dist', 'proximity', 'water_dist'],
        'housing_quality': ['house', 'dwelling', 'home', 'housing', 'building'],
        'ndvi': ['ndvi', 'vegetation', 'greenness', 'mean_ndvi'],
        'evi': ['evi', 'enhanced', 'mean_evi'],
        'mean_soil_wetness': ['soil', 'wetness', 'moisture', 'soil_wetness'],
        'flood': ['flood', 'inundation', 'water_extent'],
        'water': ['water', 'hydro', 'hydrologic'],
        'urban': ['urban', 'built', 'city']
    }
    
    # Check for matches using the mappings
    for standard_name, variants in variable_mappings.items():
        if any(variant in variable_lower for variant in variants):
            # Look for columns that match this variable
            for col in columns:
                col_lower = col.lower()
                if any(variant in col_lower for variant in variants):
                    if not is_id_column(col) and col != 'WardName':
                        logger.info(f"Found mapped match: {col} for {variable_name} via {standard_name}")
                        return col
    
    # Try partial match as fallback
    for col in columns:
        col_lower = col.lower()
        if (variable_lower in col_lower or clean_variable in col_lower or 
            any(term in col_lower for term in variable_lower.split())):
            if not is_id_column(col) and col != 'WardName':
                logger.info(f"Found partial match: {col} for {variable_name}")
                return col
    
    # Last resort: return first numeric column that's not an ID
    for col in columns:
        if col != 'WardName' and not is_id_column(col) and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]):
            logger.warning(f"No match found for {variable_name}, using {col} as fallback")
            return col
            
    logger.error(f"Could not find any suitable variable match for {variable_name}")
    return None

# Add a visualization cache class
class VisualizationCache:
    """
    Cache system for visualizations to improve performance.
    Stores visualization results to avoid regenerating the same visualizations repeatedly.
    """
    def __init__(self, cache_dir=None, max_cache_size=50, ttl=3600):
        """
        Initialize the visualization cache
        
        Args:
            cache_dir: Directory to store cache files
            max_cache_size: Maximum number of items to keep in cache
            ttl: Time to live for cache items in seconds (default: 1 hour)
        """
        self.cache_dir = cache_dir
        
        # Set a default cache directory if none is provided
        if not self.cache_dir:
            try:
                if current_app and current_app.instance_path:
                    self.cache_dir = os.path.join(current_app.instance_path, 'viz_cache')
                else:
                    # Fallback to a temp directory if no app context
                    import tempfile
                    self.cache_dir = os.path.join(tempfile.gettempdir(), 'chatmrpt_viz_cache')
            except Exception as e:
                # Final fallback - create in the current directory
                self.cache_dir = os.path.join(os.getcwd(), 'viz_cache')
                logging.warning(f"Using fallback cache directory: {self.cache_dir}")
        
        # Create cache directory if it doesn't exist
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            logging.info(f"Visualization cache directory: {self.cache_dir}")
        except Exception as e:
            logging.error(f"Failed to create cache directory: {e}")
            # If we can't create the directory, use in-memory only
            self.cache_dir = None
            logging.warning("Using in-memory cache only")
        
        self.max_cache_size = max_cache_size
        self.ttl = ttl
        self.cache = {}
        self.cache_keys = []  # Track order for LRU removal

    def _get_cache_key(self, viz_type, params=None):
        """
        Generate a unique cache key based on visualization type and parameters
        
        Args:
            viz_type: Type of visualization
            params: Visualization parameters
            
        Returns:
            str: Unique cache key
        """
        # Convert params to a serializable format
        params_str = str(convert_to_json_serializable(params or {}))
        
        # Create a hash of the type and params
        key_str = f"{viz_type}:{params_str}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, viz_type, params=None):
        """
        Get a visualization from cache
        
        Args:
            viz_type: Type of visualization
            params: Visualization parameters
            
        Returns:
            dict or None: Cached visualization or None if not found
        """
        cache_key = self._get_cache_key(viz_type, params)
        
        # First check in-memory cache
        if cache_key in self.cache:
            # Check if the cached item has expired
            cache_item = self.cache[cache_key]
            if time.time() - cache_item['timestamp'] <= self.ttl:
                # Update LRU order
                self.cache_keys.remove(cache_key)
                self.cache_keys.append(cache_key)
                return cache_item['data']
            else:
                # Expired, remove from cache
                del self.cache[cache_key]
                self.cache_keys.remove(cache_key)
        
        # Try to load from file if not in memory
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.cache")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'rb') as f:
                    cache_item = pickle.load(f)
                
                # Check if file cache has expired
                if time.time() - cache_item['timestamp'] <= self.ttl:
                    # Add to in-memory cache
                    self.cache[cache_key] = cache_item
                    self.cache_keys.append(cache_key)
                    
                    # Manage cache size
                    self._manage_cache_size()
                    
                    return cache_item['data']
                else:
                    # Expired, remove file
                    os.remove(cache_file)
            except Exception as e:
                import logging
                logging.error(f"Error loading cache file: {e}")
        
        # Not found in cache
        return None
    
    def set(self, viz_type, params, data):
        """
        Store a visualization in cache
        
        Args:
            viz_type: Type of visualization
            params: Visualization parameters
            data: Visualization data to cache
            
        Returns:
            str: Cache key
        """
        cache_key = self._get_cache_key(viz_type, params)
        
        # Create cache item
        cache_item = {
            'timestamp': time.time(),
            'data': data
        }
        
        # Update in-memory cache
        self.cache[cache_key] = cache_item
        
        # Update LRU order
        if cache_key in self.cache_keys:
            self.cache_keys.remove(cache_key)
        self.cache_keys.append(cache_key)
        
        # Manage cache size
        self._manage_cache_size()
        
        # Also save to file
        try:
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.cache")
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_item, f)
        except Exception as e:
            import logging
            logging.error(f"Error saving cache file: {e}")
        
        return cache_key
    
    def clear(self, viz_type=None):
        """
        Clear cache items
        
        Args:
            viz_type: Type of visualization to clear (None to clear all)
            
        Returns:
            int: Number of items cleared
        """
        cleared_count = 0
        
        if viz_type:
            # Clear specific visualization type
            keys_to_remove = []
            for key in self.cache_keys:
                if key.startswith(viz_type):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                self.cache_keys.remove(key)
                del self.cache[key]
                cleared_count += 1
                
                # Also remove from file
                cache_file = os.path.join(self.cache_dir, f"{key}.cache")
                if os.path.exists(cache_file):
                    os.remove(cache_file)
        else:
            # Clear all
            cleared_count = len(self.cache)
            self.cache = {}
            self.cache_keys = []
            
            # Clear all cache files
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.cache'):
                    os.remove(os.path.join(self.cache_dir, filename))
        
        return cleared_count
    
    def _manage_cache_size(self):
        """
        Ensure cache doesn't exceed max size by removing LRU items
        """
        while len(self.cache_keys) > self.max_cache_size:
            # Remove oldest item (LRU)
            oldest_key = self.cache_keys.pop(0)
            del self.cache[oldest_key]

# Initialize global visualization cache
viz_cache = VisualizationCache()

# Update the create_visualization function to use cache
def create_visualization(viz_type, data_handler, variable=None, threshold=30):
    """
    Create a visualization based on the specified type
    
    Args:
        viz_type: Type of visualization ('variable_map', 'composite_map', etc.)
        data_handler: DataHandler instance
        variable: Variable name for variable-specific visualizations
        threshold: Threshold for certain visualizations (e.g., urban extent)
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Check if data handler is available
        if data_handler is None:
            return {
                'status': 'error',
                'message': 'Data handler not available'
            }
        
        # Creating a parameter dict for cache lookup
        cache_params = {
            'variable': variable,
            'threshold': threshold
        }
        
        # Check if session_id is available for more specific caching
        if hasattr(data_handler, 'session_id'):
            cache_params['session_id'] = data_handler.session_id
        
        # Check cache first
        cached_result = viz_cache.get(viz_type, cache_params)
        if cached_result:
            import logging
            logging.info(f"Using cached visualization for {viz_type}")
            return cached_result
        
        # Generate the visualization based on type
        import logging
        logging.info(f"Generating visualization: {viz_type}")
        
        # Measure execution time
        start_time = time.time()
        
        result = None
        if viz_type == 'variable_map':
            result = create_variable_map(data_handler, variable)
        elif viz_type == 'composite_map':
            result = create_composite_map(data_handler)
        elif viz_type == 'vulnerability_map':
            result = create_vulnerability_map(data_handler)
        elif viz_type == 'urban_extent_map':
            result = create_urban_extent_map(data_handler, threshold)
        elif viz_type == 'vulnerability_plot':
            result = create_vulnerability_plot(data_handler)
        else:
            return {
                'status': 'error',
                'message': f'Unknown visualization type: {viz_type}'
            }
        
        # Log execution time
        execution_time = time.time() - start_time
        logging.info(f"Visualization generation time for {viz_type}: {execution_time:.2f} seconds")
        
        # Add execution time to result for monitoring
        if result and result['status'] == 'success':
            result['execution_time'] = execution_time
            
            # Store in cache if successful
            viz_cache.set(viz_type, cache_params, result)
        
        return result
    except Exception as e:
        import logging
        import traceback
        logging.error(f"Error creating visualization: {str(e)}")
        logging.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': f'Error creating visualization: {str(e)}'
        }

def create_variable_map(data_handler, variable_name=None):
    """
    Create a map visualizing a variable's distribution
    
    Args:
        data_handler: DataHandler instance
        variable_name: Name of the variable to visualize
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Check if shapefile data is available
        if data_handler.shapefile_data is None:
            return {
                'status': 'error',
                'message': 'Shapefile data not loaded'
            }
        
        # If no variable specified, pick the first suitable variable
        if variable_name is None:
            if data_handler.csv_data is not None:
                var_columns = [col for col in data_handler.csv_data.columns 
                            if col != 'WardName' and not is_id_column(col)]
                if var_columns:
                    variable_name = var_columns[0]
        
        # Find the best matching variable - IMPORTANT: Always allow access to all variables in CSV data
        actual_variable = get_variable_by_name(data_handler, variable_name)
        
        if not actual_variable:
            available_vars = []
            if data_handler.csv_data is not None:
                available_vars = [col for col in data_handler.csv_data.columns 
                               if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]) 
                               and not is_id_column(col)]
            
            return {
                'status': 'error',
                'message': f'Variable similar to "{variable_name}" not found in data',
                'available_variables': available_vars
            }
        
        # Check if this variable has missing values that were cleaned
        has_missing = False
        missing_count = 0
        
        # Use csv_data for original values
        df = data_handler.csv_data
        
        if actual_variable in df.columns:
            missing_count = df[actual_variable].isna().sum()
            has_missing = missing_count > 0
        
        # Get full variable name for display
        full_variable_name = get_full_variable_name(actual_variable)
        
        # Get a copy of the shapefile with standardized CRS
        shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
        
        # If we have missing values and cleaned data, show both maps
        if has_missing and data_handler.cleaned_data is not None:
            # Create figure with two subplots side by side
            fig = make_subplots(
                rows=1, cols=2,
                specs=[[{"type": "mapbox"}, {"type": "mapbox"}]],
                subplot_titles=[f"Original Data ({missing_count} missing values)", "Cleaned Data"],
                horizontal_spacing=0.02
            )
            
            # 1. Original data map
            # Create combined dataframe for plotting
            gdf_original = shapefile_data.merge(df[['WardName', actual_variable]], on='WardName', how='left')
            
            # Convert geometry to geojson with proper serialization
            gdf_prepared = prepare_geodataframe_for_json(gdf_original)
            geojson = json.loads(gdf_prepared.to_json())
            
            # Add choropleth for original data
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf_original.index,
                    z=gdf_original[actual_variable],
                    colorscale='Blues',
                    marker_opacity=0.8,
                    marker_line_width=0.5,
                    marker_line_color='black',
                    hovertemplate='<b>%{customdata}</b><br>' + f'{full_variable_name}: ' + '%{z:.2f}<extra></extra>',
                    customdata=gdf_original['WardName'],
                    showscale=False
                ),
                row=1, col=1
            )
            
            # 2. Cleaned data map
            # Create combined dataframe for plotting
            gdf_cleaned = shapefile_data.merge(data_handler.cleaned_data[['WardName', actual_variable]], on='WardName', how='left')
            
            # Convert geometry to geojson with proper serialization
            gdf_prepared = prepare_geodataframe_for_json(gdf_cleaned)
            geojson = json.loads(gdf_prepared.to_json())
            
            # Add choropleth for cleaned data
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf_cleaned.index,
                    z=gdf_cleaned[actual_variable],
                    colorscale='Blues',
                    marker_opacity=0.8,
                    marker_line_width=0.5,
                    marker_line_color='black',
                    hovertemplate='<b>%{customdata}</b><br>' + f'{full_variable_name}: ' + '%{z:.2f}<extra></extra>',
                    customdata=gdf_cleaned['WardName'],
                    colorbar=dict(
                        title=dict(
                            text=full_variable_name,
                            font=dict(size=12)
                        )
                    )
                ),
                row=1, col=2
            )
            
            # Get proper map centering
            center_lat = gdf_original.geometry.centroid.y.mean()
            center_lon = gdf_original.geometry.centroid.x.mean()
            
            # Calculate appropriate zoom level based on the bounding box
            bounds = gdf_original.geometry.total_bounds  # minx, miny, maxx, maxy
            span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
            span_y = max(0.01, bounds[3] - bounds[1])
            
            # Calculate zoom level - ensure it's reasonable
            zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
            
            # Update mapbox settings for both subplots
            fig.update_mapboxes(
                style="carto-positron",
                center={"lat": center_lat, "lon": center_lon},
                zoom=zoom_level
            )
        
        else:
            # Single map - just show the data we have
            fig = go.Figure()
            
            # Use cleaned data if available and the variable exists there, otherwise use original
            if data_handler.cleaned_data is not None and actual_variable in data_handler.cleaned_data.columns:
                df_to_use = data_handler.cleaned_data
            else:
                df_to_use = df
            
            # Create combined dataframe for plotting
            gdf = shapefile_data.merge(df_to_use[['WardName', actual_variable]], on='WardName', how='left')
            
            # Convert geometry to geojson with proper serialization
            gdf_prepared = prepare_geodataframe_for_json(gdf)
            geojson = json.loads(gdf_prepared.to_json())
            
            # Get proper map centering
            center_lat = gdf.geometry.centroid.y.mean()
            center_lon = gdf.geometry.centroid.x.mean()
            
            # Calculate appropriate zoom level based on the bounding box
            bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
            span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
            span_y = max(0.01, bounds[3] - bounds[1])
            
            # Calculate zoom level - ensure it's reasonable
            zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
            
            # Add choropleth
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf.index,
                    z=gdf[actual_variable],
                    colorscale='Blues',
                    marker_opacity=0.8,
                    marker_line_width=0.5,
                    marker_line_color='black',
                    hovertemplate='<b>%{customdata}</b><br>' + f'{full_variable_name}: ' + '%{z:.2f}<extra></extra>',
                    customdata=gdf['WardName'],
                    colorbar=dict(
                        title=dict(
                            text=full_variable_name,
                            font=dict(size=12)
                        )
                    )
                )
            )
            
            # Update mapbox settings
            fig.update_layout(
                mapbox=dict(
                    style="carto-positron",
                    center={"lat": center_lat, "lon": center_lon},
                    zoom=zoom_level
                )
            )
        
        # Update overall layout - IMPORTANT CHANGES HERE
        fig.update_layout(
            title={
                'text': f"Distribution of {full_variable_name}",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}
            },
            # Remove fixed dimensions
            # height=480,  # Remove fixed height
            # width=800,   # Remove fixed width
            margin=dict(l=20, r=20, t=80, b=20),
            autosize=True,  # Enable responsive sizing
            paper_bgcolor='rgba(255,255,255,0.8)',
            plot_bgcolor='rgba(255,255,255,0.8)'
        )
        
        # Create HTML file with improved config
        html_path = create_plotly_html(
            fig, 
            f"variable_map_{actual_variable}.html", 
            config={
                'responsive': True,
                'displayModeBar': True,
                'scrollZoom': True,
                'fillFrame': True  # Fill frame completely
            }
        )
        
        # Prepare data summary for LLM context
        if df is not None and actual_variable in df.columns:
            values = df[actual_variable].dropna().values
            data_stats = {
                'min': float(np.min(values)) if len(values) > 0 else None,
                'max': float(np.max(values)) if len(values) > 0 else None,
                'mean': float(np.mean(values)) if len(values) > 0 else None,
                'median': float(np.median(values)) if len(values) > 0 else None,
                'missing_count': int(missing_count),
                'missing_percentage': float(missing_count / len(df) * 100) if len(df) > 0 else 0
            }
        else:
            data_stats = {'error': 'Statistics not available'}

        # Create rich context for LLM
        data_summary = {
            'variable': actual_variable,
            'full_variable_name': full_variable_name,
            'missing_values': missing_count,
            'has_missing_values': has_missing,
            'cleaned_data_available': data_handler.cleaned_data is not None,
            'statistics': data_stats
        }
        
        visual_elements = {
            'map_type': 'choropleth',
            'color_scale': 'Blues',
            'color_meaning': 'darker blue = higher values',
            'split_view': has_missing and data_handler.cleaned_data is not None
        }

        # Return success with paths and metadata
        return {
           'status': 'success',
           'message': f'Successfully created map for {full_variable_name}',
           'image_path': html_path,
           'variable': actual_variable,
           'full_variable_name': full_variable_name,
           'missing_values': missing_count,
           'viz_type': 'variable_map',
           'data_summary': data_summary,
           'visual_elements': visual_elements
        }
       
    except Exception as e:
       logger.error(f"Error creating variable map: {str(e)}", exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': f'Error creating variable map: {str(e)}'
       }

def create_normalized_map(data_handler, variable_name=None):
   """
   Create a map visualizing a normalized variable
   
   Args:
       data_handler: DataHandler instance
       variable_name: Name of the variable to visualize
       
   Returns:
       dict: Status and visualization information
   """
   try:
       # If no variable specified, pick the first suitable variable
       if variable_name is None:
           if data_handler.normalized_data is not None:
               norm_cols = [col for col in data_handler.normalized_data.columns 
                           if col.startswith('normalization_')]
               if norm_cols:
                   variable_name = norm_cols[0].replace('normalization_', '')
           elif data_handler.cleaned_data is not None:
               var_columns = [col for col in data_handler.cleaned_data.columns 
                           if col != 'WardName' and not is_id_column(col)]
               if var_columns:
                   variable_name = var_columns[0]
           elif data_handler.csv_data is not None:
               var_columns = [col for col in data_handler.csv_data.columns 
                           if col != 'WardName' and not is_id_column(col)]
               if var_columns:
                   variable_name = var_columns[0]
       
       # Check if normalized data is available
       if data_handler.normalized_data is None:
           # Try to normalize the data now
           try:
               norm_result = data_handler.normalize_data()
               if norm_result['status'] != 'success':
                   return {
                       'status': 'error',
                       'message': 'Could not normalize data: ' + norm_result.get('message', 'Unknown error')
                   }
           except Exception as e:
               return {
                   'status': 'error',
                   'message': f'Normalized data not available. Error: {str(e)}'
               }
       
       if data_handler.shapefile_data is None:
           return {
               'status': 'error',
               'message': 'Shapefile data not loaded'
           }
       
       # Find the best matching variable
       actual_variable = get_variable_by_name(data_handler, variable_name)
       
       if not actual_variable:
           # Try to check available normalized columns
           norm_vars = []
           if data_handler.normalized_data is not None:
               norm_vars = [col.replace('normalization_', '') for col in data_handler.normalized_data.columns 
                          if col.startswith('normalization_')]
           
           # If no match but we do have normalized variables, use the first one
           if norm_vars:
               actual_variable = norm_vars[0]
           else:
               available_vars = []
               if data_handler.csv_data is not None:
                   available_vars = [col for col in data_handler.csv_data.columns 
                                  if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]) 
                                  and not is_id_column(col)]
               
               return {
                   'status': 'error',
                   'message': f'Variable similar to "{variable_name}" not found and no normalized variables available',
                   'available_variables': available_vars
               }
       
       # Normalized column name
       norm_col = f"normalization_{actual_variable.lower()}"
       
       # Check if the normalized column exists
       if norm_col not in data_handler.normalized_data.columns:
           # Try to find a similar normalized column
           all_norm_cols = [col for col in data_handler.normalized_data.columns if col.startswith('normalization_')]
           
           # Try direct match with variable name (case insensitive)
           similar_cols = [col for col in all_norm_cols 
                         if actual_variable.lower() == col.replace('normalization_', '').lower()]
           
           # If no direct match, try partial match
           if not similar_cols:
               similar_cols = [col for col in all_norm_cols 
                             if actual_variable.lower() in col.replace('normalization_', '').lower()]
           
           if similar_cols:
               norm_col = similar_cols[0]
               # Extract original variable name from normalized column name
               actual_variable = norm_col.replace('normalization_', '')
               logger.info(f"Found normalized column '{norm_col}' for variable '{variable_name}'")
           else:
               # Try to normalize from analysis.py
               try:
                   from app.models.analysis import normalize_variable
                   
                   # Get variable relationship
                   relationship = 'direct'
                   if hasattr(data_handler, 'variable_relationships') and actual_variable in data_handler.variable_relationships:
                       relationship = data_handler.variable_relationships[actual_variable]
                   
                   # Get data from cleaned data
                   if data_handler.cleaned_data is not None and actual_variable in data_handler.cleaned_data.columns:
                       values = data_handler.cleaned_data[actual_variable].values
                       normalized = normalize_variable(values, relationship)
                       
                       # Add to normalized data
                       if data_handler.normalized_data is None:
                           data_handler.normalized_data = data_handler.cleaned_data[['WardName']].copy()
                       
                       norm_col = f"normalization_{actual_variable.lower()}"
                       data_handler.normalized_data[norm_col] = normalized
                       
                       logger.info(f"Created normalized column '{norm_col}' for variable '{actual_variable}'")
                   else:
                       return {
                           'status': 'error',
                           'message': f'Variable {actual_variable} not found in cleaned data'
                       }
               except Exception as e:
                   return {
                       'status': 'error',
                       'message': f'Error normalizing variable {actual_variable}: {str(e)}'
                   }
       
       # Double-check that the normalized column exists now
       if norm_col not in data_handler.normalized_data.columns:
           return {
               'status': 'error', 
               'message': f'Failed to create normalized column for {actual_variable}'
           }
       
       # Get variable relationship
       relationship = 'direct'
       if hasattr(data_handler, 'variable_relationships') and actual_variable in data_handler.variable_relationships:
           relationship = data_handler.variable_relationships[actual_variable]
       
       # Get full variable name for display
       full_variable_name = get_full_variable_name(actual_variable)
       
       # Get a copy of the shapefile with standardized CRS
       shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
       
       # Create combined dataframe for plotting
       gdf = shapefile_data.merge(
           data_handler.normalized_data[['WardName', norm_col]], 
           on='WardName', 
           how='left'
       )
       
       # Convert geometry to geojson with proper serialization
       gdf_prepared = prepare_geodataframe_for_json(gdf)
       geojson = json.loads(gdf_prepared.to_json())
       
       # Get proper map centering
       center_lat = gdf.geometry.centroid.y.mean()
       center_lon = gdf.geometry.centroid.x.mean()
       
       # Calculate appropriate zoom level based on the bounding box
       bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
       span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
       span_y = max(0.01, bounds[3] - bounds[1])
       
       # Calculate zoom level - ensure it's reasonable
       zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
       
       # Create choropleth map with Plotly
       fig = go.Figure()
       
       fig.add_trace(go.Choroplethmapbox(
           geojson=geojson,
           locations=gdf.index,
           z=gdf[norm_col],
           colorscale='YlOrRd',
           marker_opacity=0.8,
           marker_line_width=0.5,
           marker_line_color='black',
           hovertemplate='<b>%{customdata}</b><br>Normalized Value: %{z:.3f}<extra></extra>',
           customdata=gdf['WardName'],
           zmin=0,
           zmax=1,
           colorbar=dict(
               title=dict(
                   text='Risk Contribution' if relationship == 'direct' else 'Risk Contribution (Inverted)',
                   font=dict(size=12)
               ),
               tickvals=[0, 0.25, 0.5, 0.75, 1],
               ticktext=['Very Low', 'Low', 'Medium', 'High', 'Very High']
           )
       ))
       
       # Update layout
       fig.update_layout(
           title={
               'text': f"Normalized {full_variable_name} ({relationship} relationship)",
               'x': 0.5,
               'xanchor': 'center',
               'font': {'size': 20}
           },
           mapbox=dict(
               style="carto-positron",
               center={"lat": center_lat, "lon": center_lon},
               zoom=zoom_level
           ),
           margin=dict(l=20, r=20, t=80, b=20),  # Adjusted margins
           autosize=True  # Enable autosize for responsiveness
       )
       
       # Create HTML file
       html_path = create_plotly_html(fig, f"normalized_map_{actual_variable}.html")
       
       # Prepare data summary for LLM context
       norm_values = gdf[norm_col].dropna().values
       data_stats = {
           'min': float(np.min(norm_values)) if len(norm_values) > 0 else None,
           'max': float(np.max(norm_values)) if len(norm_values) > 0 else None,
           'mean': float(np.mean(norm_values)) if len(norm_values) > 0 else None,
           'median': float(np.median(norm_values)) if len(norm_values) > 0 else None
       }
       
       # Create rich context for LLM
       data_summary = {
           'variable': actual_variable,
           'full_variable_name': full_variable_name,
           'relationship': relationship,
           'relationship_explanation': "Higher values correspond to higher malaria risk" if relationship == "direct" else 
                                     "Higher values correspond to lower malaria risk (the relationship is inverted)",
           'statistics': data_stats
       }
       
       visual_elements = {
           'map_type': 'choropleth',
           'color_scale': 'YlOrRd',
           'color_meaning': 'yellow to dark red (low to high risk contribution)',
           'scale_range': '0-1 normalized values'
       }
       
       # Return success with paths and metadata
       return {
           'status': 'success',
           'message': f'Successfully created normalized map for {full_variable_name}',
           'image_path': html_path,
           'variable': actual_variable,
           'full_variable_name': full_variable_name,
           'relationship': relationship,
           'viz_type': 'normalized_map',
           'data_summary': data_summary,
           'visual_elements': visual_elements
       }
       
   except Exception as e:
           logger.error(f"Error creating normalized map: {str(e)}", exc_info=True)
           import traceback
           logger.error(traceback.format_exc())
           return {
               'status': 'error',
               'message': f'Error creating normalized map: {str(e)}'
           }

def create_composite_map(data_handler, model_index=None):
   """
   Create composite risk score maps
   
   Args:
       data_handler: DataHandler instance
       model_index: Index of the model/page to visualize (None for first page)
       
   Returns:
       dict: Status and visualization information
   """
   try:
       # Check if composite scores are available
       if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
           # Try to reload from saved files
           try:
               scores_path = os.path.join(data_handler.session_folder, 'composite_scores.csv')
               formulas_path = os.path.join(data_handler.session_folder, 'model_formulas.csv')
               
               if os.path.exists(scores_path) and os.path.exists(formulas_path):
                   scores_df = pd.read_csv(scores_path)
                   formulas_df = pd.read_csv(formulas_path)
                   
                   # Recreate composite_scores structure
                   data_handler.composite_scores = {
                       'scores': scores_df,
                       'model_formulas': []
                   }
                   
                   # Convert formulas DataFrame to list of dicts
                   for _, row in formulas_df.iterrows():
                       formula_dict = {
                           'model': row['model'],
                           'variables': row['variables'].split(',') if isinstance(row['variables'], str) else []
                       }
                       data_handler.composite_scores['model_formulas'].append(formula_dict)
               else:
                   return {
                       'status': 'error',
                       'message': 'Composite scores not available. Calculate composite scores first.'
                   }
           except Exception as e:
               return {
                   'status': 'error',
                   'message': f'Error loading composite scores: {str(e)}'
               }
       
       if data_handler.shapefile_data is None:
           return {
               'status': 'error',
               'message': 'Shapefile data not loaded'
           }
       
       # Get all model columns
       model_columns = [col for col in data_handler.composite_scores['scores'].columns if col.startswith('model_')]
       model_formulas = data_handler.composite_scores['model_formulas']
       
       # Determine number of models and pages
       n_models = len(model_columns)
       models_per_page = 4
       n_pages = (n_models + models_per_page - 1) // models_per_page
       
       # If model_index is a number, treat it as a page number
       page = 1
       if isinstance(model_index, int) or isinstance(model_index, float) or (isinstance(model_index, str) and model_index.isdigit()):
           page = int(model_index)
           # Ensure page is within bounds
           page = max(1, min(page, n_pages))
       
       # Calculate start and end indices for this page
       start_idx = (page - 1) * models_per_page
       end_idx = min(start_idx + models_per_page, n_models)
       
       # Get models for this page
       page_models = model_columns[start_idx:end_idx]
       page_formulas = model_formulas[start_idx:end_idx]
       
       # Get a copy of the shapefile with standardized CRS
       shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
       
       # Check if the shapefile has an Urban column to identify non-urban wards
       urban_column = None
       for col in ['Urban', 'urban', 'URBAN', 'UrbanStatus']:
           if col in shapefile_data.columns:
               urban_column = col
               break
       
       # Combine with shapefile
       gdf = shapefile_data.merge(
           data_handler.composite_scores['scores'],
           on='WardName',
           how='left'
       )
       
       # If we have an Urban column, identify "Not Ideal" models
       not_ideal_models = {}
       if urban_column is not None:
           # For each model, check if non-urban wards (Urban="No") are in the top 5 for vulnerability
           for model in model_columns:
               # Sort wards by model score (descending) to find top 5
               top_wards = gdf.sort_values(model, ascending=False).head(5)
               
               # Check if any of these wards are non-urban
               non_urban_top_wards = top_wards[top_wards[urban_column].str.lower().isin(['no', 'false', '0', 'n'])]
               
               # If there are non-urban wards in top 5, flag this model as "Not Ideal"
               if len(non_urban_top_wards) > 0:
                   not_ideal_models[model] = non_urban_top_wards['WardName'].tolist()
       
       # Convert geometry to geojson with proper serialization
       gdf_prepared = prepare_geodataframe_for_json(gdf)
       geojson = json.loads(gdf_prepared.to_json())
       
       # Get proper map centering
       center_lat = gdf.geometry.centroid.y.mean()
       center_lon = gdf.geometry.centroid.x.mean()
       
       # Calculate appropriate zoom level based on the bounding box
       bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
       span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
       span_y = max(0.01, bounds[3] - bounds[1])
       
       # Calculate zoom level - ensure it's reasonable
       zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
       
       # Determine grid layout for subplots
       if len(page_models) == 1:
           rows, cols = 1, 1
       elif len(page_models) == 2:
           rows, cols = 1, 2
       else:
           rows, cols = 2, 2
       
       # Create subplot titles with variables on separate lines
       subplot_titles = []
       for model, formula in zip(page_models, page_formulas):
           # Get variables
           variables = formula['variables']
           
           # Check if we have any variables
           if variables and len(variables) > 0:
               # Create title with variables on separate lines
               var_names = []
               for var in variables:
                   # Get full name if available
                   var_name = get_full_variable_name(var.lower())
                   var_names.append(var_name)
               
               # Join with line breaks
               title = "<br>".join(var_names)
               
               # Add "Not Ideal" designation if this model is flagged
               if model in not_ideal_models:
                   title = f"{title}<br><span class='not-ideal-label'>(Not Ideal)</span>"
           else:
               # Fallback if no variables
               title = f"{model.replace('model_', 'Model ')}"
               if model in not_ideal_models:
                   title = f"{title}<br><span class='not-ideal-label'>(Not Ideal)</span>"
               
           subplot_titles.append(title)
       
       # Create subplots
       fig = make_subplots(
           rows=rows,
           cols=cols,
           specs=[[{"type": "mapbox"}] * cols for _ in range(rows)],
           subplot_titles=subplot_titles,
           vertical_spacing=0.22,  # Increased vertical spacing significantly
           horizontal_spacing=0.05 # Can adjust this too if needed
       )
       
       # Add choropleth for each model
       for idx, model in enumerate(page_models):
           row = idx // cols + 1
           col = idx % cols + 1
           
           # Add choropleth trace for the model
           fig.add_trace(
               go.Choroplethmapbox(
                   geojson=geojson,
                   locations=gdf.index,
                   z=gdf[model],
                   colorscale='YlOrRd',
                   marker_line_color='black',
                   marker_line_width=0.5,
                   showscale=(idx == 0),  # Only show scale for first plot
                   colorbar=dict(
                       title=dict(
                           text="Risk Score",
                           font=dict(size=12)
                       ),
                       tickvals=[0, 0.25, 0.5, 0.75, 1],  # Five tick values
                       ticktext=["Very Low", "Low", "Medium", "High", "Very High"]  # Five labels
                   ) if idx == 0 else None,
                   hovertemplate='<b>%{customdata}</b><br>Risk Score: %{z:.3f}<extra></extra>',
                   customdata=gdf['WardName'],
                   zmin=0,
                   zmax=1
               ),
               row=row, col=col
           )
           
           # If this model is flagged as "Not Ideal", add blue outlines to the non-urban wards
           if model in not_ideal_models and urban_column is not None:
               # Get non-urban wards in the top 5
               non_urban_wards = not_ideal_models[model]
               
               # Create mask for these wards
               ward_mask = gdf['WardName'].isin(non_urban_wards)
               
               # Add a separate trace with blue outlines for these wards
               if any(ward_mask):
                   fig.add_trace(
                       go.Choroplethmapbox(
                           geojson=geojson,
                           locations=gdf[ward_mask].index,
                           z=gdf[ward_mask][model],
                           colorscale='YlOrRd',
                           marker_line_color='blue',
                           marker_line_width=3,
                           showscale=False,
                           hovertemplate='<b>%{customdata}</b><br>Risk Score: %{z:.3f}<br><span style="color:blue;">Non-Urban Ward</span><extra></extra>',
                           customdata=gdf[ward_mask]['WardName'],
                           zmin=0,
                           zmax=1
                       ),
                       row=row, col=col
                   )
       
       # Update mapbox settings for each subplot
       for i in range(1, rows * cols + 1):
           if i <= len(page_models):
               fig.update_mapboxes(
                   style="carto-positron",
                   center={"lat": center_lat, "lon": center_lon},
                   zoom=zoom_level,
                   row=((i-1)//cols)+1, col=((i-1)%cols)+1
               )
       
       # Update overall layout - ensuring title doesn't overlap with subplot titles
       fig.update_layout(
           title={
               'text': f"Composite Score Distribution by Model<br><span style='font-size:16px'>Page {page} of {n_pages}</span>",
               'x': 0.5,
               'xanchor': 'center',
               'font': {'size': 18}, # Slightly smaller main title
               'y': 0.97,  # Adjusted Y position for title
               'yanchor': 'top'
           },
           height=600, # Increased height slightly for 2x2 plots
           margin=dict(t=100, b=60, l=50, r=50),  # Increased top margin, adjusted others
           autosize=True # Let Plotly try to size within the iframe
       )
       
       # Add a caption explaining the "Not Ideal" designation
       if any(model in not_ideal_models for model in page_models):
           fig.add_annotation(
               x=0.5,
               y=-0.05,
               xref="paper",
               yref="paper",
               text="Blue outlines indicate non-urban wards ranked in top 5 for vulnerability (not ideal for prioritization)",
               showarrow=False,
               font=dict(size=12, color="blue"),
               align="center"
           )
       
       # Create HTML file
       html_path = create_plotly_html(fig, f"composite_map_page{page}.html")
       
       # Get list of unique variables used across models
       all_variables = set()
       for formula in page_formulas:
           for variable in formula['variables']:
               all_variables.add(variable)
       
       # Create comma-separated list of full variable names
       full_var_names = [get_full_variable_name(var) for var in sorted(list(all_variables))]
       
       # Create model details for contextual understanding
       model_details = []
       for i, (model, formula) in enumerate(zip(page_models, page_formulas)):
           variables = formula['variables']
           full_var_names_model = [get_full_variable_name(var) for var in variables]
           
           model_detail = {
               'model_name': model,
               'variables': variables,
               'full_variable_names': full_var_names_model,
               'is_not_ideal': model in not_ideal_models,
               'non_urban_wards': not_ideal_models.get(model, []) if model in not_ideal_models else []
           }
           model_details.append(model_detail)
       
       # Create rich context for LLM
       data_summary = {
           'current_page': page,
           'total_pages': n_pages,
           'models_on_page': len(page_models),
           'all_variable_count': len(all_variables),
           'all_variables': list(all_variables),
           'all_full_variable_names': full_var_names,
           'not_ideal_count': sum(1 for model in page_models if model in not_ideal_models)
       }
       
       visual_elements = {
           'map_type': 'choropleth',
           'color_scale': 'YlOrRd',
           'color_meaning': 'yellow to dark red (low to high risk)',
           'scale_range': '0-1 normalized risk scores',
           'layout': f"{rows}x{cols} grid",
           'blue_outline': 'Indicates non-urban wards in top 5 (not ideal for prioritization)',
           'model_details': model_details
       }
       
       # Return success with pagination info
       return {
           'status': 'success',
           'message': f'Successfully created composite risk maps (page {page} of {n_pages})',
           'image_path': html_path,
           'current_page': page,
           'total_pages': n_pages,
           'viz_type': 'composite_map',
           'data_summary': data_summary,
           'visual_elements': visual_elements,
           'model_details': model_details
       }
       
   except Exception as e:
       logger.error(f"Error creating composite maps: {str(e)}", exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': f'Error creating composite maps: {str(e)}'
       }

def box_plot_function(processed_scores, wards_per_page=20):
   """
   Create paginated box plots of ward rankings
   
   Args:
       processed_scores: DataFrame with processed model scores data
       wards_per_page: Number of wards to display per page (default: 20)
       
   Returns:
       Dict with plotly objects for each page and ward rankings
   """
   try:
       # Create a copy to avoid modifying original
       df_long = processed_scores.copy()
       
       # Get model columns (starting with 'model_')
       model_cols = [col for col in df_long.columns if col.startswith('model_')]
       
       if not model_cols:
           return {
               'status': 'error',
               'message': 'No model scores found in data'
           }
       
       # Melt the dataframe to long format for plotting - vectorized operation
       melted_df = pd.melt(
           df_long, 
           id_vars=['WardName'], 
           value_vars=model_cols,
           var_name='variable', 
           value_name='value'
       )
       
       # Calculate ward rankings - lower rank value = HIGHER vulnerability
       # Vectorized operations using pandas
       ward_rankings = melted_df.groupby('WardName')['value'].median().reset_index()
       ward_rankings = ward_rankings.sort_values('value', ascending=False)
       ward_rankings['overall_rank'] = range(1, len(ward_rankings) + 1)
       
       # Create vulnerability categories (high, medium, low)
       ward_rankings['vulnerability_category'] = pd.cut(
           ward_rankings['overall_rank'],
           bins=[0, len(ward_rankings)//3, 2*len(ward_rankings)//3, len(ward_rankings)],
           labels=['High', 'Medium', 'Low']
       )
       
       # Merge rankings back to the melted dataframe - vectorized
       df_long = pd.merge(melted_df, ward_rankings[['WardName', 'overall_rank', 'vulnerability_category']], on='WardName')
       
       # Sort by overall rank (most vulnerable wards at the top)
       df_long['WardName'] = pd.Categorical(
           df_long['WardName'],
           categories=ward_rankings.sort_values('overall_rank')['WardName'],
           ordered=True
       )
       
       # Calculate the number of pages needed
       total_wards = len(ward_rankings)
       total_pages = (total_wards + wards_per_page - 1) // wards_per_page
       
       # Store page data for later reference
       page_data = {}
       
       # Define a function to create a plot for a single page
       def create_page_plot(page):
           # Calculate start and end indices for this page
           start_idx = (page - 1) * wards_per_page
           end_idx = min(start_idx + wards_per_page, total_wards)
           
           # Get ward names for this page based on ranking
           page_wards = ward_rankings.sort_values('overall_rank')['WardName'].iloc[start_idx:end_idx].tolist()
           
           # Filter data for these wards - vectorized
           page_data[str(page)] = []
           for ward in page_wards:
               ward_rank_info = ward_rankings[ward_rankings['WardName'] == ward].iloc[0]
               page_data[str(page)].append({
                   'ward_name': ward,
                   'overall_rank': int(ward_rank_info['overall_rank']),
                   'median_score': float(ward_rank_info['value']),
                   'vulnerability_category': str(ward_rank_info['vulnerability_category'])
               })
           
           page_df = df_long[df_long['WardName'].isin(page_wards)].copy()
           
           # Create helper column for sorting
           page_df = pd.merge(
               page_df,
               pd.DataFrame({'WardName': page_wards, 'sort_order': range(len(page_wards))}),
               on='WardName'
           )
           
           # Sort by the helper column
           page_df = page_df.sort_values('sort_order')
           
           # Create figure
           fig = go.Figure()
           
           # For each ward, add a box plot
           for ward in page_wards:
               ward_data = page_df[page_df['WardName'] == ward]
               rank = ward_rankings[ward_rankings['WardName'] == ward]['overall_rank'].values[0]
               category = ward_rankings[ward_rankings['WardName'] == ward]['vulnerability_category'].values[0]
               
               # Set color based on vulnerability category
               if category == 'High':
                   box_color = '#69b3a2'  # Green-blue
               elif category == 'Medium':
                   box_color = '#a8d8b9'  # Light green
               else:
                   box_color = '#c7e9c0'  # Very light green
               
               fig.add_trace(go.Box(
                   x=ward_data['value'],
                   y=[ward] * len(ward_data),
                   name=ward,
                   orientation='h',
                   marker_color=box_color,
                   marker_line=dict(color='#3c5e8b', width=1.5),  # Blue border
                   line=dict(color='#3c5e8b', width=1.5),  # Blue border for box
                   hoverinfo='all',
                   hovertemplate=f"<b>{ward}</b><br>Rank: {rank}<br>Category: {category}<br>Score: %{{x:.3f}}<extra></extra>",
                   boxmean=True,  # Show mean as a dashed line
                   showlegend=False
               ))
           
           # Update layout
           fig.update_layout(
               title={
                   'text': f'Ward Rankings Distribution (Page {page} of {total_pages})',
                   'x': 0.5,
                   'y': 0.98,
                   'xanchor': 'center',
                   'yanchor': 'top',
                   'font': {'size': 20, 'color': '#333', 'family': 'Arial, sans-serif'}
               },
               xaxis={
                   'title': {
                       'text': 'Risk Score',
                       'font': {'size': 14}
                   },
                   'zeroline': True,
                   'gridcolor': '#E5E5E5',
                   'showgrid': True
               },
               yaxis={
                   'title': '',
                   'categoryorder': 'array',
                   'categoryarray': page_wards,
                   'gridcolor': '#E5E5E5',
                   'showgrid': True
               },
               height=520,
               margin=dict(l=150, r=20, t=80, b=50),  # Left margin for ward names
               plot_bgcolor='#F8F9FA',
               paper_bgcolor='#F8F9FA',
               annotations=[
                   dict(
                       x=0.5, y=-0.15,
                       text="Most vulnerable wards at top | Least vulnerable at bottom",
                       showarrow=False,
                       xref="paper", yref="paper",
                       font=dict(size=14, color='darkred')
                   )
               ],
               autosize=True
           )
           
           return fig
       
       # Use parallelization to create plots for all pages - this is a heavy operation
       # For smaller datasets or fewer pages, we can use a simple loop
       if total_pages <= 5:
           plot_list = [create_page_plot(page) for page in range(1, total_pages + 1)]
       else:
           # Use parallel processing for larger datasets with many pages
           with concurrent.futures.ThreadPoolExecutor() as executor:
               plot_list = list(executor.map(create_page_plot, range(1, total_pages + 1)))
       
       # Return the results as a dictionary
       return {
           'status': 'success',
           'message': 'Successfully created vulnerability box plots',
           'plots': plot_list, 
           'ward_rankings': ward_rankings,
           'total_pages': total_pages,
           'current_page': 1,
           'page_data': page_data
       }
   
   except Exception as e:
       logger.error(f"Error creating vulnerability plot: {str(e)}", exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': f'Error creating vulnerability plot: {str(e)}'
       }

def create_vulnerability_map(data_handler):
   """
   Create vulnerability ranking map
   
   Args:
       data_handler: DataHandler instance
       
   Returns:
       dict: Status and visualization information
   """
   try:
       # Check if vulnerability rankings are available
       if not hasattr(data_handler, 'vulnerability_rankings') or data_handler.vulnerability_rankings is None:
           # Check if box plot function has been run
           if hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot:
               # Extract ward rankings from box plot data
               data_handler.vulnerability_rankings = data_handler.boxwhisker_plot['ward_rankings']
           else:
               # Try to load from file
               rankings_file = os.path.join(data_handler.session_folder, 'vulnerability_rankings.csv')
               if os.path.exists(rankings_file):
                   data_handler.vulnerability_rankings = pd.read_csv(rankings_file)
               else:
                   # If not available, try to run the box plot function to generate rankings
                   if data_handler.composite_scores is not None and 'scores' in data_handler.composite_scores:
                       box_plot_result = box_plot_function(data_handler.composite_scores['scores'])
                       if isinstance(box_plot_result, dict) and 'ward_rankings' in box_plot_result:
                           data_handler.vulnerability_rankings = box_plot_result['ward_rankings']
                           data_handler.boxwhisker_plot = box_plot_result
                       else:
                           return {
                               'status': 'error',
                               'message': 'Could not generate vulnerability rankings'
                           }
                   else:
                       return {
                           'status': 'error',
                           'message': 'Vulnerability rankings not available. Run vulnerability analysis first.'
                       }
       
       if data_handler.shapefile_data is None:
           return {
               'status': 'error',
               'message': 'Shapefile data not loaded'
           }
       
       # Get a copy of the shapefile with standardized CRS
       shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
       
       # Ensure vulnerability_rankings has the right data types
       # Convert columns that should be numeric
       for col in ['overall_rank', 'value']:
           if col in data_handler.vulnerability_rankings.columns:
               data_handler.vulnerability_rankings[col] = pd.to_numeric(data_handler.vulnerability_rankings[col], errors='coerce')
       
       # Merge shapefile with vulnerability rankings
       gdf = shapefile_data.merge(
           data_handler.vulnerability_rankings,
           on='WardName',
           how='left'
       )
       
       # Handle any NaN values in overall_rank (wards not in the rankings)
       if 'overall_rank' in gdf.columns:
           gdf['overall_rank'] = gdf['overall_rank'].fillna(-1).astype(int)
       
       # Convert geometry to geojson with proper serialization
       gdf_prepared = prepare_geodataframe_for_json(gdf)
       geojson = json.loads(gdf_prepared.to_json())
       
       # Get proper map centering
       center_lat = gdf.geometry.centroid.y.mean()
       center_lon = gdf.geometry.centroid.x.mean()
       
       # Calculate appropriate zoom level based on the bounding box
       bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
       span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
       span_y = max(0.01, bounds[3] - bounds[1])
       
       # Calculate zoom level - ensure it's reasonable
       zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
       
       # Create choropleth map with Plotly
       fig = go.Figure()
       
       # Create hover text with proper formatting
       hover_text = []
       for i, row in gdf.iterrows():
           ward_name = row['WardName']
           rank = row['overall_rank'] if 'overall_rank' in gdf.columns and row['overall_rank'] != -1 else "Not ranked"
           category = row['vulnerability_category'] if 'vulnerability_category' in gdf.columns else "Unknown"
           hover_text.append(f"{ward_name}<br>Rank: {rank}<br>Category: {category}")
       
       # Get categorical colors for vulnerability categories
       color_values = []
       if 'vulnerability_category' in gdf.columns:
           # Map categories to numeric values for colorscale
           category_map = {'High': 1, 'Medium': 2, 'Low': 3, None: 0}
           color_values = [category_map.get(cat, 0) for cat in gdf['vulnerability_category']]
           # Use a colorscale that visually distinguishes categories
           colorscale = [
               [0, 'rgba(200,200,200,0.5)'],  # Not ranked
               [0.25, '#d7191c'],  # High vulnerability (red)
               [0.5, '#fdae61'],   # Medium vulnerability (orange)
               [0.75, '#ffffbf']   # Low vulnerability (yellow)
           ]
           z_values = color_values
           tick_vals = [0.5, 1.5, 2.5]
           tick_text = ['High', 'Medium', 'Low']
       else:
           # Fallback to using overall_rank
           z_values = gdf['overall_rank'] if 'overall_rank' in gdf.columns else None
           colorscale = 'Plasma_r'  # Reverse plasma so high vulnerability (low rank) is dark
           # Determine tick values based on actual data
           max_rank = gdf['overall_rank'].max() if 'overall_rank' in gdf.columns else 100
           tick_vals = [1, max_rank / 2, max_rank]
           tick_text = ['High', 'Medium', 'Low']
       
       # Add the choropleth layer with error handling
       try:
        fig.add_trace(go.Choroplethmapbox(
           geojson=geojson,
           locations=gdf.index,
           z=z_values,
           colorscale=colorscale,
           marker_opacity=0.8,
           marker_line_width=0.5,
           marker_line_color='black',
           hovertemplate='%{hovertext}<extra></extra>',
           hovertext=hover_text,
           colorbar=dict(
               title=dict(
                   text="Vulnerability",
                   font=dict(size=12)
               ),
               tickmode='array',
               tickvals=tick_vals,
               ticktext=tick_text
           )
       ))
       except Exception as e:
           logger.error(f"Error adding choropleth layer: {str(e)}")
           return {
               'status': 'error',
               'message': f'Error creating vulnerability map: {str(e)}'
           }
       
       # Update layout with error handling
       try:
        fig.update_layout(
           title={
               'text': "Ward Vulnerability Map",
               'x': 0.5,
               'xanchor': 'center',
               'font': {'size': 20}
           },
           mapbox=dict(
               style="carto-positron",
               center={"lat": center_lat, "lon": center_lon},
               zoom=zoom_level
           ),
           margin=dict(l=20, r=20, t=80, b=20),
           autosize=True
       )
       except Exception as e:
           logger.error(f"Error updating layout: {str(e)}")
           return {
               'status': 'error',
               'message': f'Error updating map layout: {str(e)}'
           }
       
       # Create HTML file with error handling
       try:
        html_path = create_plotly_html(fig, "vulnerability_map.html")
       except Exception as e:
           logger.error(f"Error creating HTML file: {str(e)}")
           return {
               'status': 'error',
               'message': f'Error saving vulnerability map: {str(e)}'
           }
       
       # Get category counts for data summary
       category_counts = {}
       if 'vulnerability_category' in gdf.columns:
           category_counts = gdf['vulnerability_category'].value_counts().to_dict()
       
       # Prepare statistics for data summary
       statistics = {}
       if 'value' in gdf.columns:
           values = gdf['value'].dropna().values
           if len(values) > 0:
               statistics = {
                   'min_score': float(np.min(values)),
                   'max_score': float(np.max(values)),
                   'mean_score': float(np.mean(values)),
                   'median_score': float(np.median(values))
               }
       
       # Create rich context for LLM
       data_summary = {
           'ward_count': len(gdf),
           'ranked_ward_count': gdf['overall_rank'].notna().sum() if 'overall_rank' in gdf.columns else 0,
           'category_counts': category_counts,
           'statistics': statistics,
           'top_vulnerable_wards': gdf.sort_values('overall_rank')['WardName'].head(5).tolist() 
                                 if 'overall_rank' in gdf.columns else []
       }
       
       visual_elements = {
           'map_type': 'choropleth',
           'color_scale': 'Category-based coloring' if 'vulnerability_category' in gdf.columns else 'Plasma_r',
           'color_meaning': 'Darker colors = higher vulnerability',
           'scale_divisions': 'High, Medium, Low vulnerability'
       }
       
       # Return success with paths and metadata
       return {
           'status': 'success',
           'message': f'Successfully created vulnerability map',
           'image_path': html_path,
           'viz_type': 'vulnerability_map',
           'data_summary': data_summary,
           'visual_elements': visual_elements
       }
       
   except Exception as e:
       logger.error(f"Error creating vulnerability map: {str(e)}", exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': f'Error creating vulnerability map: {str(e)}'
       }

def create_urban_extent_map(data_handler, threshold=30):
   """
   Create urban extent and vulnerability map at a specific threshold.
   
   Args:
       data_handler: DataHandler instance
       threshold: Urban threshold percentage (0-100).
   
   Returns:
       dict: Status and visualization information
   """
   try:
       # Standardize and validate threshold
       current_threshold_value = float(threshold) if threshold is not None else 30.0
       current_threshold_value = max(0.0, min(100.0, current_threshold_value))  # Clamp
       
       logger.info(f"Creating urban extent map with threshold: {current_threshold_value}%")
       
       # Essential Data Checks
       if data_handler.csv_data is None:
           return {'status': 'error', 'message': 'CSV data not loaded for urban extent map.'}
       if data_handler.shapefile_data is None:
           return {'status': 'error', 'message': 'Shapefile data not loaded for urban extent map.'}
       
       # Check for vulnerability rankings and generate if needed
       vuln_rankings = None
       if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
           vuln_rankings = data_handler.vulnerability_rankings
       elif (hasattr(data_handler, 'composite_scores') and 
             data_handler.composite_scores is not None and 
             isinstance(data_handler.composite_scores, dict) and
             'scores' in data_handler.composite_scores):
           
           # Generate vulnerability rankings
           box_plot_result = box_plot_function(data_handler.composite_scores['scores'])
           if box_plot_result.get('status') == 'success' and 'ward_rankings' in box_plot_result:
               vuln_rankings = box_plot_result['ward_rankings']
               data_handler.vulnerability_rankings = vuln_rankings
               data_handler.boxwhisker_plot = box_plot_result
       
       # Find Urban Percentage Column using vectorized operations
       urban_percent_cols = ['UrbanPercentage', 'UrbanPercent', 'Urban_Percent', 
                           'urban_percent', 'urbanPercent', 'urbanpercent', 
                           'urban_percentage', 'percent_urban']
       
       # Standardize column names for matching
       csv_columns_lower = {col.lower(): col for col in data_handler.csv_data.columns}
       
       # Try to find an urban percentage column
       urban_percent_col = None
       for potential_col in urban_percent_cols:
           if potential_col.lower() in csv_columns_lower:
               urban_percent_col = csv_columns_lower[potential_col.lower()]
               logger.info(f"Found urban percentage column in CSV: '{urban_percent_col}'")
               break
       
       # If no percentage column found, try for binary Urban column
       if urban_percent_col is None and 'urban' in csv_columns_lower:
           urban_col_name = csv_columns_lower['urban']
           logger.info(f"Found binary Urban column '{urban_col_name}' in CSV, converting to percentage.")
           urban_percent_col = 'UrbanPercent_Generated'
           # Create the column as a vectorized operation
           data_handler.csv_data[urban_percent_col] = data_handler.csv_data[urban_col_name].apply(
               lambda x: 100.0 if str(x).lower() in ['yes', 'true', '1', 'y'] else 0.0
           )
       
       # Fallback to shapefile
       if urban_percent_col is None and data_handler.shapefile_data is not None:
           shp_columns_lower = {col.lower(): col for col in data_handler.shapefile_data.columns}
           
           # Check in shapefile columns
           for potential_col in urban_percent_cols:
               if potential_col.lower() in shp_columns_lower:
                   shp_col_name = shp_columns_lower[potential_col.lower()]
                   logger.info(f"Found urban percentage column '{shp_col_name}' in shapefile.")
                   urban_percent_col = 'UrbanPercent_From_Shapefile'
                   
                   # Merge to CSV data as a single vectorized operation
                   if urban_percent_col not in data_handler.csv_data.columns:
                       temp_shp_df = data_handler.shapefile_data[['WardName', shp_col_name]].rename(
                           columns={shp_col_name: urban_percent_col}
                       )
                       data_handler.csv_data = data_handler.csv_data.merge(
                           temp_shp_df, on='WardName', how='left'
                       )
                   break
           
           # Check for binary Urban column in shapefile
           if urban_percent_col is None and 'urban' in shp_columns_lower:
               urban_col_name = shp_columns_lower['urban']
               logger.info(f"Found binary Urban column '{urban_col_name}' in shapefile, converting to percentage.")
               urban_percent_col = 'UrbanPercent_From_Shapefile'
               
               # Process as a single vectorized operation
               if urban_percent_col not in data_handler.csv_data.columns:
                   temp_shp_df = pd.DataFrame({
                       'WardName': data_handler.shapefile_data['WardName'],
                       urban_percent_col: data_handler.shapefile_data[urban_col_name].apply(
                           lambda x: 100.0 if str(x).lower() in ['yes', 'true', '1', 'y'] else 0.0
                       )
                   })
                   data_handler.csv_data = data_handler.csv_data.merge(
                       temp_shp_df, on='WardName', how='left'
                   )
       
       if urban_percent_col is None:
           return {'status': 'error', 'message': 'Urban percentage data not found. Cannot create urban extent map.'}
       
       # Ensure urban percentage column has proper numeric values
       data_handler.csv_data[urban_percent_col] = pd.to_numeric(
           data_handler.csv_data[urban_percent_col], errors='coerce'
       ).fillna(0.0)  # Fill NaNs with 0%
       
       # Prepare Merged GeoDataFrame - vectorized operations
       shapefile_data_for_merge = ensure_wgs84_crs(data_handler.shapefile_data.copy())
       urban_data_for_merge = data_handler.csv_data[['WardName', urban_percent_col]].copy()
       
       # Create merged_data with urban data
       merged_data = shapefile_data_for_merge.merge(
           urban_data_for_merge, on='WardName', how='left'
       )
       
       # Add vulnerability data if available
       if vuln_rankings is not None:
           vuln_data_for_merge = vuln_rankings[['WardName', 'overall_rank', 'vulnerability_category']].copy()
           merged_data = merged_data.merge(
               vuln_data_for_merge, on='WardName', how='left'
           )
       
       # Calculate threshold status based on urban percentage - vectorized
       threshold_str = str(current_threshold_value).replace('.', '_')
       meets_threshold_field = f'MeetsThreshold_{threshold_str}'
       merged_data[meets_threshold_field] = merged_data[urban_percent_col] >= current_threshold_value
       
       # Count wards above/below threshold
       meets_count = int(merged_data[meets_threshold_field].sum())
       below_count = int((~merged_data[meets_threshold_field]).sum())
       
       # Prepare GeoJSON and Map Centering
       gdf_prepared = prepare_geodataframe_for_json(merged_data.copy())
       geojson = json.loads(gdf_prepared.to_json())
       
       # Calculate map center and zoom level
       center_lat = float(merged_data.geometry.centroid.y.mean())
       center_lon = float(merged_data.geometry.centroid.x.mean())
       if pd.isna(center_lat) or pd.isna(center_lon): 
           center_lat, center_lon = 0.0, 0.0
       
       bounds = merged_data.geometry.total_bounds
       span_x = max(0.01, float(bounds[2]) - float(bounds[0]))
       span_y = max(0.01, float(bounds[3]) - float(bounds[1]))
       zoom_level = float(min(10, max(4, 6 - np.log(max(span_x, span_y)))))
       
       # Create Plotly figure
       fig = go.Figure()
       
       # Prepare Hover Text (for all wards) - vectorized
       hover_texts = merged_data.apply(
           lambda row: (
               f"<b>{row['WardName']}</b><br>"
               f"Urban: {row[urban_percent_col]:.1f}%<br>"
               f"Vulnerability Rank: {int(row['overall_rank']) if 'overall_rank' in row and pd.notna(row['overall_rank']) else 'Not ranked'}<br>"
               f"Category: {row['vulnerability_category'] if 'vulnerability_category' in row and pd.notna(row['vulnerability_category']) else 'Unknown'}<br>"
               f"Status: {'Urban (Above Threshold)' if row[meets_threshold_field] else 'Non-Urban (Below Threshold)'}"
           ),
           axis=1
       ).tolist()
       
       # Draw wards above threshold (colored by vulnerability if data exists)
       wards_above_threshold = merged_data[merged_data[meets_threshold_field]].copy()
       if not wards_above_threshold.empty:
           # Determine colorscale and color values based on available data
           if 'overall_rank' in wards_above_threshold.columns and vuln_rankings is not None:
               # Use vulnerability ranks for coloring
               overall_ranks_above = wards_above_threshold['overall_rank'].fillna(0).astype(float)
               color_values = overall_ranks_above.tolist()
               colorscale = 'Plasma_r'  # Reverse plasma (dark colors = high vulnerability)
               
               # Create color bar ticks
               min_r, max_r = overall_ranks_above.min(), overall_ranks_above.max()
               # Default values 
               tickvals, ticktext = [0], ["N/A"]
               
               # Better ticks if we have real data
               if min_r != max_r and min_r > 0 and max_r > 0:
                   num_ticks = min(3, int(max_r - min_r) + 1)
                   if num_ticks <= 1: 
                       num_ticks = 2
                   tickvals = np.linspace(min_r, max_r, num=num_ticks).tolist()
                   ticktext = [f"Rank {int(round(t))}" for t in tickvals]
                   if len(tickvals) >= 1: 
                       ticktext[0] = f"High Vuln. (Rank {int(round(tickvals[0]))})"
                   if len(tickvals) > 1: 
                       ticktext[-1] = f"Low Vuln. (Rank {int(round(tickvals[-1]))})"
           else:
               # Use urban percentage for coloring if no vulnerability data
               color_values = wards_above_threshold[urban_percent_col].tolist()
               colorscale = 'YlOrRd'  # Yellow-Orange-Red
               
               # Create color bar ticks for urban percentage
               min_val = min(color_values) if color_values else 0
               max_val = max(color_values) if color_values else 100
               tickvals = np.linspace(min_val, max_val, 3).tolist()
               ticktext = [f"{val:.0f}%" for val in tickvals]
           
           # Add the choropleth trace for above-threshold wards
           fig.add_trace(go.Choroplethmapbox(
               geojson=geojson, 
               locations=wards_above_threshold.index.tolist(),
               z=color_values, 
               colorscale=colorscale,
               marker_opacity=0.8, 
               marker_line_width=0.5, 
               marker_line_color='black',
               showscale=True,
               colorbar=dict(
                   title=dict(
                       text="Vulnerability Rank<br>(Urban Areas)" if vuln_rankings is not None 
                            else "Urban Percentage",
                       font=dict(size=10)
                   ),
                   tickmode='array', 
                   tickvals=tickvals, 
                   ticktext=ticktext, 
                   len=0.7, 
                   y=0.85
               ),
               hovertext=[hover_texts[i] for i in wards_above_threshold.index],
               hovertemplate='%{hovertext}<extra></extra>',
               name=f'Urban (>{current_threshold_value}%)'
           ))
       else:
           # Add annotation if no wards meet threshold
           fig.add_annotation(
               x=0.5, 
               y=0.5, 
               text=f"No wards meet the {current_threshold_value}% urbanicity threshold.",
               showarrow=False, 
               xref="paper", 
               yref="paper", 
               font=dict(size=16, color="grey"), 
               align="center"
           )
       
       # Draw wards below threshold (grayed out)
       wards_below_threshold = merged_data[~merged_data[meets_threshold_field]].copy()
       if not wards_below_threshold.empty:
           # Add a trace with a fixed gray color for all below-threshold wards
           fig.add_trace(go.Choroplethmapbox(
               geojson=geojson, 
               locations=wards_below_threshold.index.tolist(),
               z=np.zeros(len(wards_below_threshold)),  # Dummy z values for uniform color - vectorized
               colorscale=[[0, 'rgba(200,200,200,0.4)'], [1, 'rgba(200,200,200,0.4)']],  # Light gray
               marker_opacity=0.4, 
               marker_line_width=0.2, 
               marker_line_color='rgba(150,150,150,0.3)',
               showscale=False,
               hovertext=[hover_texts[i] for i in wards_below_threshold.index],
               hovertemplate='%{hovertext}<extra></extra>',
               name=f'Non-Urban (<{current_threshold_value}%)'
           ))
       
       # Create title based on data
       title_main = f"Urban Areas & Vulnerability (Threshold: {current_threshold_value}%)"
       title_sub = "<span style='font-size:12px; color:gray;'>"
       if not wards_above_threshold.empty:
           if vuln_rankings is not None:
               title_sub += "Urban areas colored by vulnerability rank. Non-urban areas are grayed out."
           else:
               title_sub += "Urban areas colored by urban percentage. Non-urban areas are grayed out."
       elif meets_count == 0:
           title_sub += "No areas meet the urban threshold. All areas shown in gray."
       title_sub += "</span>"
       
       # Update layout
       fig.update_layout(
           title={
               'text': f"{title_main}<br>{title_sub}", 
               'x': 0.5, 
               'xanchor': 'center', 
               'font': {'size': 18}
           },
           mapbox=dict(
               style="carto-positron", 
               center={"lat": center_lat, "lon": center_lon}, 
               zoom=zoom_level
           ),
           height=650, 
           margin=dict(l=20, r=20, t=100, b=50), 
           autosize=True,
           legend=dict(
               yanchor="top", 
               y=0.99, 
               xanchor="left", 
               x=0.01, 
               bgcolor='rgba(255,255,255,0.7)'
           )
       )
       
       # Generate a unique filename with random number to avoid caching issues
       threshold_str_for_filename = str(current_threshold_value).replace('.', '_')
       filename = f"urban_extent_vuln_{threshold_str_for_filename}_{np.random.randint(10000)}.html"
       
       # Save the HTML file
       html_path = create_plotly_html(fig, filename)
       
       # Create rich context for LLM explanation
       data_summary = {
           'threshold': float(current_threshold_value),
           'meets_threshold': meets_count,
           'below_threshold': below_count,
           'urban_percentage': float(meets_count / (meets_count + below_count) * 100) if (meets_count + below_count) > 0 else 0,
           'has_vulnerability_data': vuln_rankings is not None,
           'non_urban_high_vulnerability_wards': []
       }
       
       # Check for non-urban high vulnerability wards
       if vuln_rankings is not None:
           # Identify high vulnerability wards (top 10)
           high_vuln_wards = merged_data.sort_values('overall_rank').head(10)
           
           # Find those that are non-urban
           non_urban_high_vuln = high_vuln_wards[~high_vuln_wards[meets_threshold_field]]
           
           if not non_urban_high_vuln.empty:
               data_summary['non_urban_high_vulnerability_wards'] = non_urban_high_vuln['WardName'].tolist()
               data_summary['has_non_urban_high_vulnerability'] = True
               
               # Add rank and urban percentage for each
               non_urban_details = []
               for _, row in non_urban_high_vuln.iterrows():
                   non_urban_details.append({
                       'ward_name': row['WardName'],
                       'rank': int(row['overall_rank']) if 'overall_rank' in row and pd.notna(row['overall_rank']) else None,
                       'urban_percentage': float(row[urban_percent_col]) if urban_percent_col in row else None
                   })
               data_summary['non_urban_high_vulnerability_details'] = non_urban_details
           else:
               data_summary['has_non_urban_high_vulnerability'] = False
       else:
           data_summary['has_non_urban_high_vulnerability'] = False
       
       visual_elements = {
           'map_type': 'choropleth',
           'urban_color_scale': 'Plasma_r' if vuln_rankings is not None else 'YlOrRd',
           'color_meaning': 'Urban areas colored by vulnerability rank (darker = higher vulnerability)' 
                           if vuln_rankings is not None else 'Urban areas colored by urban percentage',
           'non_urban_appearance': 'Grayed out',
           'has_legend': True,
           'has_colorbar': not wards_above_threshold.empty
       }
       
       # Return success with rich context
       return {
           'status': 'success',
           'message': f'Urban extent & vulnerability map for {current_threshold_value}% threshold generated.',
           'image_path': html_path,
           'threshold': float(current_threshold_value),
           'meets_threshold': meets_count,
           'below_threshold': below_count,
           'viz_type': 'urban_extent_map',
           'data_summary': data_summary,
           'visual_elements': visual_elements
       }
           
   except Exception as e:
       logger.error(f"Error generating urban extent map: {str(e)}", exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': f'Error in urban extent map generation: {str(e)}'
       }

def create_decision_tree_plot(data_handler):
   """
   Create a decision tree visualization flowing from left to right
   
   Args:
       data_handler: DataHandler instance
       
   Returns:
       dict: Status and visualization information
   """
   try:
       # Get all variables and selected variables
       all_variables = []
       selected_variables = []
       excluded_variables = []
       top_5_wards = []
       
       # Get all variables from original data - use vectorized operations
       if data_handler.csv_data is not None:
           all_variables = [col for col in data_handler.csv_data.columns 
                          if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]) and not is_id_column(col)]
       
       # Get selected variables from composite scores
       if hasattr(data_handler, 'composite_variables') and data_handler.composite_variables:
           selected_variables = data_handler.composite_variables
       elif data_handler.composite_scores is not None and 'model_formulas' in data_handler.composite_scores:
           # Use variables from the first model
           if data_handler.composite_scores['model_formulas']:
               selected_variables = data_handler.composite_scores['model_formulas'][0]['variables']
               # Clean up variable names if needed
               selected_variables = [var.replace('normalization_', '') for var in selected_variables]
       
       # Get excluded variables - vectorized set operation
       excluded_variables = list(set(all_variables) - set(selected_variables))
       
       # Get top 5 vulnerable wards
       if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
           top_5 = data_handler.vulnerability_rankings.sort_values('overall_rank').head(5)
           top_5_wards = top_5['WardName'].tolist()
       
       # Get full variable names
       full_all_variables = [f"{var} ({get_full_variable_name(var)})" for var in all_variables]
       full_selected_variables = [f"{var} ({get_full_variable_name(var)})" for var in selected_variables]
       full_excluded_variables = [f"{var} ({get_full_variable_name(var)})" for var in excluded_variables]
       
       # Create HTML content for the decision tree
       html_content = """
       <!DOCTYPE html>
       <html>
       <head>
           <meta charset="UTF-8">
           <title>Decision Tree Visualization</title>
           <style>
               body {
                   font-family: 'Arial', sans-serif;
                   background-color: #ffffff;
                   margin: 0;
                   padding: 0;
                   display: flex;
                   justify-content: center;
               }
               .decision-tree-container {
                   width: 100%;
                   max-width: 900px;
                   padding: 20px;
               }
               .tree-row {
                   display: flex;
                   justify-content: center;
                   margin-bottom: 20px;
                   position: relative;
               }
               .node {
                   background-color: #f5f5f5;
                   border-radius: 8px;
                   padding: 15px;
                   box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                   text-align: center;
                   margin: 0 10px;
                   width: 100%;
                   max-width: 250px;
               }
               .node-title {
                   font-weight: bold;
                   margin-bottom: 8px;
                   font-size: 16px;
               }
               .list-container {
                   max-height: 150px;
                   overflow-y: auto;
                   text-align: left;
                   margin-top: 10px;
               }
               .list-container ul, .list-container ol {
                   padding-left: 20px;
                   margin: 5px 0;
               }
               .list-container li {
                   margin-bottom: 6px;
                   font-size: 13px;
               }
               .navy {
                   background-color: #1B2631;
                   color: white;
               }
               .orange {
                   background-color: #E67E22;
                   color: white;
               }
               .teal {
                   background-color: #16A596;
                   color: white;
               }
               .gray {
                   background-color: #7F8C8D;
                   color: white;
               }
               .green {
                   background-color: #27AE60;
                   color: white;
               }
               .blue {
                   background-color: #2980B9;
                   color: white;
               }
               .purple {
                   background-color: #8E44AD;
                   color: white;
               }
               .arrow {
                   position: absolute;
                   width: 0;
                   height: 0;
                   border-left: 10px solid transparent;
                   border-right: 10px solid transparent;
                   border-top: 10px solid #666;
                   left: 50%;
                   bottom: -15px;
                   transform: translateX(-50%);
               }
               .arrow-label {
                   position: absolute;
                   background-color: white;
                   padding: 2px 8px;
                   border-radius: 10px;
                   font-size: 12px;
                   font-weight: bold;
               }
               .arrow-container {
                   position: relative;
                   height: 30px;
                   width: 100%;
               }
               .vertical-line {
                   position: absolute;
                   width: 2px;
                   background-color: #666;
                   left: 50%;
                   transform: translateX(-50%);
                   top: 0;
                   bottom: 0;
               }
               .branch-container {
                   display: flex;
                   justify-content: space-around;
                   width: 100%;
                   position: relative;
               }
               .branch-line {
                   position: absolute;
                   top: 0;
                   height: 2px;
                   background-color: #666;
               }
               .branch-label {
                   position: absolute;
                   top: -10px;
                   background-color: white;
                   padding: 0 5px;
                   font-size: 12px;
               }
           </style>
       </head>
       <body>
           <div class="decision-tree-container">
               <h1 style="text-align: center; margin-bottom: 30px;">Malaria Risk Analysis Workflow</h1>
               
               <!-- Row 1: Start Node -->
               <div class="tree-row">
                   <div class="node navy">
                       <div class="node-title">Malaria Risk Assessment</div>
                       <div>Variable Selection</div>
                   </div>
               </div>
               
               <!-- Arrow between Row 1 and 2 -->
               <div class="arrow-container">
                   <div class="vertical-line"></div>
               </div>
               
               <!-- Row 2: Variables List -->
               <div class="tree-row">
                   <div class="node navy">
                       <div class="node-title">Variables</div>
                       <div class="list-container">
                           <ul>
       """
       
       # Add all variables to HTML
       for var in full_all_variables[:10]:  # Limit to first 10 for space
           html_content += f"                                <li>{var}</li>\n"
       
       if len(full_all_variables) > 10:
           html_content += f"                                <li>...and {len(full_all_variables) - 10} more</li>\n"
           
       html_content += """
                           </ul>
                       </div>
                   </div>
               </div>
               
               <!-- Arrow between Row 2 and 3 -->
               <div class="arrow-container">
                   <div class="vertical-line"></div>
               </div>
               
               <!-- Row 3: Evaluation Diamond -->
               <div class="tree-row">
                   <div class="node orange">
                       <div class="node-title">Variable Evaluation</div>
                       <div>Assessment of variable relationships with malaria risk</div>
                   </div>
               </div>
               
               <!-- Branch Lines for Include/Exclude -->
               <div class="branch-container" style="height: 50px;">
                   <div class="branch-line" style="left: 25%; width: 25%;"></div>
                   <div class="branch-label" style="left: 32%;">Include</div>
                   
                   <div class="branch-line" style="left: 50%; width: 25%;"></div>
                   <div class="branch-label" style="left: 62%;">Exclude</div>
               </div>
               
               <!-- Row 4: Included and Excluded Variables -->
               <div class="tree-row">
                   <div class="node teal" style="flex: 1;">
                       <div class="node-title">Included Variables</div>
                       <div class="list-container">
                           <ul>
       """
       
       # Add included variables to HTML
       for var in full_selected_variables:
           html_content += f"                                <li>{var}</li>\n"
       
       if not full_selected_variables:
           html_content += "                                <li>No variables selected yet</li>\n"
           
       html_content += """
                           </ul>
                       </div>
                   </div>
                   
                   <div class="node gray" style="flex: 1;">
                       <div class="node-title">Excluded Variables</div>
                       <div class="list-container">
                           <ul>
       """
       
       # Add excluded variables to HTML
       for var in full_excluded_variables[:10]:  # Limit to first 10 for space
           html_content += f"                                <li>{var}</li>\n"
       
       if len(full_excluded_variables) > 10:
           html_content += f"                                <li>...and {len(full_excluded_variables) - 10} more</li>\n"
       
       if not full_excluded_variables:
           html_content += "                                <li>No variables excluded yet</li>\n"
           
       html_content += """
                           </ul>
                       </div>
                   </div>
               </div>
               
               <!-- Arrow from Included Variables to Normalization -->
               <div class="arrow-container">
                   <div class="vertical-line" style="left: 25%;"></div>
               </div>
               
               <!-- Row 5: Normalization and Calculation -->
               <div class="tree-row">
                   <div class="node green" style="margin-left: 0;">
                       <div class="node-title">Data Normalization &<br>Composite Score Calculation</div>
                       <div>Converting variables to common scale and calculating risk scores</div>
                   </div>
               </div>
               
               <!-- Arrow between Row 5 and 6 -->
               <div class="arrow-container">
                   <div class="vertical-line"></div>
               </div>
               
               <!-- Row 6: Risk Maps -->
               <div class="tree-row">
                   <div class="node blue">
                       <div class="node-title">Generated Risk Maps<br>for All Combinations</div>
                       <div>Maps showing risk scores for different variable combinations</div>
                   </div>
               </div>
               
               <!-- Arrow between Row 6 and 7 -->
               <div class="arrow-container">
                   <div class="vertical-line"></div>
               </div>
               
               <!-- Row 7: Vulnerability Analysis -->
               <div class="tree-row">
                   <div class="node purple">
                       <div class="node-title">Vulnerability Analysis</div>
                       <div>Box and whisker plot of ward vulnerability rankings</div>
                   </div>
               </div>
               
               <!-- Arrow between Row 7 and 8 -->
               <div class="arrow-container">
                   <div class="vertical-line"></div>
               </div>
               
               <!-- Row 8: Priority Wards -->
               <div class="tree-row">
                   <div class="node purple">
                       <div class="node-title">Top 5 Wards<br>for Reprioritization</div>
                       <div class="list-container">
                           <ol>
       """
       
       # Add top 5 wards to HTML
       for ward in top_5_wards:
           html_content += f"                                <li>{ward}</li>\n"
       
       if not top_5_wards:
           html_content += "                                <li>No wards ranked yet</li>\n"
           
       html_content += """
                           </ol>
                       </div>
                   </div>
               </div>
           </div>
       </body>
       </html>
       """
       
       # Save HTML to a file
       session_id = session.get('session_id', 'default')
       session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
       os.makedirs(session_folder, exist_ok=True)
       
       file_path = os.path.join(session_folder, 'decision_tree.html')
       with open(file_path, 'w', encoding='utf-8') as f:
           f.write(html_content)
       
       web_path = f"/serve_viz_file/{session_id}/decision_tree.html"
       
       # Create rich context for LLM
       data_summary = {
           'all_variables_count': len(all_variables),
           'selected_variables_count': len(selected_variables),
           'excluded_variables_count': len(excluded_variables),
           'top_5_wards': top_5_wards,
           'selected_variables': selected_variables,
           'full_selected_variables': full_selected_variables
       }
       
       visual_elements = {
           'visualization_type': 'decision_tree',
           'color_scheme': 'Multiple colors for different stages',
           'flow_direction': 'Top to bottom',
           'interactive_elements': 'Scrollable variable lists',
           'node_count': 8
       }
       
       # Return success with paths and metadata
       return {
           'status': 'success',
           'message': 'Successfully created decision tree visualization',
           'image_path': web_path,
           'viz_type': 'decision_tree',
           'data_summary': data_summary,
           'visual_elements': visual_elements
       }
       
   except Exception as e:
       logger.error(f"Error creating decision tree plot: {str(e)}", exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': f'Error creating decision tree plot: {str(e)}'
       }

def create_vulnerability_plot(data_handler):
    """
    Create a box and whisker plot visualization of ward vulnerability rankings
    
    Args:
        data_handler: DataHandler instance with composite scores
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Check if composite scores are available
        if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
            return {
                'status': 'error',
                'message': 'Composite scores not available. Calculate composite scores first.'
            }
        
        # Generate box plots
        box_plot_result = box_plot_function(data_handler.composite_scores['scores'])
        
        if box_plot_result['status'] == 'success':
            # Store the box plot data for pagination
            data_handler.boxwhisker_plot = box_plot_result
            
            # Get the first plot
            plot_fig = box_plot_result['plots'][0]
            
            # Save as HTML
            html_path = create_plotly_html(plot_fig, "vulnerability_plot.html")
            
            result = {
                'status': 'success',
                'message': 'Successfully created vulnerability box plot',
                'image_path': html_path,
                'current_page': 1,
                'total_pages': box_plot_result['total_pages'],
                'viz_type': 'vulnerability_plot',
                'data_summary': {
                    'ward_count': len(box_plot_result['ward_rankings']),
                    'high_vulnerability_count': len(box_plot_result['ward_rankings'][box_plot_result['ward_rankings']['vulnerability_category'] == 'High']),
                    'medium_vulnerability_count': len(box_plot_result['ward_rankings'][box_plot_result['ward_rankings']['vulnerability_category'] == 'Medium']),
                    'low_vulnerability_count': len(box_plot_result['ward_rankings'][box_plot_result['ward_rankings']['vulnerability_category'] == 'Low'])
                },
                'visual_elements': {
                    'plot_type': 'Box and whisker plot',
                    'color_scheme': 'By vulnerability category',
                    'axis_meanings': {
                        'x': 'Risk Score (0-1 scale)',
                        'y': 'Ward Names (ordered by vulnerability rank)'
                    }
                }
            }
            
            return result
        else:
            return box_plot_result
            
    except Exception as e:
        import logging
        logging.error(f"Error creating vulnerability plot: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'message': f'Error creating vulnerability plot: {str(e)}'
       }