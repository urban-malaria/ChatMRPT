# app/visualization/core.py
import json
import logging
import os
import re
import time
import hashlib
import pickle
import tempfile
import numpy as np
import pandas as pd
from flask import current_app, session
from werkzeug.utils import secure_filename
from typing import Dict, List, Optional, Any, Union

# Import for utility conversion
from app.core.utils import convert_to_json_serializable

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
        if pattern == column_lower or "{}_ ".format(pattern) in column_lower:
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
                logger.info("Found exact match: {} for {}".format(col, variable_name))
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
                        logger.info("Found mapped match: {} for {} via {}".format(col, variable_name, standard_name))
                        return col
    
    # Try partial match as fallback
    for col in columns:
        col_lower = col.lower()
        if (variable_lower in col_lower or clean_variable in col_lower or 
            any(term in col_lower for term in variable_lower.split())):
            if not is_id_column(col) and col != 'WardName':
                logger.info("Found partial match: {} for {}".format(col, variable_name))
                return col
    
    # Last resort: return first numeric column that's not an ID
    for col in columns:
        if col != 'WardName' and not is_id_column(col) and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]):
            logger.warning("No match found for {}, using {} as fallback".format(variable_name, col))
            return col
            
    logger.error("Could not find any suitable variable match for {}".format(variable_name))
    return None


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
                    self.cache_dir = os.path.join(tempfile.gettempdir(), 'chatmrpt_viz_cache')
            except Exception as e:
                # Final fallback - create in the current directory
                self.cache_dir = os.path.join(os.getcwd(), 'viz_cache')
                logging.warning("Using fallback cache directory: {}".format(self.cache_dir))
        
        # Create cache directory if it doesn't exist
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            logging.info("Visualization cache directory: {}".format(self.cache_dir))
        except Exception as e:
            logging.error("Failed to create cache directory: {}".format(e))
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
        key_str = "{}:{}".format(viz_type, params_str)
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
        
        # Try to load from file if not in memory and cache directory exists
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, "{}.cache".format(cache_key))
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
                    logging.error("Error loading cache file: {}".format(e))
        
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
        
        # Also save to file if cache directory exists
        if self.cache_dir:
            try:
                cache_file = os.path.join(self.cache_dir, "{}.cache".format(cache_key))
                with open(cache_file, 'wb') as f:
                    pickle.dump(cache_item, f)
            except Exception as e:
                logging.error("Error saving cache file: {}".format(e))
        
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
                
                # Also remove from file if cache directory exists
                if self.cache_dir:
                    cache_file = os.path.join(self.cache_dir, "{}.cache".format(key))
                    if os.path.exists(cache_file):
                        os.remove(cache_file)
        else:
            # Clear all
            cleared_count = len(self.cache)
            self.cache = {}
            self.cache_keys = []
            
            # Clear all cache files if cache directory exists
            if self.cache_dir and os.path.exists(self.cache_dir):
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
            logging.info("Using cached visualization for {}".format(viz_type))
            return cached_result
        
        # Generate the visualization based on type
        logging.info("Generating visualization: {}".format(viz_type))
        
        # Measure execution time
        start_time = time.time()
        
        # Import visualization modules here to avoid circular imports
        from . import maps, charts
        
        result = None
        if viz_type == 'variable_map':
            result = maps.create_variable_map(data_handler, variable)
        elif viz_type == 'composite_map':
            result = maps.create_composite_map(data_handler)
        elif viz_type == 'vulnerability_map':
            result = maps.create_vulnerability_map(data_handler)
        elif viz_type == 'urban_extent_map':
            result = maps.create_urban_extent_map(data_handler, threshold)
        elif viz_type == 'vulnerability_plot':
            result = charts.create_vulnerability_plot(data_handler)
        elif viz_type == 'decision_tree_plot':
            result = charts.create_decision_tree_plot(data_handler)
        elif viz_type == 'box_plot':
            result = charts.box_plot_function(data_handler)
        else:
            return {
                'status': 'error',
                'message': "Unknown visualization type: {}".format(viz_type)
            }
        
        # Log execution time
        execution_time = time.time() - start_time
        logging.info("Visualization generation time for {}: {:.2f} seconds".format(viz_type, execution_time))
        
        # Add execution time to result for monitoring
        if result and result['status'] == 'success':
            result['execution_time'] = execution_time
            
            # Store in cache if successful
            viz_cache.set(viz_type, cache_params, result)
        
        return result
    except Exception as e:
        import traceback
        logging.error("Error creating visualization: {}".format(str(e)))
        logging.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': "Error creating visualization: {}".format(str(e))
        } 