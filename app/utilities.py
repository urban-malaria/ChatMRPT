# app/utilities.py
import os
import logging
import json
import datetime # For datetime.datetime type
import numpy as np
import pandas as pd
import geopandas as gpd
import re
import time
import traceback
import concurrent.futures
from joblib import Parallel, delayed
from typing import Dict, List, Tuple, Optional, Union, Any, Callable
from functools import wraps
from flask import current_app

# Set up logging
logger = logging.getLogger(__name__)

# =============================================
# Performance Utilities
# =============================================

def parallelize(func, iterable, n_jobs=-1, prefer="threads", **kwargs):
   """
   Parallelize a function across an iterable
   
   Args:
       func: Function to parallelize
       iterable: Iterable to process
       n_jobs: Number of jobs (-1 for all cores)
       prefer: "threads" or "processes"
       **kwargs: Additional arguments to pass to the function
       
   Returns:
       list: Results from parallel execution
   """
   try:
       if n_jobs == 1:
           # Sequential execution
           return [func(item, **kwargs) for item in iterable]
       else:
           # Parallel execution using joblib
           return Parallel(n_jobs=n_jobs, prefer=prefer)(
               delayed(func)(item, **kwargs) for item in iterable
           )
   except Exception as e:
       logger.error(f"Error in parallelize: {str(e)}", exc_info=True)
       # Fall back to sequential execution
       return [func(item, **kwargs) for item in iterable]

def timeit(func):
   """
   Decorator to time function execution
   
   Args:
       func: Function to time
       
   Returns:
       Wrapped function with timing
   """
   @wraps(func)
   def wrapper(*args, **kwargs):
       start_time = time.time()
       result = func(*args, **kwargs)
       end_time = time.time()
       execution_time = end_time - start_time
       logger.info(f"Function {func.__name__} executed in {execution_time:.4f} seconds")
       return result
   return wrapper

def chunked_parallel(func, iterable, chunk_size=100, n_jobs=-1, **kwargs):
    """
    Process an iterable in parallel chunks
   
    Args:
       func: Function to apply to each chunk
       iterable: Iterable to chunk and process
       chunk_size: Size of each chunk
       n_jobs: Number of jobs (-1 for all cores)
       **kwargs: Additional arguments to pass to the function
       
    Returns:
       list: Combined results from all chunks
    """
    # Convert iterable to list if it's not already
    items = list(iterable)
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
   
    # Process chunks in parallel
    if n_jobs != 1:
       with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs if n_jobs > 0 else None) as executor:
           results = list(executor.map(lambda chunk: [func(item, **kwargs) for item in chunk], chunks))
    else:
       results = [[func(item, **kwargs) for item in chunk] for chunk in chunks]
   
    # Flatten results
    return [item for sublist in results for item in sublist]

# =============================================
# Data Validation Utilities
# =============================================

def is_numeric_column(df, column_name):
   """
   Check if a column is numeric
   
   Args:
       df: DataFrame to check
       column_name: Name of column to check
       
   Returns:
       bool: True if column is numeric
   """
   if column_name not in df.columns:
       return False
   return pd.api.types.is_numeric_dtype(df[column_name])

def is_id_column(column_name):
   """
   Check if a column name appears to be an ID column
   
   Args:
       column_name: Name of column to check
       
   Returns:
       bool: True if column appears to be an ID column
   """
   id_patterns = ['id', 'x.1', 'x', 'index', 'lga_code', 'wardid', 'ward_id', 'code']
   column_lower = column_name.lower()
   
   # Check if it matches common ID patterns
   for pattern in id_patterns:
       if pattern == column_lower or f"{pattern}_" in column_lower:
           return True
   
   return False

def validate_required_columns(df, required_columns, raise_error=False):
   """
   Validate that a DataFrame contains all required columns
   
   Args:
       df: DataFrame to validate
       required_columns: List of required column names
       raise_error: Whether to raise an error if validation fails
       
   Returns:
       tuple: (is_valid, missing_columns)
   """
   if df is None:
       if raise_error:
           raise ValueError("DataFrame is None")
       return False, required_columns
   
   missing_columns = [col for col in required_columns if col not in df.columns]
   
   if missing_columns and raise_error:
       raise ValueError(f"Missing required columns: {missing_columns}")
   
   return len(missing_columns) == 0, missing_columns

def validate_numeric_columns(df, numeric_columns, raise_error=False):
   """
   Validate that columns in a DataFrame are numeric
   
   Args:
       df: DataFrame to validate
       numeric_columns: List of column names that should be numeric
       raise_error: Whether to raise an error if validation fails
       
   Returns:
       tuple: (is_valid, non_numeric_columns)
   """
   if df is None:
       if raise_error:
           raise ValueError("DataFrame is None")
       return False, numeric_columns
   
   non_numeric_columns = [col for col in numeric_columns if col in df.columns and not is_numeric_column(df, col)]
   
   if non_numeric_columns and raise_error:
       raise ValueError(f"Non-numeric columns: {non_numeric_columns}")
   
   return len(non_numeric_columns) == 0, non_numeric_columns

def validate_non_empty(df, raise_error=False):
   """
   Validate that a DataFrame is not empty
   
   Args:
       df: DataFrame to validate
       raise_error: Whether to raise an error if validation fails
       
   Returns:
       bool: True if DataFrame is not empty
   """
   if df is None:
       if raise_error:
           raise ValueError("DataFrame is None")
       return False
   
   if len(df) == 0:
       if raise_error:
           raise ValueError("DataFrame is empty")
       return False
   
   return True

# =============================================
# GIS and Spatial Utilities
# =============================================

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
           
       # Convert any numpy numeric types to Python native types
       elif pd.api.types.is_numeric_dtype(gdf_copy[col]):
           # Handle NaN values separately
           mask = pd.isna(gdf_copy[col])
           if mask.any():
               # Convert non-NaN values to Python native types
               temp_values = gdf_copy.loc[~mask, col].copy()
               if pd.api.types.is_integer_dtype(gdf_copy[col]):
                   temp_values = temp_values.astype(int)
               else:
                   temp_values = temp_values.astype(float)
               
               # Update with converted values
               gdf_copy.loc[~mask, col] = temp_values
           else:
               # No NaN values, can convert entire column
               if pd.api.types.is_integer_dtype(gdf_copy[col]):
                   gdf_copy[col] = gdf_copy[col].astype(int)
               else:
                   gdf_copy[col] = gdf_copy[col].astype(float)
   
   return gdf_copy

def create_buffer(gdf, distance_meters):
   """
   Create a buffer around geometries in a GeoDataFrame
   
   Args:
       gdf: GeoDataFrame with geometries
       distance_meters: Buffer distance in meters
       
   Returns:
       GeoDataFrame with buffered geometries
   """
   # Ensure GeoDataFrame has a CRS
   if gdf.crs is None:
       logger.warning("GeoDataFrame has no CRS. Cannot create accurate buffer.")
       return gdf
   
   try:
       # Convert to a projected CRS for accurate buffer distances
       # UTM zones provide good distance accuracy for their regions
       # This is a simplification - for more accuracy, determine UTM zone based on centroid
       temp_gdf = gdf.to_crs(epsg=3857)  # Web Mercator for simplicity
       
       # Create buffer
       buffered = temp_gdf.copy()
       buffered.geometry = temp_gdf.geometry.buffer(distance_meters)
       
       # Convert back to original CRS
       buffered = buffered.to_crs(gdf.crs)
       
       return buffered
   except Exception as e:
       logger.error(f"Error creating buffer: {str(e)}")
       return gdf

def get_nearest_neighbors(gdf, points_gdf, k=5):
   """
   Find k nearest neighbors for each point in points_gdf
   
   Args:
       gdf: GeoDataFrame with potential neighbors
       points_gdf: GeoDataFrame with points to find neighbors for
       k: Number of neighbors to find
       
   Returns:
       dict: Dictionary mapping point indices to lists of neighbor indices
   """
   try:
       from sklearn.neighbors import BallTree
       
       # Ensure both GeoDataFrames have the same CRS
       if gdf.crs != points_gdf.crs:
           points_gdf = points_gdf.to_crs(gdf.crs)
       
       # Extract coordinates
       coords = np.array([(geom.x, geom.y) for geom in gdf.geometry.centroid])
       points_coords = np.array([(geom.x, geom.y) for geom in points_gdf.geometry.centroid])
       
       # Build BallTree
       tree = BallTree(coords, leaf_size=15)
       
       # Find k nearest neighbors
       distances, indices = tree.query(points_coords, k=min(k+1, len(coords)))
       
       # Remove self from neighbors (if present)
       neighbor_dict = {}
       for i, idx_array in enumerate(indices):
           # Skip the first index if it's the same point
           neighbors = idx_array[1:] if idx_array[0] == i and len(idx_array) > 1 else idx_array
           neighbor_dict[i] = neighbors.tolist()
       
       return neighbor_dict
   except ImportError:
       logger.warning("sklearn not installed. Using slower distance calculation.")
       # Fallback method without sklearn
       neighbor_dict = {}
       for i, point_geom in enumerate(points_gdf.geometry):
           distances = gdf.geometry.distance(point_geom)
           nearest_indices = distances.argsort()[:k+1]
           # Remove self from neighbors if present
           if nearest_indices[0] == i and len(nearest_indices) > 1:
               nearest_indices = nearest_indices[1:]
           neighbor_dict[i] = nearest_indices.tolist()
       
       return neighbor_dict
   except Exception as e:
       logger.error(f"Error finding nearest neighbors: {str(e)}")
       return {}

# =============================================
# Data Processing Utilities
# =============================================

def convert_to_json_serializable(obj):
   """
   Recursively convert objects to JSON serializable types.
   Specifically handles NumPy types which are not JSON serializable by default.
   
   Args:
       obj: Object to convert
       
   Returns:
       JSON serializable version of the object
   """
   if isinstance(obj, dict):
       return {k: convert_to_json_serializable(v) for k, v in obj.items()}
   elif isinstance(obj, list) or isinstance(obj, tuple):
       return [convert_to_json_serializable(item) for item in obj]
   
   # NumPy types
   elif hasattr(np, 'integer') and isinstance(obj, np.integer):
       return int(obj)
   elif hasattr(np, 'floating') and isinstance(obj, np.floating):
       return float(obj)
   elif hasattr(np, 'bool_') and isinstance(obj, np.bool_):
       return bool(obj)
   elif isinstance(obj, np.ndarray):
       return convert_to_json_serializable(obj.tolist())
   elif pd.isna(obj):
       return None
       
   # Other Python types
   elif obj is None or isinstance(obj, (str, int, float, bool)):
       return obj
   # For pandas Timestamp objects
   elif hasattr(obj, 'isoformat'):
       return obj.isoformat()
   # For other types, try string conversion
   else:
       try:
           return str(obj)
       except:
           return f"Unserializable object of type: {type(obj).__name__}"

def detect_variable_type(df, column_name):
   """
   Detect the type of variable in a column for analysis purposes
   
   Args:
       df: DataFrame containing the column
       column_name: Name of the column to analyze
       
   Returns:
       str: 'numeric', 'categorical', 'binary', 'datetime', or 'unknown'
   """
   if column_name not in df.columns:
       return 'unknown'
   
   # Get the column
   col = df[column_name]
   
   # Check if numeric
   if pd.api.types.is_numeric_dtype(col):
       # Check if binary (only 0/1 or True/False)
       unique_values = col.dropna().unique()
       if len(unique_values) <= 2 and all(val in [0, 1, True, False] for val in unique_values):
           return 'binary'
       return 'numeric'
   
   # Check if datetime
   if pd.api.types.is_datetime64_any_dtype(col):
       return 'datetime'
   
   # Check if could be datetime
   try:
       pd.to_datetime(col)
       return 'datetime'
   except:
       pass
   
   # Check if categorical/binary
   unique_values = col.dropna().unique()
   if len(unique_values) <= 5:  # Arbitrary threshold for categorical
       # Check if binary (yes/no, true/false)
       if len(unique_values) <= 2:
           lower_values = [str(val).lower() for val in unique_values]
           binary_values = ['yes', 'no', 'true', 'false', 't', 'f', 'y', 'n', '1', '0']
           if all(val in binary_values for val in lower_values):
               return 'binary'
       return 'categorical'
   
   # Default to unknown
   return 'unknown'

def detect_column_relationships(df, target_column, method='correlation'):
   """
   Detect relationships between columns and a target column
   
   Args:
       df: DataFrame
       target_column: Target column name
       method: Method to detect relationships ('correlation', 'chi2', or 'auto')
       
   Returns:
       dict: Dictionary mapping column names to relationship strengths
   """
   if target_column not in df.columns:
       return {}
   
   relationships = {}
   
   # Determine target column type
   target_type = detect_variable_type(df, target_column)
   
   for col in df.columns:
       if col == target_column or not pd.api.types.is_numeric_dtype(df[col]):
           continue
       
       # Calculate relationship based on method
       if method == 'correlation' or (method == 'auto' and target_type == 'numeric'):
           # Pearson correlation for numeric columns
           try:
               corr = df[[col, target_column]].corr().iloc[0, 1]
               if not pd.isna(corr):
                   relationships[col] = corr
           except:
               pass
       elif method == 'chi2' or (method == 'auto' and target_type in ['categorical', 'binary']):
           # Chi-squared test for categorical columns
           try:
               from scipy.stats import chi2_contingency
               
               # Create contingency table
               contingency = pd.crosstab(df[col], df[target_column])
               chi2, p, dof, expected = chi2_contingency(contingency)
               
               # Use Cramer's V for normalized measure
               n = contingency.sum().sum()
               phi2 = chi2 / n
               r, k = contingency.shape
               phi = np.sqrt(phi2 / min(k-1, r-1))
               
               relationships[col] = phi
           except:
               pass
   
   return relationships

def handle_missing_values_vectorized(df, columns=None, method='mean'):
   """
   Handle missing values in a DataFrame using vectorized operations
   
   Args:
       df: DataFrame with missing values
       columns: List of columns to process (None for all)
       method: Imputation method ('mean', 'median', 'mode', 'zero', 'ffill', 'bfill')
       
   Returns:
       DataFrame with missing values handled
   """
   if df is None or df.empty:
       return df
   
   # Create a copy to avoid modifying the original
   result = df.copy()
   
   # Default to all columns if none specified
   if columns is None:
       columns = df.columns
   
   # Process each column
   for col in columns:
       if col not in df.columns or not df[col].isna().any():
           continue
       
       # Apply different methods based on column type
       if pd.api.types.is_numeric_dtype(df[col]):
           if method == 'mean':
               result[col] = df[col].fillna(df[col].mean())
           elif method == 'median':
               result[col] = df[col].fillna(df[col].median())
           elif method == 'mode':
               result[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else 0)
           elif method == 'zero':
               result[col] = df[col].fillna(0)
           elif method == 'ffill':
               result[col] = df[col].fillna(method='ffill')
           elif method == 'bfill':
               result[col] = df[col].fillna(method='bfill')
           else:
               # Default to mean
               result[col] = df[col].fillna(df[col].mean())
       else:
           # For non-numeric columns, use mode or ffill/bfill
           if method == 'mode':
               result[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else '')
           elif method == 'ffill':
               result[col] = df[col].fillna(method='ffill')
           elif method == 'bfill':
               result[col] = df[col].fillna(method='bfill')
           else:
               # Default to mode for non-numeric
               result[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else '')
   
   return result

def z_score_normalization(values):
   """
   Normalize values using Z-score (mean=0, std=1)
   
   Args:
       values: NumPy array of values to normalize
       
   Returns:
       Normalized values
   """
   # Handle edge cases
   if len(values) == 0:
       return values
   
   if np.all(values == values[0]):
       return np.zeros_like(values)
   
   # Z-score normalization
   mean = np.mean(values)
   std = np.std(values)
   
   if std == 0:
       return np.zeros_like(values)
   
   return (values - mean) / std

def minmax_normalization(values, feature_range=(0, 1)):
   """
   Normalize values to a specified range (default 0-1)
   
   Args:
       values: NumPy array of values to normalize
       feature_range: Tuple with (min, max) for output range
       
   Returns:
       Normalized values
   """
   # Handle edge cases
   if len(values) == 0:
       return values
   
   if np.all(values == values[0]):
       # All values are the same, return middle of the range
       middle = (feature_range[0] + feature_range[1]) / 2
       return np.full_like(values, middle)
   
   # Min-max normalization
   min_val = np.min(values)
   max_val = np.max(values)
   
   if min_val == max_val:
       # Handle division by zero
       middle = (feature_range[0] + feature_range[1]) / 2
       return np.full_like(values, middle)
   
   # Scale to feature range
   scaled = (values - min_val) / (max_val - min_val)
   return scaled * (feature_range[1] - feature_range[0]) + feature_range[0]

def robust_normalization(values, feature_range=(0, 1)):
   """
   Normalize values using robust scaling (median, IQR)
   This is less sensitive to outliers than min-max scaling
   
   Args:
       values: NumPy array of values to normalize
       feature_range: Tuple with (min, max) for output range
       
   Returns:
       Normalized values
   """
   # Handle edge cases
   if len(values) == 0:
       return values
   
   if np.all(values == values[0]):
       # All values are the same, return middle of the range
       middle = (feature_range[0] + feature_range[1]) / 2
       return np.full_like(values, middle)
   
   # Robust scaling
   median = np.median(values)
   q1 = np.percentile(values, 25)
   q3 = np.percentile(values, 75)
   iqr = q3 - q1
   
   if iqr == 0:
       # Fall back to min-max scaling
       return minmax_normalization(values, feature_range)
   
   # Scale using IQR
   scaled = (values - median) / iqr
   
   # Clip extreme values
   scaled = np.clip(scaled, -5, 5)
   
   # Scale to feature range
   min_scaled = np.min(scaled)
   max_scaled = np.max(scaled)
   
   if min_scaled == max_scaled:
       # Handle division by zero
       middle = (feature_range[0] + feature_range[1]) / 2
       return np.full_like(values, middle)
   
   return (scaled - min_scaled) / (max_scaled - min_scaled) * (feature_range[1] - feature_range[0]) + feature_range[0]

# =============================================
# Variable Name Utilities
# =============================================

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

def get_variable_by_name(df, variable_name):
   """
   Get the actual variable name that best matches the requested name
   
   Args:
       df: DataFrame containing variables
       variable_name: Requested variable name (may be inexact)
       
   Returns:
       str: Best matching variable name, or None if not found
   """
   # Check input validity
   if df is None or not variable_name:
       return None
   
   # Convert to lowercase for case-insensitive matching
   variable_lower = variable_name.lower()
   
   # Clean up the variable name (remove articles, common words)
   clean_variable = re.sub(r'\b(the|of|for|in|a|an)\b', '', variable_lower).strip()
   clean_variable = re.sub(r'\s+', ' ', clean_variable)
   
   # Get all column names from the DataFrame
   columns = list(df.columns)
   
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
       if col != 'WardName' and not is_id_column(col) and is_numeric_column(df, col):
           logger.warning(f"No match found for {variable_name}, using {col} as fallback")
           return col
           
   logger.error(f"Could not find any suitable variable match for {variable_name}")
   return None

# =============================================
# File and Path Utilities
# =============================================

def get_session_folder(session_id):
   """
   Get the folder path for a session
   
   Args:
       session_id: Session identifier
       
   Returns:
       str: Path to session folder
   """
   upload_folder = current_app.config.get('UPLOAD_FOLDER')
   if not upload_folder:
       logger.error("UPLOAD_FOLDER not configured in Flask app config")
       return None
   
   session_folder = os.path.join(upload_folder, session_id)
   os.makedirs(session_folder, exist_ok=True)
   return session_folder

def get_reports_folder(session_id):
   """
   Get the reports folder path for a session
   
   Args:
       session_id: Session identifier
       
   Returns:
       str: Path to reports folder
   """
   reports_folder = current_app.config.get('REPORTS_FOLDER')
   if not reports_folder:
       # Default to reports subfolder in uploads
       upload_folder = current_app.config.get('UPLOAD_FOLDER')
       if not upload_folder:
           logger.error("UPLOAD_FOLDER not configured in Flask app config")
           return None
       reports_folder = os.path.join(upload_folder, 'reports')
   
   session_reports_folder = os.path.join(reports_folder, session_id)
   os.makedirs(session_reports_folder, exist_ok=True)
   return session_reports_folder

def safe_path_join(base_path, *paths):
   """
   Safely join paths, ensuring the result is within the base path
   
   Args:
       base_path: Base directory path
       *paths: Path components to join
       
   Returns:
       str: Joined path, or None if unsafe
   """
   joined_path = os.path.abspath(os.path.join(base_path, *paths))
   if not joined_path.startswith(os.path.abspath(base_path)):
       logger.warning(f"Attempted path traversal: {joined_path} is outside {base_path}")
       return None
   return joined_path

def get_file_extension(filename):
   """
   Get the extension of a file
   
   Args:
       filename: Filename to check
       
   Returns:
       str: File extension without the dot
   """
   if not filename or '.' not in filename:
       return ''
   return filename.rsplit('.', 1)[1].lower()

def is_allowed_file(filename, allowed_extensions):
   """
   Check if a file has an allowed extension
   
   Args:
       filename: Filename to check
       allowed_extensions: Set of allowed extensions
       
   Returns:
       bool: True if file is allowed
   """
   return '.' in filename and get_file_extension(filename) in allowed_extensions

def create_unique_filename(original_filename, session_id=None):
   """
   Create a unique filename based on the original
   
   Args:
       original_filename: Original filename
       session_id: Optional session ID to include
       
   Returns:
       str: Unique filename
   """
   # Get base name and extension
   base_name, ext = os.path.splitext(original_filename)
   
   # Clean the base name
   base_name = re.sub(r'[^\w\-_.]', '_', base_name)
   
   # Generate timestamp
   timestamp = time.strftime("%Y%m%d_%H%M%S")
   
   # Create unique name
   if session_id:
       unique_name = f"{base_name}_{session_id[:8]}_{timestamp}{ext}"
   else:
       unique_name = f"{base_name}_{timestamp}{ext}"
   
   return unique_name

# =============================================
# Error Handling Utilities
# =============================================

def log_error_details(error, session_id=None, context=None):
   """
   Log detailed error information
   
   Args:
       error: The exception object
       session_id: Optional session ID
       context: Optional context information
       
   Returns:
       dict: Error details
   """
   error_info = {
       'error_type': type(error).__name__,
       'error_message': str(error),
       'timestamp': time.time(),
       'stack_trace': traceback.format_exc()
   }
   
   if session_id:
       error_info['session_id'] = session_id
   
   if context:
       error_info['context'] = context
   
   # Log to application logger
   logger.error(
       f"Error: {error_info['error_type']} - {error_info['error_message']}", 
       exc_info=True,
       extra={'error_info': error_info}
   )
   
   # Log to interaction logger if available
   try:
       interaction_logger = current_app.config.get('INTERACTION_LOGGER')
       if interaction_logger and session_id:
           interaction_logger.log_error(
               session_id,
               error_info['error_type'],
               error_info['error_message'],
               error_info['stack_trace']
           )
   except Exception as e:
       logger.error(f"Error while logging to interaction logger: {str(e)}")
   
   return error_info

def format_error_response(error, user_message=None):
   """
   Format an error for API response
   
   Args:
       error: The exception object or error message
       user_message: Optional user-friendly message
       
   Returns:
       dict: Formatted error response
   """
   if isinstance(error, Exception):
       error_message = str(error)
       error_type = type(error).__name__
   else:
       error_message = str(error)
       error_type = "Error"
   
   # Default user message
   if not user_message:
       user_message = "An error occurred while processing your request. Please try again."
   
   return {
       'status': 'error',
       'message': user_message,
       'error_details': {
           'type': error_type,
           'message': error_message
       }
   }

def try_execute(func, error_message="An error occurred", *args, **kwargs):
   """
   Try to execute a function and handle errors gracefully
   
   Args:
       func: Function to execute
       error_message: Message to use if error occurs
       *args: Arguments for the function
       **kwargs: Keyword arguments for the function
       
   Returns:
       tuple: (result, error) where error is None if successful
   """
   try:
       result = func(*args, **kwargs)
       return result, None
   except Exception as e:
       logger.error(f"{error_message}: {str(e)}", exc_info=True)
       return None, e

# =============================================
# Data Extraction and Validation
# =============================================

def extract_variables_from_message(message, available_vars=None):
   """
   Extract variable names from a message
   
   Args:
       message: User message text
       available_vars: Optional list of available variables for validation
       
   Returns:
       list: Extracted variable names
   """
   if not message:
       return []
   
   # Predefined variable names to look for
   known_vars = list(VARIABLE_FULL_NAMES.keys())
   
   # Add available variables if provided
   if available_vars:
       known_vars.extend([var.lower() for var in available_vars])
   
   # Make list unique
   known_vars = list(set(known_vars))
   
   # First check for comma or 'and' separated lists
   list_pattern = r'\b(' + '|'.join(known_vars) + r')(?:,|\s+and\s+|\s*\+\s*)'
   var_list_matches = re.findall(list_pattern, message.lower())
   
   # Then check for standalone mentions
   standalone_pattern = r'\b(' + '|'.join(known_vars) + r')\b'
   standalone_matches = re.findall(standalone_pattern, message.lower())
   
   # Combine and deduplicate
   all_matches = var_list_matches + standalone_matches
   extracted_vars = list(set(all_matches))
   
   # Validate against available variables if provided
   if available_vars:
       # Normalize for comparison
       available_normalized = [v.lower() for v in available_vars]
       validated_vars = []
       
       for var in extracted_vars:
           if var in available_normalized:
               # Use the original case
               idx = available_normalized.index(var)
               validated_vars.append(available_vars[idx])
           elif any(var in av or av in var for av in available_normalized):
               # Find best partial match
               best_match = None
               for av_idx, av in enumerate(available_normalized):
                   if var in av or av in var:
                       if best_match is None or len(av) < len(available_normalized[best_match]):
                           best_match = av_idx
               
               if best_match is not None:
                   validated_vars.append(available_vars[best_match])
       
       return validated_vars
   
   return extracted_vars

def clean_and_validate_variables(variables, available_vars):
   """
   Clean and validate a list of variable names
   
   Args:
       variables: List of variable names to clean and validate
       available_vars: List of available variables
       
   Returns:
       list: Cleaned and validated variable names
   """
   if not variables or not available_vars:
       return []
   
   # Normalize available variables for comparison
   available_normalized = {v.lower(): v for v in available_vars}
   
   validated_vars = []
   
   for var in variables:
       if not var:
           continue
           
       var_lower = var.lower()
       
       # Direct match
       if var_lower in available_normalized:
           validated_vars.append(available_normalized[var_lower])
           continue
       
       # Remove common prefixes/suffixes for better matching
       clean_var = re.sub(r'^(the\s+|a\s+|an\s+)', '', var_lower)
       clean_var = re.sub(r'\s+data$|\s+values$|\s+variable$', '', clean_var)
       
       if clean_var in available_normalized:
           validated_vars.append(available_normalized[clean_var])
           continue
       
       # Try partial matching
       for av_lower, av in available_normalized.items():
           if clean_var in av_lower or av_lower in clean_var:
               validated_vars.append(av)
               break
   
   # Remove duplicates while preserving order
   seen = set()
   return [x for x in validated_vars if not (x in seen or seen.add(x))]

def match_variables_to_dataset(variables, df):
   """
   Match a list of variable names to actual columns in a DataFrame
   
   Args:
       variables: List of variable names to match
       df: DataFrame containing the actual columns
       
   Returns:
       list: Matched column names
   """
   if not variables or df is None:
       return []
   
   # Get all columns from the DataFrame
   all_columns = list(df.columns)
   
   # Normalize column names for comparison
   columns_lower = {col.lower(): col for col in all_columns}
   
   matched_columns = []
   
   for var in variables:
       var_lower = var.lower()
       
       # Direct match
       if var_lower in columns_lower:
           matched_columns.append(columns_lower[var_lower])
           continue
       
       # Try to find a partial match
       best_match = None
       for col_lower, col in columns_lower.items():
           if var_lower in col_lower or col_lower in var_lower:
               if best_match is None or len(col_lower) < len(best_match[0]):
                   best_match = (col_lower, col)
       
       if best_match:
           matched_columns.append(best_match[1])
   
   # Remove duplicates while preserving order
   seen = set()
   return [x for x in matched_columns if not (x in seen or seen.add(x))]

# =============================================
# Contextual Metadata Utilities for LLM
# =============================================

def create_variable_context(variable_name, df, relationships=None):
   """
   Create rich context about a variable for LLM explanations
   
   Args:
       variable_name: Name of the variable
       df: DataFrame containing the variable
       relationships: Optional dictionary of variable relationships
       
   Returns:
       dict: Context information about the variable
   """
   if variable_name not in df.columns:
       return {'error': f"Variable {variable_name} not found in data"}
   
   context = {
       'variable_name': variable_name,
       'full_name': get_full_variable_name(variable_name),
       'relationship': relationships.get(variable_name, 'unknown') if relationships else 'unknown'
   }
   
   # Add statistical summary
   try:
       if is_numeric_column(df, variable_name):
           values = df[variable_name].dropna().values
           context['statistics'] = {
               'min': float(np.min(values)) if len(values) > 0 else None,
               'max': float(np.max(values)) if len(values) > 0 else None,
               'mean': float(np.mean(values)) if len(values) > 0 else None,
               'median': float(np.median(values)) if len(values) > 0 else None,
               'std': float(np.std(values)) if len(values) > 0 else None,
               'missing_values': int(df[variable_name].isna().sum()),
               'missing_percentage': float(df[variable_name].isna().sum() / len(df) * 100),
               'total_values': len(df)
           }
       else:
           # For categorical columns
           value_counts = df[variable_name].value_counts()
           context['statistics'] = {
               'unique_values': int(len(value_counts)),
               'most_common': str(value_counts.index[0]) if len(value_counts) > 0 else None,
               'most_common_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
               'missing_values': int(df[variable_name].isna().sum()),
               'missing_percentage': float(df[variable_name].isna().sum() / len(df) * 100),
               'total_values': len(df)
           }
   except Exception as e:
       logger.error(f"Error getting statistics for {variable_name}: {str(e)}")
       context['statistics_error'] = str(e)
   
   # Add variable type information
   context['variable_type'] = detect_variable_type(df, variable_name)
   
   # Add relationship explanation
   if context['relationship'] == 'direct':
       context['relationship_explanation'] = "Higher values correspond to higher malaria risk"
   elif context['relationship'] == 'inverse':
       context['relationship_explanation'] = "Higher values correspond to lower malaria risk (the relationship is inverted)"
   else:
       context['relationship_explanation'] = "Relationship with malaria risk is uncertain or complex"
   
   return context

def create_ward_context(ward_name, rankings_df, csv_data=None, urban_column=None):
   """
   Create rich context about a ward for LLM explanations
   
   Args:
       ward_name: Name of the ward
       rankings_df: DataFrame containing ward rankings
       csv_data: Optional DataFrame with raw data
       urban_column: Optional name of urban percentage column
       
   Returns:
       dict: Context information about the ward
   """
   if ward_name not in rankings_df['WardName'].values:
       return {'error': f"Ward {ward_name} not found in rankings"}
   
   # Get ward ranking information
   ward_data = rankings_df[rankings_df['WardName'] == ward_name].iloc[0].to_dict()
   
   context = {
       'ward_name': ward_name,
       'ranking': {
           'overall_rank': int(ward_data.get('overall_rank', 0)),
           'total_wards': len(rankings_df),
           'percentile': float((len(rankings_df) - ward_data.get('overall_rank', 0)) / len(rankings_df) * 100),
           'vulnerability_category': str(ward_data.get('vulnerability_category', 'Unknown')),
           'median_score': float(ward_data.get('median_score', 0) if 'median_score' in ward_data else 
                               ward_data.get('value', 0))
       }
   }
   
   # Add urban information if available
   if csv_data is not None and urban_column and urban_column in csv_data.columns:
       urban_data = csv_data[csv_data['WardName'] == ward_name]
       if not urban_data.empty:
           urban_percentage = float(urban_data[urban_column].iloc[0])
           context['urban_data'] = {
               'urban_percentage': urban_percentage,
               'is_urban': urban_percentage >= 30,  # Common threshold
               'urban_column': urban_column
           }
           
           # Check for "not ideal" condition (high vulnerability but non-urban)
           if context['ranking']['overall_rank'] <= 10 and context['urban_data']['urban_percentage'] < 30:
               context['not_ideal'] = {
                   'is_not_ideal': True,
                   'reason': 'non_urban_high_vulnerability',
                   'explanation': 'This ward has high vulnerability (top 10) but is classified as non-urban, '
                                 'which may present challenges for urban-focused interventions.'
               }
   
   # Add variable data if available
   if csv_data is not None and ward_name in csv_data['WardName'].values:
       ward_row = csv_data[csv_data['WardName'] == ward_name].iloc[0]
       
       variable_values = {}
       for col in csv_data.columns:
           if col != 'WardName' and not is_id_column(col):
               if is_numeric_column(csv_data, col):
                   value = ward_row[col]
                   variable_values[col] = float(value) if pd.notna(value) else None
       
       context['variable_values'] = variable_values
   
   return context

def create_visualization_context(viz_type, data_summary=None, visual_elements=None, analysis_context=None):
   """
   Create rich context about a visualization for LLM explanations
   
   Args:
       viz_type: Type of visualization
       data_summary: Summary of the data being visualized
       visual_elements: Description of visual elements
       analysis_context: Additional context about the analysis
       
   Returns:
       dict: Context information about the visualization
   """
   context = {
       'visualization_type': viz_type
   }
   
   if data_summary:
       context['data_summary'] = data_summary
   
   if visual_elements:
       context['visual_elements'] = visual_elements
   
   if analysis_context:
       context['analysis_context'] = analysis_context
   
   # Add type-specific context
   if viz_type == 'variable_map':
       context['explanation_focus'] = 'distribution_patterns'
       context['key_visualization_elements'] = [
           'Color intensity represents variable values',
           'Spatial patterns show geographic distribution',
           'Clusters indicate areas with similar values'
       ]
   elif viz_type == 'normalized_map':
       context['explanation_focus'] = 'risk_contribution'
       context['key_visualization_elements'] = [
           'Color represents normalized risk contribution',
           'Values scaled to 0-1 range',
           'Account for direct/inverse relationship with risk'
       ]
   elif viz_type == 'composite_map':
       context['explanation_focus'] = 'combined_risk_models'
       context['key_visualization_elements'] = [
           'Multiple maps showing different variable combinations',
           'Risk scores from 0-1 (low to high)',
           'Blue outlines indicate non-urban high-risk wards'
       ]
   elif viz_type == 'vulnerability_map':
       context['explanation_focus'] = 'vulnerability_ranking'
       context['key_visualization_elements'] = [
           'Color represents vulnerability category',
           'Shows spatial distribution of risk',
           'Identifies priority areas for intervention'
       ]
   elif viz_type == 'vulnerability_plot':
       context['explanation_focus'] = 'ward_ranking_distribution'
       context['key_visualization_elements'] = [
           'Box plots show score distribution across models',
           'Wards ordered by overall vulnerability rank',
           'Color indicates vulnerability category'
       ]
   elif viz_type == 'urban_extent_map':
       context['explanation_focus'] = 'urban_rural_classification'
       context['key_visualization_elements'] = [
           'Urban areas colored by vulnerability',
           'Non-urban areas shown in gray',
           'Urban threshold applied to classify areas'
       ]
   
   return context


# Function to sanitize data for JSON serialization (if it's also in utilities)
def convert_to_json_serializable(obj):
   """
   Recursively convert objects to JSON serializable types.
   Handles common data types from pandas and numpy.
   """
   if isinstance(obj, dict):
       return {k: convert_to_json_serializable(v) for k, v in obj.items()}
   if isinstance(obj, (list, tuple)):
       return [convert_to_json_serializable(item) for item in obj]

   # Handle datetime.datetime and pd.Timestamp (including NaT)
   if isinstance(obj, (datetime.datetime, pd.Timestamp)):
       # pd.NaT is a Timestamp and pd.isna(pd.NaT) is True.
       # obj.isoformat() for pd.NaT gives 'NaT', which is not standard JSON null.
       if pd.isna(obj): # This handles pd.NaT
           return None
       return obj.isoformat()

   # Handle NumPy scalars (integers, floats, booleans)
   # np.generic is a base class for numpy scalar types
   if isinstance(obj, np.generic):
       if np.issubdtype(obj.dtype, np.integer):
           return int(obj)
       elif np.issubdtype(obj.dtype, np.floating):
           if np.isnan(obj): # Ensure np.nan (numpy float nan) becomes None
               return None
           return float(obj)
       elif np.issubdtype(obj.dtype, np.bool_):
           return bool(obj)
       # Add other specific numpy scalar types if needed, e.g., np.str_

   if isinstance(obj, np.ndarray):
       return convert_to_json_serializable(obj.tolist()) # Recurse for items

   # Standard Python types that are already JSON serializable (or None)
   # bool is already handled by np.generic if it's a np.bool_
   # Python's bool is already serializable.
   if obj is None or isinstance(obj, (str, int, bool)):
       return obj

   # Python float (including nan, inf)
   if isinstance(obj, float):
       if np.isnan(obj) or np.isinf(obj): # Convert Python's float nan/inf to None
           return None
       return obj
       
   # Fallback for other types: try string conversion
   try:
       return str(obj)
   except Exception:
       return f"Unserializable object of type: {type(obj).__name__}"
