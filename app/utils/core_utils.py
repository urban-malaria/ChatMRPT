"""
Core utility functions for ChatMRPT application.

This module provides common utility functions used throughout the application.
"""

import json
import datetime
import decimal
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Union


def _convert_dataframe_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamp columns in DataFrame to string representations and handle NaN values.
    
    Args:
        df: DataFrame that may contain timestamp columns
        
    Returns:
        DataFrame with timestamp columns converted to strings and NaN values handled
    """
    if df is None or df.empty:
        return df
    
    df_copy = df.copy()
    
    # Check each column for timestamp types and NaN values
    for col in df_copy.columns:
        # Handle timestamp columns
        if df_copy[col].dtype.name.startswith('datetime') or 'timestamp' in str(df_copy[col].dtype).lower():
            try:
                # Convert timestamp column to string
                df_copy[col] = df_copy[col].astype(str)
            except:
                # If conversion fails, leave as is
                pass
        
        # Handle NaN values in numeric columns
        elif df_copy[col].dtype.name.startswith('float'):
            try:
                # Replace NaN and infinity with None
                df_copy[col] = df_copy[col].replace([np.nan, np.inf, -np.inf], None)
            except:
                # If replacement fails, leave as is
                pass
    
    return df_copy


def convert_to_json_serializable(obj: Any) -> Any:
    """
    Convert various Python objects to JSON-serializable format.
    
    Handles:
    - NumPy arrays and data types
    - Pandas DataFrames and Series
    - Datetime objects
    - Decimal objects
    - Complex nested structures
    
    Args:
        obj: Object to convert
        
    Returns:
        JSON-serializable version of the object
    """
    if obj is None:
        return None
    
    # Handle basic types that are already JSON serializable
    if isinstance(obj, (str, int, bool)):
        return obj
    
    # Handle float values (including NaN and infinity)
    if isinstance(obj, float):
        if np.isnan(obj):
            return None  # Convert NaN to null
        elif np.isinf(obj):
            return None  # Convert infinity to null
        else:
            return obj
    
    # Handle lists and tuples
    if isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    
    # Handle dictionaries
    if isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    
    # Handle NumPy types
    if hasattr(obj, 'dtype'):  # NumPy array or scalar
        if hasattr(obj, 'tolist'):  # NumPy array
            return obj.tolist()
        else:  # NumPy scalar
            value = obj.item()
            # Handle NaN and infinity values
            if isinstance(value, float):
                if np.isnan(value):
                    return None
                elif np.isinf(value):
                    return None
            return value
    
    # Handle GeoPandas DataFrame (must come before regular DataFrame check)
    try:
        import geopandas as gpd
        if isinstance(obj, gpd.GeoDataFrame):
            # Convert to regular DataFrame by dropping geometry column for JSON serialization
            df_copy = obj.copy()
            if 'geometry' in df_copy.columns:
                df_copy = df_copy.drop(columns=['geometry'])
            # Convert any timestamp columns to strings before creating dict
            df_copy = _convert_dataframe_timestamps(df_copy)
            return df_copy.to_dict('records')
    except ImportError:
        pass  # GeoPandas not available, continue with regular DataFrame handling
    
    # Handle Pandas DataFrame
    if isinstance(obj, pd.DataFrame):
        # Convert any timestamp columns to strings before creating dict
        df_copy = _convert_dataframe_timestamps(obj)
        return df_copy.to_dict('records')
    
    # Handle Pandas Series
    if isinstance(obj, pd.Series):
        return obj.tolist()
    
    # Handle datetime objects
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    
    # Handle pandas Timestamp objects - multiple detection methods
    # Method 1: Check class name directly
    if hasattr(obj, '__class__') and 'Timestamp' in obj.__class__.__name__:
        try:
            return obj.isoformat()
        except:
            try:
                return str(obj)
            except:
                return f"<timestamp: {type(obj).__name__}>"
    
    # Method 2: Check for pandas timestamp attributes
    if hasattr(obj, 'isoformat') and hasattr(obj, 'timestamp'):
        # This handles pandas.Timestamp and similar objects
        try:
            return obj.isoformat()
        except:
            return str(obj)
    
    # Method 3: Check for pandas module in type string
    if hasattr(obj, '__class__') and 'pandas' in str(type(obj)):
        try:
            return str(obj)
        except:
            return f"<pandas-object: {type(obj).__name__}>"
    
    # Handle decimal objects
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    
    # Handle sets
    if isinstance(obj, set):
        return list(obj)
    
    # Handle complex numbers
    if isinstance(obj, complex):
        return {'real': obj.real, 'imag': obj.imag}
    
    # Handle objects with __dict__ (try to serialize their attributes)
    if hasattr(obj, '__dict__'):
        try:
            return convert_to_json_serializable(obj.__dict__)
        except:
            pass
    
    # Last resort: convert to string
    try:
        return str(obj)
    except:
        return f"<non-serializable: {type(obj).__name__}>"


def safe_json_dumps(obj: Any, indent: int = None, **kwargs) -> str:
    """
    Safely serialize an object to JSON string.
    
    Args:
        obj: Object to serialize
        indent: JSON indentation
        **kwargs: Additional arguments for json.dumps
        
    Returns:
        JSON string representation
    """
    try:
        serializable_obj = convert_to_json_serializable(obj)
        return json.dumps(serializable_obj, indent=indent, **kwargs)
    except Exception as e:
        return json.dumps({'error': f'Serialization failed: {str(e)}'})


def extract_numeric_columns(df: pd.DataFrame) -> List[str]:
    """
    Extract numeric column names from a DataFrame.
    
    Args:
        df: Pandas DataFrame
        
    Returns:
        List of numeric column names
    """
    if df is None or df.empty:
        return []
    
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    return numeric_columns


def extract_categorical_columns(df: pd.DataFrame) -> List[str]:
    """
    Extract categorical column names from a DataFrame.
    
    Args:
        df: Pandas DataFrame
        
    Returns:
        List of categorical column names
    """
    if df is None or df.empty:
        return []
    
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
    return categorical_columns


def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> Dict[str, bool]:
    """
    Validate that required columns exist in DataFrame.
    
    Args:
        df: Pandas DataFrame to check
        required_columns: List of required column names
        
    Returns:
        Dictionary mapping column names to existence status
    """
    if df is None:
        return {col: False for col in required_columns}
    
    return {col: col in df.columns for col in required_columns}


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame column names for consistency.
    
    Args:
        df: DataFrame with potentially messy column names
        
    Returns:
        DataFrame with cleaned column names
    """
    if df is None or df.empty:
        return df
    
    # Create a copy to avoid modifying original
    cleaned_df = df.copy()
    
    # Clean column names
    cleaned_df.columns = (
        cleaned_df.columns
        .str.strip()  # Remove leading/trailing whitespace
        .str.lower()  # Convert to lowercase
        .str.replace(' ', '_')  # Replace spaces with underscores
        .str.replace('[^a-z0-9_]', '', regex=True)  # Remove special characters
    )
    
    return cleaned_df


def format_number(value: Union[int, float], decimals: int = 2) -> str:
    """
    Format a number for display with appropriate decimal places.
    
    Args:
        value: Number to format
        decimals: Number of decimal places
        
    Returns:
        Formatted number string
    """
    if value is None or pd.isna(value):
        return "N/A"
    
    try:
        if isinstance(value, (int, float)) and not pd.isna(value):
            if abs(value) >= 1000:
                return f"{value:,.{decimals}f}"
            else:
                return f"{value:.{decimals}f}"
        else:
            return str(value)
    except:
        return str(value)


def calculate_percentage(numerator: float, denominator: float) -> float:
    """
    Calculate percentage safely handling division by zero.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        
    Returns:
        Percentage value (0-100)
    """
    if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
        return 0.0
    
    return (numerator / denominator) * 100


def truncate_string(text: str, max_length: int = 50) -> str:
    """
    Truncate string to maximum length with ellipsis.
    
    Args:
        text: String to truncate
        max_length: Maximum length including ellipsis
        
    Returns:
        Truncated string
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length-3] + "..."


def get_memory_usage(obj: Any) -> str:
    """
    Get approximate memory usage of an object.
    
    Args:
        obj: Object to measure
        
    Returns:
        Human-readable memory usage string
    """
    try:
        import sys
        size_bytes = sys.getsizeof(obj)
        
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/(1024**2):.1f} MB"
        else:
            return f"{size_bytes/(1024**3):.1f} GB"
    except:
        return "Unknown"


def get_analysis_variables(df: pd.DataFrame, exclude_metadata: bool = True) -> List[str]:
    """
    Get list of variables suitable for analysis, excluding metadata columns.
    
    Args:
        df: DataFrame to analyze
        exclude_metadata: Whether to exclude metadata/identifier columns
        
    Returns:
        List of column names suitable for analysis
    """
    if df is None or df.empty:
        return []
    
    # Get all numeric columns
    numeric_cols = extract_numeric_columns(df)
    
    if not exclude_metadata:
        return numeric_cols
    
    # Define metadata column patterns to exclude
    metadata_patterns = [
        'ward', 'name', 'id', 'code', 'lga', 'state', 'region', 
        'timestamp', 'date', 'time', 'created', 'updated',
        'longitude', 'latitude', 'lon', 'lat', 'x', 'y',
        'geometry', 'geom', 'shape', 'area', 'perimeter'
    ]
    
    # Filter out metadata columns
    analysis_vars = []
    for col in numeric_cols:
        col_lower = col.lower()
        is_metadata = any(pattern in col_lower for pattern in metadata_patterns)
        if not is_metadata:
            analysis_vars.append(col)
    
    return analysis_vars


def select_composite_variables(variables: List[str], target_count: int = 5) -> List[str]:
    """
    Intelligently select variables for composite analysis.
    
    Args:
        variables: List of available variables
        target_count: Target number of variables to select
        
    Returns:
        List of selected variable names
    """
    if not variables:
        return []
    
    if len(variables) <= target_count:
        return variables
    
    # Define priority patterns (higher priority variables)
    priority_patterns = [
        'malaria', 'incidence', 'prevalence', 'mortality', 'death',
        'population', 'density', 'urban', 'rural',
        'poverty', 'income', 'wealth', 'education', 'literacy',
        'health', 'clinic', 'hospital', 'access',
        'rainfall', 'temperature', 'humidity', 'climate',
        'water', 'sanitation', 'hygiene'
    ]
    
    # Score variables based on priority patterns
    variable_scores = {}
    for var in variables:
        var_lower = var.lower()
        score = 0
        
        for i, pattern in enumerate(priority_patterns):
            if pattern in var_lower:
                # Higher score for earlier patterns (more important)
                score += (len(priority_patterns) - i) * 10
        
        # Add bonus for shorter, cleaner names
        if len(var) < 20:
            score += 5
        
        variable_scores[var] = score
    
    # Sort by score (descending) and take top variables
    sorted_vars = sorted(variables, key=lambda x: variable_scores.get(x, 0), reverse=True)
    
    return sorted_vars[:target_count] 