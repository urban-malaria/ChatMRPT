"""
Core utilities for ChatMRPT application.

This module contains essential utility functions used throughout the application.
"""

import json
import logging
import re
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Union

logger = logging.getLogger(__name__)


def is_id_column(column_name: str) -> bool:
    """
    Check if a column name represents an ID column.
    
    Args:
        column_name: Name of the column to check
        
    Returns:
        True if the column appears to be an ID column
    """
    if not isinstance(column_name, str):
        return False
    
    column_lower = column_name.lower()
    
    # Check for common ID patterns
    id_patterns = [
        'id', '_id', 'objectid', 'fid', 'uid', 'key', 'pk',
        'index', 'row_id', 'record_id', 'entity_id'
    ]
    
    for pattern in id_patterns:
        if pattern in column_lower:
            return True
    
    return False


def convert_to_json_serializable(obj: Any) -> Any:
    """
    Convert objects to JSON serializable types, handling NumPy types and pandas objects.
    
    Args:
        obj: Object to convert
        
    Returns:
        JSON serializable version of the object
    """
    if obj is None:
        return None
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        # Handle numpy NaN and infinity
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, float):
        # Handle Python float NaN and infinity
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return convert_to_json_serializable(obj.tolist())
    elif isinstance(obj, pd.Series):
        return convert_to_json_serializable(obj.tolist())
    elif isinstance(obj, pd.DataFrame):
        return convert_to_json_serializable(obj.to_dict('records'))
    elif isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
        return str(obj)
    elif pd.isna(obj):
        # Handle pandas NaT and other pandas NA values
        return None
    elif hasattr(obj, 'isoformat'):  # datetime objects
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):  # Custom objects
        return convert_to_json_serializable(obj.__dict__)
    else:
        try:
            # Try direct JSON serialization
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            # Fallback to string representation
            return str(obj)


def is_numeric_column(df: pd.DataFrame, column_name: str) -> bool:
    """
    Check if a column contains numeric data.
    
    Args:
        df: DataFrame containing the column
        column_name: Name of the column to check
        
    Returns:
        True if the column is numeric
    """
    if column_name not in df.columns:
        return False
    
    return pd.api.types.is_numeric_dtype(df[column_name])


def clean_column_name(column_name: str) -> str:
    """
    Clean and standardize column names.
    
    Args:
        column_name: Original column name
        
    Returns:
        Cleaned column name
    """
    if not isinstance(column_name, str):
        return str(column_name)
    
    # Remove special characters and replace with underscores
    cleaned = re.sub(r'[^\w\s]', '_', column_name)
    
    # Replace spaces with underscores
    cleaned = re.sub(r'\s+', '_', cleaned)
    
    # Remove consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    # Convert to lowercase
    cleaned = cleaned.lower()
    
    return cleaned


def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to float.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Float value or default
    """
    if pd.isna(value):
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to integer.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Integer value or default
    """
    if pd.isna(value):
        return default
    
    try:
        return int(float(value))  # Handle strings that represent floats
    except (ValueError, TypeError):
        return default


def get_numeric_columns(df: pd.DataFrame, exclude_id_columns: bool = True) -> List[str]:
    """
    Get list of numeric columns from a DataFrame.
    
    Args:
        df: DataFrame to analyze
        exclude_id_columns: Whether to exclude ID columns
        
    Returns:
        List of numeric column names
    """
    numeric_columns = []
    
    for col in df.columns:
        if is_numeric_column(df, col):
            if exclude_id_columns and is_id_column(col):
                continue
            if col.lower() != 'wardname':  # Exclude ward name column
                numeric_columns.append(col)
    
    return numeric_columns


def format_number(value: Union[int, float], decimal_places: int = 2) -> str:
    """
    Format a number for display.
    
    Args:
        value: Number to format
        decimal_places: Number of decimal places
        
    Returns:
        Formatted number string
    """
    if pd.isna(value):
        return "N/A"
    
    try:
        if isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
            return f"{int(value):,}"
        else:
            return f"{value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return str(value)


def validate_dataframe(df: pd.DataFrame, required_columns: List[str] = None) -> Dict[str, Any]:
    """
    Validate a DataFrame structure and content.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Validation result dictionary
    """
    result = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'info': {}
    }
    
    # Basic validation
    if df is None or df.empty:
        result['is_valid'] = False
        result['errors'].append('DataFrame is empty or None')
        return result
    
    # Check required columns
    if required_columns:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            result['is_valid'] = False
            result['errors'].append(f'Missing required columns: {missing_columns}')
    
    # Add info
    result['info'] = {
        'rows': len(df),
        'columns': len(df.columns),
        'numeric_columns': len(get_numeric_columns(df)),
        'missing_values': df.isnull().sum().sum()
    }
    
    # Check for high missing value rates
    for col in df.columns:
        missing_rate = df[col].isnull().sum() / len(df)
        if missing_rate > 0.5:
            result['warnings'].append(f'Column {col} has {missing_rate:.1%} missing values')
    
    return result 