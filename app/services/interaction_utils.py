# app/interaction/utils.py
"""
Utility Functions for Interaction Package

This module contains helper utilities and convenience functions for the 
interaction logging system. It provides common functionality used across
different modules.

Functions:
- Data validation utilities
- JSON handling helpers
- Time and formatting utilities
- Helper classes for common operations
"""

import json
import logging
import datetime
from typing import Dict, List, Any, Optional, Union
import re
import uuid

# Import unified utilities to replace redundant patterns
from ..core.responses import ResponseBuilder

# Set up logging
logger = logging.getLogger(__name__)


def validate_session_id(session_id: str) -> bool:
    """
    Validate session ID format and existence
    
    Args:
        session_id: Session ID string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not session_id or not isinstance(session_id, str):
        return False
    
    # Check if it's a valid UUID format
    try:
        uuid.UUID(session_id)
        return True
    except ValueError:
        return False


def safe_json_parse(json_string, default=None):
    """
    Safely parse JSON string with fallback
    
    Args:
        json_string: String to parse as JSON
        default: Default value if parsing fails
        
    Returns:
        Parsed JSON or default value
    """
    if not json_string:
        return default
    
    try:
        return json.loads(json_string)
    except (json.JSONDecodeError, TypeError):
        return default


def safe_json_dumps(obj, default=None):
    """
    Safely convert object to JSON string
    
    Args:
        obj: Object to convert to JSON
        default: Default value if conversion fails
        
    Returns:
        JSON string or default value
    """
    try:
        return json.dumps(obj)
    except (TypeError, ValueError):
        return default


def format_timestamp(timestamp=None):
    """
    Format timestamp for consistent display
    
    Args:
        timestamp: datetime object or None for current time
        
    Returns:
        str: Formatted timestamp string
    """
    if timestamp is None:
        timestamp = datetime.datetime.now()
    
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')


def parse_timestamp(timestamp_str):
    """
    Parse timestamp string to datetime object
    
    Args:
        timestamp_str: Timestamp string to parse
        
    Returns:
        datetime: Parsed datetime object or None if parsing fails
    """
    try:
        return datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        try:
            return datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            return None


def sanitize_filename(filename):
    """
    Sanitize filename for safe file system usage
    
    Args:
        filename: Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove leading/trailing whitespace and dots
    sanitized = sanitized.strip(' .')
    
    # Ensure not empty
    if not sanitized:
        sanitized = 'untitled'
    
    return sanitized


def create_export_filename(prefix, session_id=None, format_ext='json'):
    """
    Create standardized export filename
    
    Args:
        prefix: Filename prefix
        session_id: Optional session ID to include
        format_ext: File extension (without dot)
        
    Returns:
        str: Generated filename
    """
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if session_id:
        filename = f"{prefix}_{session_id}_{timestamp}.{format_ext}"
    else:
        filename = f"{prefix}_{timestamp}.{format_ext}"
    
    return sanitize_filename(filename)


def chunk_list(lst, chunk_size):
    """
    Split list into chunks of specified size
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Yields:
        List chunks
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def merge_dicts(*dicts):
    """
    Merge multiple dictionaries with later ones overriding earlier ones
    
    Args:
        *dicts: Variable number of dictionaries to merge
        
    Returns:
        dict: Merged dictionary
    """
    result = {}
    for d in dicts:
        if isinstance(d, dict):
            result.update(d)
    return result


def deep_merge_dicts(dict1, dict2):
    """
    Deep merge two dictionaries
    
    Args:
        dict1: First dictionary
        dict2: Second dictionary (takes precedence)
        
    Returns:
        dict: Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result


def get_nested_value(data, key_path, default=None):
    """
    Get value from nested dictionary using dot notation
    
    Args:
        data: Dictionary to search
        key_path: Dot-separated key path (e.g., "analysis.steps.0.name")
        default: Default value if key not found
        
    Returns:
        Value at key path or default
    """
    try:
        current = data
        for key in key_path.split('.'):
            if key.isdigit():
                current = current[int(key)]
            else:
                current = current[key]
        return current
    except (KeyError, IndexError, TypeError):
        return default


def set_nested_value(data, key_path, value):
    """
    Set value in nested dictionary using dot notation
    
    Args:
        data: Dictionary to modify
        key_path: Dot-separated key path
        value: Value to set
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        keys = key_path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key.isdigit():
                key = int(key)
            
            if key not in current:
                current[key] = {}
            current = current[key]
        
        final_key = keys[-1]
        if final_key.isdigit():
            final_key = int(final_key)
        
        current[final_key] = value
        return True
    except (KeyError, IndexError, TypeError):
        return False


class InteractionTimer:
    """Context manager for timing operations"""
    
    def __init__(self, operation_name=None):
        """
        Initialize timer
        
        Args:
            operation_name: Optional name for the operation being timed
        """
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def __enter__(self):
        """Start timing"""
        self.start_time = datetime.datetime.now()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and calculate duration"""
        self.end_time = datetime.datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        
        if self.operation_name:
            logger.debug(f"Operation '{self.operation_name}' took {self.duration:.3f} seconds")
    
    def get_duration(self):
        """Get duration in seconds"""
        return self.duration


class DataValidator:
    """Utility class for validating interaction data"""
    
    @staticmethod
    def validate_message_data(session_id, sender, content, intent=None, entities=None):
        """
        Validate message data before logging
        
        Returns:
            tuple: (is_valid, error_message)
        """
        if not validate_session_id(session_id):
            return False, "Invalid session ID"
        
        if not sender or sender not in ['user', 'assistant', 'system']:
            return False, "Invalid sender type"
        
        if not content or not isinstance(content, str):
            return False, "Invalid message content"
        
        if entities and not isinstance(entities, (dict, list)):
            return False, "Entities must be dict or list"
        
        return True, None
    
    @staticmethod
    def validate_analysis_step_data(session_id, step_name, **kwargs):
        """
        Validate analysis step data
        
        Returns:
            tuple: (is_valid, error_message)
        """
        if not validate_session_id(session_id):
            return False, "Invalid session ID"
        
        if not step_name or not isinstance(step_name, str):
            return False, "Invalid step name"
        
        # Validate execution time if provided
        if 'execution_time' in kwargs and kwargs['execution_time'] is not None:
            try:
                float(kwargs['execution_time'])
            except (ValueError, TypeError):
                return False, "Invalid execution time"
        
        return True, None
    
    @staticmethod
    def validate_file_upload_data(session_id, file_type, file_name, file_size):
        """
        Validate file upload data
        
        Returns:
            tuple: (is_valid, error_message)
        """
        if not validate_session_id(session_id):
            return False, "Invalid session ID"
        
        if not file_type or not isinstance(file_type, str):
            return False, "Invalid file type"
        
        if not file_name or not isinstance(file_name, str):
            return False, "Invalid file name"
        
        if not isinstance(file_size, (int, float)) or file_size < 0:
            return False, "Invalid file size"
        
        return True, None


# Convenience functions for common operations
def log_with_timer(func, *args, **kwargs):
    """
    Execute function with timing and logging
    
    Args:
        func: Function to execute
        *args: Function arguments
        **kwargs: Function keyword arguments
        
    Returns:
        Function result
    """
    operation_name = kwargs.pop('_operation_name', func.__name__)
    
    with InteractionTimer(operation_name) as timer:
        result = func(*args, **kwargs)
    
    return result


def batch_process(items, process_func, batch_size=100):
    """
    Process items in batches
    
    Args:
        items: Items to process
        process_func: Function to process each batch
        batch_size: Size of each batch
        
    Returns:
        list: Results from each batch
    """
    results = []
    
    for batch in chunk_list(items, batch_size):
        batch_result = process_func(batch)
        results.append(batch_result)
    
    return results 