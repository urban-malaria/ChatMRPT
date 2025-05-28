"""
Common decorators for ChatMRPT application.

This module provides decorators for cross-cutting concerns like validation,
error handling, logging, and rate limiting.
"""

import time
import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional
from flask import session, jsonify, request, current_app

from .exceptions import (
    SessionError, 
    ValidationError, 
    RateLimitError,
    ChatMRPTException
)

logger = logging.getLogger(__name__)

# Simple in-memory rate limiting (use Redis in production)
_rate_limit_storage = {}


def validate_session(f: Callable) -> Callable:
    """
    Decorator to validate that a session exists and is properly initialized.
    
    Args:
        f: Function to decorate
        
    Returns:
        Decorated function
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'session_id' not in session:
            raise SessionError("No active session found. Please refresh the page.")
        
        # Ensure session has required fields
        required_fields = ['session_id', 'conversation_history']
        missing_fields = [field for field in required_fields if field not in session]
        
        if missing_fields:
            raise SessionError(f"Session missing required fields: {missing_fields}")
        
        return f(*args, **kwargs)
    
    return decorated_function


def require_data_loaded(data_types: Optional[list] = None) -> Callable:
    """
    Decorator to ensure required data is loaded before proceeding.
    
    Args:
        data_types: List of required data types ('csv', 'shapefile')
        
    Returns:
        Decorator function
    """
    if data_types is None:
        data_types = ['csv']
    
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            missing_data = []
            
            if 'csv' in data_types and not session.get('csv_loaded', False):
                missing_data.append('CSV data')
            
            if 'shapefile' in data_types and not session.get('shapefile_loaded', False):
                missing_data.append('Shapefile data')
            
            if missing_data:
                raise ValidationError(
                    f"Required data not loaded: {', '.join(missing_data)}. "
                    "Please upload the required files first."
                )
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator


def handle_errors(f: Callable) -> Callable:
    """
    Decorator to handle exceptions and return appropriate JSON responses.
    
    Args:
        f: Function to decorate
        
    Returns:
        Decorated function with error handling
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        
        except ChatMRPTException as e:
            # Handle our custom exceptions
            logger.error(f"ChatMRPT error in {f.__name__}: {e.message}", 
                        extra={"details": e.details})
            return jsonify(e.to_dict()), e.status_code
        
        except ValueError as e:
            # Handle validation errors
            logger.error(f"Value error in {f.__name__}: {str(e)}")
            return jsonify({
                "error": "ValidationError",
                "message": str(e),
                "status_code": 400
            }), 400
        
        except KeyError as e:
            # Handle missing key errors
            logger.error(f"Key error in {f.__name__}: {str(e)}")
            return jsonify({
                "error": "ValidationError", 
                "message": f"Missing required field: {str(e)}",
                "status_code": 400
            }), 400
        
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Unexpected error in {f.__name__}: {str(e)}", exc_info=True)
            
            # Don't expose internal errors in production
            if current_app.config.get('DEBUG'):
                error_message = str(e)
            else:
                error_message = "An unexpected error occurred. Please try again."
            
            return jsonify({
                "error": "InternalServerError",
                "message": error_message,
                "status_code": 500
            }), 500
    
    return decorated_function


def log_execution_time(f: Callable) -> Callable:
    """
    Decorator to log function execution time.
    
    Args:
        f: Function to decorate
        
    Returns:
        Decorated function with timing
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        
        try:
            result = f(*args, **kwargs)
            execution_time = time.time() - start_time
            
            logger.info(f"Function {f.__name__} executed successfully in {execution_time:.4f}s")
            return result
        
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Function {f.__name__} failed after {execution_time:.4f}s: {str(e)}")
            raise
    
    return decorated_function


def rate_limit(max_requests: int = 100, window_seconds: int = 60) -> Callable:
    """
    Decorator to implement basic rate limiting.
    
    Args:
        max_requests: Maximum requests allowed in the time window
        window_seconds: Time window in seconds
        
    Returns:
        Decorator function
        
    Note:
        This is a simple in-memory implementation. Use Redis for production.
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get client identifier (IP + session)
            client_id = f"{request.remote_addr}_{session.get('session_id', 'anonymous')}"
            current_time = time.time()
            
            # Clean old entries
            cutoff_time = current_time - window_seconds
            if client_id in _rate_limit_storage:
                _rate_limit_storage[client_id] = [
                    timestamp for timestamp in _rate_limit_storage[client_id]
                    if timestamp > cutoff_time
                ]
            
            # Check rate limit
            client_requests = _rate_limit_storage.get(client_id, [])
            
            if len(client_requests) >= max_requests:
                raise RateLimitError(
                    f"Rate limit exceeded. Maximum {max_requests} requests per {window_seconds} seconds.",
                    retry_after=window_seconds
                )
            
            # Record this request
            if client_id not in _rate_limit_storage:
                _rate_limit_storage[client_id] = []
            
            _rate_limit_storage[client_id].append(current_time)
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator


def validate_json_request(required_fields: Optional[list] = None, 
                         optional_fields: Optional[list] = None) -> Callable:
    """
    Decorator to validate JSON request data.
    
    Args:
        required_fields: List of required field names
        optional_fields: List of optional field names
        
    Returns:
        Decorator function
    """
    required_fields = required_fields or []
    optional_fields = optional_fields or []
    
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not request.is_json:
                raise ValidationError("Request must be JSON")
            
            data = request.get_json()
            if not data:
                raise ValidationError("Request body cannot be empty")
            
            # Check required fields
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise ValidationError(
                    f"Missing required fields: {', '.join(missing_fields)}"
                )
            
            # Check for unexpected fields
            allowed_fields = set(required_fields + optional_fields)
            unexpected_fields = [field for field in data.keys() if field not in allowed_fields]
            
            if unexpected_fields:
                logger.warning(f"Unexpected fields in request: {unexpected_fields}")
            
            # Add validated data to kwargs
            kwargs['validated_data'] = data
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator


def cache_result(ttl_seconds: int = 300) -> Callable:
    """
    Simple decorator to cache function results.
    
    Args:
        ttl_seconds: Time to live for cached results
        
    Returns:
        Decorator function
        
    Note:
        This is a simple in-memory cache. Use Redis for production.
    """
    cache = {}
    
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"{f.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
            current_time = time.time()
            
            # Check if we have a valid cached result
            if cache_key in cache:
                result, timestamp = cache[cache_key]
                if current_time - timestamp < ttl_seconds:
                    logger.debug(f"Cache hit for {f.__name__}")
                    return result
            
            # Execute function and cache result
            result = f(*args, **kwargs)
            cache[cache_key] = (result, current_time)
            
            # Clean old cache entries (simple cleanup)
            if len(cache) > 1000:  # Prevent memory bloat
                cutoff_time = current_time - ttl_seconds
                cache.clear()  # Simple cleanup - remove all
            
            logger.debug(f"Cache miss for {f.__name__}, result cached")
            return result
        
        return decorated_function
    
    return decorator 