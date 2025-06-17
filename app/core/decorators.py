"""
Core decorators for ChatMRPT application.
"""

import time
import logging
from functools import wraps
from typing import Callable, Any, Dict
from flask import session, request, g
from .exceptions import ValidationError, SessionError, DataError

logger = logging.getLogger(__name__)


def validate_session(f: Callable) -> Callable:
    """Decorator to validate session exists and is active."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check Flask session first, then kwargs, then g
        session_id = session.get('session_id') or kwargs.get('session_id') or getattr(g, 'session_id', None)
        
        # For web routes, we don't require session_id to be present initially
        # The session will be created if it doesn't exist
        if not session_id:
            # Generate a new session ID if none exists
            import uuid
            session_id = str(uuid.uuid4())
            session['session_id'] = session_id
            logger.info(f"Generated new session ID: {session_id}")
        
        return f(*args, **kwargs)
    return decorated_function


def handle_errors(f: Callable) -> Callable:
    """Decorator to handle and log errors consistently."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {f.__name__}: {str(e)}")
            # Re-raise the exception for now, could be modified to return error dict
            raise
    return decorated_function


def log_execution_time(f: Callable) -> Callable:
    """Decorator to log function execution time."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        try:
            result = f(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{f.__name__} executed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{f.__name__} failed after {execution_time:.2f} seconds: {str(e)}")
            raise
    return decorated_function


# Removed problematic require_data_loaded decorator - use manual validation instead


# Simple rate limiting decorator
_rate_limit_storage = {}

def rate_limit(max_calls: int = 10, window_seconds: int = 60):
    """Simple rate limiting decorator."""
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Simple in-memory rate limiting
            client_id = request.remote_addr if request else 'unknown'
            current_time = time.time()
            
            if client_id not in _rate_limit_storage:
                _rate_limit_storage[client_id] = []
            
            # Clean old entries
            _rate_limit_storage[client_id] = [
                t for t in _rate_limit_storage[client_id] 
                if current_time - t < window_seconds
            ]
            
            # Check rate limit
            if len(_rate_limit_storage[client_id]) >= max_calls:
                raise ValidationError("Rate limit exceeded")
            
            # Record this call
            _rate_limit_storage[client_id].append(current_time)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator 