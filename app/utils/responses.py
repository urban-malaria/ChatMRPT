"""
Response building utilities for ChatMRPT application.

This module provides standardized response formatting for API endpoints.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union
from flask import jsonify
from .utils import convert_to_json_serializable

logger = logging.getLogger(__name__)


class ResponseBuilder:
    """
    Builder class for creating standardized API responses.
    
    Provides consistent response format across all endpoints with
    proper error handling and JSON serialization.
    """
    
    def __init__(self):
        self.response_data = {
            'status': 'success',
            'message': '',
            'data': None,
            'errors': [],
            'metadata': {}
        }
    
    def success(self, message: str = "Operation successful", data: Any = None) -> 'ResponseBuilder':
        """Set response as successful."""
        self.response_data['status'] = 'success'
        self.response_data['message'] = message
        if data is not None:
            self.response_data['data'] = data
        return self
    
    def error(self, message: str, error_code: str = None, data: Any = None) -> 'ResponseBuilder':
        """Set response as error."""
        self.response_data['status'] = 'error'
        self.response_data['message'] = message
        if error_code:
            self.response_data['error_code'] = error_code
        if data is not None:
            self.response_data['data'] = data
        return self
    
    def warning(self, message: str, data: Any = None) -> 'ResponseBuilder':
        """Set response as warning."""
        self.response_data['status'] = 'warning'
        self.response_data['message'] = message
        if data is not None:
            self.response_data['data'] = data
        return self
    
    def add_data(self, key: str, value: Any) -> 'ResponseBuilder':
        """Add data to response."""
        if self.response_data['data'] is None:
            self.response_data['data'] = {}
        
        if isinstance(self.response_data['data'], dict):
            self.response_data['data'][key] = value
        else:
            # Convert existing data to dict and add new key
            existing_data = self.response_data['data']
            self.response_data['data'] = {'value': existing_data, key: value}
        
        return self
    
    def add_metadata(self, key: str, value: Any) -> 'ResponseBuilder':
        """Add metadata to response."""
        self.response_data['metadata'][key] = value
        return self
    
    def add_error(self, error: str) -> 'ResponseBuilder':
        """Add an error to the errors list."""
        self.response_data['errors'].append(error)
        return self
    
    def set_status_code(self, status_code: int) -> 'ResponseBuilder':
        """Set HTTP status code."""
        self.status_code = status_code
        return self
    
    def build(self) -> Dict[str, Any]:
        """Build and return the response dictionary."""
        # Clean up the response data
        cleaned_response = convert_to_json_serializable(self.response_data)
        
        # Remove empty arrays/objects
        if not cleaned_response.get('errors'):
            cleaned_response.pop('errors', None)
        
        if not cleaned_response.get('metadata'):
            cleaned_response.pop('metadata', None)
        
        return cleaned_response
    
    def jsonify(self, status_code: int = None):
        """Build and return Flask JSON response."""
        response_data = self.build()
        
        # Determine status code based on response status
        if status_code is None:
            if response_data['status'] == 'error':
                status_code = 400
            elif response_data['status'] == 'warning':
                status_code = 200
            else:
                status_code = 200
        
        return jsonify(response_data), status_code
    
        # Class methods that are actually being used in the codebase
    @classmethod
    def validation_error(cls, message: str):
        """Create a validation error response."""
        return {
            'status': 'error',
            'message': message,
            'error_code': 'VALIDATION_ERROR'
        }
    
    @classmethod
    def from_exception(cls, exception: Exception, context: str = None):
        """Create an error response from an exception."""
        message = f"{context}: {str(exception)}" if context else str(exception)
        return {
            'status': 'error',
            'message': message,
            'error_code': 'EXCEPTION_ERROR'
        }


# Convenience functions for common response patterns

def success_response(message: str = "Operation successful", data: Any = None) -> ResponseBuilder:
    """Create a success response."""
    return ResponseBuilder().success(message, data)


def error_response(message: str, error_code: str = None, data: Any = None) -> ResponseBuilder:
    """Create an error response."""
    return ResponseBuilder().error(message, error_code, data)


def warning_response(message: str, data: Any = None) -> ResponseBuilder:
    """Create a warning response."""
    return ResponseBuilder().warning(message, data)


def validation_error_response(errors: List[str]) -> ResponseBuilder:
    """Create a validation error response."""
    builder = ResponseBuilder().error("Validation failed")
    for error in errors:
        builder.add_error(error)
    return builder


def data_response(data: Any, message: str = "Data retrieved successfully") -> ResponseBuilder:
    """Create a response with data."""
    return ResponseBuilder().success(message, data)


def analysis_response(results: Dict[str, Any], status: str = "success") -> ResponseBuilder:
    """Create a response for analysis results."""
    if status == "success":
        return (ResponseBuilder()
                .success("Analysis completed successfully")
                .add_data("results", results)
                .add_metadata("analysis_type", results.get("analysis_type", "unknown"))
                .add_metadata("timestamp", results.get("timestamp")))
    else:
        return (ResponseBuilder()
                .error("Analysis failed")
                .add_data("partial_results", results))


def visualization_response(visualizations: List[Dict[str, Any]], 
                         message: str = "Visualizations created successfully") -> ResponseBuilder:
    """Create a response for visualization results."""
    return (ResponseBuilder()
            .success(message)
            .add_data("visualizations", visualizations)
            .add_metadata("count", len(visualizations)))


def file_upload_response(file_info: Dict[str, Any], 
                        message: str = "File uploaded successfully") -> ResponseBuilder:
    """Create a response for file upload results."""
    return (ResponseBuilder()
            .success(message)
            .add_data("file_info", file_info)
            .add_metadata("file_size", file_info.get("size"))
            .add_metadata("file_type", file_info.get("type")))


def session_response(session_data: Dict[str, Any], 
                    message: str = "Session data retrieved") -> ResponseBuilder:
    """Create a response for session data."""
    return (ResponseBuilder()
            .success(message)
            .add_data("session", session_data)
            .add_metadata("session_id", session_data.get("session_id")))


class APIResponse:
    """
    Static class for creating standardized API responses.
    
    Provides simple methods for common response patterns.
    """
    
    @staticmethod
    def success(message: str = "Success", data: Any = None, 
                status_code: int = 200) -> tuple:
        """Create a success response."""
        response = {
            'status': 'success',
            'message': message,
            'data': convert_to_json_serializable(data) if data is not None else None
        }
        return jsonify(response), status_code
    
    @staticmethod
    def error(message: str, status_code: int = 400, error_code: str = None) -> tuple:
        """Create an error response."""
        response = {
            'status': 'error',
            'message': message
        }
        if error_code:
            response['error_code'] = error_code
        
        return jsonify(response), status_code
    
    @staticmethod
    def not_found(message: str = "Resource not found") -> tuple:
        """Create a 404 not found response."""
        return APIResponse.error(message, 404, "NOT_FOUND")
    
    @staticmethod
    def unauthorized(message: str = "Unauthorized access") -> tuple:
        """Create a 401 unauthorized response."""
        return APIResponse.error(message, 401, "UNAUTHORIZED")
    
    @staticmethod
    def forbidden(message: str = "Access forbidden") -> tuple:
        """Create a 403 forbidden response."""
        return APIResponse.error(message, 403, "FORBIDDEN")
    
    @staticmethod
    def validation_error(errors: List[str]) -> tuple:
        """Create a validation error response."""
        response = {
            'status': 'error',
            'message': 'Validation failed',
            'errors': errors
        }
        return jsonify(response), 400
    
    @staticmethod
    def server_error(message: str = "Internal server error") -> tuple:
        """Create a 500 server error response."""
        return APIResponse.error(message, 500, "INTERNAL_ERROR")


# Legacy function for backward compatibility
def create_response(status: str, message: str, data: Any = None, 
                   errors: List[str] = None) -> Dict[str, Any]:
    """
    Create a standardized response dictionary.
    
    Args:
        status: Response status ('success', 'error', 'warning')
        message: Response message
        data: Response data
        errors: List of error messages
        
    Returns:
        Standardized response dictionary
    """
    response = {
        'status': status,
        'message': message
    }
    
    if data is not None:
        response['data'] = convert_to_json_serializable(data)
    
    if errors:
        response['errors'] = errors
    
    return response 