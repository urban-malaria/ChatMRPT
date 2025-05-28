"""
Custom exception classes for ChatMRPT application.

This module defines application-specific exceptions that provide better
error handling and more informative error messages.
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ChatMRPTException(Exception):
    """Base exception class for all ChatMRPT-specific errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, 
                 status_code: int = 500):
        """
        Initialize the exception.
        
        Args:
            message: Human-readable error message
            details: Additional error details for debugging
            status_code: HTTP status code for API responses
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.status_code = status_code
        
        # Log the error
        logger.error(f"{self.__class__.__name__}: {message}", extra={"details": details})
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON responses."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
            "status_code": self.status_code
        }


class ValidationError(ChatMRPTException):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, 
                 validation_errors: Optional[Dict[str, str]] = None):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            field: Field that failed validation
            validation_errors: Dictionary of field validation errors
        """
        details = {"field": field, "validation_errors": validation_errors}
        super().__init__(message, details, status_code=400)
        self.field = field
        self.validation_errors = validation_errors or {}


class DataProcessingError(ChatMRPTException):
    """Raised when data processing operations fail."""
    
    def __init__(self, message: str, operation: Optional[str] = None, 
                 data_type: Optional[str] = None):
        """
        Initialize data processing error.
        
        Args:
            message: Error message
            operation: The operation that failed (e.g., 'load', 'clean', 'normalize')
            data_type: Type of data being processed (e.g., 'csv', 'shapefile')
        """
        details = {"operation": operation, "data_type": data_type}
        super().__init__(message, details, status_code=422)
        self.operation = operation
        self.data_type = data_type


class AnalysisError(ChatMRPTException):
    """Raised when analysis operations fail."""
    
    def __init__(self, message: str, analysis_type: Optional[str] = None, 
                 variables: Optional[list] = None):
        """
        Initialize analysis error.
        
        Args:
            message: Error message
            analysis_type: Type of analysis (e.g., 'standard', 'custom')
            variables: Variables involved in the analysis
        """
        details = {"analysis_type": analysis_type, "variables": variables}
        super().__init__(message, details, status_code=422)
        self.analysis_type = analysis_type
        self.variables = variables or []


class VisualizationError(ChatMRPTException):
    """Raised when visualization generation fails."""
    
    def __init__(self, message: str, viz_type: Optional[str] = None, 
                 data_summary: Optional[Dict] = None):
        """
        Initialize visualization error.
        
        Args:
            message: Error message
            viz_type: Type of visualization (e.g., 'map', 'chart')
            data_summary: Summary of data being visualized
        """
        details = {"viz_type": viz_type, "data_summary": data_summary}
        super().__init__(message, details, status_code=422)
        self.viz_type = viz_type


class ExternalServiceError(ChatMRPTException):
    """Raised when external service calls fail."""
    
    def __init__(self, message: str, service: Optional[str] = None, 
                 response_code: Optional[int] = None):
        """
        Initialize external service error.
        
        Args:
            message: Error message
            service: Name of the external service (e.g., 'openai', 'geocoding')
            response_code: HTTP response code from the service
        """
        details = {"service": service, "response_code": response_code}
        super().__init__(message, details, status_code=502)
        self.service = service
        self.response_code = response_code


class ConfigurationError(ChatMRPTException):
    """Raised when configuration is invalid or missing."""
    
    def __init__(self, message: str, config_key: Optional[str] = None):
        """
        Initialize configuration error.
        
        Args:
            message: Error message
            config_key: Configuration key that is missing or invalid
        """
        details = {"config_key": config_key}
        super().__init__(message, details, status_code=500)
        self.config_key = config_key


class SessionError(ChatMRPTException):
    """Raised when session-related operations fail."""
    
    def __init__(self, message: str, session_id: Optional[str] = None):
        """
        Initialize session error.
        
        Args:
            message: Error message
            session_id: ID of the problematic session
        """
        details = {"session_id": session_id}
        super().__init__(message, details, status_code=400)
        self.session_id = session_id


class FileOperationError(ChatMRPTException):
    """Raised when file operations fail."""
    
    def __init__(self, message: str, file_path: Optional[str] = None, 
                 operation: Optional[str] = None):
        """
        Initialize file operation error.
        
        Args:
            message: Error message
            file_path: Path to the problematic file
            operation: File operation that failed (e.g., 'read', 'write', 'delete')
        """
        details = {"file_path": file_path, "operation": operation}
        super().__init__(message, details, status_code=500)
        self.file_path = file_path
        self.operation = operation


class RateLimitError(ChatMRPTException):
    """Raised when rate limits are exceeded."""
    
    def __init__(self, message: str = "Rate limit exceeded", 
                 retry_after: Optional[int] = None):
        """
        Initialize rate limit error.
        
        Args:
            message: Error message
            retry_after: Seconds to wait before retrying
        """
        details = {"retry_after": retry_after}
        super().__init__(message, details, status_code=429)
        self.retry_after = retry_after 