"""
Core exceptions for ChatMRPT application.
"""


class ChatMRPTException(Exception):
    """Base exception for ChatMRPT application."""
    pass


class ValidationError(ChatMRPTException):
    """Raised when validation fails."""
    pass


class ConfigurationError(ChatMRPTException):
    """Raised when configuration is invalid."""
    pass


class DataProcessingError(ChatMRPTException):
    """Raised when data processing operations fail."""
    pass


class DataError(ChatMRPTException):
    """Raised when data operations fail."""
    pass


class AnalysisError(ChatMRPTException):
    """Raised when analysis operations fail."""
    pass


class VisualizationError(ChatMRPTException):
    """Raised when visualization operations fail."""
    pass


class LLMError(ChatMRPTException):
    """Raised when LLM operations fail."""
    pass


class SessionError(ChatMRPTException):
    """Raised when session operations fail."""
    pass


class ExternalServiceError(ChatMRPTException):
    """Raised when external service operations fail."""
    pass 