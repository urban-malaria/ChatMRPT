"""
Core utilities and shared components for ChatMRPT application.

This module provides common utilities, exceptions, decorators, and middleware
that are used throughout the application.
"""

from .exceptions import (
    ChatMRPTException,
    DataProcessingError,
    AnalysisError,
    ValidationError,
    ConfigurationError,
    ExternalServiceError
)

from .decorators import (
    validate_session,
    handle_errors,
    log_execution_time,
    rate_limit
)

# Import new state management modules
from .session_state import (
    SessionState,
    WorkflowStage,
    DataState,
    AnalysisState,
    UserCapabilities,
    FileInfo,
    DataSummary,
    AnalysisResults,
    is_data_ready_for_analysis,
    is_analysis_complete,
    can_create_visualizations,
    get_workflow_progress_percentage
)

# Core utilities ready for use

__all__ = [
    'ChatMRPTException',
    'DataProcessingError', 
    'AnalysisError',
    'ValidationError',
    'ConfigurationError',
    'ExternalServiceError',
    'validate_session',
    'handle_errors',
    'log_execution_time',
    'rate_limit',
    'SessionState',
    'WorkflowStage',
    'DataState', 
    'AnalysisState',
    'UserCapabilities',
    'FileInfo',
    'DataSummary',
    'AnalysisResults',
    'is_data_ready_for_analysis',
    'is_analysis_complete',
    'can_create_visualizations',
    'get_workflow_progress_percentage'
] 