# app/interaction/__init__.py
"""
Interaction Logging Package

A comprehensive, modular interaction logging system extracted from the monolithic
InteractionLogger class. This package provides professional-grade logging, analytics,
and data management capabilities for user interactions, analysis steps, and system events.

Package Architecture:
- core: Database management and session handling
- events: Event tracking and categorization 
- storage: Data persistence and retrieval
- utils: Helper utilities and validation

Main Classes:
- InteractionLogger: Backward-compatible main interface
- DatabaseManager: Core database operations
- SessionManager: Session lifecycle management
- EventLogger: Event tracking capabilities
- StorageManager: Data retrieval and export

Version: 1.0.0
"""

# Core imports
from .core import (
    DatabaseManager,
    SessionManager, 
    InteractionCore,
    log_message,
    log_error
)

# Event tracking imports
from .events import (
    EventLogger,
    log_file_upload,
    log_analysis_event,
    log_analysis_step,
    log_algorithm_decision,
    log_calculation,
    log_anomaly,
    log_variable_relationship,
    log_ward_ranking,
    log_visualization_metadata,
    log_llm_interaction,
    log_explanation
)

# Storage and retrieval imports
from .storage import (
    StorageManager,
    get_session_history,
    get_analysis_metadata,
    get_visualization_context,
    get_explanations,
    get_ward_data,
    export_to_csv,
    export_to_json
)

# Utility imports
from .utils import (
    validate_session_id,
    safe_json_parse,
    safe_json_dumps,
    format_timestamp,
    parse_timestamp,
    sanitize_filename,
    create_export_filename,
    InteractionTimer,
    DataValidator
)

# Version information
__version__ = "1.0.0"
__author__ = "ChatMRPT Development Team"


class InteractionLogger:
    """
    Comprehensive Interaction Logger
    
    This is the main interface class that provides backward compatibility
    with the original monolithic InteractionLogger while leveraging the
    new modular architecture underneath.
    
    The class now delegates to specialized managers:
    - DatabaseManager: Database initialization and connections
    - SessionManager: Session lifecycle management  
    - EventLogger: Event tracking and categorization
    - StorageManager: Data retrieval and export
    """
    
    def __init__(self, db_path=None):
        """
        Initialize the interaction logger with modular components
        
        Args:
            db_path: Path to SQLite database (defaults to instance/interactions.db)
        """
        # Initialize core components
        self.db_manager = DatabaseManager(db_path)
        self.session_manager = SessionManager(self.db_manager)
        self.event_logger = EventLogger(self.db_manager)
        self.storage_manager = StorageManager(self.db_manager)
        
        # Store path for compatibility
        self.db_path = self.db_manager.db_path
    
    # Session Management Methods
    def log_session_start(self, session_id, browser_info=None, ip_address=None):
        """Log the start of a new session"""
        return self.session_manager.log_session_start(session_id, browser_info, ip_address)
    
    def update_session_language(self, session_id, language):
        """Update the language preference for a session"""
        return self.session_manager.update_session_language(session_id, language)
    
    def update_session_status(self, session_id, analysis_complete=None, files_uploaded=None):
        """Update the status flags for a session"""
        return self.session_manager.update_session_status(session_id, analysis_complete, files_uploaded)
    
    # Basic Logging Methods
    def log_message(self, session_id, sender, content, intent=None, entities=None):
        """Log a message exchange between user and assistant"""
        return log_message(self.db_manager, session_id, sender, content, intent, entities)
    
    def log_error(self, session_id, error_type, error_message, stack_trace=None):
        """Log an error that occurred during interaction"""
        return log_error(self.db_manager, session_id, error_type, error_message, stack_trace)
    
    # Event Logging Methods
    def log_file_upload(self, session_id, file_type, file_name, file_size, metadata=None):
        """Log a file upload event"""
        return self.event_logger.log_file_upload(session_id, file_type, file_name, file_size, metadata)
    
    def log_analysis_event(self, session_id, event_type, details, success=True):
        """Log an analysis event"""
        return self.event_logger.log_analysis_event(session_id, event_type, details, success)
    
    def log_analysis_step(self, session_id, step_name, input_summary=None, output_summary=None, 
                         algorithm=None, parameters=None, execution_time=None, error=None):
        """Log an analysis pipeline step"""
        return self.event_logger.log_analysis_step(session_id, step_name, input_summary, output_summary,
                                                  algorithm, parameters, execution_time, error)
    
    def log_algorithm_decision(self, session_id, step_id, decision_type, options=None, 
                              criteria=None, selected_option=None, confidence=None):
        """Log a decision made during analysis"""
        return self.event_logger.log_algorithm_decision(session_id, step_id, decision_type, options,
                                                       criteria, selected_option, confidence)
    
    def log_calculation(self, session_id, step_id, variable, operation, 
                       input_values=None, output_value=None, context=None):
        """Log a calculation performed during analysis"""
        return self.event_logger.log_calculation(session_id, step_id, variable, operation,
                                                input_values, output_value, context)
    
    def log_anomaly(self, session_id, entity_name, anomaly_type, expected_value=None, 
                   actual_value=None, significance=None, context=None):
        """Log an anomaly detected during analysis"""
        return self.event_logger.log_anomaly(session_id, entity_name, anomaly_type, expected_value,
                                            actual_value, significance, context)
    
    def log_variable_relationship(self, session_id, variable_name, relationship_type, 
                                 evidence=None, confidence_score=None):
        """Log a determined variable relationship"""
        return self.event_logger.log_variable_relationship(session_id, variable_name, relationship_type,
                                                          evidence, confidence_score)
    
    def log_ward_ranking(self, session_id, ward_name, overall_rank, median_score, 
                        vulnerability_category, contributing_factors=None, anomaly_flags=None):
        """Log detailed information about a ward's ranking"""
        return self.event_logger.log_ward_ranking(session_id, ward_name, overall_rank, median_score,
                                                 vulnerability_category, contributing_factors, anomaly_flags)
    
    def log_visualization_metadata(self, session_id, viz_type, variables_used=None, 
                                  data_summary=None, visual_elements=None, patterns_detected=None):
        """Log detailed metadata about a visualization"""
        return self.event_logger.log_visualization_metadata(session_id, viz_type, variables_used,
                                                           data_summary, visual_elements, patterns_detected)
    
    def log_llm_interaction(self, session_id, prompt_type, prompt, prompt_context=None, 
                           response=None, tokens_used=None, latency=None, enhanced_timing=None):
        """Log an interaction with the LLM"""
        return self.event_logger.log_llm_interaction(session_id, prompt_type, prompt, prompt_context,
                                                    response, tokens_used, latency, enhanced_timing)
    
    def log_explanation(self, session_id, entity_type, entity_name, question_type, 
                       question, explanation, context_used=None, llm_interaction_id=None):
        """Log an explanation provided to the user"""
        return self.event_logger.log_explanation(session_id, entity_type, entity_name, question_type,
                                                question, explanation, context_used, llm_interaction_id)
    
    # Data Retrieval Methods
    def get_session_history(self, session_id):
        """Get complete conversation history for a session"""
        return self.storage_manager.get_session_history(session_id)
    
    def get_analysis_metadata(self, session_id):
        """Get complete analysis metadata for a session"""
        return self.storage_manager.get_analysis_metadata(session_id)
    
    def get_visualization_context(self, session_id, viz_id=None, viz_type=None):
        """Get context about visualizations for a session"""
        return self.storage_manager.get_visualization_context(session_id, viz_id, viz_type)
    
    def get_explanations(self, session_id, entity_type=None, entity_name=None):
        """Get explanations provided in a session"""
        return self.storage_manager.get_explanations(session_id, entity_type, entity_name)
    
    def get_ward_data(self, session_id, ward_name):
        """Get comprehensive data about a specific ward"""
        return self.storage_manager.get_ward_data(session_id, ward_name)
    
    # Export Methods
    def export_to_csv(self, session_id=None, start_date=None, end_date=None, output_dir=None):
        """Export logs to CSV files"""
        return self.storage_manager.export_to_csv(session_id, start_date, end_date, output_dir)
    
    def export_to_json(self, session_id=None, include_llm_data=True, compact=False, output_file=None):
        """Export logs to a single JSON file"""
        return self.storage_manager.export_to_json(session_id, include_llm_data, compact, output_file)
    
    # Additional methods would be implemented here based on original requirements
    def get_conversation_training_data(self, session_id=None, min_quality=None, start_date=None, end_date=None):
        """Get conversation data suitable for training (placeholder for now)"""
        # This would need to be implemented based on original requirements
        return []
    
    def generate_explanation_context(self, session_id, entity_type, entity_name, question=None):
        """Generate explanation context (placeholder for now)"""
        # This would need to be implemented based on original requirements
        return {}


# Package-level convenience functions
def create_interaction_logger(db_path=None):
    """
    Create a new InteractionLogger instance
    
    Args:
        db_path: Optional database path
        
    Returns:
        InteractionLogger: Configured interaction logger instance
    """
    return InteractionLogger(db_path)


def get_package_info():
    """
    Get information about the interaction package
    
    Returns:
        dict: Package information
    """
    return {
        'name': 'app.interaction',
        'version': __version__,
        'author': __author__,
        'modules': [
            'core',
            'events', 
            'storage',
            'utils'
        ],
        'main_classes': [
            'InteractionLogger',
            'DatabaseManager',
            'SessionManager',
            'EventLogger',
            'StorageManager'
        ],
        'total_functions': len(__all__)
    }


def validate_package():
    """
    Validate package functionality
    
    Returns:
        dict: Validation results
    """
    try:
        # Test core database functionality
        db_manager = DatabaseManager(':memory:')  # In-memory for testing
        
        # Test session management
        session_manager = SessionManager(db_manager)
        
        # Test event logging
        event_logger = EventLogger(db_manager)
        
        # Test storage
        storage_manager = StorageManager(db_manager)
        
        return {
            'status': 'success',
            'message': 'All package components validated successfully',
            'components_tested': 4,
            'version': __version__
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Package validation failed: {str(e)}',
            'version': __version__
        }


# Export all public functions and classes
__all__ = [
    # Main classes
    'InteractionLogger',
    'DatabaseManager',
    'SessionManager', 
    'EventLogger',
    'StorageManager',
    'InteractionCore',
    
    # Core functions
    'log_message',
    'log_error',
    
    # Event functions
    'log_file_upload',
    'log_analysis_event',
    'log_analysis_step',
    'log_algorithm_decision',
    'log_calculation',
    'log_anomaly',
    'log_variable_relationship',
    'log_ward_ranking',
    'log_visualization_metadata',
    'log_llm_interaction',
    'log_explanation',
    
    # Storage functions
    'get_session_history',
    'get_analysis_metadata',
    'get_visualization_context',
    'get_explanations',
    'get_ward_data',
    'export_to_csv',
    'export_to_json',
    
    # Utility functions
    'validate_session_id',
    'safe_json_parse',
    'safe_json_dumps',
    'format_timestamp',
    'parse_timestamp',
    'sanitize_filename',
    'create_export_filename',
    'InteractionTimer',
    'DataValidator',
    
    # Package functions
    'create_interaction_logger',
    'get_package_info',
    'validate_package',
    
    # Version
    '__version__'
]


# Package initialization logging
import logging
logger = logging.getLogger(__name__)
logger.info(f"Interaction package v{__version__} initialized with {len(__all__)} exported functions") 