"""
Migration utilities for transitioning to SimpleRequestInterpreter

This module provides utilities to gradually migrate from the complex
RequestInterpreter to the simplified version.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def should_use_simple_interpreter() -> bool:
    """
    Determine whether to use the simple interpreter.
    
    Can be controlled by:
    1. Environment variable
    2. Configuration setting
    3. Feature flag
    
    Returns:
        True to use SimpleRequestInterpreter, False for legacy
    """
    # Check environment variable
    use_simple = os.environ.get('USE_SIMPLE_INTERPRETER', '').lower()
    if use_simple in ('true', '1', 'yes'):
        logger.info("Using SimpleRequestInterpreter (env var)")
        return True
    
    # Check Flask config
    try:
        from flask import current_app
        if current_app.config.get('USE_SIMPLE_INTERPRETER', False):
            logger.info("Using SimpleRequestInterpreter (Flask config)")
            return True
    except:
        pass
    
    # Default to legacy for now
    return False


def create_request_interpreter(llm_manager, data_service, analysis_service, visualization_service):
    """
    Factory function to create appropriate interpreter based on configuration.
    
    Args:
        llm_manager: LLM manager instance
        data_service: Data service instance
        analysis_service: Analysis service instance
        visualization_service: Visualization service instance
        
    Returns:
        Either SimpleRequestInterpreter or RequestInterpreter instance
    """
    if should_use_simple_interpreter():
        from .simple_request_interpreter import SimpleRequestInterpreter
        logger.info("Creating SimpleRequestInterpreter")
        return SimpleRequestInterpreter(llm_manager)
    else:
        from .request_interpreter import RequestInterpreter
        logger.info("Creating legacy RequestInterpreter")
        return RequestInterpreter(
            llm_manager, 
            data_service, 
            analysis_service, 
            visualization_service
        )


class InterpreterAdapter:
    """
    Adapter that wraps SimpleRequestInterpreter to match RequestInterpreter interface.
    
    This allows gradual migration without changing all calling code.
    """
    
    def __init__(self, simple_interpreter):
        self.interpreter = simple_interpreter
        
        # Map old method names to new ones
        self.process_user_message = self.interpreter.process_message
        self.process_user_message_streaming = self.interpreter.process_message_streaming
    
    def __getattr__(self, name):
        """Forward any other attribute access to the wrapped interpreter."""
        return getattr(self.interpreter, name)


def migrate_to_simple_interpreter(container):
    """
    Migrate a service container to use SimpleRequestInterpreter.
    
    Args:
        container: ServiceContainer instance
    """
    try:
        # Get existing services
        llm_manager = container.get('llm_manager')
        
        # Create simple interpreter
        from .simple_request_interpreter import SimpleRequestInterpreter
        simple_interpreter = SimpleRequestInterpreter(llm_manager)
        
        # Wrap in adapter for compatibility
        adapted_interpreter = InterpreterAdapter(simple_interpreter)
        
        # Replace in container
        container._singletons['request_interpreter'] = adapted_interpreter
        
        logger.info("Successfully migrated to SimpleRequestInterpreter")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False