"""
Service container for dependency injection.

This module provides a container for all application services,
allowing for cleaner dependency management.
"""
import logging
from flask import current_app

from .analysis.engine import AnalysisService
from .message_service import MessageService
from .visualization.chart_service import VisualizationService

logger = logging.getLogger(__name__)

class ServiceContainer:
    """
    Container for application services.
    
    This class initializes and provides access to all service instances,
    managing their dependencies and lifecycle.
    """
    
    def __init__(self, app_config=None):
        """
        Initialize the service container with application config.
        
        Args:
            app_config: Flask application config
        """
        self.app_config = app_config
        self._llm_manager = None
        self._interaction_logger = None
        self._analysis_service = None
        self._message_service = None
        self._visualization_service = None
        
        # Initialize core services
        self._init_core_services()
        
        # Initialize business services
        self._init_business_services()
        
        logger.info("Service container initialized")
    
    def _init_core_services(self):
        """Initialize core service dependencies"""
        # Get LLM Manager from app config if available
        if self.app_config and 'LLM_MANAGER' in self.app_config:
            self._llm_manager = self.app_config.get('LLM_MANAGER')
            logger.info("Using LLM Manager from app config")
        else:
            # Try to initialize LLM Manager
            try:
                from ..core.llm_manager import get_llm_manager
                self._llm_manager = get_llm_manager()
                logger.info("Initialized new LLM Manager")
            except Exception as e:
                logger.error(f"Failed to initialize LLM Manager: {str(e)}")
                self._llm_manager = None
        
        # Get Interaction Logger from app config if available
        if self.app_config and 'INTERACTION_LOGGER' in self.app_config:
            self._interaction_logger = self.app_config.get('INTERACTION_LOGGER')
            logger.info("Using Interaction Logger from app config")
        else:
            # Try to initialize Interaction Logger
            try:
                from ..models.interaction_logger import InteractionLogger
                db_path = self.app_config.get('INTERACTIONS_DB_FILE') if self.app_config else None
                self._interaction_logger = InteractionLogger(db_path=db_path)
                logger.info("Initialized new Interaction Logger")
            except Exception as e:
                logger.error(f"Failed to initialize Interaction Logger: {str(e)}")
                self._interaction_logger = None
    
    def _init_business_services(self):
        """Initialize business services with their dependencies"""
        # Initialize Analysis Service
        self._analysis_service = AnalysisService(
            llm_manager=self._llm_manager,
            interaction_logger=self._interaction_logger
        )
        logger.info("Initialized Analysis Service")
        
        # Initialize Visualization Service
        self._visualization_service = VisualizationService(
            llm_manager=self._llm_manager,
            interaction_logger=self._interaction_logger
        )
        logger.info("Initialized Visualization Service")
        
        # Initialize Message Service with other services as dependencies
        self._message_service = MessageService(
            llm_manager=self._llm_manager,
            interaction_logger=self._interaction_logger,
            analysis_service=self._analysis_service
        )
        logger.info("Initialized Message Service")
    
    @property
    def llm_manager(self):
        """Get LLM Manager instance"""
        return self._llm_manager
    
    @property
    def interaction_logger(self):
        """Get Interaction Logger instance"""
        return self._interaction_logger
    
    @property
    def analysis_service(self):
        """Get Analysis Service instance"""
        return self._analysis_service
    
    @property
    def message_service(self):
        """Get Message Service instance"""
        return self._message_service
    
    @property
    def visualization_service(self):
        """Get Visualization Service instance"""
        return self._visualization_service


def init_services(app):
    """
    Initialize services for the Flask application.
    
    Args:
        app: Flask application instance
    """
    # Create service container
    app.services = ServiceContainer(app.config)
    
    # Add service container to app context
    @app.context_processor
    def inject_services():
        return {
            'services': app.services
        }
    
    logger.info("Services initialized for Flask application") 