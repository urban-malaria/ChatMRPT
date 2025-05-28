"""
Modern service container with dependency injection for ChatMRPT application.

This container provides a clean way to manage service dependencies and ensures
proper initialization order and lifecycle management.
"""

import logging
from typing import Dict, Any, Optional, Type, TypeVar, Generic
from flask import Flask

from ..core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceContainer:
    """
    Modern dependency injection container for managing application services.
    
    This container supports:
    - Lazy initialization
    - Singleton pattern for services
    - Dependency resolution
    - Configuration injection
    - Service lifecycle management
    """
    
    def __init__(self, app: Optional[Flask] = None):
        """
        Initialize the service container.
        
        Args:
            app: Flask application instance
        """
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, callable] = {}
        self._singletons: Dict[str, Any] = {}
        self._app = app
        
        if app:
            self.init_app(app)
    
    def init_app(self, app: Flask) -> None:
        """
        Initialize the container with Flask app.
        
        Args:
            app: Flask application instance
        """
        self._app = app
        app.services = self
        
        # Register core services
        self._register_core_services()
        
        logger.info("Service container initialized with Flask app")
    
    def register(self, name: str, factory: callable, singleton: bool = True) -> None:
        """
        Register a service factory.
        
        Args:
            name: Service name
            factory: Factory function to create the service
            singleton: Whether to treat as singleton
        """
        self._factories[name] = factory
        
        if singleton:
            self._singletons[name] = None
        
        logger.debug(f"Registered service: {name} (singleton: {singleton})")
    
    def get(self, name: str) -> Any:
        """
        Get a service instance.
        
        Args:
            name: Service name
            
        Returns:
            Service instance
            
        Raises:
            ConfigurationError: If service is not registered
        """
        # Check if it's a singleton and already created
        if name in self._singletons:
            if self._singletons[name] is None:
                # Create singleton instance
                self._singletons[name] = self._create_service(name)
            return self._singletons[name]
        
        # Create new instance for non-singletons
        return self._create_service(name)
    
    def _create_service(self, name: str) -> Any:
        """
        Create a service instance using its factory.
        
        Args:
            name: Service name
            
        Returns:
            Service instance
        """
        if name not in self._factories:
            raise ConfigurationError(f"Service '{name}' is not registered")
        
        factory = self._factories[name]
        
        try:
            # Call factory with container for dependency injection
            service = factory(self)
            logger.debug(f"Created service instance: {name}")
            return service
        except Exception as e:
            logger.error(f"Failed to create service '{name}': {str(e)}")
            raise ConfigurationError(f"Failed to create service '{name}': {str(e)}")
    
    def _register_core_services(self) -> None:
        """Register core application services."""
        
        # Interaction Logger
        self.register('interaction_logger', self._create_interaction_logger)
        
        # LLM Manager
        self.register('llm_manager', self._create_llm_manager)
        
        # Data Services
        self.register('data_service', self._create_data_service)
        self.register('analysis_service', self._create_analysis_service)
        self.register('visualization_service', self._create_visualization_service)
        
        # AI Services
        self.register('ai_service', self._create_ai_service)
        self.register('nlu_service', self._create_nlu_service)
        
        # Report Services
        self.register('report_service', self._create_report_service)
        
        # Message Service (depends on other services)
        self.register('message_service', self._create_message_service)
    
    def _create_interaction_logger(self, container: 'ServiceContainer'):
        """Create interaction logger service."""
        try:
            from ..models.interaction_logger import InteractionLogger
            
            db_path = self._app.config.get('INTERACTIONS_DB_FILE')
            return InteractionLogger(db_path=db_path)
        except Exception as e:
            logger.warning(f"Failed to create interaction logger: {e}")
            return None
    
    def _create_llm_manager(self, container: 'ServiceContainer'):
        """Create LLM manager service."""
        try:
            from ..core.llm_manager import LLMManager
            
            interaction_logger = container.get('interaction_logger')
            
            return LLMManager(
                api_key=self._app.config.get('OPENAI_API_KEY'),
                model=self._app.config.get('OPENAI_MODEL_NAME', 'gpt-4o'),
                interaction_logger=interaction_logger
            )
        except Exception as e:
            logger.warning(f"Failed to create LLM manager: {e}")
            return None
    
    def _create_data_service(self, container: 'ServiceContainer'):
        """Create data service."""
        try:
            from .data.handler import DataService
            
            return DataService(
                upload_folder=self._app.config.get('UPLOAD_FOLDER'),
                interaction_logger=container.get('interaction_logger')
            )
        except ImportError as e:
            logger.warning(f"Data service not available: {e}")
            return None
    
    def _create_analysis_service(self, container: 'ServiceContainer'):
        """Create analysis service."""
        try:
            from .analysis.engine import AnalysisService
            
            return AnalysisService(
                llm_manager=container.get('llm_manager'),
                interaction_logger=container.get('interaction_logger')
            )
        except ImportError as e:
            logger.warning(f"Analysis service not available: {e}")
            return None
    
    def _create_visualization_service(self, container: 'ServiceContainer'):
        """Create visualization service."""
        try:
            from .visualization.chart_service import VisualizationService
            
            return VisualizationService(
                llm_manager=container.get('llm_manager'),
                interaction_logger=container.get('interaction_logger')
            )
        except ImportError as e:
            logger.warning(f"Visualization service not available: {e}")
            return None
    
    def _create_ai_service(self, container: 'ServiceContainer'):
        """Create AI service."""
        try:
            from .ai.llm_service import AIService
            
            return AIService(
                llm_manager=container.get('llm_manager'),
                interaction_logger=container.get('interaction_logger')
            )
        except ImportError as e:
            logger.warning(f"AI service not available: {e}")
            return None
    
    def _create_nlu_service(self, container: 'ServiceContainer'):
        """Create NLU service."""
        try:
            from .ai.nlu_service import NLUService
            
            return NLUService(
                llm_manager=container.get('llm_manager')
            )
        except ImportError as e:
            logger.warning(f"NLU service not available: {e}")
            return None
    
    def _create_report_service(self, container: 'ServiceContainer'):
        """Create report service."""
        try:
            from .reports.generator import ReportService
            
            return ReportService(
                reports_folder=self._app.config.get('REPORTS_FOLDER'),
                llm_manager=container.get('llm_manager')
            )
        except ImportError as e:
            logger.warning(f"Report service not available: {e}")
            return None
    
    def _create_message_service(self, container: 'ServiceContainer'):
        """Create message service (depends on other services)."""
        try:
            from .message_service import MessageService
            
            return MessageService(
                llm_manager=container.get('llm_manager'),
                interaction_logger=container.get('interaction_logger'),
                analysis_service=container.get('analysis_service')
            )
        except ImportError as e:
            logger.warning(f"Message service not available: {e}")
            return None
    
    @property
    def interaction_logger(self):
        """Get interaction logger service."""
        return self.get('interaction_logger')
    
    @property
    def llm_manager(self):
        """Get LLM manager service."""
        return self.get('llm_manager')
    
    @property
    def data_service(self):
        """Get data service."""
        return self.get('data_service')
    
    @property
    def analysis_service(self):
        """Get analysis service."""
        return self.get('analysis_service')
    
    @property
    def visualization_service(self):
        """Get visualization service."""
        return self.get('visualization_service')
    
    @property
    def ai_service(self):
        """Get AI service."""
        return self.get('ai_service')
    
    @property
    def nlu_service(self):
        """Get NLU service."""
        return self.get('nlu_service')
    
    @property
    def report_service(self):
        """Get report service."""
        return self.get('report_service')
    
    @property
    def message_service(self):
        """Get message service."""
        return self.get('message_service')
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of all registered services.
        
        Returns:
            Dictionary with service health status
        """
        health_status = {
            "container": "healthy",
            "services": {}
        }
        
        for service_name in self._factories.keys():
            try:
                service = self.get(service_name)
                if service is None:
                    health_status["services"][service_name] = "unavailable"
                elif hasattr(service, 'health_check'):
                    health_status["services"][service_name] = service.health_check()
                else:
                    health_status["services"][service_name] = "healthy"
            except Exception as e:
                health_status["services"][service_name] = f"error: {str(e)}"
        
        # Overall health
        unhealthy_services = [
            name for name, status in health_status["services"].items()
            if status not in ["healthy", "unavailable"]
        ]
        
        if unhealthy_services:
            health_status["container"] = "degraded"
            health_status["issues"] = unhealthy_services
        
        return health_status


def init_services(app: Flask) -> ServiceContainer:
    """
    Initialize and configure the service container for the Flask app.
    
    Args:
        app: Flask application instance
        
    Returns:
        Configured service container
    """
    container = ServiceContainer(app)
    
    # Add container to app context for templates
    @app.context_processor
    def inject_services():
        return {'services': container}
    
    # Add health check endpoint
    @app.route('/health')
    def health_check():
        return container.health_check()
    
    logger.info("Service container initialized and configured")
    return container 