"""
Simple service container for core ChatMRPT functionality.

This container provides basic dependency injection for the core services
that work well: data handling, analysis, visualization, and reporting.
"""

import logging
from typing import Dict, Any, Optional, List
from flask import Flask

from ..core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ServiceContainer:
    """
    Simple dependency injection container for core ChatMRPT services.
    
    Manages only the proven, working services:
    - Data handling (unified dataset processing)
    - Analysis engine (composite scoring, PCA)
    - Visualization service (6 core visualizations)
    - Report generation
    """
    
    def __init__(self, app: Optional[Flask] = None):
        """Initialize the service container."""
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, callable] = {}
        self._singletons: Dict[str, Any] = {}
        self._app = app
        
        if app:
            self.init_app(app)
    
    def init_app(self, app: Flask) -> None:
        """Initialize the container with Flask app."""
        self._app = app
        app.services = self
        
        # Register core services only
        self._register_core_services()
        
        # FIXED: Eager initialization to prevent 18-second first message delay
        self._eager_init_core_services()
        
        logger.info("Service container initialized with core services")
    
    def register(self, name: str, factory: callable, singleton: bool = True) -> None:
        """Register a service factory."""
        self._factories[name] = factory
        
        if singleton:
            self._singletons[name] = None
        
        logger.debug(f"Registered service: {name} (singleton: {singleton})")
    
    def get(self, name: str) -> Any:
        """Get a service instance."""
        # Check if it's a singleton and already created
        if name in self._singletons:
            if self._singletons[name] is None:
                # Create singleton instance
                self._singletons[name] = self._create_service(name)
            return self._singletons[name]
        
        # Create new instance for non-singletons
        return self._create_service(name)
    
    def _create_service(self, name: str) -> Any:
        """Create a service instance using its factory."""
        if name not in self._factories:
            raise ConfigurationError(f"Service '{name}' is not registered")
        
        factory = self._factories[name]
        
        try:
            service = factory(self)
            logger.debug(f"Created service instance: {name}")
            return service
        except Exception as e:
            logger.error(f"Failed to create service '{name}': {str(e)}")
            raise ConfigurationError(f"Failed to create service '{name}': {str(e)}")
    
    def _register_core_services(self) -> None:
        """Register only the core working services."""
        
        # Core Infrastructure (FAST)
        self.register('interaction_logger', self._create_interaction_logger)
        self.register('llm_manager', self._create_llm_manager)
        
        # Data & Analysis Services (PROVEN TO WORK)
        self.register('data_service', self._create_data_service)
        self.register('analysis_service', self._create_analysis_service)
        self.register('visualization_service', self._create_visualization_service)
        
        # Report Services (WORKING)
        self.register('report_service', self._create_report_service)
        
        # Request Interpreter (NEW)
        self.register('request_interpreter', self._create_request_interpreter)
    
    def _eager_init_core_services(self) -> None:
        """FIXED: Eagerly initialize all core services at startup to prevent first-message delays."""
        logger.info("🚀 Starting eager initialization of core services...")
        
        # Initialize in dependency order
        services_to_init = [
            'interaction_logger',  # No dependencies
            'llm_manager',         # Depends on interaction_logger
            'data_service',        # No dependencies 
            'analysis_service',    # Depends on data_service (heavy initialization)
            'visualization_service', # Depends on data_service
            'report_service',      # No dependencies
            'request_interpreter'  # Depends on all above services
        ]
        
        for service_name in services_to_init:
            try:
                import time
                start_time = time.time()
                
                # This will trigger creation if it's a singleton
                service = self.get(service_name)
                
                duration = time.time() - start_time
                if service:
                    logger.info(f"✅ {service_name} initialized in {duration:.2f}s")
                else:
                    logger.warning(f"⚠️  {service_name} returned None")
                    
            except Exception as e:
                logger.error(f"❌ Failed to initialize {service_name}: {e}")
        
        logger.info("🎯 Eager initialization complete - services ready for instant responses!")
    
    def _create_interaction_logger(self, container: 'ServiceContainer'):
        """Create interaction logger service."""
        try:
            from ..interaction import InteractionLogger
            
            db_path = self._app.config.get('INTERACTIONS_DB_FILE')
            return InteractionLogger(db_path=db_path)
        except Exception as e:
            logger.warning(f"Failed to create interaction logger: {e}")
            return None
    
    def _create_llm_manager(self, container: 'ServiceContainer'):
        """Create LLM manager service - Use Ollama for EVERYTHING if configured."""
        try:
            interaction_logger = container.get('interaction_logger')
            
            # Check if we should use Ollama (for EVERYTHING!)
            use_ollama = self._app.config.get('USE_OLLAMA', False)
            
            if use_ollama:
                # Use Ollama for EVERYTHING - full local processing!
                try:
                    from ..core.ollama_manager import OllamaManager
                    logger.info("🔒 Using Ollama/Qwen3 for ALL ChatMRPT operations (100% LOCAL)")
                    
                    ollama_manager = OllamaManager(
                        model=self._app.config.get('OLLAMA_MODEL', 'qwen3:8b'),
                        base_url=self._app.config.get('OLLAMA_BASE_URL', 'http://localhost:11434'),
                        interaction_logger=interaction_logger
                    )
                    
                    # Verify Ollama is accessible
                    if ollama_manager.validate_connection():
                        logger.info("✅ Ollama connection verified - using local LLM")
                        return ollama_manager
                    else:
                        logger.warning("⚠️ Ollama not accessible, falling back to OpenAI")
                        # Fall through to OpenAI
                except ImportError as e:
                    logger.warning(f"Could not import OllamaManager: {e}, falling back to OpenAI")
                except Exception as e:
                    logger.warning(f"Error initializing Ollama: {e}, falling back to OpenAI")
            
            # Fallback to OpenAI (or use if Ollama not configured)
            from ..core.llm_manager import LLMManager
            
            if not use_ollama:
                logger.info("Using OpenAI for ChatMRPT operations")
            
            return LLMManager(
                api_key=self._app.config.get('OPENAI_API_KEY'),
                model=self._app.config.get('OPENAI_MODEL_NAME', 'gpt-4o'),
                interaction_logger=interaction_logger
            )
        except Exception as e:
            logger.warning(f"Failed to create LLM manager: {e}")
            return None
    
    def _create_data_service(self, container: 'ServiceContainer'):
        """Create data service - simple wrapper for DataHandler creation."""
        try:
            class SimpleDataService:
                """Simple data service that creates DataHandler instances."""
                
                def __init__(self, upload_folder):
                    self.upload_folder = upload_folder
                
                def get_handler(self, session_id):
                    """Get DataHandler for session."""
                    import os
                    from ..data import DataHandler
                    
                    session_folder = os.path.join(self.upload_folder, session_id)
                    if os.path.exists(session_folder):
                        # Create fresh DataHandler each time
                        return DataHandler(session_folder)
                    return None
            
            upload_folder = self._app.config.get('UPLOAD_FOLDER')
            return SimpleDataService(upload_folder)
        except Exception as e:
            logger.warning(f"Failed to create data service: {e}")
            return None
    
    def _create_analysis_service(self, container: 'ServiceContainer'):
        """Create analysis service - composite scoring and PCA."""
        try:
            from ..analysis.engine import AnalysisEngine
            
            class AnalysisServiceWrapper:
                """Wrapper for AnalysisEngine to handle data handler retrieval."""
                
                def __init__(self, container):
                    self.container = container
                    self.engine = AnalysisEngine()
                
                def run_complete_analysis(self, session_id: str, variables: Optional[List[str]] = None):
                    """Run complete analysis with data handler from session."""
                    data_service = self.container.get('data_service')
                    if not data_service:
                        return {'status': 'error', 'message': 'Data service not available'}
                    
                    data_handler = data_service.get_handler(session_id)
                    if not data_handler:
                        return {'status': 'error', 'message': 'No data handler found for session'}
                    
                    self.engine.data_handler = data_handler
                    return self.engine.run_complete_analysis(session_id, variables)
                
                def run_composite_analysis(self, session_id: str, variables: Optional[List[str]] = None):
                    """Run composite analysis with data handler from session."""
                    data_service = self.container.get('data_service')
                    if not data_service:
                        return {'status': 'error', 'message': 'Data service not available'}
                    
                    data_handler = data_service.get_handler(session_id)
                    if not data_handler:
                        return {'status': 'error', 'message': 'No data handler found for session'}
                    
                    self.engine.data_handler = data_handler
                    return self.engine.run_composite_analysis(session_id, variables)
                
                def run_pca_analysis(self, session_id: str, variables: Optional[List[str]] = None):
                    """Run PCA analysis with data handler from session."""
                    data_service = self.container.get('data_service')
                    if not data_service:
                        return {'status': 'error', 'message': 'Data service not available'}
                    
                    data_handler = data_service.get_handler(session_id)
                    if not data_handler:
                        return {'status': 'error', 'message': 'No data handler found for session'}
                    
                    self.engine.data_handler = data_handler
                    return self.engine.run_pca_analysis(session_id, variables)
                
                def explain_variable_selection(self, variables: List[str], data_handler):
                    """Explain variable selection."""
                    return self.engine.explain_variable_selection(variables, data_handler)
                
                def run_standard_analysis(self, data_handler, session_id: str = None):
                    """Run standard analysis."""
                    return self.engine.run_standard_analysis(data_handler, session_id)
                
                def run_custom_analysis(self, data_handler, selected_variables: List[str], session_id: str = None):
                    """Run custom analysis."""
                    return self.engine.run_custom_analysis(data_handler, selected_variables, session_id)
            
            return AnalysisServiceWrapper(container)
        except Exception as e:
            logger.warning(f"Failed to create analysis service: {e}")
            return None
    
    def _create_visualization_service(self, container: 'ServiceContainer'):
        """Create visualization service - 6 core visualizations."""
        try:
            from ..services.agents.visualizations import (
                create_agent_composite_score_maps,
                create_agent_vulnerability_map,
                create_agent_box_plot_ranking,
                create_agent_urban_extent_map,
                create_agent_decision_tree,
                create_agent_pca_vulnerability_map
            )
            
            class CoreVisualizationService:
                """Simple wrapper for the 6 core visualization functions."""
                
                def __init__(self):
                    self.composite_score_maps = create_agent_composite_score_maps
                    self.vulnerability_map = create_agent_vulnerability_map
                    self.box_plot_ranking = create_agent_box_plot_ranking
                    self.urban_extent_map = create_agent_urban_extent_map
                    self.decision_tree = create_agent_decision_tree
                    self.pca_vulnerability_map = create_agent_pca_vulnerability_map
                
                def create_composite_workflow(self, data_handler, session_id=None):
                    """Run the standard 5-visualization composite workflow."""
                    results = {}
                    
                    # Standard composite workflow
                    visualizations = [
                        ('composite_maps', self.composite_score_maps),
                        ('vulnerability_map', self.vulnerability_map),
                        ('box_plot', self.box_plot_ranking),
                        ('urban_extent', self.urban_extent_map),
                        ('decision_tree', self.decision_tree)
                    ]
                    
                    for name, func in visualizations:
                        try:
                            # Get unified dataset using lazy loading method
                            unified_dataset = data_handler.get_unified_dataset()
                            if unified_dataset is None:
                                result = {'status': 'error', 'message': 'No unified dataset available. Please run analysis first.'}
                            else:
                                result = func(unified_dataset, session_id=session_id)
                            results[name] = result
                        except Exception as e:
                            logger.error(f"Failed to create {name}: {e}")
                            results[name] = {'status': 'error', 'message': str(e)}
                    
                    return results
                
                def create_pca_workflow(self, data_handler, session_id=None):
                    """Run the PCA workflow (1 visualization)."""
                    try:
                        # Get unified dataset using lazy loading method
                        unified_dataset = data_handler.get_unified_dataset()
                        if unified_dataset is None:
                            return {'status': 'error', 'message': 'No unified dataset available. Please run analysis first.'}
                        
                        return self.pca_vulnerability_map(unified_dataset, session_id=session_id)
                    except Exception as e:
                        logger.error(f"Failed to create PCA visualization: {e}")
                        return {'status': 'error', 'message': str(e)}
                
                # Wrapper methods for request interpreter compatibility
                def create_vulnerability_map(self, session_id, method='composite'):
                    """Create vulnerability map - wrapper for request interpreter."""
                    try:
                        # Get data handler via session
                        data_service = container.get('data_service')
                        if not data_service:
                            return {'status': 'error', 'message': 'Data service not available'}
                        
                        data_handler = data_service.get_handler(session_id)
                        if not data_handler:
                            return {'status': 'error', 'message': 'No data handler found for session'}
                        
                        # Get unified dataset with geometry for map visualization
                        unified_dataset = data_handler.get_unified_dataset(require_geometry=True)
                        if unified_dataset is None:
                            return {'status': 'error', 'message': 'No unified dataset available. Please run analysis first.'}
                        
                        # Call appropriate vulnerability map method
                        if method == 'pca':
                            return self.pca_vulnerability_map(unified_dataset, session_id=session_id)
                        else:
                            return self.vulnerability_map(unified_dataset, session_id=session_id)
                    except Exception as e:
                        logger.error(f"Failed to create vulnerability map: {e}")
                        return {'status': 'error', 'message': str(e)}
                
                def create_box_plot(self, session_id, method='composite'):
                    """Create box plot - wrapper for request interpreter."""
                    try:
                        # Get data handler via session
                        data_service = container.get('data_service')
                        if not data_service:
                            return {'status': 'error', 'message': 'Data service not available'}
                        
                        data_handler = data_service.get_handler(session_id)
                        if not data_handler:
                            return {'status': 'error', 'message': 'No data handler found for session'}
                        
                        # Get unified dataset using lazy loading method
                        unified_dataset = data_handler.get_unified_dataset()
                        if unified_dataset is None:
                            return {'status': 'error', 'message': 'No unified dataset available. Please run analysis first.'}
                        
                        return self.box_plot_ranking(unified_dataset, session_id=session_id)
                    except Exception as e:
                        logger.error(f"Failed to create box plot: {e}")
                        return {'status': 'error', 'message': str(e)}
                
                def create_pca_map(self, session_id):
                    """Create PCA map - wrapper for request interpreter."""
                    try:
                        # Get data handler via session
                        data_service = container.get('data_service')
                        if not data_service:
                            return {'status': 'error', 'message': 'Data service not available'}
                        
                        data_handler = data_service.get_handler(session_id)
                        if not data_handler:
                            return {'status': 'error', 'message': 'No data handler found for session'}
                        
                        # Get unified dataset using lazy loading method
                        unified_dataset = data_handler.get_unified_dataset()
                        if unified_dataset is None:
                            return {'status': 'error', 'message': 'No unified dataset available. Please run analysis first.'}
                        
                        return self.pca_vulnerability_map(unified_dataset, session_id=session_id)
                    except Exception as e:
                        logger.error(f"Failed to create PCA map: {e}")
                        return {'status': 'error', 'message': str(e)}
            
            return CoreVisualizationService()
        except Exception as e:
            logger.warning(f"Failed to create visualization service: {e}")
            return None
    
    def _create_report_service(self, container: 'ServiceContainer'):
        """Create report service."""
        try:
            from ..reports import ModernReportGenerator
            
            # Return a wrapper class that creates instances on demand
            class ReportServiceWrapper:
                @staticmethod
                def generate_report(data_handler, session_id, format_type='pdf', custom_sections=None, detail_level='standard'):
                    """Generate report using ModernReportGenerator instance"""
                    generator = ModernReportGenerator(data_handler, session_id)
                    return generator.generate_report(format_type, custom_sections, detail_level)
                
                @staticmethod
                def generate_dashboard(data_handler, session_id):
                    """Generate dashboard (placeholder for compatibility)"""
                    return {
                        'status': 'success',
                        'message': 'Dashboard generation not implemented',
                        'report_url': None
                    }
            
            return ReportServiceWrapper
        except Exception as e:
            logger.warning(f"Failed to create report service: {e}")
            return None
    
    def _create_request_interpreter(self, container: 'ServiceContainer'):
        """Create request interpreter - natural language processing."""
        try:
            # Use migration utility to create appropriate interpreter
            from ..core.interpreter_migration import create_request_interpreter
            
            llm_manager = container.get('llm_manager')
            data_service = container.get('data_service')
            analysis_service = container.get('analysis_service')
            visualization_service = container.get('visualization_service')
            
            return create_request_interpreter(
                llm_manager=llm_manager,
                data_service=data_service,
                analysis_service=analysis_service,
                visualization_service=visualization_service
            )
        except Exception as e:
            logger.warning(f"Failed to create request interpreter: {e}")
            return None
    
    
    # Property accessors for core services
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
    def report_service(self):
        """Get report service."""
        return self.get('report_service')
    
    @property
    def request_interpreter(self):
        """Get request interpreter."""
        return self.get('request_interpreter')
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on all core services."""
        status = {
            'overall': 'healthy',
            'services': {},
            'timestamp': None
        }
        
        import datetime
        status['timestamp'] = datetime.datetime.utcnow().isoformat()
        
        # Check each core service
        services = ['data_service', 'analysis_service', 'visualization_service', 'report_service']
        
        for service_name in services:
            try:
                service = self.get(service_name)
                if service is not None:
                    status['services'][service_name] = 'healthy'
                else:
                    status['services'][service_name] = 'unavailable'
                    status['overall'] = 'degraded'
            except Exception as e:
                status['services'][service_name] = f'error: {str(e)}'
                status['overall'] = 'unhealthy'
        
        return status


def init_services(app: Flask) -> ServiceContainer:
    """
    Initialize the service container with the Flask app.
    
    Args:
        app: Flask application instance
        
    Returns:
        Configured service container
    """
    container = ServiceContainer(app)
    
    # Make services available in templates
    @app.context_processor
    def inject_services():
        return {'services': container}
    
    # Health check endpoint
    @app.route('/health')
    def health_check():
        return container.health_check()
    
    return container


# Global variable to hold the service container instance
_service_container = None


def get_service_container():
    """
    Get the global service container instance.
    
    This function provides access to the service container from anywhere
    in the application. It uses Flask's current_app context.
    
    Returns:
        ServiceContainer: The application's service container
    """
    global _service_container
    if _service_container is None:
        from flask import current_app
        if hasattr(current_app, 'services'):
            _service_container = current_app.services
        else:
            raise RuntimeError("Service container not initialized. Make sure the app is properly configured.")
    return _service_container 