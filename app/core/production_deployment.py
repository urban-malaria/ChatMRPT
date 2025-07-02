"""
ChatMRPT Production Deployment - Phase 6 Implementation

This module implements production-ready deployment configurations, system orchestration,
and optimization features for the complete ChatMRPT conversational architecture.

Key Features:
- Production configuration management
- System health monitoring and alerts
- Performance optimization and resource management  
- Security enhancements and validation
- Deployment utilities and automation
- Complete system orchestration
- Production logging and monitoring
"""

import asyncio
import logging
import os
import time
import json
import signal
import threading
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import queue
import gc

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    # Mock psutil for basic functionality
    class MockPsutil:
        @staticmethod
        def cpu_percent(interval=None):
            return 25.0
        
        @staticmethod
        def virtual_memory():
            class MockMemory:
                percent = 45.0
                total = 8 * 1024 * 1024 * 1024  # 8GB
            return MockMemory()
        
        @staticmethod
        def disk_usage(path):
            class MockDisk:
                percent = 60.0
                free = 100 * 1024 * 1024 * 1024  # 100GB
            return MockDisk()
        
        @staticmethod
        def Process():
            class MockProcess:
                def memory_info(self):
                    class MockMemInfo:
                        rss = 512 * 1024 * 1024  # 512MB
                    return MockMemInfo()
            return MockProcess()
    
    psutil = MockPsutil()

try:
    import gunicorn
    GUNICORN_AVAILABLE = True
except ImportError:
    GUNICORN_AVAILABLE = False

try:
    import uvloop
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from .conversation_memory import get_conversation_memory
from .tool_registry import get_tool_registry
from .chatmrpt_agent import get_chatmrpt_agent
from .langchain_integration import get_conversation_chain
from .reflection_engine import get_reflection_engine
from .session_state import SessionState

logger = logging.getLogger(__name__)


@dataclass
class SystemHealthStatus:
    """System health status information."""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    active_sessions: int
    response_time_avg: float
    error_rate: float
    uptime_seconds: float
    status: str  # healthy, warning, critical
    alerts: List[str] = field(default_factory=list)


@dataclass
class DeploymentConfig:
    """Production deployment configuration."""
    environment: str = "production"
    debug: bool = False
    workers: int = 4
    max_requests: int = 1000
    max_requests_jitter: int = 100
    timeout: int = 30
    keepalive: int = 2
    preload_app: bool = True
    enable_metrics: bool = True
    enable_health_checks: bool = True
    enable_security_features: bool = True
    log_level: str = "INFO"
    log_format: str = "json"
    enable_async_processing: bool = True
    max_concurrent_requests: int = 100
    request_timeout: int = 300
    memory_limit_mb: int = 2048
    enable_auto_scaling: bool = False


@dataclass
class SecurityConfig:
    """Security configuration for production."""
    enable_rate_limiting: bool = True
    max_requests_per_minute: int = 60
    enable_input_validation: bool = True
    enable_output_sanitization: bool = True
    allowed_file_types: List[str] = field(default_factory=lambda: ['csv', 'xlsx', 'zip'])
    max_file_size_mb: int = 32
    enable_session_security: bool = True
    session_timeout_minutes: int = 60
    enable_cors_protection: bool = True
    allowed_origins: List[str] = field(default_factory=list)
    enable_csrf_protection: bool = True
    enable_xss_protection: bool = True


class ProductionMetrics:
    """Production metrics collection using Prometheus."""
    
    _instance = None
    _metrics_initialized = False
    
    def __new__(cls, enabled: bool = True):
        """Singleton pattern to prevent duplicate metric registration."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, enabled: bool = True):
        """Initialize production metrics."""
        if self._metrics_initialized:
            return
            
        self.enabled = enabled and PROMETHEUS_AVAILABLE
        
        if self.enabled:
            try:
                # Request metrics
                self.request_count = Counter(
                    'chatmrpt_requests_total',
                    'Total number of requests',
                    ['method', 'endpoint', 'status']
                )
                
                self.request_duration = Histogram(
                    'chatmrpt_request_duration_seconds',
                    'Request duration in seconds',
                    ['method', 'endpoint']
                    )
                
                # System metrics
                self.active_sessions = Gauge(
                    'chatmrpt_active_sessions',
                    'Number of active user sessions'
                )
                
                self.memory_usage = Gauge(
                    'chatmrpt_memory_usage_bytes',
                    'Memory usage in bytes'
                )
                
                self.cpu_usage = Gauge(
                    'chatmrpt_cpu_usage_percent',
                    'CPU usage percentage'
                )
                
                # Component metrics
                self.tool_executions = Counter(
                    'chatmrpt_tool_executions_total',
                    'Total tool executions',
                    ['tool_name', 'status']
                )
                
                self.conversation_turns = Counter(
                    'chatmrpt_conversation_turns_total',
                    'Total conversation turns',
                    ['mode', 'success']
                )
                
                self.cache_operations = Counter(
                    'chatmrpt_cache_operations_total',
                    'Cache operations',
                    ['operation', 'hit_miss']
                )
                
                logger.info("Production metrics initialized")
                self._metrics_initialized = True
                
            except Exception as e:
                logger.warning(f"Failed to initialize production metrics: {e}")
                self.enabled = False
        else:
            logger.warning("Production metrics disabled (Prometheus not available)")
            
        self._metrics_initialized = True
    
    def record_request(self, method: str, endpoint: str, status: int, duration: float):
        """Record request metrics."""
        if self.enabled:
            self.request_count.labels(method=method, endpoint=endpoint, status=status).inc()
            self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)
    
    def update_system_metrics(self, cpu: float, memory: int, sessions: int):
        """Update system metrics."""
        if self.enabled:
            self.cpu_usage.set(cpu)
            self.memory_usage.set(memory)
            self.active_sessions.set(sessions)
    
    def record_tool_execution(self, tool_name: str, success: bool):
        """Record tool execution metrics."""
        if self.enabled:
            status = 'success' if success else 'error'
            self.tool_executions.labels(tool_name=tool_name, status=status).inc()
    
    def record_conversation_turn(self, mode: str, success: bool):
        """Record conversation turn metrics."""
        if self.enabled:
            success_str = 'success' if success else 'error'
            self.conversation_turns.labels(mode=mode, success=success_str).inc()
    
    def record_cache_operation(self, operation: str, hit: bool):
        """Record cache operation metrics."""
        if self.enabled:
            hit_miss = 'hit' if hit else 'miss'
            self.cache_operations.labels(operation=operation, hit_miss=hit_miss).inc()


class SystemHealthMonitor:
    """Comprehensive system health monitoring."""
    
    def __init__(self, check_interval: int = 30):
        """Initialize health monitor."""
        self.check_interval = check_interval
        self.start_time = time.time()
        self.last_health_check = None
        self.health_history = queue.Queue(maxsize=100)
        self.alerts = []
        self.monitoring = False
        self.monitor_thread = None
        
        # Thresholds
        self.cpu_warning_threshold = 80.0
        self.cpu_critical_threshold = 95.0
        self.memory_warning_threshold = 80.0
        self.memory_critical_threshold = 95.0
        self.disk_warning_threshold = 85.0
        self.disk_critical_threshold = 95.0
        self.response_time_warning = 5.0
        self.response_time_critical = 10.0
        
    def start_monitoring(self):
        """Start health monitoring in background."""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("System health monitoring started")
    
    def stop_monitoring(self):
        """Stop health monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("System health monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.monitoring:
            try:
                health_status = self.check_system_health()
                self.last_health_check = health_status
                
                # Store in history
                if not self.health_history.full():
                    self.health_history.put(health_status)
                else:
                    self.health_history.get()  # Remove oldest
                    self.health_history.put(health_status)
                
                # Handle alerts
                self._process_alerts(health_status)
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Health monitoring error: {e}")
                time.sleep(self.check_interval)
    
    def check_system_health(self) -> SystemHealthStatus:
        """Check comprehensive system health."""
        current_time = datetime.now()
        
        # System metrics
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Application metrics
        reflection_engine = get_reflection_engine()
        system_insights = reflection_engine.get_system_insights() if reflection_engine.enabled else {}
        
        active_sessions = system_insights.get('system_health', {}).get('active_sessions', 0)
        response_time_avg = system_insights.get('system_health', {}).get('average_response_time', 0.0)
        
        # Error rate calculation
        error_rate = 0.0
        if system_insights.get('conversation_quality'):
            conv_quality = system_insights['conversation_quality']
            if not conv_quality.get('no_data', False):
                total_sessions = conv_quality.get('total_sessions', 1)
                low_quality = conv_quality.get('low_quality_sessions', 0)
                error_rate = low_quality / total_sessions if total_sessions > 0 else 0.0
        
        # Determine status
        status = "healthy"
        alerts = []
        
        if cpu_usage > self.cpu_critical_threshold:
            status = "critical"
            alerts.append(f"Critical CPU usage: {cpu_usage:.1f}%")
        elif cpu_usage > self.cpu_warning_threshold:
            status = "warning"
            alerts.append(f"High CPU usage: {cpu_usage:.1f}%")
        
        if memory.percent > self.memory_critical_threshold:
            status = "critical"
            alerts.append(f"Critical memory usage: {memory.percent:.1f}%")
        elif memory.percent > self.memory_warning_threshold:
            if status != "critical":
                status = "warning"
            alerts.append(f"High memory usage: {memory.percent:.1f}%")
        
        if disk.percent > self.disk_critical_threshold:
            status = "critical"
            alerts.append(f"Critical disk usage: {disk.percent:.1f}%")
        elif disk.percent > self.disk_warning_threshold:
            if status != "critical":
                status = "warning"
            alerts.append(f"High disk usage: {disk.percent:.1f}%")
        
        if response_time_avg > self.response_time_critical:
            status = "critical"
            alerts.append(f"Critical response time: {response_time_avg:.2f}s")
        elif response_time_avg > self.response_time_warning:
            if status != "critical":
                status = "warning"
            alerts.append(f"Slow response time: {response_time_avg:.2f}s")
        
        uptime = time.time() - self.start_time
        
        return SystemHealthStatus(
            timestamp=current_time,
            cpu_usage=cpu_usage,
            memory_usage=memory.percent,
            disk_usage=disk.percent,
            active_sessions=active_sessions,
            response_time_avg=response_time_avg,
            error_rate=error_rate,
            uptime_seconds=uptime,
            status=status,
            alerts=alerts
        )
    
    def _process_alerts(self, health_status: SystemHealthStatus):
        """Process health alerts."""
        if health_status.alerts:
            for alert in health_status.alerts:
                if alert not in [a['message'] for a in self.alerts[-10:]]:  # Avoid spam
                    alert_info = {
                        'timestamp': health_status.timestamp,
                        'level': health_status.status,
                        'message': alert
                    }
                    self.alerts.append(alert_info)
                    logger.warning(f"Health Alert [{health_status.status.upper()}]: {alert}")
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary."""
        current_health = self.last_health_check
        if not current_health:
            current_health = self.check_system_health()
        
        # Recent alerts
        recent_alerts = self.alerts[-5:] if self.alerts else []
        
        return {
            'current_status': current_health.status,
            'timestamp': current_health.timestamp.isoformat(),
            'system_metrics': {
                'cpu_usage': current_health.cpu_usage,
                'memory_usage': current_health.memory_usage,
                'disk_usage': current_health.disk_usage,
                'uptime_hours': current_health.uptime_seconds / 3600
            },
            'application_metrics': {
                'active_sessions': current_health.active_sessions,
                'avg_response_time': current_health.response_time_avg,
                'error_rate': current_health.error_rate
            },
            'recent_alerts': recent_alerts,
            'monitoring_active': self.monitoring
        }


class ProductionOptimizer:
    """Production performance optimization."""
    
    def __init__(self, config: DeploymentConfig):
        """Initialize production optimizer."""
        self.config = config
        self.optimization_enabled = True
        self.last_optimization = None
        
        # Resource management
        self.max_memory_usage = config.memory_limit_mb * 1024 * 1024  # Convert to bytes
        
    def optimize_system_performance(self):
        """Perform system-wide performance optimizations."""
        optimizations_applied = []
        
        try:
            # Memory optimization
            if self._check_memory_usage():
                self._optimize_memory()
                optimizations_applied.append("memory_cleanup")
            
            # Component optimization
            self._optimize_components()
            optimizations_applied.append("component_optimization")
            
            # Cache optimization
            self._optimize_caches()
            optimizations_applied.append("cache_optimization")
            
            self.last_optimization = datetime.now()
            logger.info(f"Performance optimization completed: {optimizations_applied}")
            
        except Exception as e:
            logger.error(f"Performance optimization failed: {e}")
        
        return optimizations_applied
    
    def _check_memory_usage(self) -> bool:
        """Check if memory optimization is needed."""
        memory = psutil.virtual_memory()
        process = psutil.Process()
        process_memory = process.memory_info().rss
        
        return (memory.percent > 85.0 or 
                process_memory > self.max_memory_usage * 0.9)
    
    def _optimize_memory(self):
        """Optimize memory usage."""
        # Force garbage collection
        gc.collect()
        
        # Clear reflection engine caches if memory pressure
        reflection_engine = get_reflection_engine()
        if reflection_engine.enabled:
            cache_stats = reflection_engine.embedding_cache.get_stats()
            if cache_stats['cache_size'] > 5000:
                # Clear old cache entries
                reflection_engine.embedding_cache.cache.clear()
                reflection_engine.embedding_cache.access_times.clear()
                logger.info("Cleared embedding cache due to memory pressure")
    
    def _optimize_components(self):
        """Optimize individual components."""
        # Optimize conversation memory
        memory_manager = get_conversation_memory()
        if memory_manager.enabled and hasattr(memory_manager, 'cleanup_old_conversations'):
            memory_manager.cleanup_old_conversations(days=30)
        
        # Optimize tool registry caches
        tool_registry = get_tool_registry()
        if hasattr(tool_registry, 'cleanup_caches'):
            tool_registry.cleanup_caches()
    
    def _optimize_caches(self):
        """Optimize various caches."""
        reflection_engine = get_reflection_engine()
        if reflection_engine.enabled:
            cache_stats = reflection_engine.embedding_cache.get_stats()
            hit_rate = cache_stats.get('hit_rate', 0)
            
            # If hit rate is too low, consider cache size adjustment
            if hit_rate < 0.3 and cache_stats.get('cache_size', 0) < 1000:
                logger.info("Low cache hit rate detected, consider increasing cache size")


class ProductionDeploymentManager:
    """
    Main production deployment manager that orchestrates all components.
    
    Coordinates the complete ChatMRPT conversational architecture for production use.
    """
    
    def __init__(self, config: DeploymentConfig = None, security_config: SecurityConfig = None):
        """Initialize production deployment manager."""
        self.config = config or self._load_deployment_config()
        self.security_config = security_config or self._load_security_config()
        
        # Initialize components
        self.metrics = ProductionMetrics(enabled=self.config.enable_metrics)
        self.health_monitor = SystemHealthMonitor()
        self.optimizer = ProductionOptimizer(self.config)
        
        # Component references
        self.memory_manager = None
        self.tool_registry = None
        self.agent = None
        self.langchain_chain = None
        self.reflection_engine = None
        
        # System state
        self.startup_time = datetime.now()
        self.shutdown_requested = False
        self.background_tasks = []
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        logger.info(f"Production deployment manager initialized for {self.config.environment}")
    
    def _load_deployment_config(self) -> DeploymentConfig:
        """Load deployment configuration from environment."""
        return DeploymentConfig(
            environment=os.getenv('DEPLOYMENT_ENVIRONMENT', 'production'),
            debug=os.getenv('DEBUG', 'false').lower() == 'true',
            workers=int(os.getenv('WORKERS', '4')),
            max_requests=int(os.getenv('MAX_REQUESTS', '1000')),
            timeout=int(os.getenv('TIMEOUT', '30')),
            enable_metrics=os.getenv('ENABLE_METRICS', 'true').lower() == 'true',
            enable_health_checks=os.getenv('ENABLE_HEALTH_CHECKS', 'true').lower() == 'true',
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            max_concurrent_requests=int(os.getenv('MAX_CONCURRENT_REQUESTS', '100')),
            memory_limit_mb=int(os.getenv('MEMORY_LIMIT_MB', '2048'))
        )
    
    def _load_security_config(self) -> SecurityConfig:
        """Load security configuration from environment."""
        return SecurityConfig(
            enable_rate_limiting=os.getenv('ENABLE_RATE_LIMITING', 'true').lower() == 'true',
            max_requests_per_minute=int(os.getenv('MAX_REQUESTS_PER_MINUTE', '60')),
            max_file_size_mb=int(os.getenv('MAX_FILE_SIZE_MB', '32')),
            session_timeout_minutes=int(os.getenv('SESSION_TIMEOUT_MINUTES', '60')),
            allowed_origins=os.getenv('ALLOWED_ORIGINS', '').split(',') if os.getenv('ALLOWED_ORIGINS') else []
        )
    
    def initialize_system(self):
        """Initialize the complete ChatMRPT system."""
        logger.info("Initializing ChatMRPT production system...")
        
        try:
            # Initialize core components in dependency order
            self.memory_manager = get_conversation_memory()
            logger.info("✅ Phase 1: Conversation Memory initialized")
            
            self.tool_registry = get_tool_registry()
            logger.info("✅ Phase 2: Tool Registry initialized")
            
            self.agent = get_chatmrpt_agent()
            logger.info("✅ Phase 3: ReAct Agent initialized")
            
            self.langchain_chain = get_conversation_chain()
            logger.info("✅ Phase 4: LangChain Integration initialized")
            
            self.reflection_engine = get_reflection_engine()
            logger.info("✅ Phase 5: Reflection Engine initialized")
            
            # Start monitoring and optimization
            if self.config.enable_health_checks:
                self.health_monitor.start_monitoring()
                logger.info("✅ Health monitoring started")
            
            # Start metrics server if enabled
            if self.config.enable_metrics and PROMETHEUS_AVAILABLE:
                metrics_port = int(os.getenv('METRICS_PORT', '8001'))
                start_http_server(metrics_port)
                logger.info(f"✅ Metrics server started on port {metrics_port}")
            
            # Schedule periodic optimization
            self._schedule_optimization()
            
            logger.info("🚀 ChatMRPT production system fully initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ System initialization failed: {e}")
            return False
    
    def _schedule_optimization(self):
        """Schedule periodic system optimization."""
        def optimization_task():
            while not self.shutdown_requested:
                try:
                    time.sleep(300)  # Optimize every 5 minutes
                    if not self.shutdown_requested:
                        self.optimizer.optimize_system_performance()
                except Exception as e:
                    logger.error(f"Optimization task error: {e}")
        
        if self.config.enable_async_processing:
            optimization_thread = threading.Thread(target=optimization_task, daemon=True)
            optimization_thread.start()
            self.background_tasks.append(optimization_thread)
            logger.info("✅ Periodic optimization scheduled")
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        try:
            # Only setup signal handlers in main thread
            import threading
            if threading.current_thread() is threading.main_thread():
                def signal_handler(signum, frame):
                    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
                    self.shutdown()
                
                signal.signal(signal.SIGTERM, signal_handler)
                signal.signal(signal.SIGINT, signal_handler)
                logger.debug("Signal handlers configured")
            else:
                logger.debug("Skipping signal handler setup (not main thread)")
        except Exception as e:
            logger.warning(f"Could not setup signal handlers: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        uptime = datetime.now() - self.startup_time
        
        # Component status
        component_status = {
            'memory_manager': self.memory_manager is not None,
            'tool_registry': self.tool_registry is not None,
            'agent': self.agent is not None,
            'langchain_chain': self.langchain_chain is not None,
            'reflection_engine': self.reflection_engine is not None
        }
        
        # Health status
        health_summary = self.health_monitor.get_health_summary()
        
        # Reflection insights
        reflection_insights = {}
        if self.reflection_engine and self.reflection_engine.enabled:
            reflection_insights = self.reflection_engine.get_system_insights()
        
        return {
            'system_info': {
                'environment': self.config.environment,
                'uptime_seconds': uptime.total_seconds(),
                'startup_time': self.startup_time.isoformat(),
                'version': '1.0.0',  # ChatMRPT version
                'deployment_phase': 'Phase 6: Production Deployment'
            },
            'component_status': component_status,
            'health': health_summary,
            'reflection_insights': reflection_insights,
            'configuration': {
                'workers': self.config.workers,
                'debug': self.config.debug,
                'metrics_enabled': self.config.enable_metrics,
                'health_checks_enabled': self.config.enable_health_checks
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Simple health check endpoint."""
        health_status = self.health_monitor.check_system_health()
        
        return {
            'status': health_status.status,
            'timestamp': health_status.timestamp.isoformat(),
            'uptime_seconds': health_status.uptime_seconds,
            'checks': {
                'cpu_ok': health_status.cpu_usage < 90.0,
                'memory_ok': health_status.memory_usage < 90.0,
                'disk_ok': health_status.disk_usage < 90.0,
                'response_time_ok': health_status.response_time_avg < 5.0
            }
        }
    
    def shutdown(self):
        """Graceful system shutdown."""
        logger.info("Starting graceful shutdown...")
        self.shutdown_requested = True
        
        # Stop health monitoring
        self.health_monitor.stop_monitoring()
        
        # Stop reflection engine background tasks
        if self.reflection_engine:
            self.reflection_engine.stop_background_learning()
        
        # Wait for background tasks
        for task in self.background_tasks:
            if task.is_alive():
                task.join(timeout=5)
        
        logger.info("✅ Graceful shutdown completed")


# Global deployment manager instance
_deployment_manager = None


def get_deployment_manager() -> ProductionDeploymentManager:
    """Get the global deployment manager instance."""
    global _deployment_manager
    if _deployment_manager is None:
        _deployment_manager = ProductionDeploymentManager()
    return _deployment_manager


def init_production_deployment(config: DeploymentConfig = None, 
                             security_config: SecurityConfig = None) -> ProductionDeploymentManager:
    """Initialize production deployment with custom configuration."""
    global _deployment_manager
    _deployment_manager = ProductionDeploymentManager(config, security_config)
    return _deployment_manager


# Production utilities

def setup_production_logging(log_level: str = "INFO", log_format: str = "json"):
    """Setup production logging configuration."""
    import logging.config
    
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'json': {
                'format': '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': log_format,
                'level': log_level
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': 'instance/chatmrpt_production.log',
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5,
                'formatter': log_format,
                'level': log_level
            }
        },
        'root': {
            'level': log_level,
            'handlers': ['console', 'file']
        }
    }
    
    logging.config.dictConfig(config)
    logger.info("Production logging configured")


def optimize_event_loop():
    """Optimize asyncio event loop for production."""
    if UVLOOP_AVAILABLE:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("UVLoop event loop policy set for better performance")


def validate_production_environment() -> Dict[str, Any]:
    """Validate production environment and dependencies."""
    validation_results = {
        'valid': True,
        'warnings': [],
        'errors': [],
        'recommendations': []
    }
    
    # Check required environment variables
    required_env_vars = ['OPENAI_API_KEY', 'SECRET_KEY']
    for var in required_env_vars:
        if not os.getenv(var):
            validation_results['errors'].append(f"Missing required environment variable: {var}")
            validation_results['valid'] = False
    
    # Check optional but recommended environment variables
    recommended_env_vars = ['REDIS_URL', 'DATABASE_URL']
    for var in recommended_env_vars:
        if not os.getenv(var):
            validation_results['warnings'].append(f"Missing recommended environment variable: {var}")
    
    # Check system resources
    memory = psutil.virtual_memory()
    if memory.total < 2 * 1024 * 1024 * 1024:  # 2GB
        validation_results['warnings'].append("System has less than 2GB RAM")
    
    disk = psutil.disk_usage('/')
    if disk.free < 5 * 1024 * 1024 * 1024:  # 5GB
        validation_results['warnings'].append("Less than 5GB disk space available")
    
    # Check Python version
    import sys
    if sys.version_info < (3, 8):
        validation_results['errors'].append("Python 3.8+ required for production")
        validation_results['valid'] = False
    
    # Recommendations
    if not PROMETHEUS_AVAILABLE:
        validation_results['recommendations'].append("Install prometheus_client for metrics: pip install prometheus_client")
    
    if not UVLOOP_AVAILABLE:
        validation_results['recommendations'].append("Install uvloop for better performance: pip install uvloop")
    
    return validation_results