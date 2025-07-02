"""
ChatMRPT Reflection Engine - Phase 5 Implementation

This module implements a reflection and performance optimization system that learns
from conversation patterns, monitors system performance, and optimizes tool usage
and memory operations for improved user experiences.

Key Features:
- Performance monitoring and metrics collection
- Tool usage pattern analysis and optimization
- Conversation quality assessment and learning
- Caching strategies for embeddings and tool results
- Async processing for heavy operations
- Continuous learning from user interactions
"""

import asyncio
import logging
import os
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import threading

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .conversation_memory import get_conversation_memory, ConversationTurn
from .tool_registry import get_tool_registry
from .session_state import SessionState, ConversationMode

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetric:
    """Individual performance metric."""
    metric_name: str
    value: float
    timestamp: datetime
    session_id: str
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolPerformanceStats:
    """Performance statistics for a specific tool."""
    tool_name: str
    total_executions: int = 0
    successful_executions: int = 0
    average_execution_time: float = 0.0
    total_execution_time: float = 0.0
    error_rate: float = 0.0
    last_used: Optional[datetime] = None
    usage_frequency: float = 0.0
    user_satisfaction_avg: float = 0.0


@dataclass
class ConversationQualityMetrics:
    """Metrics for conversation quality assessment."""
    session_id: str
    total_exchanges: int = 0
    successful_resolutions: int = 0
    average_response_time: float = 0.0
    user_satisfaction_score: float = 0.0
    tool_usage_effectiveness: float = 0.0
    memory_retrieval_relevance: float = 0.0
    conversation_coherence: float = 0.0


@dataclass
class OptimizationRecommendation:
    """Recommendation for system optimization."""
    recommendation_type: str
    priority: str  # high, medium, low
    description: str
    expected_improvement: float
    implementation_effort: str  # low, medium, high
    target_component: str
    specific_actions: List[str] = field(default_factory=list)


class EmbeddingCache:
    """Intelligent caching system for embeddings and expensive computations."""
    
    def __init__(self, max_size: int = 10000, ttl_hours: int = 24):
        """Initialize embedding cache."""
        self.max_size = max_size
        self.ttl = timedelta(hours=ttl_hours)
        self.cache = {}
        self.access_times = {}
        self.hit_count = 0
        self.miss_count = 0
        self.lock = threading.RLock()
        
        # Redis cache if available
        self.redis_client = self._init_redis_cache()
        
    def _init_redis_cache(self) -> Optional[Any]:
        """Initialize Redis cache if available."""
        if not REDIS_AVAILABLE:
            return None
            
        redis_url = os.getenv('REDIS_URL')
        if not redis_url:
            return None
            
        try:
            client = redis.from_url(redis_url)
            client.ping()
            logger.info("Redis embedding cache initialized")
            return client
        except Exception as e:
            logger.warning(f"Failed to initialize Redis cache: {e}")
            return None
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value."""
        with self.lock:
            # Check in-memory cache first
            if key in self.cache:
                entry_time, value = self.cache[key]
                if datetime.now() - entry_time < self.ttl:
                    self.access_times[key] = datetime.now()
                    self.hit_count += 1
                    return value
                else:
                    # Expired
                    del self.cache[key]
                    del self.access_times[key]
            
            # Check Redis cache
            if self.redis_client:
                try:
                    cached_data = self.redis_client.get(f"embedding:{key}")
                    if cached_data:
                        value = json.loads(cached_data)
                        self.hit_count += 1
                        return value
                except Exception as e:
                    logger.warning(f"Redis cache get failed: {e}")
            
            self.miss_count += 1
            return None
    
    def set(self, key: str, value: Any, ttl_override: Optional[int] = None):
        """Set cached value."""
        with self.lock:
            current_time = datetime.now()
            
            # Store in memory cache
            if len(self.cache) >= self.max_size:
                self._evict_lru()
            
            self.cache[key] = (current_time, value)
            self.access_times[key] = current_time
            
            # Store in Redis cache
            if self.redis_client:
                try:
                    ttl_seconds = ttl_override or int(self.ttl.total_seconds())
                    self.redis_client.setex(
                        f"embedding:{key}",
                        ttl_seconds,
                        json.dumps(value, default=str)
                    )
                except Exception as e:
                    logger.warning(f"Redis cache set failed: {e}")
    
    def _evict_lru(self):
        """Evict least recently used item."""
        if not self.access_times:
            return
            
        lru_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
        del self.cache[lru_key]
        del self.access_times[lru_key]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_requests if total_requests > 0 else 0
        
        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'hit_rate': hit_rate,
            'cache_size': len(self.cache),
            'max_size': self.max_size,
            'redis_available': self.redis_client is not None
        }


class PerformanceMonitor:
    """Real-time performance monitoring system."""
    
    def __init__(self, window_size: int = 1000):
        """Initialize performance monitor."""
        self.window_size = window_size
        self.metrics = deque(maxlen=window_size)
        self.tool_stats = {}  # Changed from defaultdict to regular dict
        self.conversation_metrics = {}
        self.system_health = {
            'cpu_usage': 0.0,
            'memory_usage': 0.0,
            'active_sessions': 0,
            'average_response_time': 0.0
        }
        self.lock = threading.RLock()
        
        # Async processing
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="reflection")
        
    def record_metric(self, metric: PerformanceMetric):
        """Record a performance metric."""
        with self.lock:
            self.metrics.append(metric)
            
            # Update specific metric aggregations
            if metric.metric_name == 'tool_execution_time':
                self._update_tool_stats(metric)
            elif metric.metric_name == 'conversation_quality':
                self._update_conversation_metrics(metric)
    
    def _update_tool_stats(self, metric: PerformanceMetric):
        """Update tool performance statistics."""
        tool_name = metric.context.get('tool_name')
        if not tool_name:
            return
            
        # Create stats if doesn't exist
        if tool_name not in self.tool_stats:
            self.tool_stats[tool_name] = ToolPerformanceStats(tool_name=tool_name)
            
        stats = self.tool_stats[tool_name]
        stats.total_executions += 1
        stats.total_execution_time += metric.value
        stats.average_execution_time = stats.total_execution_time / stats.total_executions
        stats.last_used = metric.timestamp
        
        # Update success rate
        if metric.context.get('success', True):
            stats.successful_executions += 1
        
        stats.error_rate = 1.0 - (stats.successful_executions / stats.total_executions)
    
    def _update_conversation_metrics(self, metric: PerformanceMetric):
        """Update conversation quality metrics."""
        session_id = metric.session_id
        if session_id not in self.conversation_metrics:
            self.conversation_metrics[session_id] = ConversationQualityMetrics(session_id=session_id)
        
        conv_metrics = self.conversation_metrics[session_id]
        conv_metrics.total_exchanges += 1
        
        # Update rolling averages based on metric context
        context = metric.context
        if 'response_time' in context:
            current_avg = conv_metrics.average_response_time
            total = conv_metrics.total_exchanges
            conv_metrics.average_response_time = (
                (current_avg * (total - 1) + context['response_time']) / total
            )
        
        if 'user_satisfaction' in context:
            current_avg = conv_metrics.user_satisfaction_score
            total = conv_metrics.total_exchanges
            conv_metrics.user_satisfaction_score = (
                (current_avg * (total - 1) + context['user_satisfaction']) / total
            )
    
    def get_tool_performance_summary(self) -> Dict[str, ToolPerformanceStats]:
        """Get summary of tool performance."""
        with self.lock:
            return dict(self.tool_stats)
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get current system health metrics."""
        with self.lock:
            recent_metrics = list(self.metrics)[-100:]  # Last 100 metrics
            
            if recent_metrics:
                response_times = [
                    m.value for m in recent_metrics 
                    if m.metric_name == 'response_time'
                ]
                if response_times:
                    self.system_health['average_response_time'] = sum(response_times) / len(response_times)
            
            self.system_health['active_sessions'] = len(self.conversation_metrics)
            
            return self.system_health.copy()
    
    def get_performance_trends(self, hours: int = 24) -> Dict[str, List[float]]:
        """Get performance trends over time."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self.lock:
            recent_metrics = [
                m for m in self.metrics 
                if m.timestamp >= cutoff_time
            ]
            
            trends = defaultdict(list)
            for metric in recent_metrics:
                trends[metric.metric_name].append(metric.value)
            
            return dict(trends)


class ReflectionEngine:
    """
    Main reflection engine for continuous learning and optimization.
    
    Monitors system performance, learns from user interactions, and provides
    recommendations for improving system efficiency and user experience.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the reflection engine."""
        self.config = config or self._load_config()
        self.enabled = self.config.get('enabled', True)
        
        if not self.enabled:
            logger.info("Reflection engine disabled by configuration")
            return
        
        # Core components
        self.performance_monitor = PerformanceMonitor(
            window_size=self.config.get('metrics_window_size', 1000)
        )
        self.embedding_cache = EmbeddingCache(
            max_size=self.config.get('cache_max_size', 10000),
            ttl_hours=self.config.get('cache_ttl_hours', 24)
        )
        
        # Learning components
        self.learning_enabled = self.config.get('enable_learning', True)
        self.optimization_threshold = self.config.get('optimization_threshold', 0.1)
        self.reflection_interval = self.config.get('reflection_interval_minutes', 30)
        
        # Integration with other components
        self.memory_manager = get_conversation_memory()
        self.tool_registry = get_tool_registry()
        
        # Background learning task
        self.learning_task = None
        if self.learning_enabled:
            self.start_background_learning()
        
        logger.info(f"Reflection engine initialized (learning: {self.learning_enabled})")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load reflection engine configuration."""
        return {
            'enabled': os.getenv('ENABLE_REFLECTION_ENGINE', 'true').lower() == 'true',
            'enable_learning': os.getenv('ENABLE_REFLECTION_LEARNING', 'true').lower() == 'true',
            'metrics_window_size': int(os.getenv('REFLECTION_METRICS_WINDOW', '1000')),
            'cache_max_size': int(os.getenv('REFLECTION_CACHE_SIZE', '10000')),
            'cache_ttl_hours': int(os.getenv('REFLECTION_CACHE_TTL_HOURS', '24')),
            'optimization_threshold': float(os.getenv('REFLECTION_OPTIMIZATION_THRESHOLD', '0.1')),
            'reflection_interval_minutes': int(os.getenv('REFLECTION_INTERVAL_MINUTES', '30')),
            'enable_async_processing': os.getenv('ENABLE_ASYNC_REFLECTION', 'true').lower() == 'true'
        }
    
    def record_tool_execution(self, tool_name: str, execution_time: float, 
                            success: bool, session_id: str, context: Dict[str, Any] = None):
        """Record tool execution for performance analysis."""
        if not self.enabled:
            return
        
        metric = PerformanceMetric(
            metric_name='tool_execution_time',
            value=execution_time,
            timestamp=datetime.now(),
            session_id=session_id,
            context={
                'tool_name': tool_name,
                'success': success,
                **(context or {})
            }
        )
        
        self.performance_monitor.record_metric(metric)
    
    def record_conversation_quality(self, session_id: str, response_time: float,
                                  user_satisfaction: float = None, context: Dict[str, Any] = None):
        """Record conversation quality metrics."""
        if not self.enabled:
            return
        
        metric_context = {
            'response_time': response_time,
            **(context or {})
        }
        
        if user_satisfaction is not None:
            metric_context['user_satisfaction'] = user_satisfaction
        
        metric = PerformanceMetric(
            metric_name='conversation_quality',
            value=response_time,
            timestamp=datetime.now(),
            session_id=session_id,
            context=metric_context
        )
        
        self.performance_monitor.record_metric(metric)
    
    def get_cached_embedding(self, text: str, embedding_type: str = 'default') -> Optional[List[float]]:
        """Get cached embedding or compute if not available."""
        if not self.enabled:
            return None
        
        cache_key = f"{embedding_type}:{hash(text)}"
        return self.embedding_cache.get(cache_key)
    
    def cache_embedding(self, text: str, embedding: List[float], embedding_type: str = 'default'):
        """Cache computed embedding."""
        if not self.enabled:
            return
        
        cache_key = f"{embedding_type}:{hash(text)}"
        self.embedding_cache.set(cache_key, embedding)
    
    def analyze_tool_performance(self) -> Dict[str, Any]:
        """Analyze tool performance and identify optimization opportunities."""
        if not self.enabled:
            return {}
        
        tool_stats = self.performance_monitor.get_tool_performance_summary()
        
        analysis = {
            'total_tools': len(tool_stats),
            'high_performance_tools': [],
            'underperforming_tools': [],
            'optimization_recommendations': []
        }
        
        for tool_name, stats in tool_stats.items():
            if stats.error_rate > 0.2:  # 20% error rate threshold
                analysis['underperforming_tools'].append({
                    'tool_name': tool_name,
                    'error_rate': stats.error_rate,
                    'avg_execution_time': stats.average_execution_time
                })
            elif stats.error_rate < 0.05 and stats.average_execution_time < 2.0:
                analysis['high_performance_tools'].append({
                    'tool_name': tool_name,
                    'error_rate': stats.error_rate,
                    'avg_execution_time': stats.average_execution_time
                })
        
        # Generate optimization recommendations
        analysis['optimization_recommendations'] = self._generate_optimization_recommendations(tool_stats)
        
        return analysis
    
    def _generate_optimization_recommendations(self, tool_stats: Dict[str, ToolPerformanceStats]) -> List[OptimizationRecommendation]:
        """Generate optimization recommendations based on performance data."""
        recommendations = []
        
        # Identify slow tools
        slow_tools = [
            (name, stats) for name, stats in tool_stats.items()
            if stats.average_execution_time > 5.0 and stats.total_executions > 10
        ]
        
        for tool_name, stats in slow_tools:
            recommendations.append(OptimizationRecommendation(
                recommendation_type='performance_optimization',
                priority='high',
                description=f"Tool '{tool_name}' has high average execution time ({stats.average_execution_time:.2f}s)",
                expected_improvement=0.3,
                implementation_effort='medium',
                target_component=f'tool:{tool_name}',
                specific_actions=[
                    'Review tool implementation for bottlenecks',
                    'Consider caching frequently used results',
                    'Optimize database queries if applicable'
                ]
            ))
        
        # Identify error-prone tools
        error_prone_tools = [
            (name, stats) for name, stats in tool_stats.items()
            if stats.error_rate > 0.15 and stats.total_executions > 5
        ]
        
        for tool_name, stats in error_prone_tools:
            recommendations.append(OptimizationRecommendation(
                recommendation_type='reliability_improvement',
                priority='high',
                description=f"Tool '{tool_name}' has high error rate ({stats.error_rate:.1%})",
                expected_improvement=0.4,
                implementation_effort='medium',
                target_component=f'tool:{tool_name}',
                specific_actions=[
                    'Add better error handling and retry logic',
                    'Improve input validation',
                    'Add graceful degradation for edge cases'
                ]
            ))
        
        return recommendations
    
    def get_system_insights(self) -> Dict[str, Any]:
        """Get comprehensive system insights and recommendations."""
        if not self.enabled:
            return {'enabled': False}
        
        insights = {
            'enabled': True,
            'system_health': self.performance_monitor.get_system_health(),
            'cache_performance': self.embedding_cache.get_stats(),
            'tool_analysis': self.analyze_tool_performance(),
            'performance_trends': self.performance_monitor.get_performance_trends(hours=24),
            'conversation_quality': self._analyze_conversation_quality(),
            'optimization_opportunities': self._identify_optimization_opportunities()
        }
        
        return insights
    
    def _analyze_conversation_quality(self) -> Dict[str, Any]:
        """Analyze conversation quality across sessions."""
        conv_metrics = self.performance_monitor.conversation_metrics
        
        if not conv_metrics:
            return {'no_data': True}
        
        total_sessions = len(conv_metrics)
        avg_response_time = sum(m.average_response_time for m in conv_metrics.values()) / total_sessions
        avg_satisfaction = sum(m.user_satisfaction_score for m in conv_metrics.values()) / total_sessions
        
        return {
            'total_sessions': total_sessions,
            'average_response_time': avg_response_time,
            'average_satisfaction': avg_satisfaction,
            'high_quality_sessions': sum(1 for m in conv_metrics.values() if m.user_satisfaction_score > 0.8),
            'low_quality_sessions': sum(1 for m in conv_metrics.values() if m.user_satisfaction_score < 0.5)
        }
    
    def _identify_optimization_opportunities(self) -> List[Dict[str, Any]]:
        """Identify system-wide optimization opportunities."""
        opportunities = []
        
        cache_stats = self.embedding_cache.get_stats()
        if cache_stats['hit_rate'] < 0.5:
            opportunities.append({
                'type': 'caching',
                'description': f"Low cache hit rate ({cache_stats['hit_rate']:.1%})",
                'impact': 'medium',
                'action': 'Increase cache size or adjust TTL settings'
            })
        
        system_health = self.performance_monitor.get_system_health()
        if system_health['average_response_time'] > 3.0:
            opportunities.append({
                'type': 'performance',
                'description': f"High average response time ({system_health['average_response_time']:.2f}s)",
                'impact': 'high',
                'action': 'Optimize slow operations and consider async processing'
            })
        
        return opportunities
    
    def start_background_learning(self):
        """Start background learning process."""
        if not self.learning_enabled or self.learning_task:
            return
        
        async def learning_loop():
            while self.learning_enabled:
                try:
                    await asyncio.sleep(self.reflection_interval * 60)  # Convert to seconds
                    await self._perform_reflection_cycle()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in learning loop: {e}")
        
        # Start the learning loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        self.learning_task = loop.create_task(learning_loop())
        logger.info("Background learning process started")
    
    async def _perform_reflection_cycle(self):
        """Perform a reflection cycle to learn and optimize."""
        logger.info("Starting reflection cycle")
        
        try:
            # Analyze recent performance
            insights = self.get_system_insights()
            
            # Update tool registry with performance feedback
            tool_stats = self.performance_monitor.get_tool_performance_summary()
            for tool_name, stats in tool_stats.items():
                if hasattr(self.tool_registry, 'update_tool_performance'):
                    self.tool_registry.update_tool_performance(
                        tool_name=tool_name,
                        success_rate=1.0 - stats.error_rate,
                        avg_execution_time=stats.average_execution_time
                    )
            
            # Optimize embedding cache
            cache_stats = insights.get('cache_performance', {})
            if cache_stats.get('hit_rate', 0) < 0.3:
                logger.info("Low cache hit rate detected, considering cache optimization")
            
            logger.info("Reflection cycle completed")
            
        except Exception as e:
            logger.error(f"Error in reflection cycle: {e}")
    
    def stop_background_learning(self):
        """Stop background learning process."""
        if self.learning_task:
            self.learning_task.cancel()
            self.learning_task = None
            logger.info("Background learning process stopped")
    
    def get_reflection_stats(self) -> Dict[str, Any]:
        """Get reflection engine statistics."""
        return {
            'enabled': self.enabled,
            'learning_enabled': self.learning_enabled,
            'background_task_running': self.learning_task is not None,
            'metrics_collected': len(self.performance_monitor.metrics),
            'tools_monitored': len(self.performance_monitor.tool_stats),
            'cache_stats': self.embedding_cache.get_stats(),
            'config': self.config
        }


# Global reflection engine instance
_reflection_engine = None


def get_reflection_engine() -> ReflectionEngine:
    """Get the global reflection engine instance."""
    global _reflection_engine
    if _reflection_engine is None:
        _reflection_engine = ReflectionEngine()
    return _reflection_engine


def init_reflection_engine(config: Dict[str, Any] = None) -> ReflectionEngine:
    """Initialize reflection engine with custom configuration."""
    global _reflection_engine
    _reflection_engine = ReflectionEngine(config)
    return _reflection_engine


# Async utility functions

async def async_embedding_computation(text: str, embedding_func: Callable) -> List[float]:
    """Compute embedding asynchronously."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        embedding = await loop.run_in_executor(executor, embedding_func, text)
    return embedding


async def async_tool_execution(tool_func: Callable, *args, **kwargs) -> Any:
    """Execute tool asynchronously."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        result = await loop.run_in_executor(executor, tool_func, *args, **kwargs)
    return result


# Performance decorators

def monitor_performance(metric_name: str, session_id: str = "unknown"):
    """Decorator to monitor function performance."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            success = True
            error = None
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                execution_time = time.time() - start_time
                
                reflection_engine = get_reflection_engine()
                if reflection_engine.enabled:
                    metric = PerformanceMetric(
                        metric_name=metric_name,
                        value=execution_time,
                        timestamp=datetime.now(),
                        session_id=session_id,
                        context={
                            'function_name': func.__name__,
                            'success': success,
                            'error': error
                        }
                    )
                    reflection_engine.performance_monitor.record_metric(metric)
        
        return wrapper
    return decorator


def cache_embeddings(embedding_type: str = 'default'):
    """Decorator to cache embedding computations."""
    def decorator(func):
        def wrapper(text: str, *args, **kwargs):
            reflection_engine = get_reflection_engine()
            
            if reflection_engine.enabled:
                # Try to get from cache
                cached_result = reflection_engine.get_cached_embedding(text, embedding_type)
                if cached_result is not None:
                    return cached_result
            
            # Compute embedding
            result = func(text, *args, **kwargs)
            
            if reflection_engine.enabled and result is not None:
                # Cache the result
                reflection_engine.cache_embedding(text, result, embedding_type)
            
            return result
        
        return wrapper
    return decorator