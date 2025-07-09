"""
Dynamic tool registry for ChatMRPT with auto-discovery and intelligent tool selection.

This module implements automatic tool discovery, registration, and schema generation
for the Pydantic-based tool system. It includes Phase 2 enhancements for intelligent
tool selection using semantic similarity and affordance scoring.
"""

import logging
import importlib
import inspect
import pkgutil
import os
from typing import Dict, List, Type, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime

from app.tools.base import BaseTool, ToolCategory

# Lazy import sentence_transformers to avoid slow startup
SENTENCE_TRANSFORMERS_AVAILABLE = None
SentenceTransformer = None

def _get_sentence_transformer():
    """Lazy import of sentence transformers - TRULY lazy, only load when scoring is needed."""
    global SENTENCE_TRANSFORMERS_AVAILABLE, SentenceTransformer
    
    if SENTENCE_TRANSFORMERS_AVAILABLE is None:
        try:
            # Check if we actually need to load this expensive model
            import os
            if os.environ.get('DISABLE_TOOL_SCORING', 'false').lower() == 'true':
                logger.info("🚫 Tool scoring disabled via environment variable")
                SENTENCE_TRANSFORMERS_AVAILABLE = False
                return None
                
            logger.info("📦 Loading sentence transformers (this may take a moment)...")
            from sentence_transformers import SentenceTransformer as ST
            SentenceTransformer = ST
            SENTENCE_TRANSFORMERS_AVAILABLE = True
            logger.info("✅ Sentence transformers loaded successfully")
        except ImportError as e:
            logger.warning(f"⚠️ Sentence transformers not available: {e}")
            SENTENCE_TRANSFORMERS_AVAILABLE = False
            SentenceTransformer = None
    
    return SentenceTransformer

import numpy as np

try:
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    cosine_similarity = None

from .tool_validator import ToolValidator
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ToolMetadata:
    """Metadata for tool registration and discovery"""
    name: str
    description: str
    category: ToolCategory
    examples: List[str] = None
    keywords: List[str] = None
    parameters: Dict[str, Any] = None
    tags: List[str] = None
    requires_data: bool = True


def create_tool_metadata(tool_class: Type[BaseTool]) -> ToolMetadata:
    """Create metadata from a tool class"""
    return ToolMetadata(
        name=tool_class.get_tool_name(),
        description=tool_class.get_description(),
        category=tool_class.get_category(),
        examples=tool_class.get_examples(),
        keywords=[],  # Could be extracted from description
        parameters=tool_class.schema() if hasattr(tool_class, 'schema') else {},
        tags=[],  # Default empty tags
        requires_data=True  # Default to requiring data
    )
    

class ToolAffordanceScorer:
    """
    Intelligent tool selection using semantic similarity and context.
    
    Phase 2 enhancement for ChatMRPT tool orchestration. This component
    analyzes user queries and scores tool relevance based on:
    - Semantic similarity between query and tool descriptions
    - Context from conversation history
    - Tool usage patterns and success rates
    """
    
    def __init__(self, similarity_model: str = None, confidence_threshold: float = None):
        """Initialize tool affordance scorer with embedding model."""
        self.similarity_model_name = similarity_model or self._get_similarity_model()
        self.confidence_threshold = confidence_threshold or self._get_confidence_threshold()
        self.enable_caching = self._get_enable_caching()
        
        # Initialize components
        self.embedder = None
        self.tool_embeddings = {}
        self.usage_patterns = {}
        self._embedder_available = self._initialize_embedder()
        self.enabled = self._embedder_available
        
        if not self.enabled:
            logger.warning("ToolAffordanceScorer running in fallback mode - using keyword matching")
    
    def _get_similarity_model(self) -> str:
        """Get similarity model from environment or use default."""
        return os.getenv('TOOL_SIMILARITY_MODEL', 'all-MiniLM-L6-v2')
    
    def _get_confidence_threshold(self) -> float:
        """Get confidence threshold from environment."""
        return float(os.getenv('TOOL_CONFIDENCE_THRESHOLD', '0.7'))
    
    def _get_enable_caching(self) -> bool:
        """Get caching setting from environment."""
        return os.getenv('ENABLE_TOOL_CACHING', 'true').lower() == 'true'
    
    def _initialize_embedder(self) -> bool:
        """Initialize sentence transformer embedder (lazy loading)."""
        # Lazy load sentence transformers
        SentenceTransformerClass = _get_sentence_transformer()
        
        if not SentenceTransformerClass:
            logger.warning("sentence-transformers not available for tool scoring")
            return False
        
        if not SKLEARN_AVAILABLE:
            logger.warning("scikit-learn not available for cosine similarity")
            return False
        
        try:
            self.embedder = SentenceTransformerClass(self.similarity_model_name)
            logger.info(f"Initialized tool affordance scorer with model: {self.similarity_model_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize embedder: {e}")
            return False
    
    def score_tool_relevance(self, query: str, tool_registry: 'ToolRegistry', 
                           conversation_context: str = "", limit: int = 5) -> List[Tuple[str, float]]:
        """
        Score tools by relevance to query and context.
        
        Args:
            query: User query to find relevant tools for
            tool_registry: Registry containing tool metadata
            conversation_context: Previous conversation context
            limit: Maximum number of tools to return
            
        Returns:
            List of (tool_name, confidence_score) tuples sorted by relevance
        """
        if not self.enabled:
            return self._fallback_keyword_scoring(query, tool_registry, limit)
        
        try:
            # Get all available tools
            available_tools = tool_registry.list_tools()
            if not available_tools:
                return []
            
            # Prepare tool descriptions for scoring
            tool_descriptions = []
            tool_names = []
            
            for tool_name in available_tools:
                metadata = tool_registry.get_tool_metadata(tool_name)
                if metadata:
                    # Create enhanced description with examples and keywords
                    description = self._create_enhanced_description(metadata)
                    tool_descriptions.append(description)
                    tool_names.append(tool_name)
            
            if not tool_descriptions:
                return []
            
            # Generate embeddings if not cached
            if self.enable_caching:
                tool_embeddings = self._get_cached_embeddings(tool_names, tool_descriptions)
            else:
                tool_embeddings = self.embedder.encode(tool_descriptions)
            
            # Create query embedding (include context if provided)
            full_query = f"{query} {conversation_context}".strip()
            query_embedding = self.embedder.encode([full_query])
            
            # Calculate similarities
            similarities = cosine_similarity(query_embedding, tool_embeddings)[0]
            
            # Create scored results
            scored_tools = []
            for i, similarity in enumerate(similarities):
                tool_name = tool_names[i]
                confidence = float(similarity)
                
                # Apply usage pattern boost if available
                confidence = self._apply_usage_pattern_boost(tool_name, confidence)
                
                scored_tools.append((tool_name, confidence))
            
            # Sort by confidence and return top results
            scored_tools.sort(key=lambda x: x[1], reverse=True)
            return scored_tools[:limit]
            
        except Exception as e:
            logger.error(f"Error in semantic tool scoring: {e}")
            return self._fallback_keyword_scoring(query, tool_registry, limit)
    
    def _create_enhanced_description(self, metadata: ToolMetadata) -> str:
        """Create enhanced description combining metadata fields."""
        parts = [metadata.description]
        
        # Add examples if available
        if metadata.examples:
            example_text = " Examples: " + " ".join(metadata.examples[:2])
            parts.append(example_text)
        
        # Add category context
        parts.append(f"Category: {metadata.category.value}")
        
        # Add tags if available
        if metadata.tags:
            tag_text = " Tags: " + " ".join(metadata.tags)
            parts.append(tag_text)
        
        return " ".join(parts)
    
    def _get_cached_embeddings(self, tool_names: List[str], descriptions: List[str]) -> np.ndarray:
        """Get or generate cached embeddings for tool descriptions."""
        embeddings = []
        to_embed = []
        to_embed_indices = []
        
        for i, (tool_name, description) in enumerate(zip(tool_names, descriptions)):
            if tool_name in self.tool_embeddings:
                embeddings.append(self.tool_embeddings[tool_name])
            else:
                embeddings.append(None)
                to_embed.append(description)
                to_embed_indices.append(i)
        
        # Generate embeddings for uncached tools
        if to_embed:
            new_embeddings = self.embedder.encode(to_embed)
            for j, idx in enumerate(to_embed_indices):
                tool_name = tool_names[idx]
                embedding = new_embeddings[j]
                self.tool_embeddings[tool_name] = embedding
                embeddings[idx] = embedding
        
        return np.array(embeddings)
    
    def _apply_usage_pattern_boost(self, tool_name: str, base_confidence: float) -> float:
        """Apply usage pattern boost to confidence score."""
        if tool_name not in self.usage_patterns:
            return base_confidence
        
        pattern = self.usage_patterns[tool_name]
        success_rate = pattern.get('success_rate', 0.5)
        usage_frequency = pattern.get('usage_frequency', 0.1)
        
        # Boost confidence based on historical success and usage
        boost_factor = 1.0 + (success_rate - 0.5) * 0.2 + (usage_frequency * 0.1)
        return min(1.0, base_confidence * boost_factor)
    
    def _fallback_keyword_scoring(self, query: str, tool_registry: 'ToolRegistry', 
                                 limit: int) -> List[Tuple[str, float]]:
        """Fallback keyword-based scoring when embeddings unavailable."""
        query_lower = query.lower()
        query_words = set(query_lower.split())
        
        scored_tools = []
        available_tools = tool_registry.list_tools()
        
        for tool_name in available_tools:
            metadata = tool_registry.get_tool_metadata(tool_name)
            if not metadata:
                continue
            
            # Score based on keyword matches
            score = 0.0
            text_to_search = f"{metadata.description} {' '.join(metadata.examples)} {metadata.category.value}".lower()
            
            # Exact phrase matches get higher score
            if query_lower in text_to_search:
                score += 0.8
            
            # Word matches get lower score
            search_words = set(text_to_search.split())
            matching_words = query_words.intersection(search_words)
            if matching_words:
                score += len(matching_words) / len(query_words) * 0.6
            
            # Tool name similarity
            if any(word in tool_name.lower() for word in query_words):
                score += 0.4
            
            if score > 0:
                scored_tools.append((tool_name, score))
        
        # Sort and return top results
        scored_tools.sort(key=lambda x: x[1], reverse=True)
        return scored_tools[:limit]
    
    def should_execute_tool(self, tool_name: str, confidence_score: float, 
                          user_context: dict = None) -> bool:
        """
        Determine if tool should be executed based on confidence and context.
        
        Args:
            tool_name: Name of the tool
            confidence_score: Confidence score from scoring
            user_context: Additional context (user role, session state, etc.)
            
        Returns:
            True if tool should be executed, False if clarification needed
        """
        # Base threshold check
        if confidence_score < self.confidence_threshold:
            return False
        
        # High confidence tools can always execute
        if confidence_score >= 0.9:
            return True
        
        # Medium confidence tools may need additional checks
        if 0.7 <= confidence_score < 0.9:
            # Check if user context supports execution
            if user_context:
                # More lenient for experienced users
                user_role = user_context.get('user_role', 'analyst')
                if user_role in ['expert', 'administrator']:
                    return confidence_score >= 0.6
                
                # Check if data is available for data-dependent tools
                has_data = user_context.get('has_data', False)
                tool_needs_data = user_context.get('tool_needs_data', True)
                if tool_needs_data and not has_data:
                    return False
        
        return confidence_score >= self.confidence_threshold
    
    def suggest_clarification(self, query: str, low_confidence_tools: List[Tuple[str, float]], 
                            tool_registry: 'ToolRegistry') -> str:
        """
        Generate clarification question for ambiguous queries.
        
        Args:
            query: Original user query
            low_confidence_tools: Tools that had low confidence scores
            tool_registry: Registry to get tool metadata
            
        Returns:
            Clarification question string
        """
        if not low_confidence_tools:
            return f"I'm not sure how to help with '{query}'. Could you be more specific about what analysis you'd like to perform?"
        
        # Get top few uncertain tools
        top_uncertain = low_confidence_tools[:3]
        
        # Create options based on tool categories
        tool_options = []
        for tool_name, score in top_uncertain:
            metadata = tool_registry.get_tool_metadata(tool_name)
            if metadata:
                option = f"- {metadata.description[:80]}..."
                tool_options.append(option)
        
        if tool_options:
            options_text = "\n".join(tool_options)
            return f"I found a few possible options for '{query}':\n\n{options_text}\n\nWhich of these best matches what you're looking for?"
        
        return f"Could you provide more details about what you'd like to do with '{query}'?"
    
    def update_usage_pattern(self, tool_name: str, success: bool, execution_time: float = None):
        """Update usage patterns for learning and improvement."""
        if tool_name not in self.usage_patterns:
            self.usage_patterns[tool_name] = {
                'total_uses': 0,
                'successful_uses': 0,
                'success_rate': 0.5,
                'usage_frequency': 0.0,
                'avg_execution_time': 0.0
            }
        
        pattern = self.usage_patterns[tool_name]
        pattern['total_uses'] += 1
        
        if success:
            pattern['successful_uses'] += 1
        
        pattern['success_rate'] = pattern['successful_uses'] / pattern['total_uses']
        pattern['usage_frequency'] = min(1.0, pattern['total_uses'] / 100.0)  # Normalize to 0-1
        
        if execution_time is not None:
            if pattern['avg_execution_time'] == 0:
                pattern['avg_execution_time'] = execution_time
            else:
                # Exponential moving average
                pattern['avg_execution_time'] = 0.7 * pattern['avg_execution_time'] + 0.3 * execution_time
    
    def filter_tools_by_category(self, tool_registry: 'ToolRegistry', 
                                categories: List[ToolCategory]) -> List[str]:
        """
        Filter tools by category.
        
        Args:
            tool_registry: Registry containing tools
            categories: List of categories to filter by
            
        Returns:
            List of tool names matching the categories
        """
        filtered_tools = []
        all_tools = tool_registry.get_all_tools()
        
        for tool_name, tool_class in all_tools.items():
            if hasattr(tool_class, 'get_category'):
                try:
                    tool_category = tool_class.get_category()
                    if tool_category in categories:
                        filtered_tools.append(tool_name)
                except:
                    pass
        
        return filtered_tools
    
    def get_affordance_stats(self) -> Dict[str, Any]:
        """Get statistics about tool affordance scoring."""
        return {
            'enabled': self.enabled,
            'similarity_model': self.similarity_model_name,
            'confidence_threshold': self.confidence_threshold,
            'cached_embeddings': len(self.tool_embeddings),
            'tracked_patterns': len(self.usage_patterns),
            'caching_enabled': self.enable_caching
        }


class ToolRegistry:
    """Production-ready tool registry with caching and intelligent selection"""
    
    def __init__(self):
        self._tools: Dict[str, Type[BaseTool]] = {}
        self._metadata: Dict[str, ToolMetadata] = {}
        self._tool_validator = ToolValidator()
        
        # Production optimization: Cache for deployment performance
        self._cached_data_loaded = False
        
        # Lazy-load affordance scorer only when needed
        self._affordance_scorer = None
        
        self._discovery_paths = [
            # Core Pydantic tools (100% converted and existing)
            'app.tools.risk_analysis_tools',
            'app.tools.ward_data_tools', 
            'app.tools.statistical_analysis_tools',
            'app.tools.visualization_maps_tools',
            'app.tools.visualization_charts_tools',
            'app.tools.intervention_targeting_tools',
            # 'app.tools.scenario_simulation_tools',  # Removed during streamlining
            'app.tools.smart_knowledge_tools',
            # 'app.tools.knowledge_tools',  # Removed during streamlining
            'app.tools.settlement_validation_tools',
            'app.tools.settlement_visualization_tools',
            # 'app.tools.data_preparation_tools',  # Removed during streamlining
            # Advanced tools (removed during streamlining)
            # 'app.tools.advanced_mapping_tools',
            'app.tools.settlement_intervention_tools',
            # 'app.tools.spatial_autocorrelation_tools',
            'app.tools.complete_analysis_tools'  # CRITICAL: Include complete analysis
        ]
        
        # Try cache first, then discovery
        self._load_from_cache()
    
    def _load_from_cache(self) -> None:
        """Load tool registry from cache if available."""
        try:
            from .tool_cache import get_tool_cache
            cache = get_tool_cache()
            
            cached_data = cache.load_cached_registry()
            if cached_data:
                # Load cached tool names and schemas
                self._cached_tool_names = cached_data.get('tool_names', [])
                self._cached_schemas = cached_data.get('tool_schemas', {})
                self._cached_data_loaded = True
                
                logger.info(f"📦 Loaded {len(self._cached_tool_names)} tools from cache")
                return
                
        except Exception as e:
            logger.warning(f"⚠️ Could not load from cache: {e}")
        
        # Cache miss - need to discover tools
        self._cached_data_loaded = False
        self.discover_tools()
    
    def discover_tools(self) -> int:
        """
        Automatically discover and register all available tools.
        
        Returns:
            Number of tools discovered and registered
        """
        discovered_count = 0
        
        for module_path in self._discovery_paths:
            try:
                discovered_count += self._discover_tools_in_module(module_path)
            except Exception as e:
                logger.warning(f"Failed to discover tools in {module_path}: {e}")
        
        logger.info(f"Discovered {discovered_count} tools across {len(self._discovery_paths)} modules")
        
        # Save to cache for future startups
        self._save_to_cache()
        
        return discovered_count
    
    def _save_to_cache(self) -> None:
        """Save current tool registry to cache."""
        try:
            from .tool_cache import get_tool_cache
            cache = get_tool_cache()
            
            # Prepare data for caching
            tool_names = list(self._tools.keys())
            tool_schemas = self.get_tool_schemas()
            discovery_stats = {
                'total_tools': len(tool_names),
                'discovery_paths': len(self._discovery_paths),
                'cache_timestamp': datetime.now().isoformat()
            }
            
            cache.save_registry_cache(tool_names, tool_schemas, discovery_stats)
            
        except Exception as e:
            logger.warning(f"⚠️ Could not save to cache: {e}")
    
    def _discover_tools_in_module(self, module_path: str) -> int:
        """Discover tools in a specific module"""
        try:
            module = importlib.import_module(module_path)
            discovered = 0
            
            # Try to discover Pydantic tools first
            pydantic_discovered = self._discover_pydantic_tools(module)
            discovered += pydantic_discovered
            
            # Also discover legacy tools for backward compatibility
            legacy_discovered = self._discover_legacy_tools(module)
            discovered += legacy_discovered
            
            logger.debug(f"Discovered {discovered} tools in {module_path} ({pydantic_discovered} Pydantic, {legacy_discovered} legacy)")
            return discovered
            
        except ImportError as e:
            logger.warning(f"Could not import {module_path}: {e}")
            return 0
        except Exception as e:
            logger.error(f"Error discovering tools in {module_path}: {e}")
            return 0
    
    def _discover_pydantic_tools(self, module) -> int:
        """Discover Pydantic-based tools in a module"""
        discovered = 0
        
        for name, obj in inspect.getmembers(module):
            if (inspect.isclass(obj) and 
                issubclass(obj, BaseTool) and 
                obj != BaseTool and
                not inspect.isabstract(obj)):
                
                try:
                    self.register_pydantic_tool(obj)
                    discovered += 1
                    logger.debug(f"Registered Pydantic tool: {name}")
                except Exception as e:
                    logger.warning(f"Failed to register Pydantic tool {name}: {e}")
        
        return discovered
    
    def _discover_legacy_tools(self, module) -> int:
        """Discover legacy function-based tools in a module"""
        discovered = 0
        
        # Look for functions that follow tool naming patterns
        for name, obj in inspect.getmembers(module):
            if (inspect.isfunction(obj) and 
                not name.startswith('_') and
                self._is_tool_function(obj)):
                
                try:
                    self.register_legacy_tool(name, obj)
                    discovered += 1
                    logger.debug(f"Registered legacy tool: {name}")
                except Exception as e:
                    logger.warning(f"Failed to register legacy tool {name}: {e}")
        
        return discovered
    
    def _is_tool_function(self, func) -> bool:
        """Check if a function is a valid tool function"""
        # Check signature - should accept session_id as first parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        
        if not params or params[0] != 'session_id':
            return False
        
        # Check return type annotation or docstring hints
        if hasattr(func, '__annotations__'):
            return_type = func.__annotations__.get('return')
            if return_type and 'Dict' in str(return_type):
                return True
        
        # Check docstring for tool indicators
        doc = func.__doc__ or ""
        if any(keyword in doc.lower() for keyword in ['tool', 'analysis', 'handles requests']):
            return True
        
        return False
    
    def register_pydantic_tool(self, tool_class: Type[BaseTool]):
        """Register a Pydantic-based tool"""
        tool_name = tool_class.get_tool_name()
        
        # Validate tool class
        if not self._tool_validator.validate_tool_class(tool_class):
            raise ValueError(f"Tool class {tool_class.__name__} failed validation")
        
        # Create metadata
        metadata = create_tool_metadata(tool_class)
        
        # Register
        self._tools[tool_name] = tool_class
        self._metadata[tool_name] = metadata
        
        logger.debug(f"Registered Pydantic tool: {tool_name}")
    
    def register_legacy_tool(self, tool_name: str, tool_function):
        """Register a legacy function-based tool"""
        # Create wrapper metadata for legacy tools
        metadata = self._create_legacy_metadata(tool_name, tool_function)
        
        # Store function reference 
        self._tools[tool_name] = tool_function
        self._metadata[tool_name] = metadata
        
        logger.debug(f"Registered legacy tool: {tool_name}")
    
    def _create_legacy_metadata(self, tool_name: str, tool_function) -> ToolMetadata:
        """Create metadata for legacy function-based tools"""
        from ..tools.base import ToolCategory
        
        # Extract description from docstring
        description = tool_function.__doc__ or f"Execute {tool_name}"
        if len(description) > 200:
            description = description[:200] + "..."
        
        # Extract parameters from signature
        sig = inspect.signature(tool_function)
        parameters = {}
        
        for param_name, param in sig.parameters.items():
            if param_name == 'session_id':
                continue
            
            param_info = {
                "type": "string",  # Default type
                "description": f"Parameter for {param_name}",
                "required": param.default == inspect.Parameter.empty
            }
            
            # Try to infer type from annotation
            if param.annotation != inspect.Parameter.empty:
                if param.annotation == str:
                    param_info["type"] = "string"
                elif param.annotation == int:
                    param_info["type"] = "integer"
                elif param.annotation == float:
                    param_info["type"] = "number"
                elif param.annotation == bool:
                    param_info["type"] = "boolean"
            
            parameters[param_name] = param_info
        
        return ToolMetadata(
            name=tool_name,
            category=ToolCategory.DATA_ANALYSIS,  # Default category
            description=description,
            parameters={"type": "object", "properties": parameters},
            examples=[],
            tags=["legacy"],
            requires_data=True
        )
    
    def get_tool(self, tool_name: str) -> Optional[Type[BaseTool]]:
        """Get a registered tool by name"""
        return self._tools.get(tool_name)
    
    def get_tool_metadata(self, tool_name: str) -> Optional[ToolMetadata]:
        """Get metadata for a tool"""
        return self._metadata.get(tool_name)
    
    def list_tools(self) -> List[str]:
        """Get list of all registered tool names"""
        # Use cached data if available for faster response
        if self._cached_data_loaded and hasattr(self, '_cached_tool_names'):
            return self._cached_tool_names
        return list(self._tools.keys())
    
    def get_tool_names(self) -> List[str]:
        """Get list of all registered tool names (alias for list_tools)"""
        return self.list_tools()
    
    def get_all_tools(self) -> Dict[str, Type['BaseTool']]:
        """Get all registered tools as a dictionary"""
        return dict(self._tools)
    
    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        """Get OpenAI-compatible schemas for all tools (alias for get_openai_function_schemas)"""
        return self.get_openai_function_schemas()
    
    def list_tools_by_category(self, category: str) -> List[str]:
        """Get tools filtered by category"""
        return [
            name for name, metadata in self._metadata.items()
            if metadata.category.value == category
        ]
    
    def search_tools(self, query: str) -> List[str]:
        """Search tools by name, description, or examples"""
        query_lower = query.lower()
        matching_tools = []
        
        for name, metadata in self._metadata.items():
            # Check name
            if query_lower in name.lower():
                matching_tools.append(name)
                continue
            
            # Check description
            if query_lower in metadata.description.lower():
                matching_tools.append(name)
                continue
            
            # Check examples
            if any(query_lower in example.lower() for example in metadata.examples):
                matching_tools.append(name)
                continue
        
        return matching_tools
    
    def get_openai_function_schemas(self) -> List[Dict[str, Any]]:
        """
        Generate OpenAI-compatible function schemas for all registered tools.
        
        Returns:
            List of function schema dictionaries for OpenAI API
        """
        # Use cached schemas if available for faster response
        if self._cached_data_loaded and hasattr(self, '_cached_schemas'):
            return self._cached_schemas
            
        schemas = []
        
        for tool_name, metadata in self._metadata.items():
            schema = {
                "name": tool_name,
                "description": metadata.description,
                "parameters": metadata.parameters
            }
            
            # Add examples as part of description if available
            if metadata.examples:
                example_text = "\n\nExample usage patterns:\n" + "\n".join(f"- {ex}" for ex in metadata.examples[:3])
                schema["description"] += example_text
            
            schemas.append(schema)
        
        return schemas
    
    def execute_tool(self, tool_name: str, session_id: str, **kwargs):
        """
        Execute a tool with the given parameters.
        
        Args:
            tool_name: Name of the tool to execute
            session_id: Session identifier
            **kwargs: Tool parameters
            
        Returns:
            Tool execution result
        """
        tool = self.get_tool(tool_name)
        if not tool:
            return {
                "status": "error",
                "message": f"Tool '{tool_name}' not found",
                "tool_name": tool_name
            }
        
        try:
            # For Pydantic tools
            if inspect.isclass(tool) and issubclass(tool, BaseTool):
                # Create instance with validated parameters
                tool_instance = tool(**kwargs)
                result = tool_instance.execute(session_id)
                
                # Convert ToolExecutionResult to dictionary format expected by request_interpreter
                if hasattr(result, 'success'):
                    # Extract web_path and chart_type from data dict if they exist
                    result_data = result.data or {}
                    web_path = result_data.get('web_path') or getattr(result, 'web_path', None)
                    chart_type = result_data.get('chart_type') or getattr(result, 'chart_type', None)
                    
                    return {
                        'status': 'success' if result.success else 'error',
                        'message': result.message,
                        'tool_name': tool_name,
                        'data': result_data,
                        'execution_time': getattr(result, 'execution_time', None),
                        'web_path': web_path,
                        'chart_type': chart_type,
                        'error_details': getattr(result, 'error_details', None)
                    }
                
                return result
            
            # For legacy function tools
            elif callable(tool):
                # Call function directly
                result = tool(session_id, **kwargs)
                
                # Normalize legacy result format
                if isinstance(result, dict):
                    if 'status' not in result:
                        result['status'] = 'success' if 'error' not in result else 'error'
                    if 'tool_name' not in result:
                        result['tool_name'] = tool_name
                
                return result
            
            else:
                return {
                    "status": "error",
                    "message": f"Invalid tool type for '{tool_name}'",
                    "tool_name": tool_name
                }
                
        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return {
                "status": "error",
                "message": f"Tool execution failed: {str(e)}",
                "tool_name": tool_name,
                "error_details": f"Exception: {type(e).__name__}"
            }
    
    def get_tool_documentation(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive documentation for a tool"""
        metadata = self.get_tool_metadata(tool_name)
        if not metadata:
            return None
        
        return {
            "name": metadata.name,
            "category": metadata.category.value,
            "description": metadata.description,
            "parameters": metadata.parameters,
            "examples": metadata.examples,
            "tags": metadata.tags,
            "is_experimental": metadata.is_experimental,
            "requires_data": metadata.requires_data,
            "estimated_execution_time": metadata.estimated_execution_time
        }
    
    def validate_tool_parameters(self, tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate parameters for a tool.
        
        Returns:
            Dictionary with 'valid' boolean and 'errors' list
        """
        tool = self.get_tool(tool_name)
        if not tool:
            return {"valid": False, "errors": [f"Tool '{tool_name}' not found"]}
        
        # For Pydantic tools, use built-in validation
        if inspect.isclass(tool) and issubclass(tool, BaseTool):
            try:
                tool(**parameters)  # This will raise ValidationError if invalid
                return {"valid": True, "errors": []}
            except Exception as e:
                return {"valid": False, "errors": [str(e)]}
        
        # For legacy tools, basic validation
        metadata = self.get_tool_metadata(tool_name)
        if metadata and metadata.parameters:
            props = metadata.parameters.get("properties", {})
            errors = []
            
            for param_name, param_info in props.items():
                if param_info.get("required", False) and param_name not in parameters:
                    errors.append(f"Missing required parameter: {param_name}")
            
            return {"valid": len(errors) == 0, "errors": errors}
        
        return {"valid": True, "errors": []}
    
    # Phase 2: Intelligent Tool Selection Methods
    
    def select_tools_for_query(self, query: str, conversation_context: str = "", 
                             user_context: dict = None, limit: int = 5) -> List[Tuple[str, float]]:
        """
        Intelligently select tools for a user query using affordance scoring.
        
        Args:
            query: User query to find tools for
            conversation_context: Previous conversation context
            user_context: User session context (role, data availability, etc.)
            limit: Maximum number of tools to return
            
        Returns:
            List of (tool_name, confidence_score) tuples sorted by relevance
        """
        return self._affordance_scorer.score_tool_relevance(
            query=query,
            tool_registry=self,
            conversation_context=conversation_context,
            limit=limit
        )
    
    def should_execute_tool_automatically(self, tool_name: str, confidence_score: float,
                                        user_context: dict = None) -> bool:
        """
        Determine if a tool should be executed automatically or if clarification is needed.
        
        Args:
            tool_name: Name of the tool
            confidence_score: Confidence score from tool selection
            user_context: User session context
            
        Returns:
            True if tool should be executed, False if clarification needed
        """
        return self._affordance_scorer.should_execute_tool(
            tool_name=tool_name,
            confidence_score=confidence_score,
            user_context=user_context
        )
    
    def generate_clarification_question(self, query: str, uncertain_tools: List[Tuple[str, float]]) -> str:
        """
        Generate a clarification question for ambiguous queries.
        
        Args:
            query: Original user query
            uncertain_tools: Tools with low confidence scores
            
        Returns:
            Clarification question string
        """
        return self._affordance_scorer.suggest_clarification(
            query=query,
            low_confidence_tools=uncertain_tools,
            tool_registry=self
        )
    
    def update_tool_usage_feedback(self, tool_name: str, success: bool, execution_time: float = None):
        """
        Update tool usage patterns for learning and improvement.
        
        Args:
            tool_name: Name of the executed tool
            success: Whether the tool execution was successful
            execution_time: Tool execution time in seconds
        """
        self._affordance_scorer.update_usage_pattern(
            tool_name=tool_name,
            success=success,
            execution_time=execution_time
        )
    
    def get_tool_selection_stats(self) -> Dict[str, Any]:
        """Get statistics about intelligent tool selection."""
        return self._affordance_scorer.get_affordance_stats()
    
    def find_similar_tools(self, tool_name: str, limit: int = 3) -> List[Tuple[str, float]]:
        """
        Find tools similar to a given tool based on descriptions.
        
        Args:
            tool_name: Name of the reference tool
            limit: Maximum number of similar tools to return
            
        Returns:
            List of (similar_tool_name, similarity_score) tuples
        """
        metadata = self.get_tool_metadata(tool_name)
        if not metadata:
            return []
        
        # Use the tool description as a query to find similar tools
        similar_tools = self.select_tools_for_query(
            query=metadata.description,
            limit=limit + 1  # +1 to account for the reference tool itself
        )
        
        # Remove the reference tool from results
        similar_tools = [(name, score) for name, score in similar_tools if name != tool_name]
        return similar_tools[:limit]
    
    def suggest_tools_for_category(self, category: str, query_hint: str = "", limit: int = 5) -> List[str]:
        """
        Suggest tools from a specific category, optionally filtered by query hint.
        
        Args:
            category: Tool category to filter by
            query_hint: Optional query to further filter tools
            limit: Maximum number of tools to return
            
        Returns:
            List of tool names from the category
        """
        category_tools = self.list_tools_by_category(category)
        
        if not query_hint:
            return category_tools[:limit]
        
        # Score tools in category by query hint
        scored_tools = []
        for tool_name in category_tools:
            metadata = self.get_tool_metadata(tool_name)
            if metadata:
                # Simple scoring based on description match
                score = 0.0
                description_lower = metadata.description.lower()
                query_lower = query_hint.lower()
                
                if query_lower in description_lower:
                    score = 1.0
                else:
                    # Word-based scoring
                    query_words = set(query_lower.split())
                    desc_words = set(description_lower.split())
                    matching_words = query_words.intersection(desc_words)
                    if matching_words:
                        score = len(matching_words) / len(query_words)
                
                if score > 0:
                    scored_tools.append((tool_name, score))
        
        # Sort by score and return tool names
        scored_tools.sort(key=lambda x: x[1], reverse=True)
        return [tool_name for tool_name, _ in scored_tools[:limit]]


# Global registry instance
_registry = None


def get_tool_registry() -> ToolRegistry:
    """Get the global tool registry instance"""
    global _registry
    if _registry is None:
        _registry = ToolRegistry()
        _registry.discover_tools()
    return _registry


def reset_tool_registry():
    """Reset the global tool registry (useful for testing)"""
    global _registry
    _registry = None