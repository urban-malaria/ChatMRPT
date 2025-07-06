"""
Unified Memory System for ChatMRPT

This module provides a centralized memory management system that combines:
- Conversation history (short-term and long-term)
- Session state management
- Analysis context tracking
- Semantic search capabilities
- Cross-session user memory
- Memory analytics and insights
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
import hashlib
from collections import defaultdict

# Disable ChromaDB telemetry and warnings
os.environ["ANONYMIZED_TELEMETRY"] = "False"
import warnings
warnings.filterwarnings("ignore", message=".*telemetry.*")
warnings.filterwarnings("ignore", message=".*capture.*")

try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    chromadb = None

try:
    from langchain_huggingface import HuggingFaceEmbeddings
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    HuggingFaceEmbeddings = None

from flask import current_app, session

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS AND DATA STRUCTURES
# ============================================================================

class MemoryType(Enum):
    """Types of memory storage."""
    CONVERSATION = "conversation"      # Individual message exchanges
    SESSION_STATE = "session_state"    # Session-specific state
    ANALYSIS_CONTEXT = "analysis"      # Analysis results and context
    USER_PREFERENCE = "preference"     # User preferences and patterns
    SYSTEM_LEARNING = "learning"       # System-wide learnings

class MemoryPriority(Enum):
    """Priority levels for memory retention."""
    CRITICAL = 1      # Never forget (analysis results, key decisions)
    HIGH = 2          # Long-term retention (important conversations)
    MEDIUM = 3        # Standard retention (regular conversations)
    LOW = 4           # Short-term retention (ephemeral data)
    TRANSIENT = 5     # Session-only (temporary state)

@dataclass
class MemoryItem:
    """Unified memory item structure."""
    id: str
    type: MemoryType
    priority: MemoryPriority
    session_id: str
    user_id: Optional[str]
    timestamp: datetime
    content: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    embeddings: Optional[List[float]] = None
    ttl_days: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'id': self.id,
            'type': self.type.value,
            'priority': self.priority.value,
            'session_id': self.session_id,
            'user_id': self.user_id,
            'timestamp': self.timestamp.isoformat(),
            'content': self.content,
            'metadata': self.metadata,
            'ttl_days': self.ttl_days
        }

@dataclass
class ConversationContext:
    """Enhanced conversation context with full history."""
    session_id: str
    messages: List[Dict[str, Any]]
    current_topic: Optional[str] = None
    entities_mentioned: List[str] = field(default_factory=list)
    tools_used: List[str] = field(default_factory=list)
    analysis_state: Optional[Dict[str, Any]] = None
    user_preferences: Dict[str, Any] = field(default_factory=dict)
    
@dataclass
class MemorySearchResult:
    """Result from memory search operations."""
    memory_item: MemoryItem
    relevance_score: float
    source: str  # 'vector_search' or 'exact_match'
    highlights: Optional[List[str]] = None


# ============================================================================
# UNIFIED MEMORY MANAGER
# ============================================================================

class UnifiedMemoryManager:
    """
    Centralized memory management system for ChatMRPT.
    
    Features:
    - Unified API for all memory operations
    - Multi-level memory hierarchy (session -> user -> system)
    - Intelligent memory prioritization and retention
    - Semantic search across all memory types
    - Memory analytics and insights
    - Automatic cleanup and optimization
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the unified memory manager."""
        self.config = config or self._get_default_config()
        self.enabled = self.config.get('enabled', True)
        
        # Memory stores
        self.vector_store = None
        self.session_cache = {}  # Fast in-memory cache
        self.user_profiles = {}  # User preference tracking
        
        # Initialize components
        if self.enabled:
            self._initialize_stores()
            
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'enabled': True,
            'vector_db_path': './memory/unified',
            'embedding_model': 'sentence-transformers/all-MiniLM-L6-v2',
            'collection_name': 'chatmrpt_unified_memory',
            'cache_size': 1000,
            'retention_days': {
                MemoryPriority.CRITICAL: 365,
                MemoryPriority.HIGH: 90,
                MemoryPriority.MEDIUM: 30,
                MemoryPriority.LOW: 7,
                MemoryPriority.TRANSIENT: 1
            },
            'search_limit': 10,
            'similarity_threshold': 0.7
        }
    
    def _initialize_stores(self):
        """Initialize memory stores."""
        if not CHROMADB_AVAILABLE or not EMBEDDINGS_AVAILABLE:
            logger.warning("Required dependencies not available - falling back to basic memory")
            self.enabled = False
            return
            
        try:
            # Initialize embeddings
            self.embeddings = HuggingFaceEmbeddings(
                model_name=self.config['embedding_model'],
                model_kwargs={'device': 'cpu'},
                encode_kwargs={'normalize_embeddings': True}
            )
            
            # Initialize ChromaDB
            persist_path = Path(self.config['vector_db_path'])
            persist_path.mkdir(parents=True, exist_ok=True)
            
            self.chroma_client = chromadb.PersistentClient(
                path=str(persist_path),
                settings=Settings(
                    anonymized_telemetry=False,
                    allow_reset=True,
                    is_persistent=True
                )
            )
            
            # Get or create collection
            try:
                self.vector_store = self.chroma_client.get_collection(
                    name=self.config['collection_name']
                )
            except:
                self.vector_store = self.chroma_client.create_collection(
                    name=self.config['collection_name'],
                    metadata={"created_at": datetime.utcnow().isoformat()}
                )
                
            logger.info("Unified memory system initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize memory system: {e}")
            self.enabled = False
    
    # ========================================================================
    # CORE MEMORY OPERATIONS
    # ========================================================================
    
    def remember(self, 
                content: Dict[str, Any],
                memory_type: MemoryType,
                priority: MemoryPriority = MemoryPriority.MEDIUM,
                metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Store a memory item.
        
        Args:
            content: The content to remember
            memory_type: Type of memory
            priority: Memory priority
            metadata: Additional metadata
            
        Returns:
            Memory item ID
        """
        if not self.enabled:
            return ""
            
        # Get session and user info
        session_id = session.get('session_id', 'unknown')
        user_id = session.get('user_id')  # Future enhancement
        
        # Create memory item
        memory_id = self._generate_memory_id(content, session_id)
        memory_item = MemoryItem(
            id=memory_id,
            type=memory_type,
            priority=priority,
            session_id=session_id,
            user_id=user_id,
            timestamp=datetime.utcnow(),
            content=content,
            metadata=metadata or {},
            ttl_days=self.config.get('retention_days', {}).get(priority, 30)
        )
        
        # Store in cache
        self.session_cache[memory_id] = memory_item
        
        # Store in vector database if applicable
        if memory_type in [MemoryType.CONVERSATION, MemoryType.ANALYSIS_CONTEXT]:
            self._store_in_vector_db(memory_item)
            
        # Update user profile if applicable
        if user_id and memory_type == MemoryType.USER_PREFERENCE:
            self._update_user_profile(user_id, content)
            
        logger.debug(f"Stored memory item: {memory_id} (type: {memory_type.value})")
        return memory_id
    
    def recall(self, 
              query: str,
              memory_types: Optional[List[MemoryType]] = None,
              limit: int = None,
              session_only: bool = False) -> List[MemorySearchResult]:
        """
        Recall relevant memories based on query.
        
        Args:
            query: Search query
            memory_types: Filter by memory types
            limit: Maximum results
            session_only: Only search current session
            
        Returns:
            List of memory search results
        """
        if not self.enabled:
            return []
            
        limit = limit or self.config['search_limit']
        results = []
        
        # Search in session cache first
        if session_only:
            results.extend(self._search_session_cache(query, memory_types, limit))
        else:
            # Perform vector search
            if self.vector_store:
                results.extend(self._vector_search(query, memory_types, limit))
                
            # Add exact matches from cache
            cache_results = self._search_session_cache(query, memory_types, limit)
            for result in cache_results:
                if result not in results:
                    results.append(result)
        
        # Sort by relevance
        results.sort(key=lambda x: x.relevance_score, reverse=True)
        return results[:limit]
    
    def get_context(self, session_id: Optional[str] = None) -> ConversationContext:
        """
        Get comprehensive conversation context.
        
        Args:
            session_id: Session ID (uses current if not provided)
            
        Returns:
            ConversationContext object
        """
        if not session_id:
            session_id = session.get('session_id', 'unknown')
            
        # Gather all relevant memories
        conversation_memories = self._get_session_memories(
            session_id, 
            MemoryType.CONVERSATION
        )
        
        # Extract conversation history
        messages = []
        entities = set()
        tools = set()
        
        for memory in conversation_memories:
            if 'message' in memory.content:
                messages.append(memory.content['message'])
            if 'entities' in memory.metadata:
                entities.update(memory.metadata['entities'])
            if 'tools_used' in memory.metadata:
                tools.update(memory.metadata['tools_used'])
                
        # Get analysis state
        analysis_memories = self._get_session_memories(
            session_id,
            MemoryType.ANALYSIS_CONTEXT
        )
        analysis_state = None
        if analysis_memories:
            # Get most recent analysis
            analysis_state = analysis_memories[-1].content
            
        # Get user preferences
        user_id = session.get('user_id')
        user_preferences = self.user_profiles.get(user_id, {}) if user_id else {}
        
        return ConversationContext(
            session_id=session_id,
            messages=messages,
            current_topic=self._detect_current_topic(messages),
            entities_mentioned=list(entities),
            tools_used=list(tools),
            analysis_state=analysis_state,
            user_preferences=user_preferences
        )
    
    def update_session_state(self, state_updates: Dict[str, Any]):
        """
        Update current session state.
        
        Args:
            state_updates: State updates to apply
        """
        session_id = session.get('session_id', 'unknown')
        
        # Get or create session state memory
        state_memories = self._get_session_memories(
            session_id,
            MemoryType.SESSION_STATE
        )
        
        if state_memories:
            # Update existing state
            current_state = state_memories[-1].content
            current_state.update(state_updates)
            content = current_state
        else:
            # Create new state
            content = state_updates
            
        # Store updated state
        self.remember(
            content=content,
            memory_type=MemoryType.SESSION_STATE,
            priority=MemoryPriority.HIGH,
            metadata={'update_type': 'session_state'}
        )
    
    # ========================================================================
    # CONVERSATION TRACKING
    # ========================================================================
    
    def add_conversation_turn(self,
                            user_message: str,
                            ai_response: str,
                            metadata: Optional[Dict[str, Any]] = None):
        """
        Add a conversation turn to memory.
        
        Args:
            user_message: User's message
            ai_response: AI's response
            metadata: Additional metadata
        """
        content = {
            'message': {
                'role': 'user',
                'content': user_message,
                'timestamp': datetime.utcnow().isoformat()
            },
            'response': {
                'role': 'assistant',
                'content': ai_response,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        # Detect entities and topics
        entities = self._extract_entities(user_message + " " + ai_response)
        
        # Determine priority based on content
        priority = self._determine_conversation_priority(user_message, ai_response, metadata)
        
        # Store conversation
        self.remember(
            content=content,
            memory_type=MemoryType.CONVERSATION,
            priority=priority,
            metadata={
                'entities': entities,
                'tools_used': metadata.get('tools_used', []) if metadata else [],
                'response_time': metadata.get('response_time', 0) if metadata else 0,
                'success': metadata.get('success', True) if metadata else True
            }
        )
    
    def get_conversation_history(self, 
                               limit: int = 10,
                               session_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get conversation history.
        
        Args:
            limit: Maximum messages to return
            session_only: Only current session
            
        Returns:
            List of conversation messages
        """
        session_id = session.get('session_id', 'unknown')
        
        if session_only:
            memories = self._get_session_memories(
                session_id,
                MemoryType.CONVERSATION
            )
        else:
            # Get all conversations for user
            results = self.recall(
                query="",
                memory_types=[MemoryType.CONVERSATION],
                limit=limit * 2  # Get more to filter
            )
            memories = [r.memory_item for r in results]
            
        # Extract messages
        messages = []
        for memory in memories[-limit:]:
            if 'message' in memory.content:
                messages.append(memory.content['message'])
            if 'response' in memory.content:
                messages.append(memory.content['response'])
                
        return messages
    
    # ========================================================================
    # ANALYSIS CONTEXT
    # ========================================================================
    
    def store_analysis_results(self,
                             analysis_type: str,
                             results: Dict[str, Any],
                             metadata: Optional[Dict[str, Any]] = None):
        """
        Store analysis results in memory.
        
        Args:
            analysis_type: Type of analysis
            results: Analysis results
            metadata: Additional metadata
        """
        content = {
            'analysis_type': analysis_type,
            'timestamp': datetime.utcnow().isoformat(),
            'results': results
        }
        
        # Analysis results are critical - keep for long term
        self.remember(
            content=content,
            memory_type=MemoryType.ANALYSIS_CONTEXT,
            priority=MemoryPriority.CRITICAL,
            metadata=metadata or {}
        )
        
    def get_analysis_context(self) -> Optional[Dict[str, Any]]:
        """Get the most recent analysis context for the session."""
        session_id = session.get('session_id', 'unknown')
        memories = self._get_session_memories(
            session_id,
            MemoryType.ANALYSIS_CONTEXT
        )
        
        if memories:
            return memories[-1].content
        return None
    
    # ========================================================================
    # MEMORY ANALYTICS
    # ========================================================================
    
    def get_memory_insights(self) -> Dict[str, Any]:
        """
        Get insights about memory usage and patterns.
        
        Returns:
            Dictionary of memory insights
        """
        insights = {
            'total_memories': len(self.session_cache),
            'memory_types': defaultdict(int),
            'priority_distribution': defaultdict(int),
            'topics_discussed': [],
            'frequent_entities': [],
            'tool_usage': defaultdict(int),
            'conversation_patterns': {},
            'memory_health': self._assess_memory_health()
        }
        
        # Analyze cache
        for memory in self.session_cache.values():
            insights['memory_types'][memory.type.value] += 1
            insights['priority_distribution'][memory.priority.value] += 1
            
            # Extract tool usage
            if 'tools_used' in memory.metadata:
                for tool in memory.metadata['tools_used']:
                    insights['tool_usage'][tool] += 1
                    
        # Get vector store stats if available
        if self.vector_store:
            try:
                insights['vector_store_count'] = self.vector_store.count()
            except:
                insights['vector_store_count'] = 0
                
        return insights
    
    def cleanup_old_memories(self, force: bool = False) -> int:
        """
        Clean up old memories based on retention policy.
        
        Args:
            force: Force cleanup regardless of policy
            
        Returns:
            Number of memories cleaned up
        """
        if not self.enabled:
            return 0
            
        cleaned = 0
        current_time = datetime.utcnow()
        
        # Clean cache
        items_to_remove = []
        for memory_id, memory in self.session_cache.items():
            if self._should_cleanup_memory(memory, current_time, force):
                items_to_remove.append(memory_id)
                
        for memory_id in items_to_remove:
            del self.session_cache[memory_id]
            cleaned += 1
            
        # Clean vector store
        if self.vector_store and force:
            # More aggressive cleanup for vector store
            cutoff_date = current_time - timedelta(days=30)
            try:
                results = self.vector_store.get(
                    where={"timestamp": {"$lt": cutoff_date.isoformat()}},
                    include=['metadatas']
                )
                if results and results['ids']:
                    self.vector_store.delete(ids=results['ids'])
                    cleaned += len(results['ids'])
            except Exception as e:
                logger.error(f"Failed to cleanup vector store: {e}")
                
        logger.info(f"Cleaned up {cleaned} old memories")
        return cleaned
    
    # ========================================================================
    # PRIVATE HELPER METHODS
    # ========================================================================
    
    def _generate_memory_id(self, content: Dict[str, Any], session_id: str) -> str:
        """Generate unique memory ID."""
        content_str = json.dumps(content, sort_keys=True)
        hash_input = f"{session_id}_{content_str}_{datetime.utcnow().isoformat()}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _store_in_vector_db(self, memory_item: MemoryItem):
        """Store memory item in vector database."""
        if not self.vector_store:
            return
            
        try:
            # Generate text for embedding
            text = self._memory_to_text(memory_item)
            
            # Generate embedding
            embedding = self.embeddings.embed_query(text)
            
            # Prepare metadata
            metadata = {
                'type': memory_item.type.value,
                'priority': memory_item.priority.value,
                'session_id': memory_item.session_id,
                'timestamp': memory_item.timestamp.isoformat(),
                'user_id': memory_item.user_id or ''
            }
            
            # Store in ChromaDB
            self.vector_store.add(
                documents=[text],
                embeddings=[embedding],
                metadatas=[metadata],
                ids=[memory_item.id]
            )
            
        except Exception as e:
            logger.error(f"Failed to store in vector DB: {e}")
    
    def _memory_to_text(self, memory_item: MemoryItem) -> str:
        """Convert memory item to searchable text."""
        parts = []
        
        if memory_item.type == MemoryType.CONVERSATION:
            if 'message' in memory_item.content:
                parts.append(f"User: {memory_item.content['message'].get('content', '')}")
            if 'response' in memory_item.content:
                parts.append(f"Assistant: {memory_item.content['response'].get('content', '')}")
        else:
            # For other types, convert content to readable format
            parts.append(json.dumps(memory_item.content, indent=2))
            
        return "\n".join(parts)
    
    def _vector_search(self, 
                      query: str,
                      memory_types: Optional[List[MemoryType]],
                      limit: int) -> List[MemorySearchResult]:
        """Perform vector similarity search."""
        if not self.vector_store:
            return []
            
        try:
            # Build where clause
            where_clause = {}
            if memory_types:
                where_clause['type'] = {"$in": [t.value for t in memory_types]}
                
            # Generate query embedding
            query_embedding = self.embeddings.embed_query(query)
            
            # Search
            results = self.vector_store.query(
                query_embeddings=[query_embedding],
                n_results=limit,
                where=where_clause,
                include=['documents', 'metadatas', 'distances']
            )
            
            # Convert to MemorySearchResult
            search_results = []
            if results and results['documents']:
                for i, (doc, metadata, distance) in enumerate(zip(
                    results['documents'][0],
                    results['metadatas'][0],
                    results['distances'][0]
                )):
                    # Reconstruct memory item from metadata
                    # This is simplified - in production, store full item
                    memory_item = MemoryItem(
                        id=results['ids'][0][i],
                        type=MemoryType(metadata['type']),
                        priority=MemoryPriority(int(metadata['priority'])),
                        session_id=metadata['session_id'],
                        user_id=metadata.get('user_id'),
                        timestamp=datetime.fromisoformat(metadata['timestamp']),
                        content={'text': doc},
                        metadata=metadata
                    )
                    
                    search_results.append(MemorySearchResult(
                        memory_item=memory_item,
                        relevance_score=1 - distance,
                        source='vector_search'
                    ))
                    
            return search_results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []
    
    def _search_session_cache(self,
                            query: str,
                            memory_types: Optional[List[MemoryType]],
                            limit: int) -> List[MemorySearchResult]:
        """Search in session cache."""
        results = []
        query_lower = query.lower()
        
        for memory in self.session_cache.values():
            # Filter by type
            if memory_types and memory.type not in memory_types:
                continue
                
            # Simple text matching
            memory_text = self._memory_to_text(memory).lower()
            if query_lower in memory_text:
                relevance = memory_text.count(query_lower) / len(memory_text)
                results.append(MemorySearchResult(
                    memory_item=memory,
                    relevance_score=min(relevance * 10, 1.0),  # Normalize
                    source='exact_match'
                ))
                
        return results[:limit]
    
    def _get_session_memories(self,
                            session_id: str,
                            memory_type: MemoryType) -> List[MemoryItem]:
        """Get all memories for a session and type."""
        memories = []
        for memory in self.session_cache.values():
            if memory.session_id == session_id and memory.type == memory_type:
                memories.append(memory)
                
        # Sort by timestamp
        memories.sort(key=lambda x: x.timestamp)
        return memories
    
    def _detect_current_topic(self, messages: List[Dict[str, Any]]) -> Optional[str]:
        """Detect current conversation topic from messages."""
        if not messages:
            return None
            
        # Simple topic detection - can be enhanced with NLP
        recent_messages = messages[-5:]  # Last 5 messages
        
        # Look for analysis-related keywords
        analysis_keywords = ['analysis', 'risk', 'vulnerability', 'distribution', 'composite', 'pca']
        data_keywords = ['upload', 'csv', 'shapefile', 'data', 'file']
        viz_keywords = ['map', 'chart', 'visualization', 'show', 'display']
        
        text = ' '.join(m.get('content', '') for m in recent_messages).lower()
        
        if any(kw in text for kw in analysis_keywords):
            return 'malaria_risk_analysis'
        elif any(kw in text for kw in data_keywords):
            return 'data_management'
        elif any(kw in text for kw in viz_keywords):
            return 'visualization'
        else:
            return 'general_conversation'
    
    def _extract_entities(self, text: str) -> List[str]:
        """Extract entities from text."""
        entities = []
        
        # Simple entity extraction - can be enhanced with NER
        # Look for ward names, tools, file names
        import re
        
        # Ward name pattern (capitalized words)
        ward_pattern = r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b'
        potential_wards = re.findall(ward_pattern, text)
        entities.extend([w for w in potential_wards if len(w) > 3])
        
        # Tool names
        tool_keywords = ['composite', 'pca', 'analysis', 'visualization', 'distribution']
        for keyword in tool_keywords:
            if keyword in text.lower():
                entities.append(keyword)
                
        return list(set(entities))  # Remove duplicates
    
    def _determine_conversation_priority(self,
                                       user_message: str,
                                       ai_response: str,
                                       metadata: Optional[Dict[str, Any]]) -> MemoryPriority:
        """Determine priority level for a conversation."""
        # High priority for tool executions
        if metadata and metadata.get('tools_used'):
            return MemoryPriority.HIGH
            
        # Critical for analysis completions
        if 'analysis complete' in ai_response.lower():
            return MemoryPriority.CRITICAL
            
        # Low for simple queries
        if len(user_message) < 20 and '?' in user_message:
            return MemoryPriority.LOW
            
        # Default to medium
        return MemoryPriority.MEDIUM
    
    def _update_user_profile(self, user_id: str, preferences: Dict[str, Any]):
        """Update user profile with preferences."""
        if user_id not in self.user_profiles:
            self.user_profiles[user_id] = {}
            
        self.user_profiles[user_id].update(preferences)
    
    def _should_cleanup_memory(self,
                             memory: MemoryItem,
                             current_time: datetime,
                             force: bool) -> bool:
        """Determine if memory should be cleaned up."""
        if force and memory.priority != MemoryPriority.CRITICAL:
            return True
            
        if memory.ttl_days:
            age_days = (current_time - memory.timestamp).days
            return age_days > memory.ttl_days
            
        return False
    
    def _assess_memory_health(self) -> Dict[str, Any]:
        """Assess overall memory system health."""
        return {
            'status': 'healthy' if self.enabled else 'degraded',
            'vector_store_available': self.vector_store is not None,
            'cache_size': len(self.session_cache),
            'cache_memory_mb': sum(
                len(str(m.to_dict())) for m in self.session_cache.values()
            ) / 1024 / 1024,
            'oldest_memory': min(
                (m.timestamp for m in self.session_cache.values()),
                default=None
            ),
            'newest_memory': max(
                (m.timestamp for m in self.session_cache.values()),
                default=None
            )
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_memory_manager = None

def get_unified_memory() -> UnifiedMemoryManager:
    """Get the global unified memory manager instance."""
    global _memory_manager
    if _memory_manager is None:
        _memory_manager = UnifiedMemoryManager()
    return _memory_manager

def init_unified_memory(app=None, config=None) -> UnifiedMemoryManager:
    """Initialize unified memory for Flask application."""
    global _memory_manager
    
    if app:
        with app.app_context():
            _memory_manager = UnifiedMemoryManager(config)
    else:
        _memory_manager = UnifiedMemoryManager(config)
    
    return _memory_manager